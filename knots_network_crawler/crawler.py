"""
Core async Bitcoin P2P crawler.

Features:
- High-concurrency handshake + getaddr using asyncio
- Semaphore + wave scheduling for controlled aggression
- Proper timeouts, partial message handling, reconnections
- Detects Knots via subversion
- Enriches with GeoIP on discovery/update
- Respects "only_update_known" mode
- Tracks global best height seen
- Live-friendly progress via callbacks
"""

from __future__ import annotations

import asyncio
import ipaddress
import random
import socket
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Deque, Dict, List, Optional, Set, Tuple

from .config import CrawlerConfig, get_default_bootstrap_nodes
from .database import Database
from .geoip import GeoIPResolver, get_geoip_resolver
from .models import Node
from .protocol import (
    BitcoinProtocolError,
    CMD_ADDR,
    CMD_ADDRV2,
    CMD_HEADERS,
    CMD_PONG,
    CMD_VERACK,
    CMD_VERSION,
    NetAddr,
    build_getaddr,
    build_getheaders,
    build_message,
    build_ping,
    build_verack,
    hash_display_to_internal,
    make_version_message,
    parse_addr_payload,
    parse_addrv2_payload,
    parse_headers_first_prevblock,
    parse_version_payload,
    read_message,
)

ProgressCallback = Callable[[str, dict], None]


@dataclass
class CrawlStats:
    discovered: int = 0
    attempted: int = 0
    succeeded: int = 0
    knots_found: int = 0
    listening_found: int = 0
    max_height: int = 0
    start_time: float = 0.0

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time if self.start_time else 0.0


class KnotsNetworkCrawler:
    def __init__(self, config: CrawlerConfig, db: Database, progress: Optional[ProgressCallback] = None):
        self.cfg = config
        self.db = db
        self.progress = progress or (lambda event, data: None)

        self.geo: GeoIPResolver = get_geoip_resolver(
            city_mmdb=config.geoip_city_mmdb,
            asn_mmdb=config.geoip_asn_mmdb,
        )

        self.stats = CrawlStats()
        self.seen: Set[str] = set()                 # ip:port during this run
        self.to_crawl: asyncio.Queue[Tuple[str, int]] = asyncio.Queue()
        self.concurrent = asyncio.Semaphore(config.max_concurrent)
        self.best_height: int = 0
        self._session_id: Optional[int] = None

        # BLAKE2b fork detection: locator = the post-fork anchor block, in wire
        # (internal, little-endian) order. A fork node returns a first header
        # whose prev_block == this; a mainnet node falls back to genesis.
        self._fork_locator: Optional[bytes] = (
            hash_display_to_internal(config.fork_anchor_hash)
            if getattr(config, "fork_detect", False) and getattr(config, "fork_anchor_hash", "")
            else None
        )
        # hash_stop = block H+1, so a fork peer returns exactly one header.
        self._fork_stop: bytes = (
            hash_display_to_internal(getattr(config, "fork_stop_hash", "") or "")
            if getattr(config, "fork_stop_hash", "") else b"\x00" * 32
        )

        # For wave scheduling (reduces thundering herd)
        self._work_scheduled: Deque[float] = deque()

    async def bootstrap(self) -> int:
        """
        Seed the queue with DNS-resolved seeds + hardcoded bootstrap nodes.
        Returns number of addresses added.
        """
        added = 0

        # Hardcoded
        for ip, port in get_default_bootstrap_nodes():
            key = f"{ip}:{port}"
            if key not in self.seen:
                self.seen.add(key)
                await self.to_crawl.put((ip, port))
                added += 1

        # Resolve DNS seeds (best effort, parallel)
        dns_tasks = []
        for seed in self.cfg.seeds:
            dns_tasks.append(asyncio.create_task(self._resolve_dns_seed(seed)))

        results = await asyncio.gather(*dns_tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, list):
                for ip, port in res:
                    key = f"{ip}:{port}"
                    if key not in self.seen:
                        self.seen.add(key)
                        await self.to_crawl.put((ip, port))
                        added += 1

        # Also load previously known nodes that are old enough (for refresh)
        if not self.cfg.only_update_known:
            # We will opportunistically re-crawl older nodes later via refill logic.
            pass

        self.progress("bootstrap", {"added": added, "queue_size": self.to_crawl.qsize()})
        return added

    async def _resolve_dns_seed(self, hostname: str, default_port: int = 8333) -> List[Tuple[str, int]]:
        out: List[Tuple[str, int]] = []
        try:
            infos = await asyncio.get_event_loop().getaddrinfo(
                hostname, default_port, family=socket.AF_UNSPEC, type=socket.SOCK_STREAM
            )
            for family, _, _, _, sockaddr in infos:
                if family == socket.AF_INET:
                    ip = sockaddr[0]
                elif family == socket.AF_INET6:
                    ip = sockaddr[0]
                else:
                    continue
                # Basic sanity: skip private
                try:
                    if ipaddress.ip_address(ip).is_private:
                        continue
                except Exception:
                    continue
                out.append((ip, default_port))
                if len(out) > 12:  # don't explode from one seed
                    break
        except Exception as e:
            self.progress("dns_error", {"seed": hostname, "error": str(e)})
        return out

    async def refill_from_db(self, max_new: int = 300) -> int:
        """
        Add more work from DB (nodes we haven't crawled recently).
        Useful for long-running or resumed crawls.
        """
        if self.cfg.only_update_known:
            return 0

        # Simple heuristic: pick nodes not crawled in last N hours or never crawled
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=self.cfg.re_crawl_after_hours)).isoformat()

        # We do a direct query (aiosqlite)
        if not self.db._conn:
            await self.db.connect()

        added = 0
        async with self.db._conn.execute(
            """
            SELECT ip, port FROM nodes
            WHERE (last_crawled IS NULL OR last_crawled < ?)
            ORDER BY last_crawled IS NULL DESC, last_seen DESC
            LIMIT ?
            """,
            (cutoff, max_new * 2),
        ) as cur:
            async for row in cur:
                key = f"{row['ip']}:{row['port']}"
                if key not in self.seen and added < max_new:
                    self.seen.add(key)
                    await self.to_crawl.put((row["ip"], row["port"]))
                    added += 1

        if added:
            self.progress("refill", {"added": added})
        return added

    async def run(self) -> CrawlStats:
        """Main entry point. Runs until limits or queue exhausted."""
        self.stats.start_time = time.time()
        await self.db.connect()

        self._session_id = await self.db.create_session(
            mode=self.cfg.mode,
            config={
                "max_concurrent": self.cfg.max_concurrent,
                "max_nodes": self.cfg.max_nodes_to_crawl,
            },
        )

        await self.bootstrap()

        if not self.cfg.only_update_known:
            await self.refill_from_db(500)

        self.progress("start", {
            "mode": self.cfg.mode,
            "concurrency": self.cfg.max_concurrent,
            "queue": self.to_crawl.qsize(),
        })

        workers = [
            asyncio.create_task(self._worker(i))
            for i in range(min(self.cfg.max_concurrent, 64))  # cap worker tasks
        ]

        # Monitor / refill task
        monitor = asyncio.create_task(self._monitor_and_refill())

        try:
            await asyncio.wait(
                [asyncio.create_task(self._stop_condition())],
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            # Cancel everything gracefully
            for w in workers:
                w.cancel()
            monitor.cancel()
            await asyncio.gather(*workers, monitor, return_exceptions=True)

        # Finalize session
        await self.db.update_session(
            self._session_id,
            ended_at=datetime.now(timezone.utc).isoformat(),
            nodes_discovered=self.stats.discovered,
            nodes_crawled=self.stats.succeeded,
            knots_found=self.stats.knots_found,
            listening_found=self.stats.listening_found,
            max_height_seen=self.stats.max_height,
        )

        self.progress("finished", {
            "discovered": self.stats.discovered,
            "crawled": self.stats.succeeded,
            "knots": self.stats.knots_found,
            "listening": self.stats.listening_found,
            "max_height": self.stats.max_height,
            "elapsed": round(self.stats.elapsed, 1),
        })
        return self.stats

    async def _stop_condition(self) -> None:
        """Waits until we should stop the crawl."""
        start = time.time()
        while True:
            await asyncio.sleep(2.0)
            qsize = self.to_crawl.qsize()

            if self.cfg.max_nodes_to_crawl and self.stats.attempted >= self.cfg.max_nodes_to_crawl:
                break
            if self.cfg.max_crawl_duration_seconds and (time.time() - start) > self.cfg.max_crawl_duration_seconds:
                break
            # If queue empty for a while and no more refills expected
            if qsize == 0 and self.stats.attempted > 50:
                # Give a small grace period
                await asyncio.sleep(4.0)
                if self.to_crawl.qsize() == 0:
                    break

    async def _monitor_and_refill(self) -> None:
        """Background task that occasionally adds fresh work from DB."""
        while True:
            await asyncio.sleep(45.0)
            try:
                if not self.cfg.only_update_known and self.to_crawl.qsize() < 80:
                    await self.refill_from_db(250)
            except Exception:
                pass

    async def _worker(self, worker_id: int) -> None:
        while True:
            try:
                ip, port = await self.to_crawl.get()
            except asyncio.CancelledError:
                break

            async with self.concurrent:
                self.stats.attempted += 1
                try:
                    await self._crawl_one(ip, port)
                except Exception as e:
                    # Never kill the worker
                    self.progress("worker_error", {"worker": worker_id, "ip": ip, "error": str(e)[:120]})
                finally:
                    self.to_crawl.task_done()

            # Small jitter to be nicer
            if self.cfg.is_aggressive:
                await asyncio.sleep(random.uniform(0.0, self.cfg.min_peer_delay))
            else:
                await asyncio.sleep(random.uniform(self.cfg.min_peer_delay * 0.6, self.cfg.min_peer_delay * 1.4))

    async def _crawl_one(self, ip: str, port: int) -> None:
        """Perform full handshake + getaddr for one peer. Update DB."""
        node_key = f"{ip}:{port}"
        start = time.perf_counter()

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, port),
                timeout=self.cfg.connect_timeout,
            )
        except Exception:
            # Connection failed or filtered - still record a "seen" if we want, but usually skip
            return

        latency = None
        version_info = None
        addrs_received: List[NetAddr] = []
        fork_verified = False

        try:
            # 1. Send version
            version_msg = make_version_message(
                addr_recv_ip=ip,
                addr_recv_port=port,
                start_height=0,
            )
            writer.write(version_msg)
            await writer.drain()

            # 2. Expect version + verack (order can vary slightly)
            verack_received = False
            version_received = False
            deadline = time.time() + self.cfg.crawl_timeout_per_peer

            while time.time() < deadline and not (version_received and verack_received):
                try:
                    cmd, payload = await asyncio.wait_for(
                        read_message(reader, timeout=self.cfg.read_timeout),
                        timeout=8.0,
                    )
                except (asyncio.TimeoutError, BitcoinProtocolError):
                    break

                if cmd == CMD_VERSION:
                    try:
                        version_info = parse_version_payload(payload)
                        version_received = True
                        # Immediately verack
                        writer.write(build_verack())
                        await writer.drain()
                    except Exception:
                        pass

                elif cmd == CMD_VERACK:
                    verack_received = True

                elif cmd == CMD_PONG:
                    # ignore for now
                    pass

            if not version_received:
                return

            # 2b. Chain-verify BLAKE2b fork membership: ask for headers using the
            # post-fork anchor as locator. Only a node on the fork chain has that
            # block and replies with a header building on it (prev_block == anchor).
            if self._fork_locator is not None:
                writer.write(build_getheaders([self._fork_locator], hash_stop=self._fork_stop))
                await writer.drain()
                # The peer sends its post-verack burst (sendheaders/sendcmpct/
                # feefilter/ping, sometimes a large addr) before the headers
                # reply, so give the exchange a generous window and keep reading
                # past unrelated messages until the headers arrive.
                # The headers reply can be ~160 KB (up to 2000 headers) and, under
                # concurrency over a VPN, arrive several seconds after the peer's
                # sendcmpct/ping/getheaders/feefilter burst. Keep reading past those
                # with a generous window so we don't miss it.
                hdr_deadline = time.time() + 45.0
                while time.time() < hdr_deadline:
                    try:
                        cmd, payload = await asyncio.wait_for(
                            read_message(reader, timeout=self.cfg.read_timeout),
                            timeout=25.0,
                        )
                    except (asyncio.TimeoutError, BitcoinProtocolError):
                        break
                    if cmd == CMD_HEADERS:
                        prev = parse_headers_first_prevblock(payload)
                        fork_verified = (prev == self._fork_locator)
                        break

            # 3. Send getaddr
            writer.write(build_getaddr())
            await writer.drain()

            # 4. Read responses for a while (addr / addrv2 / pings)
            addr_deadline = time.time() + min(18.0, self.cfg.crawl_timeout_per_peer * 0.6)
            pings_sent = 0

            while time.time() < addr_deadline and len(addrs_received) < self.cfg.max_addrs_per_peer:
                try:
                    cmd, payload = await asyncio.wait_for(
                        read_message(reader, timeout=6.0),
                        timeout=4.5,
                    )
                except (asyncio.TimeoutError, BitcoinProtocolError):
                    # Send a ping to keep things alive / measure
                    if pings_sent < 2:
                        writer.write(build_ping())
                        await writer.drain()
                        pings_sent += 1
                    continue

                if cmd in (CMD_ADDR,):
                    try:
                        new_addrs = parse_addr_payload(payload)
                        addrs_received.extend(new_addrs[: self.cfg.max_addrs_per_peer])
                    except Exception:
                        pass
                elif cmd == CMD_ADDRV2:
                    try:
                        new_addrs = parse_addrv2_payload(payload)
                        addrs_received.extend(new_addrs[: self.cfg.max_addrs_per_peer])
                    except Exception:
                        pass
                elif cmd == CMD_VERSION:
                    # rare late version, ignore
                    pass
                elif cmd == CMD_PONG:
                    pass

            # Measure latency as time to first successful response after connect
            latency = (time.perf_counter() - start) * 1000.0

        except Exception as exc:
            self.progress("peer_error", {"ip": ip, "error": str(exc)[:80]})
            return
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

        if not version_info:
            return

        # Build Node object
        node = Node(
            ip=ip,
            port=port,
            version=version_info.get("version"),
            subversion=version_info.get("user_agent"),
            services=version_info.get("services"),
            user_agent=version_info.get("user_agent"),
            start_height=version_info.get("start_height"),
            latency_ms=round(latency, 1) if latency else None,
        )

        # Detect listening
        services = node.services or 0
        node.services_listening = bool(services & 1)  # NODE_NETWORK

        # Detect Knots - common patterns
        ua = (node.subversion or "").lower()
        node.is_knots = "knots" in ua or ".knots" in ua

        # Chain-verified BLAKE2b fork membership (independent of user agent)
        node.is_fork = fork_verified

        # GeoIP
        node.geo = self.geo.lookup(ip)

        # Update global best height
        if node.start_height and node.start_height > self.best_height:
            self.best_height = node.start_height

        node.last_height_delta = (node.start_height - self.best_height) if node.start_height else None

        # Persist
        was_new = await self.db.upsert_node(node, global_best_height=self.best_height)

        if was_new:
            self.stats.discovered += 1

        self.stats.succeeded += 1
        if node.is_knots:
            self.stats.knots_found += 1
        if node.services_listening:
            self.stats.listening_found += 1
        if node.start_height and node.start_height > self.stats.max_height:
            self.stats.max_height = node.start_height

        # Enqueue discovered addresses (controlled)
        new_enqueued = 0
        random.shuffle(addrs_received)  # fairness
        for addr in addrs_received[: self.cfg.max_addrs_per_peer]:
            # Only IPv4 / IPv6, reasonable ports
            if addr.port < 1024 or addr.port > 65535:
                continue
            try:
                ipa = ipaddress.ip_address(addr.ip)
                if ipa.is_private or ipa.is_loopback or ipa.is_link_local:
                    continue
            except Exception:
                continue

            key = addr.ip + ":" + str(addr.port)
            if key not in self.seen:
                self.seen.add(key)
                await self.to_crawl.put((addr.ip, addr.port))
                new_enqueued += 1
                if new_enqueued > 180:  # per-peer discovery throttle
                    break

        self.progress("peer_ok", {
            "ip": ip,
            "port": port,
            "height": node.start_height,
            "knots": node.is_knots,
            "listening": node.services_listening,
            "new_addrs": new_enqueued,
            "latency": node.latency_ms,
            "queue": self.to_crawl.qsize(),
        })

        # Gentle wave delay
        await asyncio.sleep(self.cfg.delay_between_waves * random.uniform(0.6, 1.4))
