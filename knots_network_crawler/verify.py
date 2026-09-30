"""
Phase 2 — focused BLAKE2b fork verification.

The discovery crawl (phase 1) finds nodes and their version/height/services but
skips the fork check, because its getaddr traffic (large addr dumps) saturates a
limited uplink and starves the tiny getheaders replies. Here we revisit only the
candidates (reachable Knots nodes) with dedicated connections that do *just* the
version handshake + a single getheaders anchor probe — no getaddr — so the
81-byte header reply is never contended and detection is reliable.

A node is on the fork iff, asked for headers with the activation block as
locator, it replies with a header whose prev_block equals that locator.
"""
from __future__ import annotations

import asyncio
from typing import List, Tuple

from .config import CrawlerConfig
from .database import Database
from .protocol import (
    CMD_HEADERS,
    CMD_VERACK,
    CMD_VERSION,
    BitcoinProtocolError,
    build_getheaders,
    build_verack,
    hash_display_to_internal,
    make_version_message,
    parse_headers_first_prevblock,
    read_message,
)


async def probe_fork(ip: str, port: int, locator: bytes, stop: bytes,
                     connect_timeout: float = 10.0, read_timeout: float = 12.0) -> bool:
    """Return True iff the peer is on the BLAKE2b fork chain."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=connect_timeout)
    except Exception:
        return False
    try:
        writer.write(make_version_message(addr_recv_ip=ip, addr_recv_port=port, start_height=0))
        await writer.drain()

        # handshake: reply verack to their version, wait for their verack
        deadline = asyncio.get_event_loop().time() + 20.0
        got_version = got_verack = False
        while asyncio.get_event_loop().time() < deadline and not (got_version and got_verack):
            try:
                cmd, _ = await asyncio.wait_for(read_message(reader, timeout=read_timeout), timeout=read_timeout)
            except (asyncio.TimeoutError, BitcoinProtocolError):
                return False
            if cmd == CMD_VERSION:
                got_version = True
                writer.write(build_verack()); await writer.drain()
            elif cmd == CMD_VERACK:
                got_verack = True
        if not got_version:
            return False

        # single-header anchor probe
        writer.write(build_getheaders([locator], hash_stop=stop)); await writer.drain()
        hdr_deadline = asyncio.get_event_loop().time() + 20.0
        while asyncio.get_event_loop().time() < hdr_deadline:
            try:
                cmd, payload = await asyncio.wait_for(read_message(reader, timeout=read_timeout), timeout=read_timeout)
            except (asyncio.TimeoutError, BitcoinProtocolError):
                return False
            if cmd == CMD_HEADERS:
                return parse_headers_first_prevblock(payload) == locator
        return False
    except Exception:
        return False
    finally:
        try:
            writer.close(); await writer.wait_closed()
        except Exception:
            pass


async def verify_candidates(db: Database, cfg: CrawlerConfig,
                            candidates: List[Tuple[str, int]],
                            concurrency: int = 12, progress=None) -> dict:
    """Probe each candidate for fork membership and persist is_fork. Returns counts."""
    locator = hash_display_to_internal(cfg.fork_anchor_hash)
    stop = hash_display_to_internal(cfg.fork_stop_hash) if cfg.fork_stop_hash else b"\x00" * 32
    sem = asyncio.Semaphore(concurrency)
    counts = {"checked": 0, "fork": 0}

    async def one(ip: str, port: int):
        async with sem:
            is_fork = await probe_fork(ip, port, locator, stop)
            await db.set_fork(ip, port, is_fork)
            counts["checked"] += 1
            if is_fork:
                counts["fork"] += 1
            if progress and counts["checked"] % 25 == 0:
                progress("verify", dict(counts))

    await asyncio.gather(*[one(ip, port) for ip, port in candidates], return_exceptions=True)
    return counts
