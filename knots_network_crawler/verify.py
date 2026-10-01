"""
Phase 2 — focused BLAKE2b membership + chain-verified height.

The discovery crawl (phase 1) finds nodes and their self-declared version/height,
but skips the chain check because its getaddr traffic saturates a limited uplink.
Here we revisit only the candidates (reachable Knots nodes) with dedicated
connections that do just the version handshake and one or two tiny getheaders
probes — no getaddr — so replies are never contended and results are reliable.

Two things are verified per node, in the same connection:

1. Membership: getheaders with the activation block as locator; a node on the
   chain replies with a header whose prev_block equals that locator.

2. Height (Kilombino's method): getheaders with locators [tip-10, tip-100,
   tip-1000, anchor] and hash_stop = our tip, where our tip is sampled once at the
   start of the pass so every node is measured against the same reference. The
   node's height is (height of the locator it recognized) + (number of headers it
   returned). If the last returned header equals our tip header byte-for-byte the
   node is exactly at tip; comparing raw header bytes avoids the SHA256d-vs-BLAKE2b
   block-id question. A >20 KB reply means it's far behind (we stop, not download).
"""
from __future__ import annotations

import asyncio
from typing import List, Optional, Tuple

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
    open_p2p_connection,
    parse_headers_first_prevblock,
    parse_headers_summary,
    parse_version_payload,
    read_message,
)

MAX_HEADERS_PAYLOAD = 20_000  # bytes; larger reply => far behind, don't parse
NODE_BLAKE2B = 1 << 28  # service bit advertised by BLAKE2b nodes (NODE_BLAKE2B)


async def probe_node(ip: str, port: int, anchor: bytes, anchor_stop: bytes,
                     tip_height: Optional[int], tip_header: Optional[bytes], tip_stop: bytes,
                     height_locators: List[Tuple[bytes, int]], header_size: int = 80,
                     tor_socks=None, i2p_socks=None,
                     connect_timeout: float = 10.0, read_timeout: float = 12.0
                     ) -> Tuple[str, Optional[int]]:
    """Return (status, verified_height). status is one of:
      'member'      — confirmed on the BLAKE2b chain (never downgrade on this),
      'not_member'  — got a definitive reply that is NOT our chain,
      'unreachable' — couldn't determine (connect/handshake/timeout) — leave as-is.
    Only a definitive answer changes is_fork; a timeout must not drop a good node."""
    try:
        reader, writer = await open_p2p_connection(
            ip, port, tor_socks=tor_socks, i2p_socks=i2p_socks, timeout=connect_timeout)
    except Exception:
        return "unreachable", None
    try:
        # Per-phase budget scales with read_timeout so a slow uplink (e.g. a
        # congested VPN at ~19s/round-trip) doesn't cut a loop off before the reply
        # arrives — the whole point of raising read_timeout.
        budget = max(20.0, read_timeout * 2.0)
        writer.write(make_version_message(addr_recv_ip=ip, addr_recv_port=port, start_height=0))
        await writer.drain()
        deadline = asyncio.get_event_loop().time() + budget
        got_version = got_verack = False
        svc_blake2b = False
        while asyncio.get_event_loop().time() < deadline and not (got_version and got_verack):
            try:
                cmd, payload = await asyncio.wait_for(read_message(reader, timeout=read_timeout), timeout=read_timeout)
            except (asyncio.TimeoutError, BitcoinProtocolError):
                return "unreachable", None
            if cmd == CMD_VERSION:
                got_version = True
                try:
                    svc_blake2b = bool((parse_version_payload(payload).get("services") or 0) & NODE_BLAKE2B)
                except Exception:
                    svc_blake2b = False
                writer.write(build_verack()); await writer.drain()
            elif cmd == CMD_VERACK:
                got_verack = True
        if not got_version:
            return "unreachable", None

        # 1. membership. Fast path: the node advertised service bit 28 (NODE_BLAKE2B)
        # in its version message — membership is already settled with zero extra
        # round-trips, which is exactly what relieves a congested uplink. Only when
        # the bit is absent do we fall back to the chain probe below.
        member = svc_blake2b
        if not member:
            # Chain probe — read `headers` messages, IGNORING unsolicited block
            # announcements. A peer that mines/relays a block mid-probe pushes its own
            # `headers` (prev_block = recent tip, not our anchor); treating that as the
            # getheaders reply would falsely mark a good node 'not_member' and drop it.
            # Only a reply whose first prev_block == our anchor confirms membership;
            # anything else is left as 'unreachable' (never a downgrade).
            writer.write(build_getheaders([anchor], hash_stop=anchor_stop)); await writer.drain()
            hdr_deadline = asyncio.get_event_loop().time() + budget
            while asyncio.get_event_loop().time() < hdr_deadline:
                try:
                    cmd, payload = await asyncio.wait_for(
                        read_message(reader, timeout=read_timeout), timeout=read_timeout)
                except (asyncio.TimeoutError, BitcoinProtocolError):
                    break
                if cmd == CMD_HEADERS and parse_headers_first_prevblock(payload) == anchor:
                    member = True
                    break
        if not member:
            return "unreachable", None

        # 2. chain-verified height (best-effort; membership already confirmed).
        # Like the membership probe, IGNORE unsolicited block announcements: a peer
        # with sendheaders active pushes its own `headers` (first prev = a recent
        # block, not one of our locators). Only the reply whose first prev_block is
        # one of the locators we sent is the answer to our getheaders — accepting an
        # announcement instead would leave the height unmeasured (first_prev matches
        # no locator). This matters now that the bit-28 fast path skips the first
        # getheaders, so the height probe is the one that meets the announcement.
        verified_height: Optional[int] = None
        if height_locators and tip_height:
            loc_hashes = {h for h, _ in height_locators}
            writer.write(build_getheaders([h for h, _ in height_locators], hash_stop=tip_stop))
            await writer.drain()
            hdeadline = asyncio.get_event_loop().time() + budget
            while asyncio.get_event_loop().time() < hdeadline:
                try:
                    cmd, hpayload = await asyncio.wait_for(
                        read_message(reader, timeout=read_timeout), timeout=read_timeout)
                except (asyncio.TimeoutError, BitcoinProtocolError):
                    break
                if cmd != CMD_HEADERS:
                    continue
                first_prev = parse_headers_first_prevblock(hpayload)
                if first_prev not in loc_hashes:
                    continue  # unsolicited announcement — keep waiting for our reply
                if len(hpayload) > MAX_HEADERS_PAYLOAD:
                    verified_height = max(0, tip_height - 2000)  # far behind, don't parse
                    break
                count, _fp, last_header = parse_headers_summary(hpayload, header_size)
                loc_height = next((ht for hh, ht in height_locators if hh == first_prev), None)
                if loc_height is not None:
                    verified_height = min(tip_height, loc_height + count)
                    if tip_header is not None and last_header == tip_header:
                        verified_height = tip_height
                break
        return "member", verified_height
    except Exception:
        # Membership was already confirmed above (we'd have returned otherwise),
        # so this is a late error during the height probe — keep the membership,
        # drop the height.
        return "member", None
    finally:
        try:
            writer.close(); await writer.wait_closed()
        except Exception:
            pass


async def verify_candidates(db: Database, cfg: CrawlerConfig,
                            candidates: List[Tuple[str, int]],
                            concurrency: int = 12,
                            tip_height: Optional[int] = None,
                            tip_hash: Optional[str] = None,
                            tip_header: Optional[str] = None,
                            locators: Optional[List[Tuple[str, int]]] = None,
                            read_timeout: float = 12.0,
                            connect_timeout: float = 10.0,
                            progress=None) -> dict:
    """Probe each candidate for membership + chain-verified height; persist results."""
    anchor = hash_display_to_internal(cfg.fork_anchor_hash)
    anchor_stop = hash_display_to_internal(cfg.fork_stop_hash) if cfg.fork_stop_hash else b"\x00" * 32
    tip_header_b = bytes.fromhex(tip_header) if tip_header else None
    tip_stop_b = hash_display_to_internal(tip_hash) if tip_hash else b"\x00" * 32
    loc = [(hash_display_to_internal(h), ht) for h, ht in (locators or [])]

    sem = asyncio.Semaphore(concurrency)
    counts = {"checked": 0, "fork": 0, "heights": 0, "unreachable": 0}

    async def one(ip: str, port: int):
        async with sem:
            status, vheight = await probe_node(
                ip, port, anchor, anchor_stop, tip_height, tip_header_b, tip_stop_b, loc,
                header_size=getattr(cfg, "fork_header_size", 80),
                tor_socks=cfg.tor_socks_addr, i2p_socks=cfg.i2p_socks_addr,
                connect_timeout=connect_timeout, read_timeout=read_timeout)
            counts["checked"] += 1
            if status == "member":
                await db.set_fork(ip, port, True, vheight)
                counts["fork"] += 1
                if vheight is not None:
                    counts["heights"] += 1
            elif status == "not_member":
                await db.set_fork(ip, port, False)
            else:  # unreachable this pass — do NOT downgrade a previously verified node
                counts["unreachable"] += 1
            if progress and counts["checked"] % 25 == 0:
                progress("verify", dict(counts))

    await asyncio.gather(*[one(ip, port) for ip, port in candidates], return_exceptions=True)
    return counts
