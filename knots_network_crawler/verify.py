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
    parse_headers_first_prevblock,
    parse_headers_summary,
    read_message,
)

MAX_HEADERS_PAYLOAD = 20_000  # bytes; larger reply => far behind, don't parse


async def _read_until_headers(reader, read_timeout: float, budget: float) -> Optional[bytes]:
    deadline = asyncio.get_event_loop().time() + budget
    while asyncio.get_event_loop().time() < deadline:
        cmd, payload = await asyncio.wait_for(read_message(reader, timeout=read_timeout), timeout=read_timeout)
        if cmd == CMD_HEADERS:
            return payload
    return None


async def probe_node(ip: str, port: int, anchor: bytes, anchor_stop: bytes,
                     tip_height: Optional[int], tip_header: Optional[bytes], tip_stop: bytes,
                     height_locators: List[Tuple[bytes, int]], header_size: int = 80,
                     connect_timeout: float = 10.0, read_timeout: float = 12.0
                     ) -> Tuple[str, Optional[int]]:
    """Return (status, verified_height). status is one of:
      'member'      — confirmed on the BLAKE2b chain (never downgrade on this),
      'not_member'  — got a definitive reply that is NOT our chain,
      'unreachable' — couldn't determine (connect/handshake/timeout) — leave as-is.
    Only a definitive answer changes is_fork; a timeout must not drop a good node."""
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout=connect_timeout)
    except Exception:
        return "unreachable", None
    try:
        writer.write(make_version_message(addr_recv_ip=ip, addr_recv_port=port, start_height=0))
        await writer.drain()
        deadline = asyncio.get_event_loop().time() + 20.0
        got_version = got_verack = False
        while asyncio.get_event_loop().time() < deadline and not (got_version and got_verack):
            try:
                cmd, _ = await asyncio.wait_for(read_message(reader, timeout=read_timeout), timeout=read_timeout)
            except (asyncio.TimeoutError, BitcoinProtocolError):
                return "unreachable", None
            if cmd == CMD_VERSION:
                got_version = True
                writer.write(build_verack()); await writer.drain()
            elif cmd == CMD_VERACK:
                got_verack = True
        if not got_version:
            return "unreachable", None

        # 1. membership
        writer.write(build_getheaders([anchor], hash_stop=anchor_stop)); await writer.drain()
        try:
            payload = await _read_until_headers(reader, read_timeout, 20.0)
        except (asyncio.TimeoutError, BitcoinProtocolError):
            return "unreachable", None
        if payload is None:
            return "unreachable", None
        if parse_headers_first_prevblock(payload) != anchor:
            return "not_member", None

        # 2. chain-verified height (best-effort; membership already confirmed)
        verified_height: Optional[int] = None
        if height_locators and tip_height:
            writer.write(build_getheaders([h for h, _ in height_locators], hash_stop=tip_stop))
            await writer.drain()
            try:
                hpayload = await _read_until_headers(reader, read_timeout, 20.0)
            except (asyncio.TimeoutError, BitcoinProtocolError):
                hpayload = None
            if hpayload is not None:
                if len(hpayload) > MAX_HEADERS_PAYLOAD:
                    verified_height = max(0, tip_height - 2000)  # far behind
                else:
                    count, first_prev, last_header = parse_headers_summary(hpayload, header_size)
                    loc_height = next((ht for hh, ht in height_locators if hh == first_prev), None)
                    if loc_height is not None:
                        verified_height = min(tip_height, loc_height + count)
                        if tip_header is not None and last_header == tip_header:
                            verified_height = tip_height
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
                header_size=getattr(cfg, "fork_header_size", 80))
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
