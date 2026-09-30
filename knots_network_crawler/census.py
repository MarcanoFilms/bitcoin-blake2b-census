"""
Build a BLAKE2b-fork node census (data.json) from a crawler database.

Only chain-verified fork nodes (is_fork = 1) are counted, so the figures are
about the BLAKE2b network specifically, not the shared Bitcoin P2P network the
fork rides on. Reachable = advertises NODE_NETWORK and completed our crawl.

Optionally merges a passive-sensor list (inbound peers observed by our own
node via `bitcoin-cli getpeerinfo`) to estimate the non-listening population,
the way Luke Dashjr's counts include nodes a pure crawler can never reach.
"""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from typing import Optional

# Service bits (mirrors protocol.py; duplicated so the exporter can run stand-alone)
NODE_NETWORK = 1 << 0
NODE_BLOOM = 1 << 2
NODE_WITNESS = 1 << 3
NODE_COMPACT_FILTERS = 1 << 6
NODE_NETWORK_LIMITED = 1 << 10
NODE_P2P_V2 = 1 << 11


def _svc(services: Optional[int], bit: int) -> bool:
    return bool((services or 0) & bit)


def build_census(db_path: str, generated_ts: int, fork_tip: Optional[int] = None,
                 sensor_peers: Optional[list] = None,
                 fork_headline: str = "8-30 NYPost Deride And Conquer",
                 anchor_height: int = 961640,
                 interval_seconds: int = 7200) -> dict:
    """Read the crawler DB and return a census dict ready to serialize."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT * FROM nodes WHERE is_fork = 1"
    ).fetchall()
    con.close()

    reachable = [r for r in rows if r["services_listening"]]
    heights = [r["start_height"] for r in reachable if r["start_height"]]
    tip = fork_tip or (max(heights) if heights else 0)

    def near(h):  # within 6 blocks of tip
        return h is not None and tip and (tip - h) <= 6

    by_country = Counter(r["country_code"] for r in reachable if r["country_code"])
    by_version = Counter(r["subversion"] for r in reachable if r["subversion"])
    by_asn = Counter(r["asn_org"] for r in reachable if r["asn_org"])

    # A node is pruned only if it advertises NODE_NETWORK_LIMITED *without*
    # NODE_NETWORK. Modern full nodes set BOTH bits, so keying "pruned" off the
    # limited bit alone mislabels full nodes as pruned.
    def _pruned(services) -> bool:
        return _svc(services, NODE_NETWORK_LIMITED) and not _svc(services, NODE_NETWORK)

    services = {
        "full": sum(1 for r in reachable if _svc(r["services"], NODE_NETWORK)),
        "pruned": sum(1 for r in reachable if _pruned(r["services"])),
        "witness": sum(1 for r in reachable if _svc(r["services"], NODE_WITNESS)),
        "compact_filters": sum(1 for r in reachable if _svc(r["services"], NODE_COMPACT_FILTERS)),
        "v2_transport": sum(1 for r in reachable if _svc(r["services"], NODE_P2P_V2)),
    }

    height_buckets = {
        "tip": sum(1 for h in heights if near(h)),
        "near": sum(1 for h in heights if h is not None and tip and 6 < (tip - h) <= 144),
        "behind": sum(1 for h in heights if h is not None and tip and (tip - h) > 144),
    }

    # Passive sensor: inbound peers seen by our own node that are NOT in the
    # reachable (listening) set -> a lower bound on the non-listening population.
    reachable_ips = {r["ip"] for r in reachable}
    non_listening = 0
    if sensor_peers:
        seen = set()
        for p in sensor_peers:
            ip = p.split(":")[0] if isinstance(p, str) else p
            if ip and ip not in reachable_ips:
                seen.add(ip)
        non_listening = len(seen)

    nodes_out = [
        {
            "ip": r["ip"], "port": r["port"],
            "subversion": r["subversion"],
            "height": r["start_height"],
            "cc": r["country_code"], "country": r["country"], "city": r["city"],
            "lat": r["latitude"], "lon": r["longitude"],
            "asn_org": r["asn_org"],
            "pruned": _pruned(r["services"]),
            "v2": _svc(r["services"], NODE_P2P_V2),
            "latency_ms": r["latency_ms"],
        }
        for r in reachable
    ]

    return {
        "generated": generated_ts,
        "interval_seconds": interval_seconds,
        "fork_headline": fork_headline,
        "anchor_height": anchor_height,
        "fork_tip": tip,
        "fork_total": len(rows),
        "fork_reachable": len(reachable),
        "non_listening_seen": non_listening,
        "total_estimate": len(reachable) + non_listening,
        "heights": height_buckets,
        "services": services,
        "by_country": dict(by_country.most_common()),
        "by_version": dict(by_version.most_common()),
        "by_asn": dict(by_asn.most_common(15)),
        "countries_count": len(by_country),
        "nodes": nodes_out,
    }


def write_census(db_path: str, out_path: str, generated_ts: int,
                 fork_tip: Optional[int] = None, sensor_peers: Optional[list] = None,
                 fork_headline: str = "8-30 NYPost Deride And Conquer",
                 anchor_height: int = 961640, interval_seconds: int = 7200) -> dict:
    data = build_census(db_path, generated_ts, fork_tip, sensor_peers,
                        fork_headline, anchor_height, interval_seconds)
    with open(out_path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    return data
