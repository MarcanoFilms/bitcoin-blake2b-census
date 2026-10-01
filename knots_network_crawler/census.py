"""
Build the dashboard (data.json) from a list of node records.

Key design: the dashboard is built from the PERSISTENT registry (nodes seen within
an "active" window), NOT from whatever a single crawl pass happened to reach. A
node confirmed yesterday still counts today even if a congested pass missed it —
so the numbers reflect the real network over time, not per-pass tunnel luck.

`db_reachable_nodes()` extracts this pass's reachable, chain-verified fork nodes
from the crawler DB (full fields). Those get merged into the registry; the
dashboard is then built from the registry's active set via `build_census(nodes)`.
"""
from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter
from typing import List, Optional

# Service bits
NODE_NETWORK = 1 << 0
NODE_WITNESS = 1 << 3
NODE_COMPACT_FILTERS = 1 << 6
NODE_NETWORK_LIMITED = 1 << 10
NODE_P2P_V2 = 1 << 11


def _svc(services, bit) -> bool:
    return bool((services or 0) & bit)


def net_type(ip: str) -> str:
    ip = (ip or "")
    if ip.endswith(".onion"):
        return "tor"
    if ip.endswith(".i2p"):
        return "i2p"
    return "ipv6" if ":" in ip else "ipv4"


def db_reachable_nodes(db_path: str) -> List[dict]:
    """Return this pass's reachable, chain-verified fork nodes from the crawler DB
    as normalized dicts (the shape the registry stores and the dashboard consumes)."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT * FROM nodes WHERE is_fork = 1 AND services_listening = 1"
    ).fetchall()
    con.close()
    out = []
    for r in rows:
        k = r.keys()
        services = r["services"]
        vh = r["verified_height"] if "verified_height" in k else None
        out.append({
            "ip": r["ip"], "port": r["port"], "net": net_type(r["ip"]),
            "cc": r["country_code"], "country": r["country"], "city": r["city"],
            "lat": r["latitude"], "lon": r["longitude"], "asn_org": r["asn_org"],
            "subversion": r["subversion"],
            "height": vh if vh else r["start_height"],
            "verified": bool(vh),
            "full": _svc(services, NODE_NETWORK),
            "pruned": _svc(services, NODE_NETWORK_LIMITED) and not _svc(services, NODE_NETWORK),
            "witness": _svc(services, NODE_WITNESS),
            "compact": _svc(services, NODE_COMPACT_FILTERS),
            "v2": _svc(services, NODE_P2P_V2),
            "latency_ms": r["latency_ms"],
            "first_seen": r["first_seen"] if "first_seen" in k else None,
            "last_seen": r["last_seen"] if "last_seen" in k else None,
        })
    return out


def build_census(nodes: List[dict], generated_ts: int, fork_tip: Optional[int] = None,
                 sensor_peers: Optional[list] = None,
                 fork_headline: str = "8-30 NYPost Deride And Conquer",
                 anchor_height: int = 961640, interval_seconds: int = 7200,
                 all_time: int = 0) -> dict:
    """Build the dashboard dict from a list of node records (the active registry set)."""
    reachable = nodes
    heights = [n["height"] for n in reachable if n.get("height")]
    tip = fork_tip or (max(heights) if heights else 0)

    by_country = Counter(n["cc"] for n in reachable if n.get("cc"))
    by_version = Counter(n["subversion"] for n in reachable if n.get("subversion"))
    by_asn = Counter(n["asn_org"] for n in reachable if n.get("asn_org"))
    by_network = Counter(n.get("net") or net_type(n["ip"]) for n in reachable)

    services = {
        "full": sum(1 for n in reachable if n.get("full")),
        "pruned": sum(1 for n in reachable if n.get("pruned")),
        "witness": sum(1 for n in reachable if n.get("witness")),
        "compact_filters": sum(1 for n in reachable if n.get("compact")),
        "v2_transport": sum(1 for n in reachable if n.get("v2")),
    }

    TIP_TOLERANCE = 2
    n_verified = sum(1 for n in reachable if n.get("verified") and n.get("height"))
    at_tip = sum(1 for n in reachable if n.get("verified") and n.get("height")
                 and tip and (tip - n["height"]) <= TIP_TOLERANCE)
    height_buckets = {
        "tip": at_tip,
        "behind": n_verified - at_tip,
        "unknown": len(reachable) - n_verified,
    }

    reachable_ips = {n["ip"] for n in reachable}
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
            "ip": n["ip"], "port": n["port"], "net": n.get("net"),
            "subversion": n.get("subversion"), "height": n.get("height"),
            "cc": n.get("cc"), "country": n.get("country"), "city": n.get("city"),
            "lat": n.get("lat"), "lon": n.get("lon"), "asn_org": n.get("asn_org"),
            "pruned": n.get("pruned"), "v2": n.get("v2"),
            "latency_ms": n.get("latency_ms"),
            "first_seen": n.get("first_seen"), "last_seen": n.get("last_seen"),
        }
        for n in reachable
    ]

    return {
        "generated": generated_ts,
        "interval_seconds": interval_seconds,
        "fork_headline": fork_headline,
        "anchor_height": anchor_height,
        "fork_tip": tip,
        "fork_total": all_time or len(reachable),
        "fork_reachable": len(reachable),
        "non_listening_seen": non_listening,
        "total_estimate": len(reachable) + non_listening,
        "heights": height_buckets,
        "services": services,
        "by_country": dict(by_country.most_common()),
        "by_version": dict(by_version.most_common()),
        "by_asn": dict(by_asn.most_common(15)),
        "by_network": dict(by_network),
        "countries_count": len(by_country),
        "nodes": nodes_out,
    }


def append_history(out_path: str, data: dict, max_points: int = 2160) -> None:
    hist_path = os.path.join(os.path.dirname(out_path) or ".", "history.json")
    try:
        try:
            with open(hist_path) as f:
                hist = json.load(f)
                if not isinstance(hist, list):
                    hist = []
        except Exception:
            hist = []
        hist.append({
            "t": data["generated"], "reachable": data["fork_reachable"],
            "estimate": data["total_estimate"], "countries": data["countries_count"],
            "tip": data["fork_tip"],
        })
        hist = hist[-max_points:]
        with open(hist_path, "w") as f:
            json.dump(hist, f, separators=(",", ":"))
    except Exception:
        pass


def _write_nodes_csv(out_path: str, data: dict) -> None:
    import csv
    csv_path = os.path.join(os.path.dirname(out_path) or ".", "nodes.csv")
    fields = ["ip", "port", "cc", "country", "city", "asn_org",
              "subversion", "height", "pruned", "v2", "latency_ms"]
    try:
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            for n in data.get("nodes", []):
                w.writerow(n)
    except Exception:
        pass


def write_census(nodes: List[dict], out_path: str, generated_ts: int,
                 fork_tip: Optional[int] = None, sensor_peers: Optional[list] = None,
                 fork_headline: str = "8-30 NYPost Deride And Conquer",
                 anchor_height: int = 961640, interval_seconds: int = 7200,
                 all_time: int = 0) -> dict:
    data = build_census(nodes, generated_ts, fork_tip, sensor_peers,
                        fork_headline, anchor_height, interval_seconds, all_time)
    with open(out_path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    append_history(out_path, data)
    _write_nodes_csv(out_path, data)
    return data
