"""
Persistent node registry — the durable, growing record of every unique BLAKE2b
node we've ever confirmed, kept as a CSV committed to GitHub. It is also the
SOURCE OF TRUTH for the dashboard: the page is built from the registry's "active"
set (nodes seen within a window), so a node confirmed recently still counts even
if one congested pass missed it. The crawler DB is just the per-pass working set
that feeds this record.

One row per unique address with everything the dashboard needs:
  address, port, net, first_seen, last_seen, times_seen,
  cc, country, city, lat, lon, asn_org, version, height,
  full, pruned, witness, compact, v2, latency_ms, verified
"""
from __future__ import annotations

import csv
import os
import sqlite3
from datetime import datetime, timezone
from typing import List

FIELDS = ["address", "port", "net", "first_seen", "last_seen", "times_seen",
          "cc", "country", "city", "lat", "lon", "asn_org", "version", "height",
          "full", "pruned", "witness", "compact", "v2", "latency_ms", "verified"]


def _b(v) -> str:
    return "1" if v else "0"


def update_registry(csv_path: str, nodes: List[dict], now_iso: str) -> dict:
    reg = {}
    if os.path.exists(csv_path):
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                reg[(row.get("address"), row.get("port"))] = row

    new = 0
    for n in nodes:
        key = (n.get("ip"), str(n.get("port")))
        if not key[0]:
            continue
        base = reg.get(key, {})
        is_new = key not in reg
        if is_new:
            new += 1
        reg[key] = {
            "address": n.get("ip"), "port": str(n.get("port")),
            "net": n.get("net") or base.get("net", ""),
            "first_seen": base.get("first_seen") or now_iso,
            "last_seen": now_iso,
            "times_seen": str(int(base.get("times_seen") or 0) + 1),
            "cc": n.get("cc") or base.get("cc", ""),
            "country": n.get("country") or base.get("country", ""),
            "city": n.get("city") or base.get("city", ""),
            "lat": "" if n.get("lat") is None else n.get("lat"),
            "lon": "" if n.get("lon") is None else n.get("lon"),
            "asn_org": n.get("asn_org") or base.get("asn_org", ""),
            "version": n.get("subversion") or base.get("version", ""),
            "height": str(n.get("height") or base.get("height") or ""),
            "full": _b(n.get("full")), "pruned": _b(n.get("pruned")),
            "witness": _b(n.get("witness")), "compact": _b(n.get("compact")),
            "v2": _b(n.get("v2")),
            "latency_ms": "" if n.get("latency_ms") is None else n.get("latency_ms"),
            "verified": _b(n.get("verified")),
        }

    rows = sorted(reg.values(), key=lambda r: r.get("first_seen") or "")
    tmp = csv_path + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, csv_path)
    return {"total": len(rows), "new": new}


def _truthy(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes")


def _num(v, cast):
    try:
        return cast(v)
    except (TypeError, ValueError):
        return None


def load_active_nodes(csv_path: str, now_ts: int, active_hours: int = 48) -> List[dict]:
    """Return node dicts from the registry seen within active_hours — the set the
    dashboard is built from (persistent, not per-pass)."""
    if not os.path.exists(csv_path):
        return []
    out = []
    with open(csv_path, newline="") as f:
        for r in csv.DictReader(f):
            ls = r.get("last_seen")
            if ls:
                try:
                    if now_ts - datetime.fromisoformat(ls).timestamp() > active_hours * 3600:
                        continue
                except Exception:
                    pass
            out.append({
                "ip": r.get("address"), "port": _num(r.get("port"), int) or 8333,
                "net": r.get("net"),
                "cc": r.get("cc"), "country": r.get("country"), "city": r.get("city"),
                "lat": _num(r.get("lat"), float), "lon": _num(r.get("lon"), float),
                "asn_org": r.get("asn_org"), "subversion": r.get("version"),
                "height": _num(r.get("height"), int),
                "verified": _truthy(r.get("verified")),
                "full": _truthy(r.get("full")), "pruned": _truthy(r.get("pruned")),
                "witness": _truthy(r.get("witness")), "compact": _truthy(r.get("compact")),
                "v2": _truthy(r.get("v2")),
                "latency_ms": _num(r.get("latency_ms"), float),
                "first_seen": r.get("first_seen"), "last_seen": r.get("last_seen"),
            })
    return out


def registry_count(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0
    with open(csv_path, newline="") as f:
        return max(0, sum(1 for _ in f) - 1)


def prune_db(db_path: str, now_ts: int, keep_days: int = 90) -> int:
    cutoff = datetime.fromtimestamp(now_ts - keep_days * 86400, timezone.utc).isoformat()
    try:
        con = sqlite3.connect(db_path)
        cur = con.execute("DELETE FROM nodes WHERE last_seen IS NOT NULL AND last_seen < ?", (cutoff,))
        n = cur.rowcount
        con.commit()
        con.close()
        return n
    except Exception:
        return 0
