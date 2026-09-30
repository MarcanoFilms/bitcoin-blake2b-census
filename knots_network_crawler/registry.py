"""
Persistent node registry — the durable, growing record of every unique BLAKE2b
node we've ever confirmed, kept as a CSV committed to GitHub (off-machine storage,
public and auditable). One row per unique address:

  address, port, net, first_seen, last_seen, times_seen, country, version, height

Each census pass merges the currently-reachable, chain-verified nodes in:
new addresses get a first_seen; known ones bump last_seen / times_seen and refresh
their last-known country / version / height. Nodes are never dropped from this
record (that's the point — a growing history); the local crawler DB is what gets
pruned to keep disk small (see prune_db).
"""
from __future__ import annotations

import csv
import os
import sqlite3
from typing import List

FIELDS = ["address", "port", "net", "first_seen", "last_seen",
          "times_seen", "country", "version", "height"]


def update_registry(csv_path: str, nodes: List[dict], now_iso: str) -> dict:
    reg = {}
    if os.path.exists(csv_path):
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                reg[(row["address"], row["port"])] = row

    new = 0
    for n in nodes:
        key = (n.get("ip"), str(n.get("port")))
        if not key[0]:
            continue
        if key in reg:
            r = reg[key]
            r["last_seen"] = now_iso
            try:
                r["times_seen"] = str(int(r.get("times_seen") or 0) + 1)
            except ValueError:
                r["times_seen"] = "1"
            r["net"] = n.get("net") or r.get("net", "")
            r["country"] = n.get("country") or r.get("country", "")
            r["version"] = n.get("subversion") or r.get("version", "")
            if n.get("height"):
                r["height"] = str(n["height"])
        else:
            new += 1
            reg[key] = {
                "address": n.get("ip"), "port": str(n.get("port")),
                "net": n.get("net") or "",
                "first_seen": now_iso, "last_seen": now_iso, "times_seen": "1",
                "country": n.get("country") or "", "version": n.get("subversion") or "",
                "height": str(n.get("height") or ""),
            }

    rows = sorted(reg.values(), key=lambda r: r.get("first_seen") or "")
    tmp = csv_path + ".tmp"
    with open(tmp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, csv_path)
    return {"total": len(rows), "new": new}


def prune_db(db_path: str, now_ts: int, keep_days: int = 90) -> int:
    """Delete crawler DB rows not seen in keep_days (the historical record lives in
    the registry CSV). Returns rows removed. last_seen is ISO8601; compared as a
    string cutoff, which is chronological for that format."""
    from datetime import datetime, timezone
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
