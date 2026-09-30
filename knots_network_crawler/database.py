"""
SQLite database layer for knots-network-crawler.

Schema designed for rich analysis:
- nodes: main fact table (upsert on ip:port)
- crawl_sessions: run metadata
- node_changes: lightweight history of interesting mutations (height, version, last_seen jumps)

All operations are async-friendly via aiosqlite.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite

from .models import GeoData, Node

DB_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS nodes (
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    first_seen TEXT,
    last_seen TEXT,
    last_crawled TEXT,

    version INTEGER,
    subversion TEXT,
    services INTEGER,
    user_agent TEXT,
    start_height INTEGER,

    services_listening INTEGER DEFAULT 0,
    is_knots INTEGER DEFAULT 0,
    is_fork INTEGER DEFAULT 0,
    verified_height INTEGER,

    latency_ms REAL,

    country TEXT,
    country_code TEXT,
    city TEXT,
    asn INTEGER,
    asn_org TEXT,
    latitude REAL,
    longitude REAL,

    crawl_count INTEGER DEFAULT 0,
    last_height_delta INTEGER,

    PRIMARY KEY (ip, port)
);

CREATE INDEX IF NOT EXISTS idx_nodes_last_crawled ON nodes(last_crawled);
CREATE INDEX IF NOT EXISTS idx_nodes_subversion ON nodes(subversion);
CREATE INDEX IF NOT EXISTS idx_nodes_country ON nodes(country);
CREATE INDEX IF NOT EXISTS idx_nodes_asn ON nodes(asn);
CREATE INDEX IF NOT EXISTS idx_nodes_listening ON nodes(services_listening);
CREATE INDEX IF NOT EXISTS idx_nodes_knots ON nodes(is_knots);
CREATE INDEX IF NOT EXISTS idx_nodes_fork ON nodes(is_fork);
CREATE INDEX IF NOT EXISTS idx_nodes_height ON nodes(start_height);

CREATE TABLE IF NOT EXISTS crawl_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    mode TEXT,
    nodes_discovered INTEGER DEFAULT 0,
    nodes_crawled INTEGER DEFAULT 0,
    knots_found INTEGER DEFAULT 0,
    listening_found INTEGER DEFAULT 0,
    max_height_seen INTEGER DEFAULT 0,
    config_json TEXT
);

CREATE TABLE IF NOT EXISTS node_changes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    changed_at TEXT NOT NULL,
    field TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT
);

CREATE INDEX IF NOT EXISTS idx_changes_node ON node_changes(ip, port, changed_at);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.executescript(DB_SCHEMA)
            await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    @asynccontextmanager
    async def transaction(self):
        if not self._conn:
            await self.connect()
        async with self._conn:  # type: ignore
            yield

    # ------------------------- Node operations -------------------------

    async def upsert_node(self, node: Node, global_best_height: Optional[int] = None) -> bool:
        """
        Insert or update a node. Returns True if it was newly discovered in this run.
        Also records interesting changes in node_changes.
        """
        if not self._conn:
            await self.connect()

        now = utcnow()
        key = (node.ip, node.port)

        # Fetch existing
        async with self._conn.execute(
            "SELECT * FROM nodes WHERE ip=? AND port=?", key
        ) as cur:
            row = await cur.fetchone()

        is_new = row is None

        # Compute fields
        height_delta = None
        if node.start_height is not None and global_best_height is not None:
            height_delta = node.start_height - global_best_height

        services_listening = 1 if node.services_listening else 0
        is_knots = 1 if node.is_knots else 0
        is_fork = 1 if node.is_fork else 0

        if is_new:
            await self._conn.execute(
                """
                INSERT INTO nodes (
                    ip, port, first_seen, last_seen, last_crawled,
                    version, subversion, services, user_agent, start_height,
                    services_listening, is_knots, is_fork, latency_ms,
                    country, country_code, city, asn, asn_org, latitude, longitude,
                    crawl_count, last_height_delta
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    node.ip, node.port, now, now, now,
                    node.version, node.subversion, node.services, node.user_agent, node.start_height,
                    services_listening, is_knots, is_fork, node.latency_ms,
                    node.geo.country, node.geo.country_code, node.geo.city,
                    node.geo.asn, node.geo.asn_org, node.geo.latitude, node.geo.longitude,
                    1, height_delta,
                ),
            )
            await self._conn.commit()
            return True

        # Update path - detect changes for history
        old = dict(row)  # type: ignore

        # Record interesting deltas
        changes = []
        if node.start_height is not None and old.get("start_height") != node.start_height:
            changes.append(("start_height", old.get("start_height"), node.start_height))
        if node.subversion and old.get("subversion") != node.subversion:
            changes.append(("subversion", old.get("subversion"), node.subversion))
        if node.version and old.get("version") != node.version:
            changes.append(("version", old.get("version"), node.version))

        for field, old_v, new_v in changes:
            await self._conn.execute(
                "INSERT INTO node_changes (ip, port, changed_at, field, old_value, new_value) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (node.ip, node.port, now, field, str(old_v) if old_v is not None else None, str(new_v)),
            )

        # Merge geo only if better (we don't want to lose previous good geo)
        geo_country = node.geo.country or old.get("country")
        geo_cc = node.geo.country_code or old.get("country_code")
        geo_city = node.geo.city or old.get("city")
        geo_asn = node.geo.asn or old.get("asn")
        geo_asn_org = node.geo.asn_org or old.get("asn_org")
        geo_lat = node.geo.latitude or old.get("latitude")
        geo_lon = node.geo.longitude or old.get("longitude")

        # Always bump last_seen / last_crawled / count / latency if better
        new_last_seen = now
        new_last_crawled = now
        new_crawl_count = (old.get("crawl_count") or 0) + 1
        new_latency = node.latency_ms if node.latency_ms is not None else old.get("latency_ms")

        await self._conn.execute(
            """
            UPDATE nodes SET
                last_seen = ?,
                last_crawled = ?,
                version = COALESCE(?, version),
                subversion = COALESCE(?, subversion),
                services = COALESCE(?, services),
                user_agent = COALESCE(?, user_agent),
                start_height = COALESCE(?, start_height),
                services_listening = ?,
                is_knots = ?,
                is_fork = ?,
                latency_ms = ?,
                country = ?,
                country_code = ?,
                city = ?,
                asn = ?,
                asn_org = ?,
                latitude = ?,
                longitude = ?,
                crawl_count = ?,
                last_height_delta = COALESCE(?, last_height_delta)
            WHERE ip = ? AND port = ?
            """,
            (
                new_last_seen,
                new_last_crawled,
                node.version, node.subversion, node.services, node.user_agent, node.start_height,
                services_listening, is_knots, is_fork, new_latency,
                geo_country, geo_cc, geo_city, geo_asn, geo_asn_org, geo_lat, geo_lon,
                new_crawl_count,
                height_delta,
                node.ip, node.port,
            ),
        )
        await self._conn.commit()
        return False

    async def get_node(self, ip: str, port: int) -> Optional[Node]:
        if not self._conn:
            await self.connect()
        async with self._conn.execute("SELECT * FROM nodes WHERE ip=? AND port=?", (ip, port)) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            return self._row_to_node(row)

    async def get_all_nodes(self, limit: Optional[int] = None) -> List[Node]:
        if not self._conn:
            await self.connect()
        sql = "SELECT * FROM nodes ORDER BY last_seen DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        async with self._conn.execute(sql) as cur:
            rows = await cur.fetchall()
            return [self._row_to_node(r) for r in rows]

    async def get_knots_nodes(self, limit: int = 500) -> List[Node]:
        if not self._conn:
            await self.connect()
        async with self._conn.execute(
            "SELECT * FROM nodes WHERE is_knots=1 ORDER BY start_height DESC, last_seen DESC LIMIT ?",
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
            return [self._row_to_node(r) for r in rows]

    async def get_fork_candidates(self, limit: int = 20000) -> List[tuple]:
        """Reachable nodes that could be on the chain (Knots user agent, at or
        past the activation height). Returns (ip, port) for a focused verify pass."""
        if not self._conn:
            await self.connect()
        async with self._conn.execute(
            "SELECT ip, port FROM nodes WHERE services_listening=1 AND is_knots=1 "
            "ORDER BY last_seen DESC LIMIT ?",
            (limit,),
        ) as cur:
            return [(r["ip"], r["port"]) for r in await cur.fetchall()]

    async def set_fork(self, ip: str, port: int, is_fork: bool,
                       verified_height: Optional[int] = None) -> None:
        if not self._conn:
            await self.connect()
        if verified_height is not None:
            await self._conn.execute(
                "UPDATE nodes SET is_fork = ?, verified_height = ? WHERE ip = ? AND port = ?",
                (1 if is_fork else 0, verified_height, ip, port),
            )
        else:
            await self._conn.execute(
                "UPDATE nodes SET is_fork = ? WHERE ip = ? AND port = ?",
                (1 if is_fork else 0, ip, port),
            )
        await self._conn.commit()

    async def get_listening_nodes(self, limit: int = 500) -> List[Node]:
        if not self._conn:
            await self.connect()
        async with self._conn.execute(
            "SELECT * FROM nodes WHERE services_listening=1 ORDER BY start_height DESC, last_seen DESC LIMIT ?",
            (limit,),
        ) as cur:
            rows = await cur.fetchall()
            return [self._row_to_node(r) for r in rows]

    async def get_nodes_by_country(self, country: str) -> List[Node]:
        if not self._conn:
            await self.connect()
        async with self._conn.execute(
            "SELECT * FROM nodes WHERE country = ? OR country_code = ? ORDER BY last_seen DESC",
            (country, country),
        ) as cur:
            return [self._row_to_node(r) for r in await cur.fetchall()]

    async def get_top_by_height(self, limit: int = 50) -> List[Node]:
        if not self._conn:
            await self.connect()
        async with self._conn.execute(
            "SELECT * FROM nodes WHERE start_height IS NOT NULL ORDER BY start_height DESC, last_seen DESC LIMIT ?",
            (limit,),
        ) as cur:
            return [self._row_to_node(r) for r in await cur.fetchall()]

    async def get_stats(self) -> Dict[str, Any]:
        if not self._conn:
            await self.connect()

        stats: Dict[str, Any] = {}

        async with self._conn.execute("SELECT COUNT(*) FROM nodes") as cur:
            stats["total_nodes"] = (await cur.fetchone())[0]

        async with self._conn.execute("SELECT COUNT(*) FROM nodes WHERE is_knots=1") as cur:
            stats["knots_nodes"] = (await cur.fetchone())[0]

        async with self._conn.execute("SELECT COUNT(*) FROM nodes WHERE services_listening=1") as cur:
            stats["listening_nodes"] = (await cur.fetchone())[0]

        async with self._conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE last_crawled IS NOT NULL"
        ) as cur:
            stats["crawled_at_least_once"] = (await cur.fetchone())[0]

        # Version distribution (top)
        async with self._conn.execute(
            """
            SELECT COALESCE(subversion, 'unknown'), COUNT(*) as c
            FROM nodes
            GROUP BY COALESCE(subversion, 'unknown')
            ORDER BY c DESC
            LIMIT 15
            """
        ) as cur:
            stats["version_distribution"] = [(r[0], r[1]) for r in await cur.fetchall()]

        # Geo top countries
        async with self._conn.execute(
            """
            SELECT COALESCE(country, 'Unknown'), COUNT(*) as c
            FROM nodes
            GROUP BY COALESCE(country, 'Unknown')
            ORDER BY c DESC
            LIMIT 20
            """
        ) as cur:
            stats["top_countries"] = [(r[0], r[1]) for r in await cur.fetchall()]

        # Top ASNs
        async with self._conn.execute(
            """
            SELECT COALESCE(asn_org || ' (AS' || asn || ')', 'Unknown'), COUNT(*) as c
            FROM nodes
            WHERE asn IS NOT NULL
            GROUP BY asn, asn_org
            ORDER BY c DESC
            LIMIT 15
            """
        ) as cur:
            stats["top_asns"] = [(r[0], r[1]) for r in await cur.fetchall()]

        # Best height seen
        async with self._conn.execute("SELECT MAX(start_height) FROM nodes") as cur:
            stats["max_height"] = (await cur.fetchone())[0] or 0

        # Average latency among crawled
        async with self._conn.execute(
            "SELECT AVG(latency_ms) FROM nodes WHERE latency_ms IS NOT NULL"
        ) as cur:
            avg_lat = (await cur.fetchone())[0]
            stats["avg_latency_ms"] = round(avg_lat, 1) if avg_lat else None

        return stats

    async def get_geo_stats(self) -> Dict[str, Any]:
        """Extra geo-focused stats."""
        if not self._conn:
            await self.connect()
        out: Dict[str, Any] = {}
        async with self._conn.execute(
            """
            SELECT country, country_code, COUNT(*) as count
            FROM nodes
            WHERE country IS NOT NULL
            GROUP BY country, country_code
            ORDER BY count DESC
            """
        ) as cur:
            out["by_country"] = [dict(r) for r in await cur.fetchall()]

        async with self._conn.execute(
            """
            SELECT asn, asn_org, COUNT(*) as count
            FROM nodes
            WHERE asn IS NOT NULL
            GROUP BY asn, asn_org
            ORDER BY count DESC
            LIMIT 30
            """
        ) as cur:
            out["by_asn"] = [dict(r) for r in await cur.fetchall()]
        return out

    async def create_session(self, mode: str, config: Dict[str, Any]) -> int:
        if not self._conn:
            await self.connect()
        now = utcnow()
        async with self._conn.execute(
            "INSERT INTO crawl_sessions (started_at, mode, config_json) VALUES (?, ?, ?)",
            (now, mode, json.dumps(config, default=str)),
        ) as cur:
            await self._conn.commit()
            return cur.lastrowid  # type: ignore

    async def update_session(self, session_id: int, **kwargs: Any) -> None:
        if not self._conn:
            await self.connect()
        fields = []
        values = []
        for k, v in kwargs.items():
            fields.append(f"{k}=?")
            values.append(v)
        if not fields:
            return
        values.append(session_id)
        sql = f"UPDATE crawl_sessions SET {', '.join(fields)} WHERE id=?"
        await self._conn.execute(sql, values)
        await self._conn.commit()

    async def get_recent_sessions(self, limit: int = 10) -> List[Dict[str, Any]]:
        if not self._conn:
            await self.connect()
        async with self._conn.execute(
            "SELECT * FROM crawl_sessions ORDER BY started_at DESC LIMIT ?",
            (limit,),
        ) as cur:
            return [dict(r) for r in await cur.fetchall()]

    # ------------------------- Helpers -------------------------

    @staticmethod
    def _row_to_node(row: sqlite3.Row | aiosqlite.Row) -> Node:
        geo = GeoData(
            country=row["country"],
            country_code=row["country_code"],
            city=row["city"],
            asn=row["asn"],
            asn_org=row["asn_org"],
            latitude=row["latitude"],
            longitude=row["longitude"],
        )
        return Node(
            ip=row["ip"],
            port=row["port"],
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
            last_crawled=row["last_crawled"],
            version=row["version"],
            subversion=row["subversion"],
            services=row["services"],
            user_agent=row["user_agent"],
            start_height=row["start_height"],
            services_listening=bool(row["services_listening"]),
            is_knots=bool(row["is_knots"]),
            is_fork=bool(row["is_fork"]),
            latency_ms=row["latency_ms"],
            geo=geo,
            crawl_count=row["crawl_count"] or 0,
            last_height_delta=row["last_height_delta"],
        )
