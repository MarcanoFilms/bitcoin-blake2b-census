"""
Core data models for nodes and crawl sessions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class GeoData:
    """GeoIP enrichment for a node."""
    country: Optional[str] = None
    country_code: Optional[str] = None
    city: Optional[str] = None
    asn: Optional[int] = None
    asn_org: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    def is_complete(self) -> bool:
        return bool(self.country or self.asn)


@dataclass
class Node:
    """
    Represents a Bitcoin network node with rich metadata.
    All timestamps are UTC ISO strings or datetime.
    """
    ip: str
    port: int = 8333

    # Protocol / version info
    version: Optional[int] = None
    subversion: Optional[str] = None
    services: Optional[int] = None
    user_agent: Optional[str] = None
    start_height: Optional[int] = None

    # Reachability
    services_listening: bool = False   # NODE_NETWORK (bit 0) set
    is_knots: bool = False             # detected from subversion
    is_fork: bool = False              # chain-verified BLAKE2b fork member (getheaders anchor)

    # Timing
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    last_crawled: Optional[str] = None

    # Performance
    latency_ms: Optional[float] = None

    # Geo
    geo: GeoData = field(default_factory=GeoData)

    # Crawl metadata
    crawl_count: int = 0
    last_height_delta: Optional[int] = None   # relative to global best known at time of crawl

    def key(self) -> str:
        return f"{self.ip}:{self.port}"

    def display_user_agent(self) -> str:
        if self.subversion:
            return self.subversion
        if self.user_agent:
            return self.user_agent
        return "?"

    @property
    def is_reachable(self) -> bool:
        """We successfully handshaked with it at least once."""
        return self.last_crawled is not None


@dataclass
class CrawlSession:
    """Metadata about a crawl run."""
    started_at: str
    ended_at: Optional[str] = None
    mode: str = "normal"
    nodes_discovered: int = 0
    nodes_crawled: int = 0
    knots_found: int = 0
    listening_found: int = 0
    max_height_seen: int = 0
    config_snapshot: dict = field(default_factory=dict)
