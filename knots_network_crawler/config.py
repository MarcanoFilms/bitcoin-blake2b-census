"""
Configuration handling for knots-network-crawler.

Priorities (highest wins):
  1. Explicit CLI flags
  2. Environment variables (KNOTS_*)
  3. .env file (loaded via python-dotenv)
  4. Sensible defaults for a "decent machine" (i5 + 16-32GB)

Two main operating modes:
  - normal: polite but effective (good default for long runs)
  - aggressive: higher concurrency, lower delays (use with care, monitor your resources + network)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

# Load .env early if present
load_dotenv()


@dataclass
class CrawlerConfig:
    # Database
    db_path: Path = Path("data/nodes.db")

    # GeoIP paths (None = auto-detect)
    geoip_city_mmdb: Optional[Path] = None
    geoip_asn_mmdb: Optional[Path] = None

    # Crawling behavior
    mode: str = "normal"                    # "normal" or "aggressive"
    max_concurrent: int = 80
    connect_timeout: float = 12.0
    read_timeout: float = 25.0
    crawl_timeout_per_peer: float = 55.0    # total time budget per connection
    delay_between_waves: float = 1.2        # seconds between batches of getaddr results processing
    min_peer_delay: float = 0.8             # minimum spacing when scheduling new work

    # Crawl limits
    max_nodes_to_crawl: int = 50_000
    max_crawl_duration_seconds: int = 3600 * 6   # 6 hours default safety
    max_addrs_per_peer: int = 2000               # how many addrs we accept from one getaddr
    re_crawl_after_hours: int = 18               # only re-crawl nodes older than this

    # Bootstrap
    seeds: List[str] = field(default_factory=lambda: [
        # DNS seeds (will be resolved)
        "seed.bitcoin.sipa.be",
        "dnsseed.bluematt.me",
        "dnsseed.bitcoin.dashjr.org",
        "seed.bitcoinstats.com",
        "seed.bitcoin.jonasschnelli.ch",
        "seed.btc.petertodd.org",
        "seed.bitcoin.sprovoost.nl",
        "dnsseed.emzy.de",
    ])
    bootstrap_ips: List[str] = field(default_factory=lambda: [
        # Hardcoded recent reliable nodes (mainnet) - updated occasionally
        "8.8.8.8:8333",   # placeholder, real list populated at runtime from DNS
    ])

    # BLAKE2b chain detection (V2, chain-verified).
    # We ask each peer for headers with a post-activation block as locator; only a node
    # on the BLAKE2b chain has it and returns the next header building on it. The
    # locator is block height fork_anchor_height, and a BLAKE2b node's reply has a
    # first header whose prev_block == fork_anchor_hash (display/big-endian hex).
    # The anchor is the BLAKE2b ACTIVATION block itself (the one whose coinbase
    # carries the canonical headline). It is the block that *defines* the chain:
    # it can never reorg out and the anchor never needs updating. fork_anchor_hash
    # is the locator; fork_stop_hash is its child, used as getheaders hash_stop so
    # a BLAKE2b peer replies with a SINGLE header (whose prev_block == the locator)
    # instead of a 2000-header/160 KB dump. A mainnet node lacks this chain-only
    # block and falls back to genesis.
    fork_detect: bool = True
    fork_anchor_height: int = 961640
    fork_anchor_hash: str = "0000000000000050c1e5f69672f459293be14f46e5a494e7a8c8541396f18eeb"
    fork_stop_hash: str = "0000000000000010ef13157db08c138ea82aa1ac0ec360bdb9f101ce3ed7f7b6"
    # The BLAKE2b PoW extends the block header beyond Bitcoin's 80 bytes; each
    # `headers` entry is this many header bytes + a tx_count varint (usually 1).
    # Needed to walk a headers reply for chain-verified height. (Bitcoin = 80.)
    fork_header_size: int = 164
    # Genesis-style headline in the activation block's coinbase — the chain's
    # consensus signature. Shown on the census page; implicitly enforced by every
    # node the anchor check confirms (they all accepted this exact block).
    fork_headline: str = "8-30 NYPost Deride And Conquer"

    # Output / behavior
    user_agent: str = "/knots-crawler:0.1.0/"
    our_services: int = 0   # we don't serve, 0 is fine
    only_update_known: bool = False   # if True, do not discover new nodes (only refresh)
    verbose: bool = False

    # Derived
    @property
    def is_aggressive(self) -> bool:
        return self.mode.lower() == "aggressive"

    def apply_mode_defaults(self) -> None:
        """Adjust parameters according to selected mode."""
        if self.is_aggressive:
            self.max_concurrent = max(self.max_concurrent, 220)
            self.delay_between_waves = min(self.delay_between_waves, 0.25)
            self.min_peer_delay = min(self.min_peer_delay, 0.05)
            self.connect_timeout = min(self.connect_timeout, 8.0)
            self.crawl_timeout_per_peer = min(self.crawl_timeout_per_peer, 35.0)
            self.max_addrs_per_peer = max(self.max_addrs_per_peer, 3000)
        else:
            # Normal: conservative but still powerful
            self.max_concurrent = max(40, min(self.max_concurrent, 120))
            self.delay_between_waves = max(self.delay_between_waves, 0.9)
            self.min_peer_delay = max(self.min_peer_delay, 0.4)


def load_config(
    *,
    db_path: Optional[str] = None,
    mode: Optional[str] = None,
    max_concurrent: Optional[int] = None,
    max_nodes: Optional[int] = None,
    duration: Optional[int] = None,
    geoip_city: Optional[str] = None,
    geoip_asn: Optional[str] = None,
    only_update_known: Optional[bool] = None,
    verbose: bool = False,
    extra_seeds: Optional[List[str]] = None,
) -> CrawlerConfig:
    """
    Build final config from CLI + env + defaults.
    """
    cfg = CrawlerConfig()

    # Env overrides (if not passed explicitly)
    env_mode = os.getenv("KNOTS_MODE", "").lower()
    if env_mode in ("normal", "aggressive"):
        cfg.mode = env_mode

    env_conc = os.getenv("KNOTS_MAX_CONCURRENT")
    if env_conc:
        try:
            cfg.max_concurrent = int(env_conc)
        except ValueError:
            pass

    env_db = os.getenv("KNOTS_DB_PATH")
    if env_db:
        cfg.db_path = Path(env_db)

    env_geo_city = os.getenv("KNOTS_GEOIP_CITY_MMDB")
    env_geo_asn = os.getenv("KNOTS_GEOIP_ASN_MMDB")
    if env_geo_city:
        cfg.geoip_city_mmdb = Path(env_geo_city)
    if env_geo_asn:
        cfg.geoip_asn_mmdb = Path(env_geo_asn)

    env_max_nodes = os.getenv("KNOTS_MAX_NODES")
    if env_max_nodes:
        try:
            cfg.max_nodes_to_crawl = int(env_max_nodes)
        except ValueError:
            pass

    env_duration = os.getenv("KNOTS_MAX_DURATION_SEC")
    if env_duration:
        try:
            cfg.max_crawl_duration_seconds = int(env_duration)
        except ValueError:
            pass

    # CLI wins
    if mode:
        cfg.mode = mode
    if max_concurrent is not None:
        cfg.max_concurrent = max_concurrent
    if db_path:
        cfg.db_path = Path(db_path)
    if max_nodes is not None:
        cfg.max_nodes_to_crawl = max_nodes
    if duration is not None:
        cfg.max_crawl_duration_seconds = duration
    if geoip_city:
        cfg.geoip_city_mmdb = Path(geoip_city)
    if geoip_asn:
        cfg.geoip_asn_mmdb = Path(geoip_asn)
    if only_update_known is not None:
        cfg.only_update_known = only_update_known
    cfg.verbose = verbose

    if extra_seeds:
        # prepend custom seeds
        cfg.seeds = list(extra_seeds) + cfg.seeds

    # Apply mode tuning AFTER all overrides
    cfg.apply_mode_defaults()

    # Ensure data dir
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)

    return cfg


def get_default_bootstrap_nodes() -> List[tuple[str, int]]:
    """
    Return a list of (ip, port) that are known-good for initial bootstrap.
    In practice we resolve DNS seeds at startup.
    """
    # These are stable public nodes often seen in the network.
    # We will also resolve the DNS seeds dynamically.
    hardcoded = [
        ("198.199.109.59", 8333),
        ("138.68.15.194", 8333),
        ("159.65.235.180", 8333),
        ("178.128.48.177", 8333),
        ("167.99.0.48", 8333),
        ("134.122.28.169", 8333),
    ]
    return hardcoded
