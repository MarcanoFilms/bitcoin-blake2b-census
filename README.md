# knots-network-crawler

**A serious, sovereign Bitcoin network crawler** focused on discovering the maximum number of real nodes (especially **Bitcoin Knots**) using actual P2P protocol (`version` / `getaddr` / `addr`), enriching them with **MaxMind GeoLite2**, and giving you powerful terminal analysis + exports.

Designed for a decent machine (i5 / Ryzen 5 + 16-32 GB RAM). Aggressive but **configurable and responsible**.

## Features

- Real P2P crawling (no relying on third-party APIs like Bitnodes).
- High concurrency with asyncio + semaphores (Normal vs Aggressive modes).
- Excellent GeoIP (country, city, ASN, lat/long) via official free MaxMind databases.
- Rich data model in SQLite: version, subversion, services (listening detection), height + delta, latency, first/last seen, full GeoIP, crawl history.
- Knots detection (looks for "knots" in user agent).
- Beautiful retro yellow-on-black `rich` terminal UI.
- Multiple analysis views: global stats, geo distribution, top height, Knots-only, Listening-only.
- Exports: JSON (full), CSV (analysis), Graphviz DOT (for Gephi / graph analysis of network structure).
- Resume-friendly: run it multiple times, it keeps accumulating and refreshing knowledge.
- Configurable via CLI flags + environment variables.

## Installation

```bash
cd knots-network-crawler
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# or pip install -e .
```

After install you can run:
```bash
knots-network-crawler --help
```

Or directly:
```bash
python -m knots_network_crawler crawl --help
```

## GeoIP Setup (MANDATORY for good data)

The crawler works without GeoIP (you just won't have country/ASN), but for serious analysis you want it.

1. Create a free account at MaxMind: https://www.maxmind.com/en/geolite2/signup
2. Generate a license key (Account → My License Keys).
3. Download the two databases:
   - **GeoLite2 City** (MMDB format)
   - **GeoLite2 ASN** (MMDB format)
4. Extract the `.mmdb` files and place them in `data/`:

```
data/
  GeoLite2-City.mmdb
  GeoLite2-ASN.mmdb
  nodes.db          (created automatically)
```

There is a helper script:
```bash
bash scripts/download-geoip.sh
```

Environment variables you can use:
```bash
export KNOTS_GEOIP_CITY_MMDB=/path/to/GeoLite2-City.mmdb
export KNOTS_GEOIP_ASN_MMDB=/path/to/GeoLite2-ASN.mmdb
```

## Quick Start

### 1. Do a first crawl (Normal mode - recommended)

```bash
knots-network-crawler crawl --mode normal -c 80 --max-nodes 8000
```

### 2. Aggressive crawl (more discovery power)

On a good connection + machine this can discover a lot more:

```bash
knots-network-crawler crawl --mode aggressive -c 280 --max-nodes 25000 --duration 14400
```

### 3. Just update what you already know (no new discovery)

```bash
knots-network-crawler crawl --only-known --concurrency 120
```

### 4. Analysis

```bash
# Global picture
knots-network-crawler stats

# Full geographic + hosting provider breakdown
knots-network-crawler stats --geo

# Best Knots nodes right now
knots-network-crawler view --knots --limit 80

# Most interesting reachable nodes (great for manual peering / mining)
knots-network-crawler view --listening -n 100

# Nodes claiming the highest block height
knots-network-crawler view --top
```

### 5. Export for external tools

```bash
# Full dataset
knots-network-crawler export --format json -o data/full-nodes.json

# Only Knots for analysis
knots-network-crawler export --format csv --only-knots -o knots-nodes.csv

# Graphviz for Gephi / network mapping (highly recommended)
knots-network-crawler export --format dot --only-listening -o listening-graph.dot
```

Open `listening-graph.dot` or the full export in Gephi to see ASN/country clusters, etc.

## Configuration & Environment

All important tunables can be set via flags or env vars (prefix `KNOTS_`):

| Variable                    | Description                              | Default (Normal)     |
|----------------------------|------------------------------------------|----------------------|
| `KNOTS_MODE`               | normal / aggressive                      | normal               |
| `KNOTS_MAX_CONCURRENT`     | Max simultaneous connections             | 80 (normal) / ~250 (agg) |
| `KNOTS_DB_PATH`            | SQLite file                              | data/nodes.db        |
| `KNOTS_MAX_NODES`          | Hard stop on attempts                    | 50000                |
| `KNOTS_MAX_DURATION_SEC`   | Safety time limit                        | 6 hours              |
| `KNOTS_GEOIP_*_MMDB`       | Explicit paths to mmdb files             | auto-detect in data/ |

## Architecture Notes (for the curious / sovereign operator)

- Pure asyncio P2P implementation (no heavy bitcoin libraries).
- Proper message framing + checksums. Supports `addr` + `addrv2`.
- Version handshake is honest but minimal (we don't relay or serve).
- `getaddr` is the main discovery primitive. We are greedy but bounded per peer.
- GeoIP is applied on every successful crawl (and merged intelligently - we don't overwrite good data with bad).
- DB uses WAL + good indexes. Safe for concurrent reads while crawling.
- The DOT export is intentionally node-only (no fabricated edges) because `getaddr` is a gossip view, not "current connections". This is still extremely useful when combined with ASN/country attributes.

## Ethics & Responsibility

- This tool can generate significant traffic. Use `--mode normal` by default.
- Aggressive mode is for when you have a good reason and good connectivity.
- Never point it at testnet/mainnet nodes you don't have permission for in a way that could be seen as abuse.
- Many operators run similar crawlers (Bitnodes, Luke-Jr's crawler, etc.). You are participating in the public P2P network.

## Recommended Workflow for a Sovereign Knots User / Miner

1. Run a long aggressive crawl once every few weeks (or when you want fresh data).
2. Regularly run `view --listening` + `view --knots`.
3. Export the listening nodes and feed good candidates into your own node via `addnode` or just use them as trusted `connect=` peers if you want very sovereign outbound.
4. Watch the ASN distribution — heavy concentration in a few providers is a centralization signal.
5. Track how many Knots nodes exist vs Core over time (great for political / technical analysis).

## Development / Hacking

```bash
pip install -e ".[dev]"
```

Structure is deliberately clean and modular:

```
knots_network_crawler/
  cli.py          # Typer commands + live UI
  config.py       # All tunables + mode logic
  crawler.py      # The actual async engine
  protocol.py     # Bitcoin wire messages (the serious part)
  database.py     # Rich SQLite model + history
  geoip.py        # MaxMind integration (graceful degradation)
  models.py
  views.py        # All rich rendering
  exporters.py    # JSON / CSV / DOT
```

## License

MIT. Use it to understand and strengthen the network.

---

Built with respect for the cypherpunk / sovereign mindset. Run Knots. Verify everything.
