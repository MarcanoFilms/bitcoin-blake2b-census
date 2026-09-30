# Oracle Knots · BLAKE2b Node Census

A **sovereign crawler and census for the Bitcoin Knots BLAKE2b fork** (XBT / BTCB2).
It discovers reachable nodes over the real P2P protocol, **verifies fork
membership by the chain a node follows — not by its user agent**, enriches
everything with GeoIP, estimates the non-listening population with a passive
sensor, and renders it all on a self-contained web page.

No third-party APIs. No external fonts or trackers. Your node, your seed, your data.

## Why chain verification (V2)

The BLAKE2b fork shares Bitcoin mainnet's network magic and port 8333, and its
nodes run **vanilla Knots binaries** — their user agent (`/Satoshi:29.4.2/Knots:…/`)
is indistinguishable from a mainnet Knots node. Filtering by user agent is therefore
wrong: it catches mainnet Knots too, and misses nothing about the actual chain.

Instead, after the version handshake the crawler sends a `getheaders` with a
**post-fork block as the locator and its child as `hash_stop`**:

- A node **on the fork chain** has that block and replies with a single header
  whose `prev_block` equals the locator → **fork member**.
- A **mainnet** node doesn't have it and falls back to genesis → not a member.

Reading `prev_block` (instead of recomputing a block id) makes this agnostic to
whether the chain's id hash is SHA256d or BLAKE2b. The single-header `hash_stop`
reply (~81 bytes vs a 160 KB, 2000-header dump) keeps detection fast and reliable
under concurrency over a VPN.

The anchor is configurable in [`config.py`](knots_network_crawler/config.py)
(`fork_anchor_hash` / `fork_stop_hash`).

## Features

- Real P2P crawl (`version` / `getaddr` / `addr` / `addrv2`), asyncio, tunable concurrency.
- **Chain-verified fork detection** as above (`is_fork`), independent of user agent.
- **GeoIP** (country / city / ASN) via the free [DB-IP lite](https://db-ip.com/db/lite.php)
  databases — no license key required.
- **Passive non-listening sensor**: merges the inbound peers your own node sees
  (`getpeerinfo`) that no crawler can reach, for a truer network-size estimate —
  the same gap Luke Dashjr's counts close.
- Service-bit composition (full / pruned / witness / compact filters / v2 transport).
- Self-contained census page in the Oracle Knots look (`web/index.html`).
- SQLite store that accumulates and refreshes across runs.

## Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### GeoIP databases (optional but recommended)

```bash
cd data
M=$(date +%Y-%m)
curl -fsSL "https://download.db-ip.com/free/dbip-city-lite-$M.mmdb.gz" | gunzip > GeoLite2-City.mmdb
curl -fsSL "https://download.db-ip.com/free/dbip-asn-lite-$M.mmdb.gz"  | gunzip > GeoLite2-ASN.mmdb
```

## Run

One-shot census (crawl → passive sensor → `web/data.json`), seeded from your own node:

```bash
BITCOIN_DATADIR=/path/to/datadir CENSUS_DURATION=600 ./scripts/run_census.sh
```

Serve the page (any static server):

```bash
python -m http.server 8891 --directory web
```

Or run the crawler directly:

```bash
python -m knots_network_crawler crawl --mode normal --concurrency 10 --duration 600 --yes
python -m knots_network_crawler stats
python -m knots_network_crawler export --format json -o out.json --only-listening
```

### Scheduled census (systemd --user)

`census.service` + `census.timer` run the census every 2 hours; the persistent
DB accumulates fork-node coverage across passes.

## Notes

- Figures are for **reachable** (listening) fork nodes — a lower bound — plus a
  non-listening estimate from the passive sensor.
- Coverage of the fork set builds up over successive passes, since the fork lives
  inside the larger shared P2P network and a single pass samples it.

---

Made with 🦉 by [MarcanoFilms](https://marcanotrades.com) · Oracle Knots
