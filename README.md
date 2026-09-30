# Oracle Knots · BLAKE2b Node Census

A **sovereign crawler and census for the Bitcoin-Blake2b network** (XBT / BTCB2).
It discovers reachable nodes over the real P2P protocol, **verifies network
membership by the chain a node follows — not by its user agent**, enriches
everything with GeoIP, estimates the non-listening population with a passive
sensor, and renders it all on a self-contained web page.

No third-party APIs. No external fonts or trackers. Your node, your seed, your data.

## Why chain verification (V2)

The Bitcoin-Blake2b network shares Bitcoin mainnet's network magic and port 8333,
and its nodes run **vanilla Knots binaries** — their user agent
(`/Satoshi:29.4.2/Knots:…/`) is indistinguishable from a mainnet Knots node.
Filtering by user agent is therefore wrong: it catches mainnet Knots too, and
tells you nothing about the actual chain.

Instead, after the version handshake the crawler sends a `getheaders` with the
**activation block as the locator and its child as `hash_stop`**:

- A node **on the BLAKE2b chain** has that block and replies with a single header
  whose `prev_block` equals the locator → **member**.
- A **mainnet** node doesn't have it and falls back to genesis → not a member.

The anchor is the BLAKE2b **activation block** — the one whose coinbase carries
the canonical headline. It defines the chain, can never reorg out, and never
needs updating. Reading `prev_block` (instead of recomputing a block id) makes
this agnostic to whether the chain's id hash is SHA256d or BLAKE2b. The
single-header `hash_stop` reply (~81 bytes vs a 160 KB, 2000-header dump) keeps
detection fast and reliable under concurrency over a VPN.

The anchor is configurable in [`config.py`](knots_network_crawler/config.py).

## Features

- Real P2P crawl (`version` / `getaddr` / `addr` / `addrv2`), asyncio, tunable concurrency.
- **Chain-verified BLAKE2b membership** as above, independent of user agent.
- **Two-phase census**: a fast discovery crawl, then a focused verify pass that
  re-checks candidates with dedicated `getheaders` probes (no `getaddr`
  contention), so detection stays reliable over a limited uplink.
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

One-shot census (discover → verify → passive sensor → `web/data.json`), seeded
from your own node:

```bash
BITCOIN_DATADIR=/path/to/datadir CENSUS_DURATION=600 ./scripts/run_census.sh
```

Serve the page (any static server):

```bash
python -m http.server 8891 --directory web
```

Or run the crawler directly:

```bash
# phase 1: fast discovery (no per-peer chain check)
python -m knots_network_crawler crawl --mode normal --concurrency 10 --duration 600 --no-fork-detect --yes
# phase 2: verify BLAKE2b membership for reachable Knots candidates
python -m knots_network_crawler verify --concurrency 12
python -m knots_network_crawler stats
```

### Scheduled census (systemd --user)

`census.service` + `census.timer` run the census hourly; the persistent DB
accumulates node coverage across passes. `data.json` carries `interval_seconds`
so the page's "next update" countdown matches the cadence.

## Notes

- Figures are for **reachable** (listening) nodes — a lower bound — plus a
  non-listening estimate from the passive sensor.
- Coverage builds up over successive passes, since the Bitcoin-Blake2b network
  lives inside the larger shared P2P network and a single pass samples it.

---

Made with 🦉 by [MarcanoFilms](https://x.com/MarcanoFilms) · Oracle Knots
