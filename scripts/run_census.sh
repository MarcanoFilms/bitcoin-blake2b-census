#!/usr/bin/env bash
# BLAKE2b fork node census: crawl -> passive sensor -> export data.json.
# Seeds from our own node's current peers (guaranteed fork nodes), so it does
# not depend on any third-party seed. Meant to run on a timer.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
DB="$ROOT/data/census.db"
WEB="$ROOT/web"
DATADIR="${BITCOIN_DATADIR:-/mnt/t7/asus-fullnode/bitcoin}"
CLI="${BITCOIN_CLI:-bitcoin-cli} -datadir=$DATADIR"
DURATION="${CENSUS_DURATION:-600}"
CONCURRENCY="${CENSUS_CONCURRENCY:-10}"
# Should match the systemd timer period so the page's "next update" countdown is accurate.
INTERVAL="${CENSUS_INTERVAL:-3600}"

cd "$ROOT"
mkdir -p "$ROOT/data" "$WEB"

# --- seeds (IPv4 only) ---
# Outbound peers are listening nodes we're actually connected to (reliable), plus
# a sample of the node's addrman for breadth. Inbound peers are skipped (they
# can't be reached back at :8333). We keep only IPv4 — the crawler has no Tor/I2P
# transport, so .onion/.i2p (and IPv6) seeds just burn the run's time on timeouts.
IPV4_FILTER='import sys,json,re; f=re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")'
SEEDS=$($CLI getpeerinfo 2>/dev/null \
  | "$PY" -c "$IPV4_FILTER"$'\nips=[p["addr"].rsplit(":",1)[0] for p in json.load(sys.stdin) if p.get("addr") and not p.get("inbound")]\nprint(" ".join("--seed "+ip for ip in ips if f.match(ip)))' \
  || true)
ADDRMAN=$($CLI getnodeaddresses 800 2>/dev/null \
  | "$PY" -c "$IPV4_FILTER"$'\nips=[x.get("address","") for x in json.load(sys.stdin)]\nprint(" ".join("--seed "+ip for ip in ips if f.match(ip)))' \
  || true)
SEEDS="$SEEDS $ADDRMAN"

# --- Phase 1: discovery crawl (no per-peer fork check → fast, broad) ---
# Hard wall-clock cap with `timeout`: the crawl's internal --duration doesn't
# always wind down cleanly under a slow uplink, so we bound it externally
# (SIGTERM at the cap, SIGKILL 20s later) to guarantee the run fits its budget.
# shellcheck disable=SC2086
timeout -k 20 "$((DURATION + 90))" "$PY" -m knots_network_crawler crawl \
  --mode normal --concurrency "$CONCURRENCY" --duration "$DURATION" \
  --max-nodes 20000 --no-fork-detect --yes --db "$DB" $SEEDS >/dev/null 2>&1 || true

# --- Phase 2: verify BLAKE2b membership for reachable Knots candidates ---
# Dedicated version+getheaders probes, no getaddr contention → reliable.
timeout -k 20 "${CENSUS_VERIFY_TIMEOUT:-600}" "$PY" -m knots_network_crawler verify \
  --db "$DB" --concurrency "${CENSUS_VERIFY_CONCURRENCY:-12}" >/dev/null 2>&1 || true

# --- fork tip + passive sensor (inbound peers = non-listening candidates) ---
FORK_TIP=$($CLI getblockcount 2>/dev/null || echo 0)
SENSOR_JSON=$($CLI getpeerinfo 2>/dev/null \
  | "$PY" -c 'import sys,json; print(json.dumps([p["addr"] for p in json.load(sys.stdin) if p.get("inbound") and p.get("addr")]))' \
  || echo "[]")

# --- export data.json ---
GEN_TS=$(date +%s)
FORK_TIP="$FORK_TIP" SENSOR="$SENSOR_JSON" GEN_TS="$GEN_TS" DB="$DB" OUT="$WEB/data.json" INTERVAL="$INTERVAL" \
"$PY" - <<'PYEOF'
import os, json
from knots_network_crawler.census import write_census
d = write_census(
    os.environ["DB"], os.environ["OUT"],
    generated_ts=int(os.environ["GEN_TS"]),
    fork_tip=int(os.environ["FORK_TIP"]) or None,
    sensor_peers=json.loads(os.environ["SENSOR"]),
    interval_seconds=int(os.environ["INTERVAL"]),
)
print(f"census: {d['fork_reachable']} reachable / {d['total_estimate']} est / {d['countries_count']} countries / tip {d['fork_tip']}")
PYEOF
