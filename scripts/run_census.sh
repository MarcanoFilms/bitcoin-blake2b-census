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

cd "$ROOT"
mkdir -p "$ROOT/data" "$WEB"

# --- seeds ---
# Outbound peers are listening fork nodes we're actually connected to (reliable),
# plus a sample of the node's addrman for breadth. Inbound peers are skipped:
# they connected to us and usually can't be reached back at :8333.
SEEDS=$($CLI getpeerinfo 2>/dev/null \
  | "$PY" -c 'import sys,json; print(" ".join("--seed "+p["addr"].rsplit(":",1)[0] for p in json.load(sys.stdin) if p.get("addr") and not p.get("inbound")))' \
  || true)
ADDRMAN=$($CLI getnodeaddresses 400 2>/dev/null \
  | "$PY" -c 'import sys,json; a=json.load(sys.stdin); print(" ".join("--seed "+x["address"] for x in a if ":" not in x.get("address","")))' \
  || true)
SEEDS="$SEEDS $ADDRMAN"

# --- Phase 1: discovery crawl (no per-peer fork check → fast, broad) ---
# shellcheck disable=SC2086
"$PY" -m knots_network_crawler crawl \
  --mode normal --concurrency "$CONCURRENCY" --duration "$DURATION" \
  --max-nodes 20000 --no-fork-detect --yes --db "$DB" $SEEDS >/dev/null 2>&1 || true

# --- Phase 2: verify fork membership for reachable Knots candidates ---
# Dedicated version+getheaders probes, no getaddr contention → reliable.
"$PY" -m knots_network_crawler verify \
  --db "$DB" --concurrency "${CENSUS_VERIFY_CONCURRENCY:-12}" >/dev/null 2>&1 || true

# --- fork tip + passive sensor (inbound peers = non-listening candidates) ---
FORK_TIP=$($CLI getblockcount 2>/dev/null || echo 0)
SENSOR_JSON=$($CLI getpeerinfo 2>/dev/null \
  | "$PY" -c 'import sys,json; print(json.dumps([p["addr"] for p in json.load(sys.stdin) if p.get("inbound") and p.get("addr")]))' \
  || echo "[]")

# --- export data.json ---
GEN_TS=$(date +%s)
FORK_TIP="$FORK_TIP" SENSOR="$SENSOR_JSON" GEN_TS="$GEN_TS" DB="$DB" OUT="$WEB/data.json" \
"$PY" - <<'PYEOF'
import os, json
from knots_network_crawler.census import write_census
d = write_census(
    os.environ["DB"], os.environ["OUT"],
    generated_ts=int(os.environ["GEN_TS"]),
    fork_tip=int(os.environ["FORK_TIP"]) or None,
    sensor_peers=json.loads(os.environ["SENSOR"]),
)
print(f"census: {d['fork_reachable']} reachable / {d['total_estimate']} est / {d['countries_count']} countries / tip {d['fork_tip']}")
PYEOF
