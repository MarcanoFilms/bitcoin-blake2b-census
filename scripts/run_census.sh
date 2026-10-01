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

# --- seeds ---
# Outbound peers (reliable, we're connected to them) + a sample of the addrman.
# We now reach .onion/.i2p via local SOCKS proxies, so seed those too — but cap
# per type so slow Tor/I2P connects don't dominate the pass (IPv4 bootstraps fast;
# the rest is discovered via addr gossip and crawled through SOCKS).
SEEDS=$($CLI getpeerinfo 2>/dev/null \
  | "$PY" -c $'import sys,json,re\nf=re.compile(r"^\\d{1,3}(\\.\\d{1,3}){3}$")\nips=[p["addr"].rsplit(":",1)[0] for p in json.load(sys.stdin) if p.get("addr") and not p.get("inbound")]\nprint(" ".join("--seed "+ip for ip in ips if f.match(ip)))' \
  || true)
ADDRMAN=$($CLI getnodeaddresses 0 2>/dev/null \
  | "$PY" -c $'import sys,json,re\nf=re.compile(r"^\\d{1,3}(\\.\\d{1,3}){3}$")\na=json.load(sys.stdin)\nv4=[x["address"] for x in a if f.match(x.get("address",""))][:800]\ntor=[x["address"] for x in a if x.get("address","").endswith(".onion")][:150]\ni2p=[x["address"] for x in a if x.get("address","").endswith(".i2p")][:50]\nprint(" ".join("--seed "+s for s in v4+tor+i2p))' \
  || true)
# Extra known-good nodes contributed by the community (host:port honored, so
# nodes on non-standard ports are reached too).
EXTRA_SEEDS="--seed nodoblake2b.airdns.org:11010"
SEEDS="$SEEDS $ADDRMAN $EXTRA_SEEDS"

# --- Phase 1: discovery crawl (no per-peer fork check → fast, broad) ---
# Hard wall-clock cap with `timeout`: the crawl's internal --duration doesn't
# always wind down cleanly under a slow uplink, so we bound it externally
# (SIGTERM at the cap, SIGKILL 20s later) to guarantee the run fits its budget.
# shellcheck disable=SC2086
timeout -k 20 "$((DURATION + 90))" "$PY" -m knots_network_crawler crawl \
  --mode normal --concurrency "$CONCURRENCY" --duration "$DURATION" \
  --max-nodes 20000 --no-fork-detect --yes --db "$DB" $SEEDS >/dev/null 2>&1 || true

# --- Height reference (sampled ONCE so every node is measured against it) ---
# Our tip height/hash/raw-header + getheaders locators [tip-10,-100,-1000,anchor].
export CENSUS_TIP_HEIGHT=$($CLI getblockcount 2>/dev/null || echo 0)
if [ "$CENSUS_TIP_HEIGHT" -gt 0 ] 2>/dev/null; then
  CENSUS_TIP_HASH=$($CLI getblockhash "$CENSUS_TIP_HEIGHT" 2>/dev/null)
  export CENSUS_TIP_HASH
  export CENSUS_TIP_HEADER=$($CLI getblockheader "$CENSUS_TIP_HASH" false 2>/dev/null)
  ANCHOR_H=961640
  ANCHOR_HASH=$($CLI getblockhash "$ANCHOR_H" 2>/dev/null)
  LOC_JSON="[[\"$($CLI getblockhash $((CENSUS_TIP_HEIGHT-10)) 2>/dev/null)\",$((CENSUS_TIP_HEIGHT-10))]"
  LOC_JSON="$LOC_JSON,[\"$($CLI getblockhash $((CENSUS_TIP_HEIGHT-100)) 2>/dev/null)\",$((CENSUS_TIP_HEIGHT-100))]"
  LOC_JSON="$LOC_JSON,[\"$($CLI getblockhash $((CENSUS_TIP_HEIGHT-1000)) 2>/dev/null)\",$((CENSUS_TIP_HEIGHT-1000))]"
  LOC_JSON="$LOC_JSON,[\"$ANCHOR_HASH\",$ANCHOR_H]]"
  export CENSUS_LOCATORS="$LOC_JSON"
fi

# --- Phase 2: verify membership + chain-verified height for reachable candidates ---
# Dedicated version+getheaders probes, no getaddr contention → reliable.
timeout -k 20 "${CENSUS_VERIFY_TIMEOUT:-600}" "$PY" -m knots_network_crawler verify \
  --db "$DB" --concurrency "${CENSUS_VERIFY_CONCURRENCY:-12}" >/dev/null 2>&1 || true

# --- fork tip + passive sensor (non-listening candidates) ---
# Only count inbound peers that are plausibly BLAKE2b: the shared port 8333 means
# our node also gets mainnet Core, wallets, and network scanners (dsn.*, Metrika-
# Bitnodes, bitcoinj…) plus local services on 127.0.0.1. Filter to Knots subver +
# a height at/after activation + a public address so the estimate isn't inflated.
FORK_TIP=$($CLI getblockcount 2>/dev/null || echo 0)
SENSOR_JSON=$($CLI getpeerinfo 2>/dev/null \
  | "$PY" -c $'import sys,json,ipaddress\ndef pub(ip):\n try:\n  a=ipaddress.ip_address(ip); return not (a.is_private or a.is_loopback or a.is_link_local)\n except Exception:\n  return ip.endswith(".onion") or ip.endswith(".i2p")\nout=[]\nfor x in json.load(sys.stdin):\n if not x.get("inbound"): continue\n if "knots" not in (x.get("subver","") or "").lower(): continue\n if (x.get("startingheight") or 0) < 961640: continue\n a=x.get("addr","")\n ip=a.rsplit(":",1)[0].strip("[]") if a.count(":")==1 else a.strip("[]")\n if pub(ip): out.append(a)\nprint(json.dumps(out))' \
  || echo "[]")

# --- export data.json + update registry + prune old DB rows ---
GEN_TS=$(date +%s)
FORK_TIP="$FORK_TIP" SENSOR="$SENSOR_JSON" GEN_TS="$GEN_TS" DB="$DB" OUT="$WEB/data.json" \
INTERVAL="$INTERVAL" WEB="$WEB" REG_KEEP_DAYS="${CENSUS_KEEP_DAYS:-90}" \
"$PY" - <<'PYEOF'
import os, json
from datetime import datetime, timezone
from knots_network_crawler.census import write_census
from knots_network_crawler.registry import update_registry, prune_db
gen = int(os.environ["GEN_TS"])
d = write_census(
    os.environ["DB"], os.environ["OUT"],
    generated_ts=gen,
    fork_tip=int(os.environ["FORK_TIP"]) or None,
    sensor_peers=json.loads(os.environ["SENSOR"]),
    interval_seconds=int(os.environ["INTERVAL"]),
)
now_iso = datetime.fromtimestamp(gen, timezone.utc).isoformat()
reg = update_registry(os.path.join(os.environ["WEB"], "registry.csv"), d["nodes"], now_iso)
pruned = prune_db(os.environ["DB"], gen, keep_days=int(os.environ["REG_KEEP_DAYS"]))
print(f"census: {d['fork_reachable']} reachable / {d['total_estimate']} est / {d['countries_count']} countries / tip {d['fork_tip']}")
print(f"registry: {reg['total']} unique nodes ({reg['new']} new) | pruned {pruned} stale DB rows")
PYEOF

# --- keep our node peered with stable, active BLAKE2b nodes from the registry ---
if [ "${CENSUS_ADDNODE:-1}" = "1" ]; then
  "$ROOT/scripts/addpeers.sh" || true
fi
