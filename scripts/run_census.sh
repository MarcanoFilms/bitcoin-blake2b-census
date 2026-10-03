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
# Discovery is now light: the bit-28 fast path + registry re-seeding mean we no
# longer need to sweep 20k nodes per pass. A smaller cap keeps getaddr traffic off
# the node's uplink; coverage still accumulates via the persistent registry.
MAX_NODES="${CENSUS_MAX_NODES:-3000}"
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
  | "$PY" -c $'import sys,json\npeers=json.load(sys.stdin)\n# keep the FULL addr (host:port, [v6]:port, onion/i2p) of every outbound peer:\n# these are live, chain-verified fork nodes. Stripping the port mis-seeded nodes\n# on non-standard ports as :8333; keeping it reaches them, and keeping onion/i2p\n# seeds the darknet lane from guaranteed-good nodes.\nprint(" ".join("--seed "+p["addr"] for p in peers if p.get("addr") and not p.get("inbound")))' \
  || true)
ADDRMAN=$($CLI getnodeaddresses 0 2>/dev/null \
  | "$PY" -c $'import sys,json\na=json.load(sys.stdin)\ndef s(x):\n host=x.get("address","");port=x.get("port",8333);net=x.get("network","")\n if not host: return None\n return "[%s]:%d"%(host,port) if net=="ipv6" else "%s:%d"%(host,port)\n# preserve the real port (addrman knows it) and raise the darknet caps now that the\n# Tor/I2P lane actually completes (per-network timeouts). Clearnet still dominates.\nv4=[s(x) for x in a if x.get("network")=="ipv4"][:800]\ntor=[s(x) for x in a if str(x.get("address","")).endswith(".onion")][:40]\ni2p=[s(x) for x in a if str(x.get("address","")).endswith(".i2p")][:25]\nprint(" ".join("--seed "+z for z in v4+tor+i2p if z))' \
  || true)
# Extra known-good nodes contributed by the community (host:port honored, so
# nodes on non-standard ports and Tor are reached too). Curated list from Kilombino.
EXTRA_SEEDS="--seed 179.27.118.130:8343 --seed nodoblake2b.airdns.org:11010 \
--seed 173.24.24.140:8333 --seed 47.189.218.206:8435 --seed 172.235.154.147:8333 \
--seed 64.181.91.48:8333 --seed 84.106.7.182:8333 --seed 23.114.198.28:9333 \
--seed 94.59.27.202:9333 --seed 64.68.204.49:8333 --seed 84.213.189.64:9333 \
--seed 82.67.102.15:8333 \
--seed w2okrqbcuvkqg75aa6lodlso2rfoxe3c7arlp5kznawxi7wev26skead.onion:8333"
# Registry-first: always re-seed every BLAKE2b node we've confirmed recently, so a
# light discovery crawl still re-verifies the known set instead of re-finding it.
# This is what lets us shrink --max-nodes without losing the known fork nodes.
REG_SEEDS=$("$PY" - <<'PY' 2>/dev/null || true
import csv, os
from datetime import datetime, timezone
reg = os.path.join(os.path.dirname(os.environ.get("WEB","web")) or ".", "registry.csv") if False else "web/registry.csv"
now = datetime.now(timezone.utc).timestamp()
out = []
try:
    for r in csv.DictReader(open(reg)):
        try:
            if now - datetime.fromisoformat(r["last_seen"]).timestamp() > 7*86400:
                continue
        except Exception:
            continue
        a = r.get("address",""); p = r.get("port","8333")
        if a:
            out.append(f"--seed [{a}]:{p}" if ":" in a and not a.endswith((".onion",".i2p")) else f"--seed {a}:{p}")
except FileNotFoundError:
    pass
print(" ".join(out))
PY
)
SEEDS="$SEEDS $ADDRMAN $EXTRA_SEEDS $REG_SEEDS"

# --- Phase 1: discovery crawl (no per-peer fork check → fast, broad) ---
# Hard wall-clock cap with `timeout`: the crawl's internal --duration doesn't
# always wind down cleanly under a slow uplink, so we bound it externally
# (SIGTERM at the cap, SIGKILL 20s later) to guarantee the run fits its budget.
# shellcheck disable=SC2086
timeout -k 20 "$((DURATION + 90))" "$PY" -m knots_network_crawler crawl \
  --mode normal --concurrency "$CONCURRENCY" --duration "$DURATION" \
  --max-nodes "$MAX_NODES" --no-fork-detect --yes --db "$DB" $SEEDS >/dev/null 2>&1 || true

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
# Use the SAME tip sampled at the start of the pass (CENSUS_TIP_HEIGHT) as the
# dashboard reference, NOT a fresh getblockcount. Node heights were measured
# against that sampled tip (their reply is capped at it), so a node that is caught
# up reports exactly it. Re-sampling here would advance the bar by however many
# blocks arrived during the ~30-min pass, making every caught-up node look
# "behind" purely from clock drift. Fall back to a fresh count only if unset.
FORK_TIP="${CENSUS_TIP_HEIGHT:-$($CLI getblockcount 2>/dev/null || echo 0)}"
SENSOR_JSON=$($CLI getpeerinfo 2>/dev/null \
  | "$PY" -c $'import sys,json,ipaddress\ndef pub(ip):\n try:\n  a=ipaddress.ip_address(ip); return not (a.is_private or a.is_loopback or a.is_link_local)\n except Exception:\n  return ip.endswith(".onion") or ip.endswith(".i2p")\nout=[]\nfor x in json.load(sys.stdin):\n if not x.get("inbound"): continue\n if "knots" not in (x.get("subver","") or "").lower(): continue\n if (x.get("startingheight") or 0) < 961640: continue\n a=x.get("addr","")\n ip=a.rsplit(":",1)[0].strip("[]") if a.count(":")==1 else a.strip("[]")\n if pub(ip): out.append(a)\nprint(json.dumps(out))' \
  || echo "[]")

# --- export data.json + update registry + prune old DB rows ---
GEN_TS=$(date +%s)
FORK_TIP="$FORK_TIP" SENSOR="$SENSOR_JSON" GEN_TS="$GEN_TS" DB="$DB" OUT="$WEB/data.json" \
INTERVAL="$INTERVAL" WEB="$WEB" REG_KEEP_DAYS="${CENSUS_KEEP_DAYS:-90}" \
ACTIVE_HOURS="${CENSUS_ACTIVE_HOURS:-48}" "$PY" - <<'PYEOF'
import os, json
from datetime import datetime, timezone
from knots_network_crawler.census import db_reachable_nodes, write_census
from knots_network_crawler.registry import update_registry, load_active_nodes, registry_count, prune_db, write_seeds
gen = int(os.environ["GEN_TS"])
reg_csv = os.path.join(os.environ["WEB"], "registry.csv")
now_iso = datetime.fromtimestamp(gen, timezone.utc).isoformat()

# 1) this pass's reachable fork nodes (from the DB) -> merge into the registry
pass_nodes = db_reachable_nodes(os.environ["DB"])
reg = update_registry(reg_csv, pass_nodes, now_iso)

# 2) build the dashboard from the PERSISTENT registry's active set (not just this pass)
active = load_active_nodes(reg_csv, gen, active_hours=int(os.environ["ACTIVE_HOURS"]))
d = write_census(
    active, os.environ["OUT"], generated_ts=gen,
    fork_tip=int(os.environ["FORK_TIP"]) or None,
    sensor_peers=json.loads(os.environ["SENSOR"]),
    interval_seconds=int(os.environ["INTERVAL"]),
    all_time=registry_count(reg_csv),
)
pruned = prune_db(os.environ["DB"], gen, keep_days=int(os.environ["REG_KEEP_DAYS"]))

# 3) publish the HTTP seed list (no-VPS equivalent of a DNS seed) from the registry
seeds = write_seeds(reg_csv, os.environ["WEB"], gen)
print(f"pass: {len(pass_nodes)} verified this run | dashboard: {d['fork_reachable']} active / {d['countries_count']} countries / tip {d['fork_tip']}")
print(f"registry: {reg['total']} unique ({reg['new']} new) | pruned {pruned} DB rows")
print(f"seeds: {seeds['stable']} stable / {seeds['all']} total published")
PYEOF

# --- keep our node peered with stable, active BLAKE2b nodes from the registry ---
if [ "${CENSUS_ADDNODE:-1}" = "1" ]; then
  "$ROOT/scripts/addpeers.sh" || true
fi
