#!/usr/bin/env bash
# Keep our node peered with the stable, active BLAKE2b nodes from the registry
# (chain-verified fork nodes only — never the scanners/mainnet clients that just
# connect to burn bandwidth). Grows the peer set over time as the registry grows.
#
# Uses RPC `addnode <addr> add` (persistent manual peer the node keeps reconnecting).
# Not saved to disk, but this script re-applies every run, so it survives restarts
# within one cycle without touching the node's config.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
REG="$ROOT/web/registry.csv"
DATADIR="${BITCOIN_DATADIR:-/mnt/t7/asus-fullnode/bitcoin}"
CLI="bitcoin-cli -datadir=$DATADIR"
MIN_SEEN="${ADDPEERS_MIN_SEEN:-2}"       # seen in >= N passes => stable
MAX_AGE_DAYS="${ADDPEERS_MAX_AGE_DAYS:-7}"  # active within N days
MAX_PEERS="${ADDPEERS_MAX:-40}"          # cap: enough real BLAKE2b peers without
                                          # 150 persistent conns riding the uplink 24/7

[ -f "$REG" ] || { echo "addpeers: no registry yet"; exit 0; }

PEERS=$(ADDPEERS_MIN_SEEN="$MIN_SEEN" ADDPEERS_MAX_AGE_DAYS="$MAX_AGE_DAYS" \
        ADDPEERS_MAX="$MAX_PEERS" REG="$REG" "$PY" - <<'PY'
import os, csv, ipaddress
from datetime import datetime, timezone
reg = os.environ["REG"]; minseen = int(os.environ["ADDPEERS_MIN_SEEN"])
maxage = int(os.environ["ADDPEERS_MAX_AGE_DAYS"]); cap = int(os.environ["ADDPEERS_MAX"])
now = datetime.now(timezone.utc).timestamp()
rows = []
for r in csv.DictReader(open(reg)):
    try:
        if int(r.get("times_seen") or 0) < minseen:
            continue
        if now - datetime.fromisoformat(r["last_seen"]).timestamp() > maxage * 86400:
            continue
    except Exception:
        continue
    addr = r.get("address", "")
    try:
        a = ipaddress.ip_address(addr)
        if a.is_private or a.is_loopback or a.is_link_local:
            continue
    except Exception:
        pass  # .onion / .i2p are fine (node reaches them via its proxies)
    rows.append((int(r.get("times_seen") or 0), f"{addr}:{r.get('port','8333')}"))
rows.sort(reverse=True)  # most-seen (most stable) first
print("\n".join(p for _, p in rows[:cap]))
PY
)

# Pinned peers: always kept, independent of the registry (community nodes, incl.
# non-standard ports and Tor that a pass may not have crawled yet). Space-separated
# host:port. Curated list shared by Kilombino (our own Macanotrades entries excluded).
PINNED="${ADDPEERS_PINNED:-\
179.27.118.130:8343 \
nodoblake2b.airdns.org:11010 \
173.24.24.140:8333 \
47.189.218.206:8435 \
172.235.154.147:8333 \
64.181.91.48:8333 \
84.106.7.182:8333 \
23.114.198.28:9333 \
94.59.27.202:9333 \
64.68.204.49:8333 \
84.213.189.64:9333 \
82.67.102.15:8333 \
w2okrqbcuvkqg75aa6lodlso2rfoxe3c7arlp5kznawxi7wev26skead.onion:8333}"

count=0
while IFS= read -r peer; do
    [ -z "$peer" ] && continue
    $CLI addnode "$peer" add >/dev/null 2>&1 || true   # ignore "already added"
    count=$((count + 1))
done <<< "$PEERS"
for peer in $PINNED; do
    [ -z "$peer" ] && continue
    $CLI addnode "$peer" add >/dev/null 2>&1 || true
    count=$((count + 1))
done

# Self-clean: drop addnode entries that have aged out — not currently connected,
# not pinned, and no longer in the active registry (>MAX_AGE_DAYS since last seen).
# Comparison is bracket-insensitive: `getaddednodeinfo` returns IPv6 without the
# [..] that the registry/addnode form carries, so we normalize both sides first
# (a mismatch here would wrongly drop a live IPv6 node).
PRUNED=$(REG="$REG" DD="$DATADIR" MAX_AGE_DAYS="$MAX_AGE_DAYS" PINNED="$PINNED" "$PY" - <<'PY'
import os, csv, json, subprocess
from datetime import datetime, timezone
DD = os.environ["DD"]; reg = os.environ["REG"]; maxage = int(os.environ["MAX_AGE_DAYS"])
norm = lambda s: s.replace("[", "").replace("]", "").strip().lower()
pinned = {norm(p) for p in os.environ.get("PINNED", "").split()}
now = datetime.now(timezone.utc).timestamp()
alive = set()
try:
    for r in csv.DictReader(open(reg)):
        a = r.get("address", ""); p = r.get("port", "8333")
        try:
            if now - datetime.fromisoformat(r["last_seen"]).timestamp() > maxage * 86400:
                continue
        except Exception:
            continue
        if a:
            alive.add(norm(f"{a}:{p}"))
except Exception:
    pass
try:
    added = json.loads(subprocess.run(["bitcoin-cli", f"-datadir={DD}", "getaddednodeinfo"],
                                       capture_output=True, text=True).stdout)
except Exception:
    added = []
pruned = 0
for x in added:
    node = x.get("addednode", "")
    key = norm(node)
    if x.get("connected") or key in pinned or key in alive:
        continue
    if subprocess.run(["bitcoin-cli", f"-datadir={DD}", "addnode", node, "remove"],
                      capture_output=True, text=True).returncode == 0:
        pruned += 1
print(pruned)
PY
)
echo "addpeers: ensured $count stable BLAKE2b peers (min_seen=$MIN_SEEN, active<=${MAX_AGE_DAYS}d, +pinned); pruned ${PRUNED:-0} dead"
