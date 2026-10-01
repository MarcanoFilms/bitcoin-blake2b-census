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
echo "addpeers: ensured $count stable BLAKE2b peers (min_seen=$MIN_SEEN, active<=${MAX_AGE_DAYS}d, +pinned)"
