#!/usr/bin/env bash
# Commit + push the durable node registry (and aggregate history) to GitHub once a
# day. Kept off the per-pass cadence so git history stays small. No-op if nothing
# changed. Relies on the repo's SSH remote + a passphrase-less key.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1

git add web/registry.csv web/history.json 2>/dev/null || true
if git diff --cached --quiet 2>/dev/null; then
  echo "registry: no changes to push"
  exit 0
fi

DATE=$(date +%Y-%m-%d)
COUNT=$(tail -n +2 web/registry.csv 2>/dev/null | wc -l | tr -d ' ')
git -c user.name="MarcanoFilms" -c user.email="marcanofilms@gmail.com" \
  commit -q -m "registry: node record $DATE (${COUNT} unique nodes)" || exit 1
git push origin main 2>&1 | tail -1
