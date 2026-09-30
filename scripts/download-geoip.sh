#!/usr/bin/env bash
# Helper script to remind / guide downloading MaxMind GeoLite2 databases.
# MaxMind requires a (free) account now.
#
# 1. Sign up at https://www.maxmind.com/en/geolite2/signup (or https://dev.maxmind.com/geoip/geolite2-free-geolocation-data)
# 2. Generate license key in your account.
# 3. Download GeoLite2-City and GeoLite2-ASN (MMDB format).
#
# You can use the web downloader or their geoipupdate tool (see docs).
#
# After download, extract the .mmdb files into ./data/
#
# Expected files:
#   data/GeoLite2-City.mmdb
#   data/GeoLite2-ASN.mmdb
#
# Then run the crawler; it will auto-detect them.

set -euo pipefail

DATA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../data" && pwd)"
mkdir -p "$DATA_DIR"

echo "=== knots-network-crawler GeoIP setup ==="
echo "1. Create a free MaxMind account: https://www.maxmind.com/en/geolite2/signup"
echo "2. Log in -> 'My License Keys' and generate a key if you don't have one."
echo "3. Go to 'Download Databases' and download:"
echo "     - GeoLite2 City (MMDB)"
echo "     - GeoLite2 ASN (MMDB)"
echo "4. Extract the .tar.gz and copy the .mmdb files to:"
echo "     $DATA_DIR/GeoLite2-City.mmdb"
echo "     $DATA_DIR/GeoLite2-ASN.mmdb"
echo ""
echo "Alternative (if you have 'geoipupdate' configured):"
echo "  geoipupdate"
echo ""
echo "After placing the files, you can run:"
echo "  knots-network-crawler crawl --help"
echo ""
echo "The crawler will work without GeoIP but country/ASN data will be missing."
