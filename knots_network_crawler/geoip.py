"""
GeoIP resolver using MaxMind GeoLite2 databases (City + ASN).

The user MUST download the free GeoLite2 databases from MaxMind.
See scripts/download-geoip.sh and the README for instructions.

This module is intentionally graceful: if no DBs are present, all lookups return None data.
"""

from __future__ import annotations

import ipaddress
from pathlib import Path
from typing import Optional

try:
    import maxminddb
except ImportError:
    maxminddb = None  # type: ignore

from .models import GeoData


class GeoIPResolver:
    """
    Thread-safe enough reader (maxminddb readers are safe for concurrent reads).

    Preferred files (searched in order):
      1. Explicit path passed to constructor
      2. Environment: KNOTS_GEOIP_CITY_MMDB and KNOTS_GEOIP_ASN_MMDB
      3. data/GeoLite2-City.mmdb and data/GeoLite2-ASN.mmdb relative to cwd
      4. Common system locations (/usr/share/GeoIP, /var/lib/GeoIP, etc.)
    """

    DEFAULT_CITY_NAMES = [
        "data/GeoLite2-City.mmdb",
        "data/GeoLite2-City_*.mmdb",  # in case they keep versioned name
    ]
    DEFAULT_ASN_NAMES = [
        "data/GeoLite2-ASN.mmdb",
        "data/GeoLite2-ASN_*.mmdb",
    ]

    def __init__(
        self,
        city_mmdb: Optional[str | Path] = None,
        asn_mmdb: Optional[str | Path] = None,
    ):
        self.city_reader = None
        self.asn_reader = None
        self._loaded = False

        self._city_path: Optional[Path] = None
        self._asn_path: Optional[Path] = None

        if maxminddb is None:
            return

        candidates_city = self._build_candidates(city_mmdb, "city")
        candidates_asn = self._build_candidates(asn_mmdb, "asn")

        for p in candidates_city:
            if p and p.exists():
                try:
                    self.city_reader = maxminddb.open_database(str(p))
                    self._city_path = p
                    break
                except Exception:
                    pass

        for p in candidates_asn:
            if p and p.exists():
                try:
                    self.asn_reader = maxminddb.open_database(str(p))
                    self._asn_path = p
                    break
                except Exception:
                    pass

        self._loaded = bool(self.city_reader or self.asn_reader)

    def _build_candidates(self, explicit: Optional[str | Path], kind: str) -> list[Optional[Path]]:
        import os
        import glob

        cands: list[Optional[Path]] = []
        if explicit:
            cands.append(Path(explicit))

        env_city = os.environ.get("KNOTS_GEOIP_CITY_MMDB")
        env_asn = os.environ.get("KNOTS_GEOIP_ASN_MMDB")
        if kind == "city" and env_city:
            cands.append(Path(env_city))
        if kind == "asn" and env_asn:
            cands.append(Path(env_asn))

        # relative data dir
        base = Path.cwd()
        if kind == "city":
            for pat in self.DEFAULT_CITY_NAMES:
                for match in glob.glob(str(base / pat)):
                    cands.append(Path(match))
                cands.append(base / pat)
        else:
            for pat in self.DEFAULT_ASN_NAMES:
                for match in glob.glob(str(base / pat)):
                    cands.append(Path(match))
                cands.append(base / pat)

        # system locations (common on Linux)
        sys_paths = [
            Path("/usr/share/GeoIP"),
            Path("/var/lib/GeoIP"),
            Path("/opt/GeoIP"),
            Path.home() / ".local/share/GeoIP",
        ]
        for sp in sys_paths:
            if kind == "city":
                cands.extend(sp.glob("GeoLite2-City*.mmdb"))
            else:
                cands.extend(sp.glob("GeoLite2-ASN*.mmdb"))

        # dedup while preserving order
        seen = set()
        uniq = []
        for p in cands:
            if p and str(p) not in seen:
                seen.add(str(p))
                uniq.append(p if isinstance(p, Path) else Path(p))
        return uniq

    def available(self) -> bool:
        return self._loaded and (self.city_reader is not None or self.asn_reader is not None)

    def status(self) -> str:
        city = f"City: {self._city_path}" if self._city_path else "City: MISSING"
        asn = f"ASN: {self._asn_path}" if self._asn_path else "ASN: MISSING"
        return f"GeoIP [{city} | {asn}]"

    def lookup(self, ip: str) -> GeoData:
        """Return GeoData for IP (best effort). Never raises."""
        if not self.available():
            return GeoData()

        # Skip private / loopback / link-local etc.
        try:
            ip_obj = ipaddress.ip_address(ip)
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_reserved:
                return GeoData()
        except ValueError:
            return GeoData()

        geo = GeoData()

        # City DB
        if self.city_reader:
            try:
                rec = self.city_reader.get(ip)
                if rec:
                    # Country
                    if "country" in rec:
                        c = rec["country"]
                        geo.country = c.get("names", {}).get("en")
                        geo.country_code = c.get("iso_code")
                    elif "registered_country" in rec:
                        c = rec["registered_country"]
                        geo.country = c.get("names", {}).get("en")
                        geo.country_code = c.get("iso_code")

                    # City
                    if "city" in rec:
                        geo.city = rec["city"].get("names", {}).get("en")

                    # Location
                    if "location" in rec:
                        loc = rec["location"]
                        if "latitude" in loc and "longitude" in loc:
                            geo.latitude = float(loc["latitude"])
                            geo.longitude = float(loc["longitude"])
            except Exception:
                pass

        # ASN DB (often more reliable for org)
        if self.asn_reader:
            try:
                rec = self.asn_reader.get(ip)
                if rec:
                    if "autonomous_system_number" in rec:
                        geo.asn = int(rec["autonomous_system_number"])
                    if "autonomous_system_organization" in rec:
                        geo.asn_org = rec["autonomous_system_organization"]
                    # If city didn't give country, sometimes ASN has it (rare)
                    if not geo.country and "country" in rec:
                        c = rec["country"]
                        geo.country = c.get("names", {}).get("en") if isinstance(c, dict) else None
                        geo.country_code = c.get("iso_code") if isinstance(c, dict) else None
            except Exception:
                pass

        return geo

    def close(self) -> None:
        if self.city_reader:
            try:
                self.city_reader.close()
            except Exception:
                pass
        if self.asn_reader:
            try:
                self.asn_reader.close()
            except Exception:
                pass


# Global singleton (lazy)
_resolver: Optional[GeoIPResolver] = None


def get_geoip_resolver(
    city_mmdb: Optional[str | Path] = None,
    asn_mmdb: Optional[str | Path] = None,
) -> GeoIPResolver:
    global _resolver
    if _resolver is None:
        _resolver = GeoIPResolver(city_mmdb=city_mmdb, asn_mmdb=asn_mmdb)
    return _resolver
