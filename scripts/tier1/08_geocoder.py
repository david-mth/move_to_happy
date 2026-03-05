#!/usr/bin/env python3
"""08 — Reverse-geocode communities to obtain zip codes.

Uses OpenStreetMap Nominatim reverse geocoding API to enrich each community
with its zip code, place name, county, and neighbourhood.

Runtime: ~22 minutes (1 req/sec rate limit, ~1,307 communities).
Cached per-community on disk for safe resume.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Make helpers importable
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _helpers import (
    PROJECT_ROOT,
    TIER1_DIR,
    api_get,
    ensure_dirs,
    load_cached,
    load_communities,
    register_athena_table,
    save_cache,
    upload_to_s3,
)

CACHE_SOURCE = "geocoder"
APP_TIER1_DIR = PROJECT_ROOT / "app" / "data" / "tier1"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
HEADERS = {"User-Agent": "MoveToHappy/1.0 (community-enrichment)"}


def reverse_geocode(lat: float, lon: float) -> dict:
    """Reverse-geocode a single lat/lon via Nominatim."""
    params = {
        "format": "json",
        "lat": lat,
        "lon": lon,
        "zoom": 18,
        "addressdetails": 1,
    }
    resp = api_get(NOMINATIM_URL, params=params, headers=HEADERS, rate_limit=1.0)
    return resp.json()


def extract_fields(data: dict) -> dict:
    """Extract zip_code, place_name, county, neighbourhood from Nominatim response."""
    addr = data.get("address", {})
    return {
        "zip_code": addr.get("postcode", ""),
        "place_name": addr.get("city")
        or addr.get("town")
        or addr.get("village")
        or addr.get("hamlet")
        or "",
        "county_nominatim": addr.get("county", ""),
        "neighbourhood": addr.get("neighbourhood") or addr.get("suburb") or "",
        "display_name": data.get("display_name", ""),
    }


def main() -> None:
    ensure_dirs()
    APP_TIER1_DIR.mkdir(parents=True, exist_ok=True)

    communities = load_communities()
    total = len(communities)
    print(f"Reverse-geocoding {total} communities via Nominatim...")

    rows = []
    cached_count = 0

    for i in range(total):
        cid = str(communities.at[i, "canonical_id"])
        lat = float(communities.at[i, "latitude"])
        lon = float(communities.at[i, "longitude"])

        # Check cache first
        cached = load_cached(CACHE_SOURCE, cid)
        if cached is not None and isinstance(cached, dict):
            fields = extract_fields(cached)
            cached_count += 1
        else:
            try:
                data = reverse_geocode(lat, lon)
                save_cache(CACHE_SOURCE, cid, data)
                fields = extract_fields(data)
            except Exception as e:
                print(f"  [{i + 1}/{total}] FAILED {cid}: {e}")
                fields = {
                    "zip_code": "",
                    "place_name": "",
                    "county_nominatim": "",
                    "neighbourhood": "",
                    "display_name": "",
                }

        rows.append({"canonical_id": cid, **fields})

        if (i + 1) % 50 == 0 or i + 1 == total:
            print(f"  [{i + 1}/{total}] processed ({cached_count} cached)")

    df = pd.DataFrame(rows)

    # Save to pipeline output dir
    pipeline_path = TIER1_DIR / "geocoder.csv"
    df.to_csv(pipeline_path, index=False)
    print(f"Saved pipeline output: {pipeline_path} ({len(df)} rows)")

    # Save to app runtime dir
    app_path = APP_TIER1_DIR / "geocoder.csv"
    df.to_csv(app_path, index=False)
    print(f"Saved app data: {app_path} ({len(df)} rows)")

    # Show stats
    has_zip = df["zip_code"].astype(bool).sum()
    print(f"\nResults: {has_zip}/{len(df)} communities have zip codes")

    # Optional: S3 upload + Athena registration
    try:
        s3_uri = upload_to_s3(pipeline_path, "geocoder")
        register_athena_table(
            table_name="tier1_geocoder",
            columns=[
                ("canonical_id", "string"),
                ("zip_code", "string"),
                ("place_name", "string"),
                ("county_nominatim", "string"),
                ("neighbourhood", "string"),
                ("display_name", "string"),
            ],
            s3_location=s3_uri,
        )
    except Exception as e:
        print(f"\nSkipping S3/Athena (no AWS creds): {e}")


if __name__ == "__main__":
    main()
