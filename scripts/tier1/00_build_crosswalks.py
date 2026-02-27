"""Build county FIPS crosswalk for all 1,307 communities.

Uses the Census Geocoder API (coordinates endpoint) to resolve each
community's latitude/longitude to a county FIPS code.  Results are
cached per-community so the script is fully idempotent — interrupted
runs resume from where they left off.

Must be run before any downstream script that needs county FIPS
(01, 03, 04, 05, 06).

Output
------
data/prepared/crosswalks/county_fips_crosswalk.csv
  canonical_id, state_fips, county_fips, fips_5digit, county_name_census

S3 / Athena
-----------
s3://{bucket}/tier1-data/crosswalk/csv/county_fips_crosswalk.csv
Athena table: mth_lme.crosswalk_county_fips
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tier1._helpers import (  # noqa: E402
    CROSSWALK_DIR,
    api_get,
    ensure_dirs,
    load_cached,
    load_communities,
    register_athena_table,
    save_cache,
    upload_to_s3,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
GEOCODER_PARAMS_BASE: dict[str, str] = {
    "benchmark": "Public_AR_Current",
    "vintage": "Current_Current",
    "format": "json",
}
# Conservative: Census Geocoder is public but fragile under load
RATE_LIMIT_SECS = 1.0
# Coordinate offsets (degrees) used when the primary lookup returns no county
OFFSETS = [
    (0.001, 0.0),
    (-0.001, 0.0),
    (0.0, 0.001),
    (0.0, -0.001),
]
OUTPUT_CSV = CROSSWALK_DIR / "county_fips_crosswalk.csv"
ATHENA_TABLE = "crosswalk_county_fips"
ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("state_fips", "string"),
    ("county_fips", "string"),
    ("fips_5digit", "string"),
    ("county_name_census", "string"),
]
PROGRESS_INTERVAL = 50


# ---------------------------------------------------------------------------
# Census Geocoder helpers
# ---------------------------------------------------------------------------
def _extract_county(response_json: dict[str, Any]) -> dict[str, str] | None:
    """Return county FIPS fields from a Census Geocoder response, or None."""
    try:
        counties: list[dict[str, Any]] = response_json["result"]["geographies"][
            "Counties"
        ]
    except (KeyError, TypeError):
        return None

    if not counties:
        return None

    county = counties[0]
    state_fips: str = str(county.get("STATE", "")).zfill(2)
    county_fips: str = str(county.get("COUNTY", "")).zfill(3)
    geoid: str = str(county.get("GEOID", "")).zfill(5)
    name: str = str(county.get("NAME", ""))

    # Validate we got real data (not empty strings after zfill)
    if not state_fips.strip("0") and not county_fips.strip("0"):
        return None

    return {
        "state_fips": state_fips,
        "county_fips": county_fips,
        "fips_5digit": geoid if len(geoid) == 5 else state_fips + county_fips,
        "county_name_census": name,
    }


def _geocode_point(lat: float, lng: float) -> dict[str, Any] | None:
    """Call Census Geocoder for one coordinate pair.

    Returns the parsed JSON response dict on success, None on failure.
    """
    params = {**GEOCODER_PARAMS_BASE, "x": str(lng), "y": str(lat)}
    try:
        resp: requests.Response = api_get(
            GEOCODER_URL,
            params=params,
            rate_limit=RATE_LIMIT_SECS,
            max_retries=3,
            timeout=30,
        )
        return resp.json()
    except Exception as exc:  # noqa: BLE001
        print(f"    Geocoder error at ({lat}, {lng}): {exc}")
        return None


def resolve_fips(
    canonical_id: str,
    lat: float,
    lng: float,
) -> dict[str, str]:
    """Resolve county FIPS for a community, using cache + offset retries.

    Returns a dict with keys:
        canonical_id, state_fips, county_fips, fips_5digit, county_name_census
    UNRESOLVED sentinel values are empty strings.
    """
    # --- Cache check ---
    cached = load_cached("geocoder", canonical_id)
    if isinstance(cached, dict):
        fips = _extract_county(cached)
        if fips is not None:
            return {"canonical_id": canonical_id, **fips}
        # Cached but empty → fall through to retry below

    # --- Primary lookup ---
    response = _geocode_point(lat, lng)
    if response is not None:
        fips = _extract_county(response)
        if fips is not None:
            save_cache("geocoder", canonical_id, response)
            return {"canonical_id": canonical_id, **fips}
        # Save the empty response so we don't repeat the primary call
        save_cache("geocoder", canonical_id, response)

    # --- Offset retries ---
    for dlat, dlng in OFFSETS:
        offset_resp = _geocode_point(lat + dlat, lng + dlng)
        if offset_resp is None:
            continue
        fips = _extract_county(offset_resp)
        if fips is not None:
            # Cache the successful offset response under the canonical_id
            save_cache("geocoder", canonical_id, offset_resp)
            print(f"    Resolved {canonical_id} via offset ({dlat:+.3f}, {dlng:+.3f})")
            return {"canonical_id": canonical_id, **fips}

    # --- Still unresolved ---
    print(f"    UNRESOLVED: {canonical_id} ({lat}, {lng})")
    return {
        "canonical_id": canonical_id,
        "state_fips": "",
        "county_fips": "",
        "fips_5digit": "",
        "county_name_census": "UNRESOLVED",
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Build county FIPS crosswalk for all communities."""
    print("=" * 60)
    print("00_build_crosswalks.py — County FIPS crosswalk")
    print("=" * 60)

    # 1. Setup directories
    ensure_dirs()

    # 2. Load communities
    communities: pd.DataFrame = load_communities()
    total = len(communities)
    print(f"\nLoaded {total} communities from mth_communities.csv")

    # 3. Resolve FIPS for each community
    print("\nResolving county FIPS codes via Census Geocoder...")
    rows: list[dict[str, str]] = []

    for i, row in enumerate(communities.itertuples(index=False), start=1):
        canonical_id: str = str(row.canonical_id)
        lat: float = float(row.latitude)
        lng: float = float(row.longitude)

        result = resolve_fips(canonical_id, lat, lng)
        rows.append(result)

        if i % PROGRESS_INTERVAL == 0 or i == total:
            resolved = sum(1 for r in rows if r["fips_5digit"])
            print(
                f"  Progress: {i}/{total} processed, "
                f"{resolved} resolved, {i - resolved} unresolved"
            )

    # 4. Build output DataFrame
    crosswalk_df = pd.DataFrame(
        rows,
        columns=[
            "canonical_id",
            "state_fips",
            "county_fips",
            "fips_5digit",
            "county_name_census",
        ],
    )

    # 5. Save CSV
    crosswalk_df.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved crosswalk to {OUTPUT_CSV}")

    # 6. Upload to S3
    print("\nUploading to S3...")
    s3_uri = upload_to_s3(OUTPUT_CSV, "crosswalk")

    # 7. Register Athena table
    print("\nRegistering Athena table...")
    register_athena_table(ATHENA_TABLE, ATHENA_COLUMNS, s3_uri)

    # 8. Validation summary
    null_mask = crosswalk_df["fips_5digit"].eq("")
    null_count = null_mask.sum()
    unique_states = crosswalk_df.loc[~null_mask, "state_fips"].nunique()
    unique_counties = crosswalk_df.loc[~null_mask, "fips_5digit"].nunique()

    print("\n" + "=" * 60)
    print("Validation Summary")
    print("=" * 60)
    print(f"  Total rows:        {len(crosswalk_df)}")
    print(f"  Resolved:          {len(crosswalk_df) - null_count}")
    print(f"  Unresolved (null): {null_count}")
    print(f"  Unique state FIPS: {unique_states}")
    print(f"  Unique counties:   {unique_counties}")
    if null_count > 0:
        unresolved_ids = crosswalk_df.loc[null_mask, "canonical_id"].tolist()
        print(f"  Unresolved IDs:    {unresolved_ids}")
    print("=" * 60)
    print("Done.")


if __name__ == "__main__":
    main()
