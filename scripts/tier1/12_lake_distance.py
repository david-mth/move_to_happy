#!/usr/bin/env python3
"""12 — Lake distance enrichment (nearest significant lake for each community).

Integrates pre-computed nearest-lake distances from an external engineer's
analysis using the HydroLAKES global geodatabase.  The source data filters
to 68 significant lakes (>= 6.58 sq mi) across GA, AL, FL and computes
haversine straight-line distances from each community to the nearest lake.

This data fills the 128 NaN gaps in `miles_to_lake` and adds new columns:
  - lake_name, lake_distance_miles, lake_area_sq_mi, lake_state,
    lake_latitude, lake_longitude

Source files (in ~/Downloads):
  - CityToLake.xlsx  (1,308 cities with pre-computed nearest-lake data)
  - lakes.csv        (68 curated significant lakes)

No API calls required — reads from local files only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _helpers import (
    PROJECT_ROOT,
    TIER1_DIR,
    ensure_dirs,
    load_communities,
    register_athena_table,
    upload_to_s3,
)

APP_TIER1_DIR = PROJECT_ROOT / "app" / "data" / "tier1"

DOWNLOADS_DIR = Path.home() / "Downloads"
CITY_TO_LAKE_XLSX = DOWNLOADS_DIR / "CityToLake.xlsx"
LAKES_CSV = DOWNLOADS_DIR / "lakes.csv"

OUTPUT_COLUMNS = [
    "canonical_id",
    "lake_name",
    "lake_distance_miles",
    "lake_area_sq_mi",
    "lake_state",
    "lake_latitude",
    "lake_longitude",
]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("lake_name", "string"),
    ("lake_distance_miles", "double"),
    ("lake_area_sq_mi", "double"),
    ("lake_state", "string"),
    ("lake_latitude", "double"),
    ("lake_longitude", "double"),
]


def _load_city_to_lake() -> pd.DataFrame:
    """Load the CityToLake.xlsx output file."""
    df = pd.read_excel(CITY_TO_LAKE_XLSX)
    print(f"  CityToLake.xlsx: {len(df)} rows, {len(df.columns)} cols")
    return df


def _match_communities(
    comm: pd.DataFrame,
    lake_df: pd.DataFrame,
) -> pd.DataFrame:
    """Match lake data to communities on city+state."""
    comm["_join_key"] = (
        comm["city"].str.strip().str.lower()
        + "|"
        + comm["state_name"].str.strip().str.lower()
    )
    lake_df["_join_key"] = (
        lake_df["City"].str.strip().str.lower()
        + "|"
        + lake_df["state"].str.strip().str.lower()
    )

    merged = comm[["canonical_id", "_join_key"]].merge(
        lake_df[
            [
                "_join_key",
                "LakeName",
                "LakeDistanceMiles",
                "LakeAreaSqMi",
                "LakeState",
                "LakeLatitude",
                "LakeLongitude",
            ]
        ],
        on="_join_key",
        how="left",
    )

    merged = merged.drop(columns=["_join_key"])
    merged = merged.rename(
        columns={
            "LakeName": "lake_name",
            "LakeDistanceMiles": "lake_distance_miles",
            "LakeAreaSqMi": "lake_area_sq_mi",
            "LakeState": "lake_state",
            "LakeLatitude": "lake_latitude",
            "LakeLongitude": "lake_longitude",
        }
    )
    return merged


def main() -> None:
    ensure_dirs()
    APP_TIER1_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("12 — Lake Distance Enrichment")
    print("=" * 60)

    if not CITY_TO_LAKE_XLSX.exists():
        print(f"\n  ERROR: {CITY_TO_LAKE_XLSX} not found.")
        print("  Place CityToLake.xlsx in ~/Downloads/ and re-run.")
        sys.exit(1)

    print("\n[1/4] Loading source data ...")
    lake_df = _load_city_to_lake()
    comm = load_communities()
    print(f"  Communities: {len(comm)} rows")

    print("\n[2/4] Matching communities to lake data ...")
    result = _match_communities(comm, lake_df)

    matched = result["lake_name"].notna().sum()
    total = len(result)
    print(f"  Matched: {matched}/{total} ({matched / total * 100:.1f}%)")

    missing = result[result["lake_name"].isna()]
    if len(missing) > 0:
        print(f"  Unmatched: {len(missing)} communities")

    result = result[OUTPUT_COLUMNS]

    print("\n[3/4] Saving CSV outputs ...")
    out_path = TIER1_DIR / "lake_distance.csv"
    result.to_csv(out_path, index=False)
    print(f"  Saved: {out_path} ({len(result)} rows)")

    app_path = APP_TIER1_DIR / "lake_distance.csv"
    result.to_csv(app_path, index=False)
    print(f"  Saved: {app_path}")

    print("\n[4/4] Uploading to S3 + Athena ...")
    try:
        s3_uri = upload_to_s3(out_path, "lake_distance")
        register_athena_table("tier1_lake_distance", ATHENA_COLUMNS, s3_uri)
    except Exception as e:
        print(f"  S3/Athena upload skipped: {e}")

    print("\n" + "=" * 60)
    print("Done!  Lake distance enrichment complete.")
    print(f"  Output: {out_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
