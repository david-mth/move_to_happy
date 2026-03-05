#!/usr/bin/env python3
"""09 — Geodesic distance enrichment (ocean + mountain polygon distances).

Integrates pre-computed geodesic distances from an external engineer's
analysis.  The source data uses Shapely + pyproj (EPSG:5070 Albers Equal
Area) to compute straight-line distances from each community to:

  - Nearest Atlantic or Gulf of Mexico coastline (from EastCoastOcean_split.geojson)
  - Nearest Appalachian mountain region polygon edge (from mountain_regions.geojson)

This data fills coverage gaps in the base communities CSV (399 NaN mountain
distances, 129 NaN ocean distances) and adds new categorical columns
(ocean_type, mountain_region_inside) not previously available.

Source files (in project root / Downloads):
  - updatedcitydatawithocean.xlsx
  - updatedcitydatawithmountaindistance.xlsx

Reference GeoJSON polygons stored in data/reference/.

No API calls required — reads from local Excel files only.
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
OCEAN_XLSX = DOWNLOADS_DIR / "updatedcitydatawithocean.xlsx"
MOUNTAIN_XLSX = DOWNLOADS_DIR / "updatedcitydatawithmountaindistance.xlsx"

OUTPUT_COLUMNS = [
    "canonical_id",
    "mountain_region_distance_miles",
    "mountain_region_inside",
    "mountain_region_name",
    "ocean_distance_miles",
    "ocean_type",
    "ocean_side",
]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("mountain_region_distance_miles", "double"),
    ("mountain_region_inside", "boolean"),
    ("mountain_region_name", "string"),
    ("ocean_distance_miles", "double"),
    ("ocean_type", "string"),
    ("ocean_side", "boolean"),
]


def _build_match_key(city_col: pd.Series, state_col: pd.Series) -> pd.Series:
    """Normalised city|state key for matching across datasets."""
    return city_col.str.strip().str.lower() + "|" + state_col.str.strip().str.lower()


def main() -> None:
    print("=" * 60)
    print("09_geodistance.py — Geodesic Distance Enrichment")
    print("=" * 60)

    ensure_dirs()
    APP_TIER1_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Load source data
    # ------------------------------------------------------------------
    print("\n[1/5] Loading source data ...")

    communities = load_communities()
    communities["_match"] = _build_match_key(
        communities["city"], communities["state_name"]
    )
    print(f"  Communities: {len(communities)}")

    print(f"  Reading {OCEAN_XLSX.name} ...")
    ocean_df = pd.read_excel(OCEAN_XLSX)
    ocean_df["_match"] = _build_match_key(ocean_df["City"], ocean_df["state"])
    print(f"    Rows: {len(ocean_df)}")

    print(f"  Reading {MOUNTAIN_XLSX.name} ...")
    mountain_df = pd.read_excel(MOUNTAIN_XLSX)
    mountain_df["_match"] = _build_match_key(mountain_df["City"], mountain_df["state"])
    print(f"    Rows: {len(mountain_df)}")

    # ------------------------------------------------------------------
    # 2. Match and extract
    # ------------------------------------------------------------------
    print("\n[2/5] Matching to communities ...")

    ocean_lookup = (
        ocean_df[["_match", "OceanDistanceMiles", "OceanType", "OceanSide"]]
        .drop_duplicates(subset=["_match"], keep="first")
        .set_index("_match")
    )

    mountain_lookup = (
        mountain_df[
            [
                "_match",
                "MountainRegionDistanceMiles",
                "MountainRegionInside",
                "MountainRegionName",
            ]
        ]
        .drop_duplicates(subset=["_match"], keep="first")
        .set_index("_match")
    )

    merged = communities[["canonical_id", "_match"]].copy()
    merged = merged.join(ocean_lookup, on="_match", how="left")
    merged = merged.join(mountain_lookup, on="_match", how="left")

    matched_ocean = merged["OceanDistanceMiles"].notna().sum()
    matched_mountain = merged["MountainRegionDistanceMiles"].notna().sum()
    print(f"  Ocean matched: {matched_ocean}/{len(merged)}")
    print(f"  Mountain matched: {matched_mountain}/{len(merged)}")

    # ------------------------------------------------------------------
    # 3. Rename and format output
    # ------------------------------------------------------------------
    print("\n[3/5] Building output ...")

    output = pd.DataFrame(
        {
            "canonical_id": merged["canonical_id"],
            "mountain_region_distance_miles": merged[
                "MountainRegionDistanceMiles"
            ].round(2),
            "mountain_region_inside": merged["MountainRegionInside"]
            .fillna(False)
            .astype(bool),
            "mountain_region_name": merged["MountainRegionName"].fillna(""),
            "ocean_distance_miles": merged["OceanDistanceMiles"].round(2),
            "ocean_type": merged["OceanType"].fillna(""),
            "ocean_side": merged["OceanSide"].fillna(False).astype(bool),
        }
    )

    csv_path = TIER1_DIR / "geodistance.csv"
    output.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path} ({len(output)} rows)")

    app_path = APP_TIER1_DIR / "geodistance.csv"
    output.to_csv(app_path, index=False)
    print(f"  Saved: {app_path} ({len(output)} rows)")

    # ------------------------------------------------------------------
    # 4. Upload to S3 + Athena
    # ------------------------------------------------------------------
    print("\n[4/5] Uploading to S3 and registering Athena table ...")
    try:
        s3_uri = upload_to_s3(csv_path, "geodistance")
        register_athena_table("tier1_geodistance", ATHENA_COLUMNS, s3_uri)
    except Exception as e:
        print(f"\n  Skipping S3/Athena: {e}")

    # ------------------------------------------------------------------
    # 5. Validation
    # ------------------------------------------------------------------
    print("\n[5/5] Validation ...")
    total = len(output)

    for col in OUTPUT_COLUMNS[1:]:
        if col in ("mountain_region_inside", "ocean_side"):
            true_ct = output[col].sum()
            print(f"  {col}: {true_ct}/{total} True")
        elif col in ("mountain_region_name", "ocean_type"):
            print(f"  {col}: {output[col].value_counts().to_dict()}")
        else:
            non_null = output[col].notna().sum()
            valid = output[col].dropna()
            if not valid.empty:
                print(
                    f"  {col}: {non_null}/{total} "
                    f"[{valid.min():.1f} - {valid.max():.1f}], "
                    f"mean={valid.mean():.1f}"
                )
            else:
                print(f"  {col}: {non_null}/{total} (all NaN)")

    print("\nDone.")


if __name__ == "__main__":
    main()
