#!/usr/bin/env python3
"""09b — Backfill NaN distance values in mth_communities.csv.

Uses geodesic distances from the external engineer's analysis to fill
coverage gaps in the base communities dataset:

  - 399 communities missing miles_to_mountains
  - 129 communities missing miles_to_atlantic / miles_to_gulf / miles_to_beach

Only fills NaN values — existing non-null values are never overwritten.

Source files:
  - updatedcitydatawithocean.xlsx      (ocean distances)
  - updatedcitydatawithmountaindistance.xlsx  (mountain distances)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _helpers import PREPARED_DIR

DOWNLOADS_DIR = Path.home() / "Downloads"
OCEAN_XLSX = DOWNLOADS_DIR / "updatedcitydatawithocean.xlsx"
MOUNTAIN_XLSX = DOWNLOADS_DIR / "updatedcitydatawithmountaindistance.xlsx"
COMMUNITIES_CSV = PREPARED_DIR / "mth_communities.csv"


def _build_match_key(city_col: pd.Series, state_col: pd.Series) -> pd.Series:
    return city_col.str.strip().str.lower() + "|" + state_col.str.strip().str.lower()


def main() -> None:
    print("=" * 60)
    print("09b_backfill_distances.py — Fill NaN distances in communities")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    print("\n[1/4] Loading data ...")
    communities = pd.read_csv(COMMUNITIES_CSV)
    communities["_match"] = _build_match_key(
        communities["city"], communities["state_name"]
    )
    print(f"  Communities: {len(communities)}")

    ocean_df = pd.read_excel(OCEAN_XLSX)
    ocean_df["_match"] = _build_match_key(ocean_df["City"], ocean_df["state"])

    mountain_df = pd.read_excel(MOUNTAIN_XLSX)
    mountain_df["_match"] = _build_match_key(mountain_df["City"], mountain_df["state"])

    # Build lookups
    ocean_lookup = (
        ocean_df[["_match", "OceanDistanceMiles", "OceanType"]]
        .drop_duplicates(subset=["_match"], keep="first")
        .set_index("_match")
    )
    mountain_lookup = (
        mountain_df[["_match", "MountainRegionDistanceMiles"]]
        .drop_duplicates(subset=["_match"], keep="first")
        .set_index("_match")
    )

    # ------------------------------------------------------------------
    # Pre-backfill stats
    # ------------------------------------------------------------------
    print("\n[2/4] Pre-backfill NaN counts:")
    for col in [
        "miles_to_mountains",
        "miles_to_atlantic",
        "miles_to_gulf",
        "miles_to_beach",
    ]:
        nan_ct = communities[col].isna().sum()
        print(f"  {col}: {nan_ct}/{len(communities)} NaN")

    # ------------------------------------------------------------------
    # Backfill mountains
    # ------------------------------------------------------------------
    print("\n[3/4] Backfilling ...")
    mtn_filled = 0
    for idx, row in communities.iterrows():
        if pd.isna(row["miles_to_mountains"]):
            match_key = row["_match"]
            if match_key in mountain_lookup.index:
                dist = mountain_lookup.at[match_key, "MountainRegionDistanceMiles"]
                if pd.notna(dist):
                    communities.at[idx, "miles_to_mountains"] = round(float(dist), 2)
                    mtn_filled += 1
    print(f"  miles_to_mountains filled: {mtn_filled}")

    # ------------------------------------------------------------------
    # Backfill ocean distances
    # ------------------------------------------------------------------
    ocean_filled = 0
    for idx, row in communities.iterrows():
        if pd.isna(row["miles_to_beach"]):
            match_key = row["_match"]
            if match_key in ocean_lookup.index:
                dist = ocean_lookup.at[match_key, "OceanDistanceMiles"]
                ocean_type = ocean_lookup.at[match_key, "OceanType"]
                if pd.notna(dist):
                    dist_rounded = round(float(dist), 2)
                    communities.at[idx, "miles_to_beach"] = dist_rounded

                    if isinstance(ocean_type, str) and "Atlantic" in ocean_type:
                        communities.at[idx, "miles_to_atlantic"] = dist_rounded
                    elif isinstance(ocean_type, str) and "Gulf" in ocean_type:
                        communities.at[idx, "miles_to_gulf"] = dist_rounded

                    ocean_filled += 1
    print(f"  miles_to_beach (+ atlantic/gulf) filled: {ocean_filled}")

    # ------------------------------------------------------------------
    # Post-backfill stats and save
    # ------------------------------------------------------------------
    print("\n[4/4] Post-backfill NaN counts:")
    for col in [
        "miles_to_mountains",
        "miles_to_atlantic",
        "miles_to_gulf",
        "miles_to_beach",
    ]:
        nan_ct = communities[col].isna().sum()
        print(f"  {col}: {nan_ct}/{len(communities)} NaN")

    communities = communities.drop(columns=["_match"])
    communities.to_csv(COMMUNITIES_CSV, index=False)
    print(f"\n  Saved: {COMMUNITIES_CSV}")
    print("Done.")


if __name__ == "__main__":
    main()
