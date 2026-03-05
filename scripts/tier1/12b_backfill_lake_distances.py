#!/usr/bin/env python3
"""12b — Backfill NaN miles_to_lake values in mth_communities.csv.

Uses the haversine distances from CityToLake.xlsx to fill the 128 NaN
values in the miles_to_lake column.  Only fills gaps — does NOT overwrite
existing non-null values.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _helpers import PREPARED_DIR

DOWNLOADS_DIR = Path.home() / "Downloads"
CITY_TO_LAKE_XLSX = DOWNLOADS_DIR / "CityToLake.xlsx"
COMMUNITIES_CSV = PREPARED_DIR / "mth_communities.csv"


def main() -> None:
    print("=" * 60)
    print("12b — Backfill miles_to_lake NaN values")
    print("=" * 60)

    comm = pd.read_csv(COMMUNITIES_CSV)
    before_nan = comm["miles_to_lake"].isna().sum()
    print(f"\n  Before: {before_nan} NaN in miles_to_lake")

    if before_nan == 0:
        print("  Nothing to backfill — all values present.")
        return

    lake_df = pd.read_excel(CITY_TO_LAKE_XLSX)
    print(f"  CityToLake.xlsx: {len(lake_df)} rows")

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

    lake_lookup = lake_df.set_index("_join_key")["LakeDistanceMiles"]

    filled = 0
    for idx, row in comm.iterrows():
        if pd.isna(row["miles_to_lake"]):
            key = row["_join_key"]
            if key in lake_lookup.index:
                val = lake_lookup[key]
                if pd.notna(val):
                    comm.at[idx, "miles_to_lake"] = round(float(val), 2)
                    filled += 1

    comm = comm.drop(columns=["_join_key"])

    after_nan = comm["miles_to_lake"].isna().sum()
    print(f"\n  Filled: {filled} values")
    print(f"  After:  {after_nan} NaN remaining")

    comm.to_csv(COMMUNITIES_CSV, index=False)
    print(f"\n  Saved: {COMMUNITIES_CSV}")

    app_csv = PREPARED_DIR.parent.parent / "app" / "data" / "mth_communities.csv"
    if app_csv.parent.exists():
        comm.to_csv(app_csv, index=False)
        print(f"  Saved: {app_csv}")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
