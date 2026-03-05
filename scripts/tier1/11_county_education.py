#!/usr/bin/env python3
"""11 — County education, employment, and demographics enrichment.

Integrates county-level data from the Lumina Foundation's county_2022 dataset:

  - High school graduation rate
  - Postsecondary enrollment and completion rates
  - Employment rate (all demographics)
  - Median earnings (indexed)
  - Labor force participation rate
  - Unemployment rate
  - Median age
  - Population count (indexed)

Joins to communities via 5-digit FIPS code from the national_county sheet.

Source file (in ~/Downloads):
  - county_2022.xlsx (sheets: county_2022, national_county)

No API calls required — reads from local Excel file only.
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
    load_crosswalk,
    register_athena_table,
    upload_to_s3,
)

APP_TIER1_DIR = PROJECT_ROOT / "app" / "data" / "tier1"
DOWNLOADS_DIR = Path.home() / "Downloads"

COUNTY_2022_XLSX = DOWNLOADS_DIR / "county_2022.xlsx"

OUTPUT_COLUMNS = [
    "canonical_id",
    "fips_5digit",
    "hs_graduation_rate",
    "postsecondary_enrollment_rate",
    "postsecondary_completion_rate",
    "employment_rate",
    "median_earnings",
    "labor_force_participation_rate",
    "unemployment_rate",
    "median_age",
    "population_count",
]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("fips_5digit", "string"),
    ("hs_graduation_rate", "double"),
    ("postsecondary_enrollment_rate", "double"),
    ("postsecondary_completion_rate", "double"),
    ("employment_rate", "double"),
    ("median_earnings", "double"),
    ("labor_force_participation_rate", "double"),
    ("unemployment_rate", "double"),
    ("median_age", "double"),
    ("population_count", "double"),
]

COLUMN_MAP = {
    "High School Graduation Rate": "hs_graduation_rate",
    "Postsecondary Enrollment Rate": "postsecondary_enrollment_rate",
    "Postsecondary Completion Rate": "postsecondary_completion_rate",
    "Employment Rate (All)": "employment_rate",
    "Median Earnings": "median_earnings",
    "Labor Force Participation Rate (All)": "labor_force_participation_rate",
    "Unemployment Rate (All)": "unemployment_rate",
    "Median Age": "median_age",
    "Population Count": "population_count",
}


def _load_county_data() -> pd.DataFrame:
    """Load county_2022 data for AL/FL/GA, keyed by 5-digit FIPS.

    The Excel file has a peculiar structure: row 0 contains metric codes,
    row 1 is state-level aggregate, and rows 2+ are county-level data.
    The county code is in column 'Unnamed: 0'.
    """
    raw = pd.read_excel(COUNTY_2022_XLSX, sheet_name="county_2022")

    # Skip the metric-code header row and state-level rows
    data = raw.iloc[1:].copy()
    data.columns = raw.columns

    # Filter to our states, skip state-level rows (county name is NaN)
    data = data[data["Unnamed: 3"].isin(["AL", "FL", "GA"])]
    data = data[data["Unnamed: 2"].notna()]

    # Build FIPS from countyCode column
    data["fips_5digit"] = data["Unnamed: 0"].astype(str).str.zfill(5)

    # Select and rename columns
    keep = {"fips_5digit": "fips_5digit"}
    for src, dst in COLUMN_MAP.items():
        if src in data.columns:
            keep[src] = dst

    result = data[list(keep.keys())].rename(columns=keep)

    for col in COLUMN_MAP.values():
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")

    return result


def main() -> None:
    print("=" * 60)
    print("11_county_education.py — County Education & Demographics")
    print("=" * 60)

    ensure_dirs()
    APP_TIER1_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Load crosswalk
    # ------------------------------------------------------------------
    print("\n[1/4] Loading crosswalk ...")
    crosswalk = load_crosswalk()
    print(f"  Crosswalk rows: {len(crosswalk)}")

    # ------------------------------------------------------------------
    # 2. Load county_2022 data
    # ------------------------------------------------------------------
    print("\n[2/4] Loading county_2022 data ...")
    county_data = _load_county_data()
    print(f"  County rows for AL/FL/GA: {len(county_data)}")

    # ------------------------------------------------------------------
    # 3. Join to crosswalk via FIPS
    # ------------------------------------------------------------------
    print("\n[3/4] Joining to communities via FIPS ...")
    merged = crosswalk[["canonical_id", "fips_5digit"]].merge(
        county_data,
        on="fips_5digit",
        how="left",
    )

    matched = merged["hs_graduation_rate"].notna().sum()
    print(f"  Matched: {matched}/{len(merged)}")

    output = merged[OUTPUT_COLUMNS].copy()

    # Round numeric columns
    for col in OUTPUT_COLUMNS[2:]:
        output[col] = pd.to_numeric(output[col], errors="coerce").round(6)

    # ------------------------------------------------------------------
    # 4. Save and upload
    # ------------------------------------------------------------------
    csv_path = TIER1_DIR / "county_education.csv"
    output.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path} ({len(output)} rows)")

    app_path = APP_TIER1_DIR / "county_education.csv"
    output.to_csv(app_path, index=False)
    print(f"  Saved: {app_path} ({len(output)} rows)")

    print("\n[4/4] Uploading to S3 and registering Athena table ...")
    try:
        s3_uri = upload_to_s3(csv_path, "county_education")
        register_athena_table("tier1_county_education", ATHENA_COLUMNS, s3_uri)
    except Exception as e:
        print(f"\n  Skipping S3/Athena: {e}")

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    print("\n--- Validation ---")
    total = len(output)
    print(f"  Total rows: {total}")

    for col in OUTPUT_COLUMNS[2:]:
        vals = pd.to_numeric(output[col], errors="coerce")
        non_null = vals.notna().sum()
        if non_null > 0:
            print(
                f"  {col}: {non_null}/{total} "
                f"[{vals.min():.4f} - {vals.max():.4f}], "
                f"mean={vals.mean():.4f}"
            )
        else:
            print(f"  {col}: {non_null}/{total} (all NaN)")

    print("\nDone.")


if __name__ == "__main__":
    main()
