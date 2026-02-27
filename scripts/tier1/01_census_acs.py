"""Census ACS 2024 economic and housing data for all MTH communities.

Fetches DP03 (economic) and DP04 (housing) profile variables from the
Census Bureau ACS 5-year estimates API for every county covering the
~1,305 communities across Georgia, Alabama, and Florida.

Strategy: one API call per state (3 total) returning all counties,
then join to the county FIPS crosswalk to fan out to canonical_id.

Usage:
    CENSUS_API_KEY=<key> poetry run python scripts/tier1/01_census_acs.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _config import get_session, load_pipeline_config  # noqa: F401

from tier1._helpers import (
    STATE_FIPS,
    TIER1_DIR,
    api_get,
    ensure_dirs,
    load_cached,
    load_crosswalk,
    register_athena_table,
    require_env,
    save_cache,
    upload_to_s3,
)

# ---------------------------------------------------------------------------
# Census variable definitions
# ---------------------------------------------------------------------------

# Ordered list of (census_code, friendly_name) tuples
VARIABLES: list[tuple[str, str]] = [
    # DP03 – economic
    ("DP03_0062E", "median_household_income"),
    ("DP03_0009PE", "unemployment_rate"),
    ("DP03_0119PE", "poverty_rate"),
    ("DP03_0021PE", "commute_public_transit_pct"),
    ("DP03_0024PE", "commute_work_from_home_pct"),
    ("DP03_0025E", "mean_commute_minutes"),
    ("DP03_0088E", "per_capita_income"),
    # DP04 – housing
    ("DP04_0089E", "median_home_value"),
    ("DP04_0134E", "median_rent"),
    ("DP04_0046PE", "pct_owner_occupied"),
    ("DP04_0047PE", "pct_renter_occupied"),
    ("DP04_0003PE", "vacancy_rate"),
    ("DP04_0007PE", "single_family_pct"),
]

CENSUS_CODES = [code for code, _ in VARIABLES]
FRIENDLY_NAMES = {code: name for code, name in VARIABLES}

NUMERIC_COLS = [name for _, name in VARIABLES]

# Output column order
OUTPUT_COLUMNS = [
    "canonical_id",
    "fips_5digit",
    "median_household_income",
    "unemployment_rate",
    "poverty_rate",
    "commute_public_transit_pct",
    "commute_work_from_home_pct",
    "mean_commute_minutes",
    "per_capita_income",
    "median_home_value",
    "median_rent",
    "pct_owner_occupied",
    "pct_renter_occupied",
    "vacancy_rate",
    "single_family_pct",
]

# Athena column schema: canonical_id and fips_5digit are strings, rest double
ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("fips_5digit", "string"),
] + [(col, "double") for col in OUTPUT_COLUMNS[2:]]

ACS_URL = "https://api.census.gov/data/2024/acs/acs5/profile"


# ---------------------------------------------------------------------------
# Per-state fetch
# ---------------------------------------------------------------------------


def fetch_state(state_fips: str, api_key: str) -> list[list[str]]:
    """Fetch all ACS variables for every county in one state.

    Returns the raw JSON array (first row = headers, rest = data rows).
    Uses disk cache to avoid repeat calls.
    """
    cache_key = f"state_{state_fips}"
    cached = load_cached("census_acs", cache_key)
    if cached is not None:
        print(f"  Cache hit: state {state_fips} ({len(cached) - 1} counties)")
        return cached  # type: ignore[return-value]

    var_list = ",".join(CENSUS_CODES)
    params = {
        "get": var_list,
        "for": "county:*",
        "in": f"state:{state_fips}",
        "key": api_key,
    }

    print(f"  Fetching state FIPS {state_fips} from Census ACS API …")
    resp = api_get(ACS_URL, params=params, rate_limit=0.0, timeout=60)
    data: list[list[str]] = resp.json()

    save_cache("census_acs", cache_key, data)
    print(f"  Fetched and cached: {len(data) - 1} counties")
    return data


# ---------------------------------------------------------------------------
# Parse raw Census JSON into a DataFrame
# ---------------------------------------------------------------------------


def parse_response(raw: list[list[str]]) -> pd.DataFrame:
    """Convert Census JSON array (header row + data rows) to a DataFrame.

    The Census API appends 'state' and 'county' columns automatically.
    Numeric columns are coerced to float; sentinel values like '-666666666'
    or '-' are converted to NaN.
    """
    headers = raw[0]
    rows = raw[1:]
    df = pd.DataFrame(rows, columns=headers)

    # Rename Census codes to friendly names
    df = df.rename(columns=FRIENDLY_NAMES)

    # Build fips_5digit from the state / county geo columns appended by API
    df["fips_5digit"] = df["state"].str.zfill(2) + df["county"].str.zfill(3)

    # Coerce numeric columns: Census uses "-" and large negatives as nulls
    sentinel = -666_666_666
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].where(df[col] != sentinel, other=np.nan)
        df[col] = df[col].where(df[col] > sentinel, other=np.nan)

    return df[["fips_5digit"] + NUMERIC_COLS]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Fetch, process, and store Census ACS data for all MTH communities."""
    print("=" * 60)
    print("01_census_acs.py – Census ACS 2024 economic & housing data")
    print("=" * 60)

    ensure_dirs()

    api_key = require_env("CENSUS_API_KEY")

    # Load county FIPS crosswalk: provides canonical_id → fips_5digit mapping
    print("\n[1/5] Loading county FIPS crosswalk …")
    crosswalk = load_crosswalk()
    print(f"  Crosswalk rows: {len(crosswalk)}")
    print(f"  Unique FIPS: {crosswalk['fips_5digit'].nunique()}")
    print(f"  Unique canonical_ids: {crosswalk['canonical_id'].nunique()}")

    # Fetch all three states
    print("\n[2/5] Fetching Census ACS data (3 state calls) …")
    state_frames: list[pd.DataFrame] = []

    for state_name, state_fips in sorted(STATE_FIPS.items(), key=lambda x: x[1]):
        print(f"\n  {state_name} (FIPS {state_fips}):")
        raw = fetch_state(state_fips, api_key)
        df_state = parse_response(raw)
        state_frames.append(df_state)

    # Combine all states
    print("\n[3/5] Combining states and merging with crosswalk …")
    acs_df = pd.concat(state_frames, ignore_index=True)
    print(f"  Total counties fetched: {len(acs_df)}")
    print(f"  Unique FIPS in ACS data: {acs_df['fips_5digit'].nunique()}")

    # Merge with crosswalk: many communities share one county
    merged = crosswalk[["canonical_id", "fips_5digit"]].merge(
        acs_df,
        on="fips_5digit",
        how="left",
    )

    # Select and order output columns
    result = merged[OUTPUT_COLUMNS].copy()

    coverage = result["median_household_income"].notna().sum()
    coverage_pct = coverage / len(result) * 100
    print(f"  Output rows (communities × counties): {len(result)}")
    print(
        f"  Coverage (non-null median_hhi): {coverage}/{len(result)} "
        f"({coverage_pct:.1f}%)"
    )

    # Save CSV
    print("\n[4/5] Saving CSV …")
    csv_path = TIER1_DIR / "census_acs.csv"
    result.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    # Upload to S3
    print("\n[5/5] Uploading to S3 and registering Athena table …")
    s3_uri = upload_to_s3(csv_path, "census_acs")
    register_athena_table("tier1_census_acs", ATHENA_COLUMNS, s3_uri)

    # Validation summary
    print("\n--- Validation ---")
    print(f"  Total rows: {len(result)}")
    numeric_summary = {
        "median_household_income": result["median_household_income"].median(),
        "median_home_value": result["median_home_value"].median(),
        "median_rent": result["median_rent"].median(),
        "unemployment_rate": result["unemployment_rate"].median(),
        "poverty_rate": result["poverty_rate"].median(),
    }
    for col, val in numeric_summary.items():
        print(
            f"  Median {col}: {val:,.1f}" if pd.notna(val) else f"  Median {col}: N/A"
        )

    null_counts = result[NUMERIC_COLS].isna().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    if not cols_with_nulls.empty:
        print("  Columns with nulls:")
        for col, cnt in cols_with_nulls.items():
            print(f"    {col}: {cnt} null(s)")
    else:
        print("  No null values in numeric columns.")

    print("\nDone.")


if __name__ == "__main__":
    main()
