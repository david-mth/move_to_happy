"""EPA AQI Annual Summaries for all MTH community counties.

Fetches annual average concentrations for four pollutants (PM2.5, Ozone,
SO2, CO) from the EPA AQS API for every county in Georgia, Alabama,
and Florida.  Three states x four pollutants = 12 API calls total
(byState returns all county-level monitors at once).

Usage:
    EPA_API_EMAIL=<email> EPA_API_KEY=<key> \
        poetry run python scripts/tier1/03_epa_air_quality.py
"""

from __future__ import annotations

import sys
from pathlib import Path

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
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://aqs.epa.gov/data/api/annualData/byState"

# (param_code, friendly_column_name)
POLLUTANTS: list[tuple[str, str]] = [
    ("88101", "pm25_mean"),
    ("44201", "ozone_mean"),
    ("42401", "so2_mean"),
    ("42101", "co_mean"),
]

RATE_LIMIT = 6.0  # 10 req/min -> ~6 seconds between calls

OUTPUT_COLUMNS = [
    "canonical_id",
    "fips_5digit",
    "pm25_mean",
    "ozone_mean",
    "so2_mean",
    "co_mean",
    "aqi_monitors_count",
]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("fips_5digit", "string"),
    ("pm25_mean", "double"),
    ("ozone_mean", "double"),
    ("so2_mean", "double"),
    ("co_mean", "double"),
    ("aqi_monitors_count", "int"),
]


# ---------------------------------------------------------------------------
# Fetch one state x pollutant combination
# ---------------------------------------------------------------------------
def fetch_state_pollutant(
    state_fips: str,
    param_code: str,
    email: str,
    api_key: str,
) -> list[dict]:
    """Fetch annual summary data for one state/pollutant pair.

    Returns the list of monitor records from the API ``Data`` key.
    Uses disk cache to avoid repeat calls.
    """
    cache_key = f"{state_fips}_{param_code}"
    cached = load_cached("epa", cache_key)
    if cached is not None:
        print(f"    Cache hit: state {state_fips} param {param_code}")
        return cached  # type: ignore[return-value]

    params = {
        "email": email,
        "key": api_key,
        "param": param_code,
        "bdate": "20240101",
        "edate": "20241231",
        "state": state_fips,
    }

    print(f"    Fetching state {state_fips} param {param_code} ...")
    resp = api_get(BASE_URL, params=params, rate_limit=RATE_LIMIT, timeout=60)
    payload = resp.json()
    data: list[dict] = payload.get("Data", [])

    save_cache("epa", cache_key, data)
    print(f"    Cached: {len(data)} monitor records")
    return data


# ---------------------------------------------------------------------------
# Aggregate monitor records to county-level means
# ---------------------------------------------------------------------------
def aggregate_to_county(
    records: list[dict],
) -> pd.DataFrame:
    """Group monitor records by county FIPS and take mean of arithmetic_mean.

    Returns a DataFrame with columns ``fips_5digit`` and ``arithmetic_mean``.
    """
    if not records:
        return pd.DataFrame(columns=["fips_5digit", "arithmetic_mean"])

    df = pd.DataFrame(records)
    # Build 5-digit FIPS from state_code + county_code
    df["fips_5digit"] = df["state_code"].astype(str).str.zfill(2) + df[
        "county_code"
    ].astype(str).str.zfill(3)
    df["arithmetic_mean"] = pd.to_numeric(df["arithmetic_mean"], errors="coerce")

    county_mean = df.groupby("fips_5digit")["arithmetic_mean"].mean().reset_index()
    return county_mean


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Fetch, process, and store EPA AQI annual summaries."""
    print("=" * 60)
    print("03_epa_air_quality.py - EPA AQI Annual Summaries 2024")
    print("=" * 60)

    ensure_dirs()

    email = require_env("EPA_API_EMAIL")
    api_key = require_env("EPA_API_KEY")

    # ------------------------------------------------------------------
    # 1. Load crosswalk
    # ------------------------------------------------------------------
    print("\n[1/6] Loading county FIPS crosswalk ...")
    crosswalk = load_crosswalk()
    print(f"  Crosswalk rows: {len(crosswalk)}")
    print(f"  Unique FIPS: {crosswalk['fips_5digit'].nunique()}")

    # ------------------------------------------------------------------
    # 2. Fetch all state x pollutant combinations (12 calls)
    # ------------------------------------------------------------------
    print("\n[2/6] Fetching EPA AQS data (3 states x 4 pollutants = 12 calls) ...")

    # Collect per-pollutant county-level DataFrames
    pollutant_frames: dict[str, pd.DataFrame] = {}

    # Track total monitor records per county (across all pollutants)
    all_monitor_records: list[dict] = []

    for param_code, col_name in POLLUTANTS:
        state_dfs: list[pd.DataFrame] = []
        for state_name, state_fips in sorted(STATE_FIPS.items(), key=lambda x: x[1]):
            print(f"\n  {state_name} (FIPS {state_fips}) - {col_name}:")
            records = fetch_state_pollutant(state_fips, param_code, email, api_key)
            all_monitor_records.extend(records)
            county_df = aggregate_to_county(records)
            state_dfs.append(county_df)

        combined = pd.concat(state_dfs, ignore_index=True)
        combined = combined.rename(columns={"arithmetic_mean": col_name})
        pollutant_frames[col_name] = combined

    # ------------------------------------------------------------------
    # 3. Pivot pollutants into columns and count monitors
    # ------------------------------------------------------------------
    print("\n[3/6] Pivoting pollutants into columns ...")

    # Start with unique FIPS from all pollutant frames
    all_fips: set[str] = set()
    for df in pollutant_frames.values():
        all_fips.update(df["fips_5digit"].tolist())

    result_df = pd.DataFrame({"fips_5digit": sorted(all_fips)})

    for col_name, pdf in pollutant_frames.items():
        result_df = result_df.merge(
            pdf[["fips_5digit", col_name]],
            on="fips_5digit",
            how="left",
        )

    # Count unique monitors per county across all pollutants
    if all_monitor_records:
        monitor_df = pd.DataFrame(all_monitor_records)
        monitor_df["fips_5digit"] = monitor_df["state_code"].astype(str).str.zfill(
            2
        ) + monitor_df["county_code"].astype(str).str.zfill(3)
        monitor_counts = (
            monitor_df.groupby("fips_5digit")
            .size()
            .reset_index(name="aqi_monitors_count")
        )
        result_df = result_df.merge(monitor_counts, on="fips_5digit", how="left")
    else:
        result_df["aqi_monitors_count"] = 0

    result_df["aqi_monitors_count"] = (
        result_df["aqi_monitors_count"].fillna(0).astype(int)
    )

    print(f"  Counties with AQI data: {len(result_df)}")

    # ------------------------------------------------------------------
    # 4. Join crosswalk -> canonical_id
    # ------------------------------------------------------------------
    print("\n[4/6] Joining with crosswalk ...")
    merged = crosswalk[["canonical_id", "fips_5digit"]].merge(
        result_df,
        on="fips_5digit",
        how="left",
    )

    output = merged[OUTPUT_COLUMNS].copy()
    print(f"  Output rows: {len(output)}")

    # ------------------------------------------------------------------
    # 5. Save CSV
    # ------------------------------------------------------------------
    print("\n[5/6] Saving CSV ...")
    csv_path = TIER1_DIR / "epa_air_quality.csv"
    output.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    # ------------------------------------------------------------------
    # 6. Upload to S3 and register Athena table
    # ------------------------------------------------------------------
    print("\n[6/6] Uploading to S3 and registering Athena table ...")
    s3_uri = upload_to_s3(csv_path, "epa_air_quality")
    register_athena_table("tier1_epa_air_quality", ATHENA_COLUMNS, s3_uri)

    # ------------------------------------------------------------------
    # Validation summary
    # ------------------------------------------------------------------
    print("\n--- Validation ---")
    total = len(output)
    coverage = output["pm25_mean"].notna().sum()
    coverage_pct = coverage / total * 100 if total > 0 else 0.0
    print(f"  Total rows: {total}")
    print(f"  Coverage (non-null PM2.5): {coverage}/{total} ({coverage_pct:.1f}%)")

    for col_name in ["pm25_mean", "ozone_mean", "so2_mean", "co_mean"]:
        mean_val = output[col_name].mean()
        if pd.notna(mean_val):
            print(f"  Mean {col_name}: {mean_val:.4f}")
        else:
            print(f"  Mean {col_name}: N/A (no data)")

    aqi_cols = ["pm25_mean", "ozone_mean", "so2_mean", "co_mean"]
    null_counts = output[aqi_cols].isna().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    if not cols_with_nulls.empty:
        print("  Columns with nulls:")
        for col, cnt in cols_with_nulls.items():
            print(f"    {col}: {cnt} null(s)")

    print("\nDone.")


if __name__ == "__main__":
    main()
