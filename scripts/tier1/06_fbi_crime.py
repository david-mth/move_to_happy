"""FBI Crime Data Explorer — crime rates for all MTH community counties.

Fetches crime statistics from the FBI Crime Data Explorer (CDE) API.
The CDE API is known to have inconsistent availability, so this script
uses a tiered fallback strategy:

  1. Static state-level rates from the FBI's published "Crime in the
     United States, 2024" report — always available, used as baseline
  2. County-level NIBRS offense data per state (via API) — used to
     upgrade from state-level to county-level granularity when available
  3. State-level crime estimates from the API — used to get fresher
     numbers than the static fallback when the API is reachable
  4. Graceful degradation: static rates guarantee 100% coverage even
     when the API is completely down (403/503 errors are common)

The FBI CDE API migrated from /sapi to /cde in 2025.  Both base URLs
are attempted.

All crime rates are normalized to per-100,000 population.

Usage:
    FBI_API_KEY=<key> poetry run python scripts/tier1/06_fbi_crime.py
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
# Constants
# ---------------------------------------------------------------------------
BASE_URLS = [
    "https://api.usa.gov/crime/fbi/cde",
    "https://api.usa.gov/crime/fbi/sapi",
]

# State FIPS -> two-letter abbreviation (for FBI API paths)
STATE_ABBREV: dict[str, str] = {
    "01": "AL",
    "12": "FL",
    "13": "GA",
}

# Crime rate columns in output
RATE_COLUMNS = [
    "violent_crime_rate",
    "property_crime_rate",
    "murder_rate",
    "robbery_rate",
    "agg_assault_rate",
    "burglary_rate",
    "larceny_rate",
    "motor_vehicle_theft_rate",
]

OUTPUT_COLUMNS = [
    "canonical_id",
    "fips_5digit",
    *RATE_COLUMNS,
]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("fips_5digit", "string"),
    *[(col, "double") for col in RATE_COLUMNS],
]

RATE_LIMIT = 0.5  # Moderate rate limit for FBI API
YEAR = 2023  # Latest year with reliable data (2024 may not be fully available)

# ---------------------------------------------------------------------------
# Static fallback: FBI "Crime in the United States, 2024" (released Summer 2025)
# Rates are per 100,000 inhabitants.
# Source: https://cde.ucr.cjis.gov / exiledpolicy.com/state-crime-rates-2024/
# ---------------------------------------------------------------------------
_STATIC_STATE_RATES: dict[str, dict[str, float]] = {
    "01": {  # Alabama
        "violent_crime_rate": 359.9,
        "property_crime_rate": 1565.1,
        "murder_rate": 8.7,
        "robbery_rate": 34.0,
        "agg_assault_rate": 291.4,
        "burglary_rate": 243.5,
        "larceny_rate": 1148.5,
        "motor_vehicle_theft_rate": 173.0,
    },
    "12": {  # Florida
        "violent_crime_rate": 267.1,
        "property_crime_rate": 1420.4,
        "murder_rate": 3.9,
        "robbery_rate": 38.2,
        "agg_assault_rate": 197.1,
        "burglary_rate": 152.5,
        "larceny_rate": 1144.9,
        "motor_vehicle_theft_rate": 123.0,
    },
    "13": {  # Georgia
        "violent_crime_rate": 325.7,
        "property_crime_rate": 1674.9,
        "murder_rate": 6.9,
        "robbery_rate": 40.8,
        "agg_assault_rate": 245.9,
        "burglary_rate": 202.8,
        "larceny_rate": 1247.3,
        "motor_vehicle_theft_rate": 224.7,
    },
}


# ---------------------------------------------------------------------------
# FBI API helpers
# ---------------------------------------------------------------------------
def _fbi_get(
    endpoint: str,
    api_key: str,
    cache_source: str,
    cache_key: str,
) -> dict | list | None:
    """Call the FBI CDE API with caching and error handling.

    Tries each base URL in BASE_URLS until one succeeds.
    Returns the parsed JSON response, or None on failure.
    """
    cached = load_cached(cache_source, cache_key)
    if cached is not None:
        print(f"    Cache hit: {cache_key}")
        return cached

    params = {"API_KEY": api_key}

    for base_url in BASE_URLS:
        url = f"{base_url}{endpoint}"
        try:
            resp = api_get(url, params=params, rate_limit=RATE_LIMIT, timeout=30)
            payload = resp.json()
            save_cache(cache_source, cache_key, payload)
            return payload
        except Exception as exc:  # noqa: BLE001
            print(f"    FBI API error ({base_url}{endpoint}): {exc}")
            continue

    return None


# ---------------------------------------------------------------------------
# Strategy 1: County-level NIBRS data
# ---------------------------------------------------------------------------
def fetch_county_data(
    state_abbr: str,
    api_key: str,
) -> pd.DataFrame | None:
    """Try to fetch county-level crime data for one state.

    Tries multiple endpoint formats since the FBI API has migrated
    from /sapi to /cde with different path structures.

    Returns a DataFrame with fips_5digit and crime rate columns,
    or None if the endpoint is unavailable.
    """
    for endpoint in [
        f"/summarized/state/{state_abbr}/county/all/{YEAR}/{YEAR}",
        f"/api/data/nibrs/offense/states/{state_abbr}/county",
    ]:
        cache_key = f"county_{state_abbr}_{endpoint.split('/')[1]}"
        payload = _fbi_get(endpoint, api_key, "fbi", cache_key)
        if payload is not None:
            break
    else:
        payload = None
    if payload is None:
        return None

    # The response can be a dict with "results" or a list directly
    if isinstance(payload, dict):
        records = payload.get("results", payload.get("data", []))
    elif isinstance(payload, list):
        records = payload
    else:
        return None

    if not records:
        print(f"    No county-level records for {state_abbr}")
        return None

    return _parse_county_records(records, state_abbr)


def _parse_county_records(
    records: list[dict],
    state_abbr: str,
) -> pd.DataFrame | None:
    """Parse FBI county-level records into a DataFrame with crime rates.

    The FBI API county-level response format varies; this handles
    common structures.
    """
    rows: list[dict] = []
    state_fips = {v: k for k, v in STATE_ABBREV.items()}.get(state_abbr)

    for rec in records:
        if not isinstance(rec, dict):
            continue

        # Extract county FIPS — try multiple field names
        county_fips = rec.get(
            "county_fips",
            rec.get("fips", rec.get("county_id", "")),
        )
        if not county_fips:
            continue
        county_fips = str(county_fips).zfill(3)

        # Build 5-digit FIPS
        fips_5digit = (
            str(state_fips).zfill(2) + county_fips[-3:] if state_fips else county_fips
        )

        population = _safe_float(rec.get("population", rec.get("census_population")))

        row: dict[str, object] = {"fips_5digit": fips_5digit}
        row.update(_extract_crime_rates(rec, population))
        rows.append(row)

    if not rows:
        return None

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Strategy 2: State-level estimates
# ---------------------------------------------------------------------------
def fetch_state_estimates(
    state_abbr: str,
    api_key: str,
) -> dict[str, float | None] | None:
    """Fetch state-level crime estimates for a given year.

    Tries multiple endpoint formats since the FBI API has migrated
    from /sapi to /cde with different path structures.

    Returns a dict of crime rate columns -> values (per 100k), or
    None if the endpoint is unavailable.
    """
    cache_key = f"state_{state_abbr}"

    # Try CDE-style endpoint first, then legacy SAPI-style
    for endpoint in [
        f"/estimate/state/{state_abbr}/{YEAR}",
        f"/api/estimates/states/{state_abbr}/{YEAR}",
    ]:
        tag = endpoint.split("/")[1]
        payload = _fbi_get(endpoint, api_key, "fbi", f"{cache_key}_{tag}")
        if payload is not None:
            break
    else:
        payload = None
    if payload is None:
        return None

    # The response may be a list of annual estimates or a dict
    if isinstance(payload, list) and len(payload) > 0:
        # Find the record for our target year, or use the latest
        rec = payload[-1]  # Most recent
        for entry in payload:
            if isinstance(entry, dict) and entry.get("year") == YEAR:
                rec = entry
                break
    elif isinstance(payload, dict):
        rec = payload.get("results", payload.get("data", payload))
        if isinstance(rec, list) and len(rec) > 0:
            rec = rec[-1]
    else:
        return None

    if not isinstance(rec, dict):
        return None

    population = _safe_float(rec.get("population"))
    return _extract_crime_rates(rec, population)


def _extract_crime_rates(
    rec: dict,
    population: float | None,
) -> dict[str, float | None]:
    """Extract standardized crime rates from an FBI API record.

    If raw counts are present and population is known, calculates
    per-100,000 rates.  If rates are already provided, uses those
    directly.
    """
    result: dict[str, float | None] = {col: None for col in RATE_COLUMNS}

    # Map of output column -> (count_keys, rate_keys)
    crime_map: dict[str, tuple[list[str], list[str]]] = {
        "violent_crime_rate": (
            ["violent_crime", "violent_crime_total"],
            ["violent_crime_rate"],
        ),
        "property_crime_rate": (
            ["property_crime", "property_crime_total"],
            ["property_crime_rate"],
        ),
        "murder_rate": (
            [
                "homicide",
                "murder_and_nonnegligent_manslaughter",
                "murder",
            ],
            ["murder_rate", "homicide_rate"],
        ),
        "robbery_rate": (
            ["robbery"],
            ["robbery_rate"],
        ),
        "agg_assault_rate": (
            [
                "aggravated_assault",
                "agg_assault",
                "aggravated-assault",
            ],
            ["agg_assault_rate", "aggravated_assault_rate"],
        ),
        "burglary_rate": (
            ["burglary"],
            ["burglary_rate"],
        ),
        "larceny_rate": (
            ["larceny", "larceny_theft", "larceny-theft"],
            ["larceny_rate", "larceny_theft_rate"],
        ),
        "motor_vehicle_theft_rate": (
            [
                "motor_vehicle_theft",
                "motor-vehicle-theft",
            ],
            ["motor_vehicle_theft_rate"],
        ),
    }

    for out_col, (count_keys, rate_keys) in crime_map.items():
        # First try pre-calculated rate
        for rk in rate_keys:
            val = _safe_float(rec.get(rk))
            if val is not None:
                result[out_col] = val
                break

        # If no rate found, try to calculate from count + population
        if result[out_col] is None and population and population > 0:
            for ck in count_keys:
                count = _safe_float(rec.get(ck))
                if count is not None:
                    result[out_col] = count / population * 100_000
                    break

    return result


def _safe_float(val: object) -> float | None:
    """Safely convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        result = float(val)
        return result if not np.isnan(result) else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Fetch, process, and store FBI crime rate data."""
    print("=" * 60)
    print("06_fbi_crime.py - FBI Crime Data Explorer")
    print("=" * 60)

    ensure_dirs()

    api_key = require_env("FBI_API_KEY")

    # ------------------------------------------------------------------
    # 1. Load crosswalk
    # ------------------------------------------------------------------
    print("\n[1/5] Loading county FIPS crosswalk ...")
    crosswalk = load_crosswalk()
    print(f"  Crosswalk rows: {len(crosswalk)}")
    print(f"  Unique FIPS: {crosswalk['fips_5digit'].nunique()}")

    # ------------------------------------------------------------------
    # 2. Apply static baseline (guarantees 100% coverage)
    # ------------------------------------------------------------------
    print("\n[2/5] Applying static FBI 2024 published rates as baseline ...")

    all_fips = crosswalk[["fips_5digit"]].drop_duplicates().copy()
    all_fips["state_fips_2"] = all_fips["fips_5digit"].str[:2]

    crime_df = all_fips.copy()
    for col in RATE_COLUMNS:
        crime_df[col] = np.nan

    static_filled = 0
    for state_fips, rates in _STATIC_STATE_RATES.items():
        mask = crime_df["state_fips_2"] == state_fips
        count = mask.sum()
        if count > 0:
            for col in RATE_COLUMNS:
                crime_df.loc[mask, col] = rates[col]
            static_filled += count

    print(f"  Baseline applied to {static_filled} counties from published rates")

    # ------------------------------------------------------------------
    # 3. Try API for county-level upgrades (best-effort)
    # ------------------------------------------------------------------
    print("\n[3/5] Attempting FBI API for finer-grained data (best-effort) ...")

    county_frames: list[pd.DataFrame] = []
    state_estimates: dict[str, dict[str, float | None]] = {}

    for state_fips, state_abbr in sorted(STATE_ABBREV.items()):
        state_name = {v: k for k, v in STATE_FIPS.items()}.get(state_fips, state_abbr)
        print(f"\n  {state_name} ({state_abbr}):")

        print("    Trying county-level data ...")
        county_df = fetch_county_data(state_abbr, api_key)
        if county_df is not None and not county_df.empty:
            print(f"    County-level: {len(county_df)} counties found")
            county_frames.append(county_df)
        else:
            print(f"    County-level data unavailable for {state_abbr}")

        print("    Trying state-level estimates ...")
        estimates = fetch_state_estimates(state_abbr, api_key)
        if estimates is not None:
            has_data = any(v is not None for v in estimates.values())
            if has_data:
                state_estimates[state_fips] = estimates
                print("    State-level API estimates available")
            else:
                print("    State-level API estimates empty")
        else:
            print("    State-level API estimates unavailable")

    # Overlay county-level data where available (upgrades from state averages)
    if county_frames:
        county_crime = pd.concat(county_frames, ignore_index=True)
        county_crime = county_crime.drop_duplicates(
            subset=["fips_5digit"], keep="first"
        )
        print(f"\n  Overlaying {len(county_crime)} county-level records")
        for _, row in county_crime.iterrows():
            fips = row["fips_5digit"]
            mask = crime_df["fips_5digit"] == fips
            for col in RATE_COLUMNS:
                if pd.notna(row.get(col)):
                    crime_df.loc[mask, col] = row[col]
    else:
        print("\n  No county-level API data available (using static baseline)")

    # Overlay API state-level estimates where they provide fresher data
    api_upgraded = 0
    for state_fips, estimates in state_estimates.items():
        mask = crime_df["state_fips_2"] == state_fips
        for col in RATE_COLUMNS:
            if estimates.get(col) is not None:
                crime_df.loc[mask, col] = estimates[col]
        api_upgraded += mask.sum()

    if api_upgraded > 0:
        print(f"  Upgraded {api_upgraded} counties with API state-level estimates")

    crime_df = crime_df.drop(columns=["state_fips_2"])

    # ------------------------------------------------------------------
    # 4. Join crosswalk -> canonical_id
    # ------------------------------------------------------------------
    print("\n[4/5] Joining with crosswalk (all counties guaranteed) ...")
    merged = crosswalk[["canonical_id", "fips_5digit"]].merge(
        crime_df,
        on="fips_5digit",
        how="left",
    )

    output = merged[OUTPUT_COLUMNS].copy()
    print(f"  Output rows: {len(output)}")

    # Save CSV
    csv_path = TIER1_DIR / "fbi_crime.csv"
    output.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    # ------------------------------------------------------------------
    # 5. Upload to S3 and register Athena table
    # ------------------------------------------------------------------
    print("\n[5/5] Uploading to S3 and registering Athena table ...")
    s3_uri = upload_to_s3(csv_path, "fbi_crime")
    register_athena_table("tier1_fbi_crime", ATHENA_COLUMNS, s3_uri)

    # ------------------------------------------------------------------
    # Validation summary
    # ------------------------------------------------------------------
    print("\n--- Validation ---")
    total = len(output)

    # Overall coverage
    has_any = output[RATE_COLUMNS].notna().any(axis=1).sum()
    has_any_pct = has_any / total * 100 if total > 0 else 0.0
    print(f"  Total rows: {total}")
    print(f"  Coverage (any crime data): {has_any}/{total} ({has_any_pct:.1f}%)")

    # Per-column coverage
    print("  Per-column coverage:")
    for col in RATE_COLUMNS:
        non_null = output[col].notna().sum()
        pct = non_null / total * 100 if total > 0 else 0.0
        valid = output[col].dropna()
        if not valid.empty:
            print(
                f"    {col}: {non_null}/{total} ({pct:.1f}%) "
                f"range [{valid.min():.1f} - {valid.max():.1f}]"
            )
        else:
            print(f"    {col}: {non_null}/{total} ({pct:.1f}%) N/A")

    missing_all = output[RATE_COLUMNS].isna().all(axis=1).sum()
    if missing_all > 0:
        print(f"\n  WARNING: {missing_all} communities have no crime data at all.")
        print(
            "  FBI CDE API has known coverage gaps. State-level "
            "fallback was used where available."
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
