"""FBI Crime Data Explorer — county-level crime rates for all MTH communities.

Fetches crime statistics from the FBI Crime Data Explorer (CDE) API at
agency granularity, then aggregates to county level.

Strategy:
  1. Fetch agency lists per state (keyed by county name, gives ORI codes)
  2. For each county, pick the largest agency by population as representative
  3. Query violent-crime and property-crime for that agency (2 API calls)
  4. Query all 8 offense types at state level (24 API calls total)
  5. Use agency-level composite rates for county differentiation, and
     distribute into sub-categories using state-level proportions
  6. Fall back to state-level rates for counties with no agency data

The FBI CDE API base: https://api.usa.gov/crime/fbi/cde

All crime rates are normalized to per-100,000 population (annual).

Usage:
    FBI_API_KEY=<key> poetry run python scripts/tier1/06_fbi_crime.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

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
BASE_URL = "https://api.usa.gov/crime/fbi/cde"

STATE_ABBREV: dict[str, str] = {"01": "AL", "12": "FL", "13": "GA"}
STATE_NAME_MAP: dict[str, str] = {"AL": "Alabama", "FL": "Florida", "GA": "Georgia"}

VIOLENT_SUBCATEGORIES = [
    ("homicide", "murder_rate"),
    ("robbery", "robbery_rate"),
    ("aggravated-assault", "agg_assault_rate"),
]
PROPERTY_SUBCATEGORIES = [
    ("burglary", "burglary_rate"),
    ("larceny", "larceny_rate"),
    ("motor-vehicle-theft", "motor_vehicle_theft_rate"),
]

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

OUTPUT_COLUMNS = ["canonical_id", "fips_5digit", *RATE_COLUMNS]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("fips_5digit", "string"),
    *[(col, "double") for col in RATE_COLUMNS],
]

RATE_LIMIT = 0.3
YEAR = 2022
FROM_DATE = f"01-{YEAR}"
TO_DATE = f"12-{YEAR}"
CACHE_SOURCE = "fbi"

APP_TIER1_DIR = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "tier1"


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def _fbi_get(endpoint: str, api_key: str, cache_key: str) -> dict | None:
    """Call the FBI CDE API with caching. Returns parsed JSON or None."""
    cached = load_cached(CACHE_SOURCE, cache_key)
    if cached is not None:
        return cached

    url = f"{BASE_URL}{endpoint}"
    params = {"API_KEY": api_key}
    try:
        resp = api_get(url, params=params, rate_limit=RATE_LIMIT, timeout=120)
        payload = resp.json()
        save_cache(CACHE_SOURCE, cache_key, payload)
        return payload
    except Exception as exc:  # noqa: BLE001
        print(f"    API error ({endpoint}): {exc}")
        return None


def _extract_annual_rate(
    data: dict,
    state_abbr: str,
) -> tuple[float | None, float | None]:
    """Extract annual offense rate and actuals from a summarized response.

    Returns (annual_rate_per_100k, annual_actuals) for the agency, or
    for the state if no agency data found.
    """
    offenses = data.get("offenses", {})
    rates = offenses.get("rates", {})
    actuals = offenses.get("actuals", {})
    state_name = STATE_NAME_MAP.get(state_abbr, state_abbr)

    # Find agency-specific data (not state, not US)
    skip_prefixes = {state_name, "United States"}

    agency_rate = None
    for key, monthly in rates.items():
        if "Offenses" in key and not any(key.startswith(s) for s in skip_prefixes):
            if isinstance(monthly, dict):
                agency_rate = sum(v for v in monthly.values() if v is not None)
            break

    agency_actuals = None
    for key, monthly in actuals.items():
        if "Offenses" in key and not any(key.startswith(s) for s in skip_prefixes):
            if isinstance(monthly, dict):
                agency_actuals = sum(v for v in monthly.values() if v is not None)
            break

    return agency_rate, agency_actuals


def _extract_state_annual_rate(data: dict, state_abbr: str) -> float | None:
    """Extract the state-level annual rate from a summarized response."""
    rates = data.get("offenses", {}).get("rates", {})
    state_name = STATE_NAME_MAP.get(state_abbr, state_abbr)
    state_key = f"{state_name} Offenses"
    monthly = rates.get(state_key, {})
    if monthly and isinstance(monthly, dict):
        return sum(v for v in monthly.values() if v is not None)
    return None


# ---------------------------------------------------------------------------
# Fetch agency list per state
# ---------------------------------------------------------------------------
def fetch_agencies(state_abbr: str, api_key: str) -> dict[str, list[dict]]:
    """Fetch agencies grouped by county name for a state."""
    cache_key = f"agencies_{state_abbr}"
    data = _fbi_get(f"/agency/byStateAbbr/{state_abbr}", api_key, cache_key)
    if data is None or not isinstance(data, dict):
        return {}
    return data


def pick_representative_agency(agencies: list[dict]) -> dict | None:
    """Pick the best representative agency for a county.

    Prefers county-type agencies (sheriff), then the first available.
    """
    county_agencies = [a for a in agencies if a.get("agency_type_name") == "County"]
    if county_agencies:
        return county_agencies[0]
    return agencies[0] if agencies else None


# ---------------------------------------------------------------------------
# Fetch state-level rates for all offense types
# ---------------------------------------------------------------------------
def fetch_all_state_rates(
    state_abbr: str,
    api_key: str,
) -> dict[str, float | None]:
    """Fetch state-level annual rates for all offense types."""
    result: dict[str, float | None] = {col: None for col in RATE_COLUMNS}

    offense_map = [
        ("violent-crime", "violent_crime_rate"),
        ("property-crime", "property_crime_rate"),
        *VIOLENT_SUBCATEGORIES,
        *PROPERTY_SUBCATEGORIES,
    ]

    for offense_slug, rate_col in offense_map:
        cache_key = f"state_{state_abbr}_{offense_slug}_{YEAR}"
        endpoint = (
            f"/summarized/state/{state_abbr}/{offense_slug}"
            f"?from={FROM_DATE}&to={TO_DATE}"
        )
        data = _fbi_get(endpoint, api_key, cache_key)
        if data is not None:
            rate = _extract_state_annual_rate(data, state_abbr)
            result[rate_col] = round(rate, 2) if rate is not None else None

    return result


# ---------------------------------------------------------------------------
# Fetch agency-level composite rates (violent + property only)
# ---------------------------------------------------------------------------
def fetch_agency_composites(
    ori: str,
    state_abbr: str,
    api_key: str,
) -> dict[str, float | None]:
    """Fetch violent-crime and property-crime annual rates for an agency."""
    result: dict[str, float | None] = {
        "violent_crime_rate": None,
        "property_crime_rate": None,
    }

    for offense_slug, rate_col in [
        ("violent-crime", "violent_crime_rate"),
        ("property-crime", "property_crime_rate"),
    ]:
        cache_key = f"agency_{ori}_{offense_slug}_{YEAR}"
        endpoint = (
            f"/summarized/agency/{ori}/{offense_slug}?from={FROM_DATE}&to={TO_DATE}"
        )
        data = _fbi_get(endpoint, api_key, cache_key)
        if data is not None:
            rate, _ = _extract_annual_rate(data, state_abbr)
            result[rate_col] = round(rate, 2) if rate is not None else None

    return result


# ---------------------------------------------------------------------------
# Distribute composite rates into sub-categories using state proportions
# ---------------------------------------------------------------------------
def distribute_subcategories(
    agency_rates: dict[str, float | None],
    state_rates: dict[str, float | None],
) -> dict[str, float | None]:
    """Given agency violent/property rates, estimate sub-category rates.

    Uses the state-level proportions to distribute the agency composite
    into murder, robbery, agg_assault (from violent) and burglary, larceny,
    motor_vehicle_theft (from property).
    """
    result = dict(agency_rates)

    # Distribute violent crime sub-categories
    state_violent = state_rates.get("violent_crime_rate")
    agency_violent = agency_rates.get("violent_crime_rate")
    if state_violent and agency_violent and state_violent > 0:
        ratio = agency_violent / state_violent
        for _, rate_col in VIOLENT_SUBCATEGORIES:
            state_val = state_rates.get(rate_col)
            if state_val is not None:
                result[rate_col] = round(state_val * ratio, 2)

    # Distribute property crime sub-categories
    state_property = state_rates.get("property_crime_rate")
    agency_property = agency_rates.get("property_crime_rate")
    if state_property and agency_property and state_property > 0:
        ratio = agency_property / state_property
        for _, rate_col in PROPERTY_SUBCATEGORIES:
            state_val = state_rates.get(rate_col)
            if state_val is not None:
                result[rate_col] = round(state_val * ratio, 2)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Fetch, process, and store FBI crime rate data."""
    print("=" * 60)
    print("06_fbi_crime.py - FBI Crime Data Explorer (Agency-Level)")
    print("=" * 60)

    ensure_dirs()
    APP_TIER1_DIR.mkdir(parents=True, exist_ok=True)

    api_key = require_env("FBI_API_KEY")

    # ------------------------------------------------------------------
    # 1. Load crosswalk
    # ------------------------------------------------------------------
    print("\n[1/6] Loading county FIPS crosswalk ...")
    crosswalk = load_crosswalk()
    print(f"  Crosswalk rows: {len(crosswalk)}")
    unique_counties = crosswalk[["fips_5digit", "county_name_census"]].drop_duplicates()
    print(f"  Unique counties: {len(unique_counties)}")

    county_name_to_fips: dict[str, str] = {}
    for _, row in unique_counties.iterrows():
        name = str(row["county_name_census"])
        clean = name.replace(" County", "").strip().upper()
        county_name_to_fips[clean] = row["fips_5digit"]

    # ------------------------------------------------------------------
    # 2. Fetch agency lists and build county -> ORI mapping
    # ------------------------------------------------------------------
    print("\n[2/6] Fetching agency lists per state ...")
    county_agency: dict[str, tuple[str, str, str]] = {}  # fips -> (ori, name, st_abbr)

    for state_fips, state_abbr in sorted(STATE_ABBREV.items()):
        state_name = {v: k for k, v in STATE_FIPS.items()}.get(state_fips, state_abbr)
        print(f"\n  {state_name} ({state_abbr}):")

        agencies_by_county = fetch_agencies(state_abbr, api_key)
        matched = 0

        for county_name_upper, agency_list in agencies_by_county.items():
            fips = county_name_to_fips.get(county_name_upper)
            if fips is None:
                continue

            rep = pick_representative_agency(agency_list)
            if rep is not None:
                county_agency[fips] = (rep["ori"], rep["agency_name"], state_abbr)
                matched += 1

        print(f"    Matched {matched} counties to agencies")

    print(f"\n  Total counties with agencies: {len(county_agency)}")

    # ------------------------------------------------------------------
    # 3. Fetch state-level rates (all 8 offense types)
    # ------------------------------------------------------------------
    print("\n[3/6] Fetching state-level rates (all offense types) ...")
    state_rates: dict[str, dict[str, float | None]] = {}

    for state_fips, state_abbr in sorted(STATE_ABBREV.items()):
        state_name = {v: k for k, v in STATE_FIPS.items()}.get(state_fips, state_abbr)
        print(f"  {state_name} ({state_abbr}) ...")
        rates = fetch_all_state_rates(state_abbr, api_key)
        state_rates[state_fips] = rates
        has = sum(1 for v in rates.values() if v is not None)
        print(f"    {has}/{len(RATE_COLUMNS)} offense types available")
        for col, val in rates.items():
            if val is not None:
                print(f"      {col}: {val}")

    # ------------------------------------------------------------------
    # 4. Fetch agency-level composite rates (violent + property only)
    # ------------------------------------------------------------------
    print("\n[4/6] Fetching agency-level crime data (violent + property) ...")
    county_rates: dict[str, dict[str, float | None]] = {}
    total_counties = len(county_agency)

    for processed, (fips, (ori, _agency_name, st_abbr)) in enumerate(
        sorted(county_agency.items()), 1
    ):
        composites = fetch_agency_composites(ori, st_abbr, api_key)

        has_any = any(v is not None for v in composites.values())
        if has_any:
            st_fips = fips[:2]
            full_rates = distribute_subcategories(
                composites, state_rates.get(st_fips, {})
            )
            county_rates[fips] = full_rates

        if processed % 25 == 0 or processed == total_counties:
            print(f"  [{processed}/{total_counties}] agencies queried")

    print(f"\n  Counties with agency-level data: {len(county_rates)}")

    # ------------------------------------------------------------------
    # 5. Build output DataFrame
    # ------------------------------------------------------------------
    print("\n[5/6] Building output ...")

    all_fips = crosswalk[["fips_5digit"]].drop_duplicates().copy()
    all_fips["state_fips_2"] = all_fips["fips_5digit"].str[:2]

    crime_df = all_fips.copy()
    for col in RATE_COLUMNS:
        crime_df[col] = np.nan

    # Layer 1: state-level fallback
    state_filled = 0
    for state_fips, rates in state_rates.items():
        mask = crime_df["state_fips_2"] == state_fips
        count = mask.sum()
        if count > 0:
            for col in RATE_COLUMNS:
                if rates.get(col) is not None:
                    crime_df.loc[mask, col] = rates[col]
            state_filled += count
    print(f"  State-level baseline applied to {state_filled} counties")

    # Layer 2: agency-level data overrides
    agency_upgraded = 0
    for fips, rates in county_rates.items():
        mask = crime_df["fips_5digit"] == fips
        for col in RATE_COLUMNS:
            if rates.get(col) is not None:
                crime_df.loc[mask, col] = rates[col]
        if mask.any():
            agency_upgraded += 1
    print(f"  Agency-level data applied to {agency_upgraded} counties")

    crime_df = crime_df.drop(columns=["state_fips_2"])

    merged = crosswalk[["canonical_id", "fips_5digit"]].merge(
        crime_df, on="fips_5digit", how="left"
    )
    output = merged[OUTPUT_COLUMNS].copy()

    # Clamp negative rates to zero (API data anomalies)
    for col in RATE_COLUMNS:
        output[col] = output[col].clip(lower=0)

    # Save CSVs
    csv_path = TIER1_DIR / "fbi_crime.csv"
    output.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path} ({len(output)} rows)")

    app_path = APP_TIER1_DIR / "fbi_crime.csv"
    output.to_csv(app_path, index=False)
    print(f"  Saved: {app_path} ({len(output)} rows)")

    # ------------------------------------------------------------------
    # 6. Upload to S3 and register Athena table
    # ------------------------------------------------------------------
    print("\n[6/6] Uploading to S3 and registering Athena table ...")
    try:
        s3_uri = upload_to_s3(csv_path, "fbi_crime")
        register_athena_table("tier1_fbi_crime", ATHENA_COLUMNS, s3_uri)
    except Exception as e:
        print(f"\nSkipping S3/Athena (no AWS creds): {e}")

    # ------------------------------------------------------------------
    # Validation summary
    # ------------------------------------------------------------------
    print("\n--- Validation ---")
    total = len(output)

    has_any = output[RATE_COLUMNS].notna().any(axis=1).sum()
    has_any_pct = has_any / total * 100 if total > 0 else 0.0
    print(f"  Total rows: {total}")
    print(f"  Coverage (any crime data): {has_any}/{total} ({has_any_pct:.1f}%)")
    print(f"  Counties with agency-level granularity: {agency_upgraded}")

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

    unique_violent = output["violent_crime_rate"].nunique()
    unique_property = output["property_crime_rate"].nunique()
    print(f"\n  Distinct violent_crime_rate values: {unique_violent}")
    print(f"  Distinct property_crime_rate values: {unique_property}")

    missing_all = output[RATE_COLUMNS].isna().all(axis=1).sum()
    if missing_all > 0:
        print(f"\n  WARNING: {missing_all} communities have no crime data at all.")

    print("\nDone.")


if __name__ == "__main__":
    main()
