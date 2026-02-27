"""FCC Broadband Availability data for all MTH community counties.

Attempts to fetch county-level broadband availability metrics from
the FCC Broadband Data Collection (BDC) API.  Because the FCC data
format and API endpoints change frequently, this script implements
a multi-tier fallback strategy:

  1. Per-county API call to the BDC county summary endpoint
  2. National summary endpoint (all counties at once)
  3. Graceful degradation: output CSV with NULLs so the Athena table
     schema is registered and downstream scripts are unblocked

Usage:
    poetry run python scripts/tier1/05_fcc_broadband.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _config import get_session, load_pipeline_config  # noqa: F401

from tier1._helpers import (
    TIER1_DIR,
    api_get,
    ensure_dirs,
    load_cached,
    load_crosswalk,
    register_athena_table,
    save_cache,
    upload_to_s3,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PRIMARY_URL = (
    "https://broadbandmap.fcc.gov/api/pub/map/listAvailability/fixed/county/{fips}"
)
NATIONAL_URL = "https://broadbandmap.fcc.gov/api/pub/map/summarize/fixed/county/all"

RATE_LIMIT = 0.5  # Be polite to FCC servers

OUTPUT_COLUMNS = [
    "canonical_id",
    "fips_5digit",
    "pct_broadband_25_3",
    "pct_broadband_100_20",
    "num_providers",
    "max_download_mbps",
]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("fips_5digit", "string"),
    ("pct_broadband_25_3", "double"),
    ("pct_broadband_100_20", "double"),
    ("num_providers", "int"),
    ("max_download_mbps", "double"),
]

BROADBAND_COLS = [
    "pct_broadband_25_3",
    "pct_broadband_100_20",
    "num_providers",
    "max_download_mbps",
]


# ---------------------------------------------------------------------------
# Strategy 1: Per-county API
# ---------------------------------------------------------------------------
def fetch_county_primary(fips: str) -> dict | None:
    """Fetch broadband summary for one county from the primary BDC API.

    Returns a dict with broadband metrics, or None if the endpoint
    fails or returns an unexpected format.
    """
    cached = load_cached("fcc", f"county_{fips}")
    if cached is not None:
        return cached  # type: ignore[return-value]

    url = PRIMARY_URL.format(fips=fips)
    try:
        resp = api_get(url, rate_limit=RATE_LIMIT, timeout=30)
        payload = resp.json()
    except (requests.exceptions.RequestException, ValueError) as exc:
        print(f"    Primary API failed for {fips}: {exc}")
        return None

    # Attempt to extract availability data from the response
    # The FCC API format is not well-documented; try common structures
    data = payload if isinstance(payload, dict) else None
    if data is not None:
        save_cache("fcc", f"county_{fips}", data)
    return data


def parse_county_response(payload: dict) -> dict[str, float | None]:
    """Extract broadband metrics from a county API response.

    Handles multiple possible response formats since the FCC API
    changes frequently.  Returns a dict of metric -> value or None.
    """
    result: dict[str, float | None] = {
        "pct_broadband_25_3": None,
        "pct_broadband_100_20": None,
        "num_providers": None,
        "max_download_mbps": None,
    }

    # Try common response structures
    data = payload.get("data", payload)
    if isinstance(data, list) and len(data) > 0:
        data = data[0]
    if not isinstance(data, dict):
        return result

    # Look for percentage fields under various key names
    for key in ("pct_25_3", "percent_25_3", "broadband_25_3", "pct_bb_25_3"):
        if key in data:
            result["pct_broadband_25_3"] = _safe_float(data[key])
            break

    for key in ("pct_100_20", "percent_100_20", "broadband_100_20", "pct_bb_100_20"):
        if key in data:
            result["pct_broadband_100_20"] = _safe_float(data[key])
            break

    for key in ("num_providers", "provider_count", "providers"):
        if key in data:
            result["num_providers"] = _safe_float(data[key])
            break

    for key in ("max_download", "max_download_speed", "max_dl_speed", "max_down"):
        if key in data:
            result["max_download_mbps"] = _safe_float(data[key])
            break

    return result


def _safe_float(val: object) -> float | None:
    """Safely convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Strategy 2: National summary endpoint
# ---------------------------------------------------------------------------
def fetch_national_summary() -> pd.DataFrame | None:
    """Fetch the national county-level broadband summary.

    Returns a DataFrame indexed by fips_5digit, or None if the
    endpoint fails.
    """
    cached = load_cached("fcc", "national_summary")
    if cached is not None:
        if isinstance(cached, list):
            return _parse_national_data(cached)
        if isinstance(cached, dict):
            data = cached.get("data", cached.get("results", []))
            if isinstance(data, list):
                return _parse_national_data(data)
        return None

    try:
        resp = api_get(NATIONAL_URL, rate_limit=RATE_LIMIT, timeout=120)
        payload = resp.json()
    except (requests.exceptions.RequestException, ValueError) as exc:
        print(f"  National summary API failed: {exc}")
        return None

    if isinstance(payload, dict):
        data = payload.get("data", payload.get("results", []))
    elif isinstance(payload, list):
        data = payload
    else:
        return None

    save_cache("fcc", "national_summary", payload)

    if isinstance(data, list):
        return _parse_national_data(data)
    return None


def _parse_national_data(records: list) -> pd.DataFrame | None:
    """Parse national summary records into a DataFrame."""
    if not records:
        return None

    rows: list[dict] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        fips = rec.get("geoid", rec.get("fips", rec.get("county_fips")))
        if fips is None:
            continue
        fips = str(fips).zfill(5)
        parsed = parse_county_response(rec)
        row: dict = {"fips_5digit": fips, **parsed}
        rows.append(row)

    if not rows:
        return None

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    """Fetch, process, and store FCC broadband availability data."""
    print("=" * 60)
    print("05_fcc_broadband.py - FCC Broadband Availability")
    print("=" * 60)

    ensure_dirs()

    # ------------------------------------------------------------------
    # 1. Load crosswalk
    # ------------------------------------------------------------------
    print("\n[1/5] Loading county FIPS crosswalk ...")
    crosswalk = load_crosswalk()
    target_fips = sorted(crosswalk["fips_5digit"].unique())
    print(f"  Crosswalk rows: {len(crosswalk)}")
    print(f"  Unique target FIPS: {len(target_fips)}")

    # ------------------------------------------------------------------
    # 2. Try Strategy 1: per-county API
    # ------------------------------------------------------------------
    print("\n[2/5] Attempting per-county FCC BDC API ...")
    county_data: dict[str, dict[str, float | None]] = {}
    strategy_1_failed = False

    # Test with first county before doing all
    test_fips = target_fips[0]
    test_result = fetch_county_primary(test_fips)
    if test_result is not None:
        parsed = parse_county_response(test_result)
        has_data = any(v is not None for v in parsed.values())
        if has_data:
            county_data[test_fips] = parsed
            print(f"  Primary API works. Fetching {len(target_fips)} counties ...")

            for i, fips in enumerate(target_fips[1:], start=2):
                result = fetch_county_primary(fips)
                if result is not None:
                    county_data[fips] = parse_county_response(result)
                if i % 50 == 0 or i == len(target_fips):
                    print(
                        f"    Progress: {i}/{len(target_fips)} counties "
                        f"({len(county_data)} with data)"
                    )
        else:
            print("  Primary API returned data but no recognized fields.")
            strategy_1_failed = True
    else:
        print("  Primary API failed for test county.")
        strategy_1_failed = True

    # ------------------------------------------------------------------
    # 3. Try Strategy 2: national summary (if Strategy 1 failed)
    # ------------------------------------------------------------------
    if strategy_1_failed or not county_data:
        print("\n[3/5] Attempting national summary endpoint ...")
        national_df = fetch_national_summary()
        if national_df is not None and not national_df.empty:
            target_set = set(target_fips)
            national_df = national_df.loc[national_df["fips_5digit"].isin(target_set)]
            for _, row in national_df.iterrows():
                fips = row["fips_5digit"]
                county_data[fips] = {
                    "pct_broadband_25_3": row.get("pct_broadband_25_3"),
                    "pct_broadband_100_20": row.get("pct_broadband_100_20"),
                    "num_providers": row.get("num_providers"),
                    "max_download_mbps": row.get("max_download_mbps"),
                }
            print(f"  National summary: {len(county_data)} counties matched")
        else:
            print("  National summary endpoint also failed.")
    else:
        print("\n[3/5] Skipping national summary (primary API succeeded).")

    # ------------------------------------------------------------------
    # 4. Build output DataFrame
    # ------------------------------------------------------------------
    print("\n[4/5] Building output DataFrame ...")

    if county_data:
        broadband_rows: list[dict] = []
        for fips, metrics in county_data.items():
            broadband_rows.append({"fips_5digit": fips, **metrics})
        broadband_df = pd.DataFrame(broadband_rows)
    else:
        print(
            "  WARNING: No broadband data retrieved from any source.\n"
            "  Creating output with NULL broadband columns.\n"
            "  This ensures the Athena table schema is registered for\n"
            "  future runs when the API becomes available."
        )
        broadband_df = pd.DataFrame(
            {
                "fips_5digit": target_fips,
                "pct_broadband_25_3": None,
                "pct_broadband_100_20": None,
                "num_providers": None,
                "max_download_mbps": None,
            }
        )

    # Join crosswalk -> canonical_id
    merged = crosswalk[["canonical_id", "fips_5digit"]].merge(
        broadband_df,
        on="fips_5digit",
        how="left",
    )

    output = merged[OUTPUT_COLUMNS].copy()
    print(f"  Output rows: {len(output)}")

    # Save CSV
    csv_path = TIER1_DIR / "fcc_broadband.csv"
    output.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    # ------------------------------------------------------------------
    # 5. Upload to S3 and register Athena table
    # ------------------------------------------------------------------
    print("\n[5/5] Uploading to S3 and registering Athena table ...")
    s3_uri = upload_to_s3(csv_path, "fcc_broadband")
    register_athena_table("tier1_fcc_broadband", ATHENA_COLUMNS, s3_uri)

    # ------------------------------------------------------------------
    # Validation summary
    # ------------------------------------------------------------------
    print("\n--- Validation ---")
    total = len(output)
    for col in BROADBAND_COLS:
        non_null = output[col].notna().sum()
        pct = non_null / total * 100 if total > 0 else 0.0
        print(f"  {col}: {non_null}/{total} non-null ({pct:.1f}%)")

    has_any = output[BROADBAND_COLS].notna().any(axis=1).sum()
    has_any_pct = has_any / total * 100 if total > 0 else 0.0
    print(
        f"  Communities with any broadband data: {has_any}/{total} ({has_any_pct:.1f}%)"
    )

    if has_any == 0:
        print(
            "\n  NOTE: Zero broadband data was retrieved. This is expected if\n"
            "  the FCC API has changed. The Athena table has been registered\n"
            "  with the correct schema. Re-run when the API is available."
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
