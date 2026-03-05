#!/usr/bin/env python3
"""10 — Tax rate enrichment (property, sales, income).

Integrates tax data from three Tax Foundation sources:

  - County-level effective property tax rates (2023 data, 2025 publication)
  - State-level sales tax rates (state + avg local + combined, 2026)
  - State-level income tax rates (top marginal rate, 2026)

Property tax is county-level (joins via county name + state); sales and
income tax are state-level (applied uniformly to all communities in a state).

Source files (in ~/Downloads):
  - Property Taxes by State and County, 2025  Tax Foundation Maps.xlsx
  - 2026-Sales-Tax-Data.xlsx
  - 2026 State Income Tax Rates and Brackets  Tax Foundation.xlsx

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
    load_crosswalk,
    register_athena_table,
    upload_to_s3,
)

APP_TIER1_DIR = PROJECT_ROOT / "app" / "data" / "tier1"
DOWNLOADS_DIR = Path.home() / "Downloads"

PROPERTY_TAX_XLSX = (
    DOWNLOADS_DIR / "Property Taxes by State and County, 2025  Tax Foundation Maps.xlsx"
)
SALES_TAX_XLSX = DOWNLOADS_DIR / "2026-Sales-Tax-Data.xlsx"
INCOME_TAX_XLSX = (
    DOWNLOADS_DIR / "2026 State Income Tax Rates and Brackets  Tax Foundation.xlsx"
)

STATE_ABBREV_TO_NAME = {"AL": "Alabama", "FL": "Florida", "GA": "Georgia"}
STATE_NAME_TO_ABBREV = {v: k for k, v in STATE_ABBREV_TO_NAME.items()}

OUTPUT_COLUMNS = [
    "canonical_id",
    "fips_5digit",
    "effective_property_tax_rate",
    "median_property_tax_paid",
    "state_sales_tax_rate",
    "avg_local_sales_tax_rate",
    "combined_sales_tax_rate",
    "state_income_tax_rate",
    "has_state_income_tax",
]

ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("fips_5digit", "string"),
    ("effective_property_tax_rate", "double"),
    ("median_property_tax_paid", "double"),
    ("state_sales_tax_rate", "double"),
    ("avg_local_sales_tax_rate", "double"),
    ("combined_sales_tax_rate", "double"),
    ("state_income_tax_rate", "double"),
    ("has_state_income_tax", "boolean"),
]


def _load_property_tax() -> pd.DataFrame:
    """Load county-level property tax rates for AL/FL/GA.

    Returns DataFrame with columns: state, county, median_home_value,
    median_property_tax_paid, effective_property_tax_rate.
    """
    df = pd.read_excel(PROPERTY_TAX_XLSX, header=None, skiprows=2)
    df.columns = [
        "state",
        "county",
        "median_home_value",
        "median_property_tax_paid",
        "effective_property_tax_rate",
    ]
    df = df[df["state"].isin(["Alabama", "Florida", "Georgia"])].copy()
    df["county"] = df["county"].str.strip()
    df["state"] = df["state"].str.strip()

    for col in [
        "median_home_value",
        "median_property_tax_paid",
        "effective_property_tax_rate",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _load_sales_tax() -> dict[str, dict[str, float]]:
    """Load state-level sales tax rates for AL/FL/GA.

    Returns {state_name: {state_rate, avg_local_rate, combined_rate}}.
    """
    df = pd.read_excel(SALES_TAX_XLSX, header=None, skiprows=2)
    df.columns = [
        "state",
        "state_rate",
        "state_rank",
        "avg_local_rate",
        "max_local",
        "combined_rate",
        "combined_rank",
    ]
    result = {}
    for _, row in df.iterrows():
        state = str(row["state"]).strip()
        if state in STATE_NAME_TO_ABBREV:
            result[state] = {
                "state_sales_tax_rate": float(row["state_rate"] or 0),
                "avg_local_sales_tax_rate": float(row["avg_local_rate"] or 0),
                "combined_sales_tax_rate": float(row["combined_rate"] or 0),
            }
    return result


def _load_income_tax() -> dict[str, dict[str, object]]:
    """Load state-level income tax info for AL/FL/GA.

    Returns {state_name: {top_rate, has_income_tax}}.
    For states with brackets, uses the top marginal rate.
    """
    df = pd.read_excel(INCOME_TAX_XLSX, header=None, skiprows=2)

    result: dict[str, dict[str, object]] = {
        "Alabama": {"state_income_tax_rate": 0.05, "has_state_income_tax": True},
        "Florida": {"state_income_tax_rate": 0.0, "has_state_income_tax": False},
        "Georgia": {"state_income_tax_rate": 0.0519, "has_state_income_tax": True},
    }

    # Parse from file to verify / update
    current_state = None
    top_rate: dict[str, float] = {}

    for _, row in df.iterrows():
        label = str(row.iloc[0]).strip()
        rate_val = row.iloc[1]

        if label.startswith("- "):
            # Continuation row for bracket
            state_name = label.replace("- ", "").strip()
            if state_name in STATE_NAME_TO_ABBREV:
                current_state = state_name
                try:
                    r = float(rate_val)
                    top_rate[current_state] = max(top_rate.get(current_state, 0), r)
                except (ValueError, TypeError):
                    pass
        else:
            # First row for a state (may have annotations like "(a, b, c)")
            for state_name in STATE_NAME_TO_ABBREV:
                if label.startswith(state_name):
                    current_state = state_name
                    if rate_val == "none" or rate_val is None:
                        top_rate[current_state] = 0.0
                        result[state_name] = {
                            "state_income_tax_rate": 0.0,
                            "has_state_income_tax": False,
                        }
                    else:
                        try:
                            r = float(rate_val)
                            top_rate[current_state] = max(
                                top_rate.get(current_state, 0), r
                            )
                        except (ValueError, TypeError):
                            pass
                    break

    for state_name, rate in top_rate.items():
        if rate > 0:
            result[state_name] = {
                "state_income_tax_rate": rate,
                "has_state_income_tax": True,
            }

    return result


def main() -> None:
    print("=" * 60)
    print("10_tax_rates.py — Tax Rate Enrichment")
    print("=" * 60)

    ensure_dirs()
    APP_TIER1_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Load crosswalk
    # ------------------------------------------------------------------
    print("\n[1/5] Loading crosswalk ...")
    crosswalk = load_crosswalk()
    print(f"  Crosswalk rows: {len(crosswalk)}")

    # ------------------------------------------------------------------
    # 2. Load tax data
    # ------------------------------------------------------------------
    print("\n[2/5] Loading tax data ...")

    ptax = _load_property_tax()
    print(f"  Property tax: {len(ptax)} county rows for AL/FL/GA")

    sales = _load_sales_tax()
    for state, rates in sales.items():
        print(f"  Sales tax {state}: {rates}")

    income = _load_income_tax()
    for state, info in income.items():
        print(f"  Income tax {state}: {info}")

    # ------------------------------------------------------------------
    # 3. Join property tax to crosswalk via county name + state
    # ------------------------------------------------------------------
    print("\n[3/5] Joining property tax to communities ...")

    # Build state_name column on crosswalk from state_fips
    fips_to_state = {"13": "Georgia", "01": "Alabama", "12": "Florida"}
    crosswalk["state_name"] = crosswalk["state_fips"].map(fips_to_state)

    merged = crosswalk.merge(
        ptax[
            [
                "state",
                "county",
                "median_property_tax_paid",
                "effective_property_tax_rate",
            ]
        ],
        left_on=["state_name", "county_name_census"],
        right_on=["state", "county"],
        how="left",
    )

    ptax_matched = merged["effective_property_tax_rate"].notna().sum()
    print(f"  Property tax matched: {ptax_matched}/{len(merged)}")

    # ------------------------------------------------------------------
    # 4. Add sales + income tax (state-level)
    # ------------------------------------------------------------------
    print("\n[4/5] Adding sales and income tax ...")

    for col in [
        "state_sales_tax_rate",
        "avg_local_sales_tax_rate",
        "combined_sales_tax_rate",
    ]:
        merged[col] = merged["state_name"].map(lambda s, c=col: sales.get(s, {}).get(c))

    merged["state_income_tax_rate"] = merged["state_name"].map(
        lambda s: income.get(s, {}).get("state_income_tax_rate")
    )
    merged["has_state_income_tax"] = merged["state_name"].map(
        lambda s: income.get(s, {}).get("has_state_income_tax", False)
    )

    output = merged[OUTPUT_COLUMNS].copy()

    # Round rates
    for col in [
        "effective_property_tax_rate",
        "state_sales_tax_rate",
        "avg_local_sales_tax_rate",
        "combined_sales_tax_rate",
        "state_income_tax_rate",
    ]:
        output[col] = pd.to_numeric(output[col], errors="coerce").round(6)

    # ------------------------------------------------------------------
    # 5. Save and upload
    # ------------------------------------------------------------------
    csv_path = TIER1_DIR / "tax_rates.csv"
    output.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path} ({len(output)} rows)")

    app_path = APP_TIER1_DIR / "tax_rates.csv"
    output.to_csv(app_path, index=False)
    print(f"  Saved: {app_path} ({len(output)} rows)")

    print("\n[5/5] Uploading to S3 and registering Athena table ...")
    try:
        s3_uri = upload_to_s3(csv_path, "tax_rates")
        register_athena_table("tier1_tax_rates", ATHENA_COLUMNS, s3_uri)
    except Exception as e:
        print(f"\n  Skipping S3/Athena: {e}")

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    print("\n--- Validation ---")
    total = len(output)
    print(f"  Total rows: {total}")

    for col in OUTPUT_COLUMNS[2:]:
        if col == "has_state_income_tax":
            true_ct = output[col].sum()
            print(f"  {col}: {true_ct}/{total} True")
        else:
            non_null = output[col].notna().sum()
            valid = pd.to_numeric(output[col], errors="coerce").dropna()
            if not valid.empty:
                print(
                    f"  {col}: {non_null}/{total} "
                    f"[{valid.min():.4f} - {valid.max():.4f}], "
                    f"mean={valid.mean():.4f}"
                )
            else:
                print(f"  {col}: {non_null}/{total} (all NaN)")

    unique_ptax = output["effective_property_tax_rate"].nunique()
    print(f"\n  Distinct property tax rates: {unique_ptax}")

    print("\nDone.")


if __name__ == "__main__":
    main()
