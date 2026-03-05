"""Tier-1 enrichment data loader.

Loads all seven tier-1 CSV files at startup and merges them into a single
lookup dict keyed by canonical_id so scored communities can be enriched
without touching the LME engine internals.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

TIER1_DIR = Path(__file__).resolve().parent.parent / "data" / "tier1"

TIER1_FILES: dict[str, str] = {
    "census": "census_acs.csv",
    "crime": "fbi_crime.csv",
    "broadband": "fcc_broadband.csv",
    "air_quality": "epa_air_quality.csv",
    "employment": "bls_employment.csv",
    "hospitals": "cms_hospitals.csv",
    "physicians": "cms_physicians.csv",
    "geocoder": "geocoder.csv",
}

HIGHLIGHT_FIELDS: dict[str, list[str]] = {
    "census": [
        "median_household_income",
        "poverty_rate",
        "median_home_value",
        "median_rent",
        "pct_owner_occupied",
        "commute_work_from_home_pct",
        "mean_commute_minutes",
    ],
    "crime": [
        "violent_crime_rate",
        "property_crime_rate",
    ],
    "broadband": [
        "pct_broadband_100_20",
        "num_providers",
        "max_download_mbps",
    ],
    "air_quality": [
        "pm25_mean",
        "ozone_mean",
    ],
    "employment": [
        "avg_weekly_wage",
        "avg_annual_salary",
        "annual_avg_establishments",
    ],
    "hospitals": [
        "nearest_hospital_name",
        "nearest_hospital_miles",
        "nearest_hospital_rating",
        "hospitals_within_30mi",
        "avg_rating_within_30mi",
    ],
    "physicians": [
        "total_providers",
        "primary_care_count",
        "providers_per_1000_pop",
    ],
    "geocoder": [
        "zip_code",
    ],
}


class EnrichmentStore:
    """In-memory store for tier-1 enrichment data."""

    def __init__(self) -> None:
        self._data: dict[str, dict] = {}

    def load(self) -> None:
        merged = pd.DataFrame()
        for source, filename in TIER1_FILES.items():
            path = TIER1_DIR / filename
            if not path.exists():
                continue
            df = pd.read_csv(path)
            cols = ["canonical_id"] + [
                c for c in HIGHLIGHT_FIELDS.get(source, []) if c in df.columns
            ]
            df = df[cols].drop_duplicates(subset=["canonical_id"])
            if merged.empty:
                merged = df
            else:
                merged = merged.merge(df, on="canonical_id", how="outer")

        for _, row in merged.iterrows():
            cid = row["canonical_id"]
            record = {}
            for col in merged.columns:
                if col == "canonical_id":
                    continue
                val = row[col]
                if pd.isna(val):
                    continue
                record[col] = val
            self._data[cid] = record

    def enrich(self, canonical_id: str) -> dict:
        return self._data.get(canonical_id, {})
