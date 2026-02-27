"""CMS NPPES physician density data for all MTH communities.

Queries the CMS National Plan and Provider Enumeration System (NPPES) NPI
Registry API to count providers by specialty type for every unique city/state
pair across the ~1,307 MTH communities.

Provider taxonomy classification:
    Primary care  – family medicine (207Q), internal medicine (207R),
                    general practice (208D), nurse practitioners (363L)
    Specialist    – any "207" prefix that is not primary care
    Mental health – counselors (101Y), psychologists (103T),
                    psychiatry (2084P)
    Dental        – 122300000X prefix

Output columns:
    canonical_id, city_searched, total_providers, primary_care_count,
    specialist_count, mental_health_count, dental_count,
    providers_per_1000_pop

Usage:
    poetry run python scripts/tier1/07b_cms_physicians.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _config import get_session, load_pipeline_config  # noqa: F401

from tier1._helpers import (
    TIER1_DIR,
    api_get,
    ensure_dirs,
    load_cached,
    load_communities,
    register_athena_table,
    save_cache,
    upload_to_s3,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NPPES_URL = "https://npiregistry.cms.hhs.gov/api/"
NPPES_PAGE_SIZE = 200
NPPES_MAX_SKIP = 1200  # API hard limit; subdivide by taxonomy above this
NPPES_RATE = 0.5  # seconds between requests

STATE_ABBREV: dict[str, str] = {
    "Georgia": "GA",
    "Alabama": "AL",
    "Florida": "FL",
}

# Taxonomy code prefixes used for classification
TAXONOMY_PRIMARY_CARE = ("207Q", "207R", "208D", "363L")
TAXONOMY_SPECIALIST_PREFIX = "207"
TAXONOMY_MENTAL_HEALTH = ("101Y", "103T", "2084P")
TAXONOMY_DENTAL_PREFIX = "1223"

# Athena schema
ATHENA_COLUMNS: list[tuple[str, str]] = [
    ("canonical_id", "string"),
    ("city_searched", "string"),
    ("total_providers", "int"),
    ("primary_care_count", "int"),
    ("specialist_count", "int"),
    ("mental_health_count", "int"),
    ("dental_count", "int"),
    ("providers_per_1000_pop", "double"),
]

OUTPUT_COLUMNS = [col for col, _ in ATHENA_COLUMNS]


# ---------------------------------------------------------------------------
# Taxonomy classification
# ---------------------------------------------------------------------------


def classify_taxonomy(code: str) -> str:
    """Map a 10-character taxonomy code to a provider category.

    Returns one of: 'primary_care', 'mental_health', 'dental',
    'specialist', or 'other'.
    """
    if not code:
        return "other"

    if code.startswith(TAXONOMY_PRIMARY_CARE):
        return "primary_care"

    if code.startswith(TAXONOMY_MENTAL_HEALTH):
        return "mental_health"

    if code.startswith(TAXONOMY_DENTAL_PREFIX):
        return "dental"

    if code.startswith(TAXONOMY_SPECIALIST_PREFIX):
        return "specialist"

    return "other"


def extract_primary_taxonomy(provider: dict) -> str:
    """Extract the primary taxonomy code from an NPPES provider record.

    NPPES returns a 'taxonomies' list where 'primary': True marks the
    primary classification.  Falls back to the first entry if none is
    flagged primary.
    """
    taxonomies: list[dict] = provider.get("taxonomies", [])
    if not taxonomies:
        return ""

    for t in taxonomies:
        if t.get("primary"):
            return t.get("code", "")

    return taxonomies[0].get("code", "")


# ---------------------------------------------------------------------------
# Normalise city name for cache keys
# ---------------------------------------------------------------------------


def normalise_city(city: str) -> str:
    """Return a filesystem-safe cache key for a city name."""
    return re.sub(r"[^a-z0-9_]", "_", city.strip().lower())


# ---------------------------------------------------------------------------
# NPPES API pagination
# ---------------------------------------------------------------------------


def fetch_city_providers(
    city: str,
    state_abbr: str,
) -> list[dict]:
    """Fetch all individual (NPI-1) providers for a city/state pair.

    Paginates up to NPPES_MAX_SKIP records.  Results are cached per
    city/state so re-runs are free.
    """
    cache_key = f"{state_abbr.upper()}_{normalise_city(city)}"
    cached = load_cached("cms_physicians", cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    all_results: list[dict] = []
    skip = 0

    while skip <= NPPES_MAX_SKIP:
        params: dict[str, str | int] = {
            "version": "2.1",
            "enumeration_type": "NPI-1",
            "state": state_abbr,
            "city": city,
            "limit": NPPES_PAGE_SIZE,
            "skip": skip,
        }

        try:
            resp = api_get(
                NPPES_URL,
                params=params,
                rate_limit=NPPES_RATE,
                max_retries=3,
                timeout=30,
            )
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            print(f"    Warning: NPPES request failed for {city}, {state_abbr}: {exc}")
            break

        results: list[dict] = data.get("results", [])
        all_results.extend(results)

        result_count = data.get("result_count", 0)
        if result_count < NPPES_PAGE_SIZE:
            # Last page
            break

        skip += NPPES_PAGE_SIZE

    save_cache("cms_physicians", cache_key, all_results)
    return all_results


# ---------------------------------------------------------------------------
# Aggregate provider counts
# ---------------------------------------------------------------------------


def aggregate_providers(providers: list[dict]) -> dict[str, int]:
    """Count providers by taxonomy category from a list of NPPES records."""
    counts: dict[str, int] = {
        "total_providers": 0,
        "primary_care_count": 0,
        "specialist_count": 0,
        "mental_health_count": 0,
        "dental_count": 0,
    }

    for p in providers:
        code = extract_primary_taxonomy(p)
        category = classify_taxonomy(code)
        counts["total_providers"] += 1
        if category == "primary_care":
            counts["primary_care_count"] += 1
        elif category == "specialist":
            counts["specialist_count"] += 1
        elif category == "mental_health":
            counts["mental_health_count"] += 1
        elif category == "dental":
            counts["dental_count"] += 1

    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Query NPPES API and build provider density metrics for all communities."""
    print("=" * 60)
    print("07b_cms_physicians.py – CMS NPPES Physician Density")
    print("=" * 60)

    ensure_dirs()

    # [1/6] Load communities
    print("\n[1/6] Loading communities …")
    communities = load_communities()
    print(f"  Communities: {len(communities)}")

    # Map state names to abbreviations; drop unknown states
    communities = communities.copy()
    communities["state_abbr"] = communities["state_name"].map(STATE_ABBREV)
    unknown_states = communities["state_abbr"].isna().sum()
    if unknown_states:
        unknown = communities.loc[
            communities["state_abbr"].isna(), "state_name"
        ].unique()
        print(
            f"  Warning: {unknown_states} communities with unknown states "
            f"(will be skipped): {unknown.tolist()}"
        )
    communities = communities.dropna(subset=["state_abbr"])

    # [2/6] Build unique city/state pairs
    print("\n[2/6] Building unique city/state pairs …")
    city_state_df = communities.groupby(["city", "state_abbr"], as_index=False).agg(
        community_count=("canonical_id", "count"),
        total_population=("population", "sum"),
    )
    city_state_df["city_clean"] = city_state_df["city"].str.strip().str.title()
    print(f"  Unique city/state pairs: {len(city_state_df)}")

    # [3/6] Query NPPES for each city/state pair
    print("\n[3/6] Querying NPPES API for each city …")
    city_counts: dict[tuple[str, str], dict[str, int]] = {}

    total_cities = len(city_state_df)
    for i, (_, row) in enumerate(city_state_df.iterrows()):
        city_clean: str = row["city_clean"]
        state_abbr: str = row["state_abbr"]

        providers = fetch_city_providers(city_clean, state_abbr)
        counts = aggregate_providers(providers)
        city_counts[(city_clean, state_abbr)] = counts

        if (i + 1) % 50 == 0 or (i + 1) == total_cities:
            total_found = sum(v["total_providers"] for v in city_counts.values())
            print(
                f"  Progress: {i + 1}/{total_cities} cities | "
                f"Total providers found: {total_found}"
            )

    # [4/6] Map counts back to communities
    print("\n[4/6] Mapping provider counts to communities …")
    records: list[dict[str, object]] = []

    for _, community in communities.iterrows():
        city_clean = str(community["city"]).strip().title()
        state_abbr = str(community["state_abbr"])
        pop = community.get("population", 0)

        counts = city_counts.get(
            (city_clean, state_abbr),
            {
                "total_providers": 0,
                "primary_care_count": 0,
                "specialist_count": 0,
                "mental_health_count": 0,
                "dental_count": 0,
            },
        )

        total = counts["total_providers"]
        try:
            pop_val = float(pop)
        except (TypeError, ValueError):
            pop_val = 0.0

        providers_per_1k: float | None = (
            round(total / pop_val * 1000, 2) if pop_val > 0 else None
        )

        records.append(
            {
                "canonical_id": community["canonical_id"],
                "city_searched": city_clean,
                "total_providers": total,
                "primary_care_count": counts["primary_care_count"],
                "specialist_count": counts["specialist_count"],
                "mental_health_count": counts["mental_health_count"],
                "dental_count": counts["dental_count"],
                "providers_per_1000_pop": providers_per_1k,
            }
        )

    result = pd.DataFrame(records, columns=OUTPUT_COLUMNS)

    # [5/6] Save CSV
    print("\n[5/6] Saving CSV …")
    csv_path = TIER1_DIR / "cms_physicians.csv"
    result.to_csv(csv_path, index=False)
    print(f"  Saved: {csv_path}")

    # [6/6] Upload to S3 and register Athena
    print("\n[6/6] Uploading to S3 and registering Athena table …")
    s3_uri = upload_to_s3(csv_path, "cms_physicians")
    register_athena_table("tier1_cms_physicians", ATHENA_COLUMNS, s3_uri)

    # Validation summary
    print("\n--- Validation ---")
    print(f"  Total rows: {len(result)}")

    avg_providers_city = (
        result.groupby("city_searched")["total_providers"].first().mean()
    )
    total_providers_found = sum(c["total_providers"] for c in city_counts.values())
    unique_cities_queried = len(city_counts)
    avg_per_1k = result["providers_per_1000_pop"].dropna().mean()

    print(f"  Unique cities queried:          {unique_cities_queried}")
    print(f"  Total providers found:          {total_providers_found}")
    print(f"  Avg providers per city:         {avg_providers_city:.1f}")
    print(
        f"  Avg providers per 1,000 pop:    {avg_per_1k:.2f}"
        if pd.notna(avg_per_1k)
        else "  Avg providers per 1,000 pop:    N/A"
    )

    # Breakdown by category
    for col in (
        "primary_care_count",
        "specialist_count",
        "mental_health_count",
        "dental_count",
    ):
        total_col = result[col].sum()
        pct = total_col / total_providers_found * 100 if total_providers_found else 0
        label = col.replace("_count", "").replace("_", " ").title()
        print(f"  {label:<22} {total_col:>6} ({pct:.1f}%)")

    zero_providers = (result["total_providers"] == 0).sum()
    if zero_providers:
        print(f"  Communities with 0 providers: {zero_providers}")

    print("\nDone.")


if __name__ == "__main__":
    main()
