"""Load community profiles + tier1 enrichment data into RAG documents."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..types import RAGDocument

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "data"
PREPARED_DIR = DATA_DIR / "prepared"
TIER1_DIR = PREPARED_DIR / "tier1"

TIER1_FILES = {
    "census": "census_acs.csv",
    "crime": "fbi_crime.csv",
    "broadband": "fcc_broadband.csv",
    "air_quality": "epa_air_quality.csv",
    "employment": "bls_employment.csv",
    "tax_rates": "tax_rates.csv",
    "education": "county_education.csv",
}


def _fmt(val: object, prefix: str = "", suffix: str = "") -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    if isinstance(val, float):
        if abs(val) >= 1000:
            return f"{prefix}{val:,.0f}{suffix}"
        return f"{prefix}{val:.2f}{suffix}"
    return f"{prefix}{val}{suffix}"


def _build_community_narrative(
    row: pd.Series,
    enrichments: dict[str, pd.Series],
) -> str:
    """Build a narrative profile for a single community."""
    parts = [
        f"{row['city_state']} ({row['canonical_id']})",
        f"State: {row['state_name']}, County: {row.get('county_name', 'N/A')}",
        f"Population: {_fmt(row.get('population'))}",
        f"Terrain: {row.get('terrain', 'N/A')}, Climate: {row.get('climate', 'N/A')}",
        f"Cost of Living: {_fmt(row.get('cost_of_living'))}",
    ]

    census = enrichments.get("census")
    if census is not None:
        inc = _fmt(census.get("median_household_income"), "$")
        hv = _fmt(census.get("median_home_value"), "$")
        pov = _fmt(census.get("poverty_rate"), suffix="%")
        wfh = _fmt(census.get("commute_work_from_home_pct"), suffix="%")
        parts.append(
            f"Median household income: {inc}. "
            f"Median home value: {hv}. "
            f"Poverty rate: {pov}. "
            f"Work from home: {wfh}."
        )

    crime = enrichments.get("crime")
    if crime is not None:
        parts.append(
            f"Violent crime rate: {_fmt(crime.get('violent_crime_rate'))} per 100k. "
            f"Property crime rate: {_fmt(crime.get('property_crime_rate'))} per 100k."
        )

    broadband = enrichments.get("broadband")
    if broadband is not None:
        bb = _fmt(broadband.get("pct_broadband_100_20"), suffix="%")
        dl = _fmt(broadband.get("max_download_mbps"))
        parts.append(
            f"Broadband (100/20 Mbps): {bb} coverage. Max download: {dl} Mbps."
        )

    tax = enrichments.get("tax_rates")
    if tax is not None:
        pt = _fmt(tax.get("effective_property_tax_rate"), suffix="%")
        st = _fmt(tax.get("combined_sales_tax_rate"), suffix="%")
        parts.append(f"Property tax: {pt}. Combined sales tax: {st}.")

    edu = enrichments.get("education")
    if edu is not None:
        hs = _fmt(edu.get("hs_graduation_rate"), suffix="%")
        ps = _fmt(edu.get("postsecondary_completion_rate"), suffix="%")
        parts.append(f"HS graduation rate: {hs}. Postsecondary completion: {ps}.")

    employment = enrichments.get("employment")
    if employment is not None:
        parts.append(
            f"Avg annual salary: {_fmt(employment.get('avg_annual_salary'), '$')}. "
            f"Avg weekly wage: {_fmt(employment.get('avg_weekly_wage'), '$')}."
        )

    air = enrichments.get("air_quality")
    if air is not None:
        parts.append(
            f"PM2.5: {_fmt(air.get('pm25_mean'))}. "
            f"Ozone: {_fmt(air.get('ozone_mean'))}."
        )

    return "\n".join(parts)


def load_community_profiles() -> list[RAGDocument]:
    """Load all community profiles with tier1 enrichment as RAG documents."""
    communities_path = PREPARED_DIR / "mth_communities.csv"
    if not communities_path.exists():
        logger.warning("Communities CSV not found: %s", communities_path)
        return []

    comm = pd.read_csv(communities_path)
    logger.info("Loading %d community profiles", len(comm))

    tier1_data: dict[str, pd.DataFrame] = {}
    for name, filename in TIER1_FILES.items():
        path = TIER1_DIR / filename
        if path.exists():
            tier1_data[name] = pd.read_csv(path)

    tier1_indexed: dict[str, dict[str, pd.Series]] = {}
    for name, df in tier1_data.items():
        if "canonical_id" in df.columns:
            tier1_indexed[name] = {row["canonical_id"]: row for _, row in df.iterrows()}

    docs: list[RAGDocument] = []
    for _, row in comm.iterrows():
        cid = row["canonical_id"]
        enrichments: dict[str, pd.Series] = {}
        for name, indexed in tier1_indexed.items():
            if cid in indexed:
                enrichments[name] = indexed[cid]

        narrative = _build_community_narrative(row, enrichments)
        docs.append(
            RAGDocument(
                content=narrative,
                canonical_city_id=cid,
                source_type="community",
                metadata={
                    "city_state": str(row.get("city_state", "")),
                    "state_name": str(row.get("state_name", "")),
                },
            )
        )

    logger.info("Built %d community profile documents", len(docs))
    return docs
