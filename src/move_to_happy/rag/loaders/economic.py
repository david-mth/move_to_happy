"""Load employment + census data into economic narrative documents."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..types import RAGDocument

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "data"
TIER1_DIR = DATA_DIR / "prepared" / "tier1"


def load_economic_narratives() -> list[RAGDocument]:
    """Build per-community economic narrative documents."""
    census_path = TIER1_DIR / "census_acs.csv"
    employment_path = TIER1_DIR / "bls_employment.csv"
    education_path = TIER1_DIR / "county_education.csv"

    docs: list[RAGDocument] = []

    census_df = None
    if census_path.exists():
        census_df = pd.read_csv(census_path)
        if "canonical_id" in census_df.columns:
            census_df = census_df.set_index("canonical_id")

    emp_df = None
    if employment_path.exists():
        emp_df = pd.read_csv(employment_path)
        if "canonical_id" in emp_df.columns:
            emp_df = emp_df.set_index("canonical_id")

    edu_df = None
    if education_path.exists():
        edu_df = pd.read_csv(education_path)
        if "canonical_id" in edu_df.columns:
            edu_df = edu_df.set_index("canonical_id")

    all_ids: set[str] = set()
    for df in [census_df, emp_df, edu_df]:
        if df is not None:
            all_ids.update(df.index)

    for cid in sorted(all_ids):
        parts = [f"Economic profile for {cid}:"]

        if census_df is not None and cid in census_df.index:
            c = census_df.loc[cid]
            income = c.get("median_household_income")
            home_val = c.get("median_home_value")
            poverty = c.get("poverty_rate")
            rent = c.get("median_rent")
            owner = c.get("pct_owner_occupied")
            wfh = c.get("commute_work_from_home_pct")
            commute = c.get("mean_commute_minutes")

            if pd.notna(income):
                parts.append(f"Median household income: ${income:,.0f}.")
            if pd.notna(home_val):
                parts.append(f"Median home value: ${home_val:,.0f}.")
            if pd.notna(poverty):
                parts.append(f"Poverty rate: {poverty:.1f}%.")
            if pd.notna(rent):
                parts.append(f"Median rent: ${rent:,.0f}/month.")
            if pd.notna(owner):
                parts.append(f"Owner-occupied: {owner:.1f}%.")
            if pd.notna(wfh):
                parts.append(f"Work from home: {wfh:.1f}%.")
            if pd.notna(commute):
                parts.append(f"Mean commute: {commute:.0f} minutes.")

        if emp_df is not None and cid in emp_df.index:
            e = emp_df.loc[cid]
            salary = e.get("avg_annual_salary")
            wage = e.get("avg_weekly_wage")
            estab = e.get("annual_avg_establishments")
            if pd.notna(salary):
                parts.append(f"Average annual salary: ${salary:,.0f}.")
            if pd.notna(wage):
                parts.append(f"Average weekly wage: ${wage:,.0f}.")
            if pd.notna(estab):
                parts.append(f"Business establishments: {estab:,.0f}.")

        if edu_df is not None and cid in edu_df.index:
            ed = edu_df.loc[cid]
            hs = ed.get("hs_graduation_rate")
            ps = ed.get("postsecondary_completion_rate")
            earn = ed.get("median_earnings")
            if pd.notna(hs):
                parts.append(f"HS graduation rate: {hs:.1f}%.")
            if pd.notna(ps):
                parts.append(f"Postsecondary completion: {ps:.1f}%.")
            if pd.notna(earn):
                parts.append(f"Median earnings: ${earn:,.0f}.")

        if len(parts) > 1:
            docs.append(
                RAGDocument(
                    content="\n".join(parts),
                    canonical_city_id=cid,
                    source_type="economic",
                    metadata={"data_source": "census+bls+education"},
                )
            )

    logger.info("Built %d economic narrative documents", len(docs))
    return docs
