"""Load hospital + physician data into healthcare narrative documents."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..types import RAGDocument

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "data"
TIER1_DIR = DATA_DIR / "prepared" / "tier1"


def load_healthcare_narratives() -> list[RAGDocument]:
    """Build per-community healthcare narrative documents."""
    hospitals_path = TIER1_DIR / "cms_hospitals.csv"
    physicians_path = TIER1_DIR / "cms_physicians.csv"

    docs: list[RAGDocument] = []

    hosp_df = None
    if hospitals_path.exists():
        hosp_df = pd.read_csv(hospitals_path)
        if "canonical_id" in hosp_df.columns:
            hosp_df = hosp_df.set_index("canonical_id")

    phys_df = None
    if physicians_path.exists():
        phys_df = pd.read_csv(physicians_path)
        if "canonical_id" in phys_df.columns:
            phys_df = phys_df.set_index("canonical_id")

    if hosp_df is None and phys_df is None:
        logger.warning("No healthcare data found")
        return docs

    all_ids = set()
    if hosp_df is not None:
        all_ids.update(hosp_df.index)
    if phys_df is not None:
        all_ids.update(phys_df.index)

    for cid in sorted(all_ids):
        parts = [f"Healthcare profile for {cid}:"]

        if hosp_df is not None and cid in hosp_df.index:
            h = hosp_df.loc[cid]
            name = h.get("nearest_hospital_name", "N/A")
            dist = h.get("nearest_hospital_miles")
            rating = h.get("nearest_hospital_rating")
            count30 = h.get("hospitals_within_30mi")
            avg_rat = h.get("avg_rating_within_30mi")
            parts.append(
                f"Nearest hospital: {name} ({dist:.1f} miles, rating {rating}/5)."
                if pd.notna(dist) and pd.notna(rating)
                else f"Nearest hospital: {name}."
            )
            if pd.notna(count30):
                parts.append(
                    f"{int(count30)} hospitals within 30 miles "
                    f"(avg rating: {avg_rat:.1f}/5)."
                    if pd.notna(avg_rat)
                    else f"{int(count30)} hospitals within 30 miles."
                )

        if phys_df is not None and cid in phys_df.index:
            p = phys_df.loc[cid]
            total = p.get("total_providers")
            pcp = p.get("primary_care_count")
            per1k = p.get("providers_per_1000_pop")
            if pd.notna(total):
                parts.append(
                    f"Total healthcare providers: {int(total)}. "
                    f"Primary care: {int(pcp) if pd.notna(pcp) else 'N/A'}. "
                    f"Providers per 1,000 pop: {per1k:.1f}."
                    if pd.notna(per1k)
                    else f"Total healthcare providers: {int(total)}."
                )

        if len(parts) > 1:
            docs.append(
                RAGDocument(
                    content="\n".join(parts),
                    canonical_city_id=cid,
                    source_type="health",
                    metadata={"data_source": "cms_hospitals+physicians"},
                )
            )

    logger.info("Built %d healthcare narrative documents", len(docs))
    return docs
