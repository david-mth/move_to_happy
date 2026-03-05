"""Load detailed hospital-level documents for RAG."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..types import RAGDocument

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "data"
TIER1_DIR = DATA_DIR / "prepared" / "tier1"


def load_hospital_documents() -> list[RAGDocument]:
    """Load individual hospital records as RAG documents.

    These provide detailed hospital-level context when a user asks about
    specific healthcare facilities near a community.
    """
    hospitals_path = TIER1_DIR / "cms_hospitals.csv"
    if not hospitals_path.exists():
        logger.warning("Hospital CSV not found: %s", hospitals_path)
        return []

    df = pd.read_csv(hospitals_path)
    docs: list[RAGDocument] = []

    for _, row in df.iterrows():
        cid = row.get("canonical_id")
        if not cid:
            continue

        name = row.get("nearest_hospital_name", "Unknown")
        dist = row.get("nearest_hospital_miles")
        rating = row.get("nearest_hospital_rating")
        count15 = row.get("hospitals_within_15mi")
        count30 = row.get("hospitals_within_30mi")
        avg_rat = row.get("avg_rating_within_30mi")
        er_dist = row.get("nearest_er_miles")

        parts = [f"Hospital access for community {cid}:"]
        parts.append(f"Nearest hospital: {name}.")

        if pd.notna(dist):
            parts.append(f"Distance: {dist:.1f} miles.")
        if pd.notna(rating):
            parts.append(f"Rating: {rating:.1f}/5 stars.")
        if pd.notna(er_dist):
            parts.append(f"Nearest ER: {er_dist:.1f} miles.")
        if pd.notna(count15):
            parts.append(f"Hospitals within 15 miles: {int(count15)}.")
        if pd.notna(count30):
            parts.append(f"Hospitals within 30 miles: {int(count30)}.")
        if pd.notna(avg_rat):
            parts.append(f"Average rating within 30 miles: {avg_rat:.1f}/5.")

        docs.append(
            RAGDocument(
                content="\n".join(parts),
                canonical_city_id=cid,
                source_type="hospital",
                metadata={
                    "hospital_name": str(name),
                    "data_source": "cms_hospitals",
                },
            )
        )

    logger.info("Built %d hospital documents", len(docs))
    return docs
