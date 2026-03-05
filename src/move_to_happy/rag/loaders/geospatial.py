"""Load distance/proximity data into geospatial narrative documents."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..types import RAGDocument

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "data"
PREPARED_DIR = DATA_DIR / "prepared"
TIER1_DIR = PREPARED_DIR / "tier1"


def load_geospatial_narratives() -> list[RAGDocument]:
    """Build per-community proximity narrative documents."""
    comm_path = PREPARED_DIR / "mth_communities.csv"
    geodist_path = TIER1_DIR / "geodistance.csv"
    lake_path = TIER1_DIR / "lake_distance.csv"

    docs: list[RAGDocument] = []

    if not comm_path.exists():
        logger.warning("Communities CSV not found")
        return docs

    comm = pd.read_csv(comm_path)

    geodist = None
    if geodist_path.exists():
        geodist = pd.read_csv(geodist_path)
        if "canonical_id" in geodist.columns:
            geodist = geodist.set_index("canonical_id")

    lake = None
    if lake_path.exists():
        lake = pd.read_csv(lake_path)
        if "canonical_id" in lake.columns:
            lake = lake.set_index("canonical_id")

    for _, row in comm.iterrows():
        cid = row["canonical_id"]
        city = row.get("city_state", cid)
        parts = [f"Proximity profile for {city} ({cid}):"]

        mtns = row.get("miles_to_mountains")
        if pd.notna(mtns):
            parts.append(f"Distance to mountains: {mtns:.1f} miles.")

        beach = row.get("miles_to_beach")
        if pd.notna(beach):
            parts.append(f"Distance to nearest beach: {beach:.1f} miles.")

        atlantic = row.get("miles_to_atlantic")
        gulf = row.get("miles_to_gulf")
        if pd.notna(atlantic):
            parts.append(f"Distance to Atlantic coast: {atlantic:.1f} miles.")
        if pd.notna(gulf):
            parts.append(f"Distance to Gulf coast: {gulf:.1f} miles.")

        lake_mi = row.get("miles_to_lake")
        if pd.notna(lake_mi):
            parts.append(f"Distance to nearest lake: {lake_mi:.1f} miles.")

        airport = row.get("closest_intl_airport_iata")
        airport_mi = row.get("closest_intl_airport_miles")
        if pd.notna(airport) and pd.notna(airport_mi):
            parts.append(
                f"Closest international airport: {airport} ({airport_mi:.1f} miles)."
            )

        if geodist is not None and cid in geodist.index:
            g = geodist.loc[cid]
            ocean_type = g.get("ocean_type")
            ocean_dist = g.get("ocean_distance_miles")
            mtn_inside = g.get("mountain_region_inside")
            if pd.notna(ocean_type) and pd.notna(ocean_dist):
                parts.append(
                    f"Nearest coastline: {ocean_type} "
                    f"({ocean_dist:.1f} miles geodesic)."
                )
            if mtn_inside:
                parts.append("Located inside the Appalachian mountain region.")

        if lake is not None and cid in lake.index:
            lk = lake.loc[cid]
            lake_name = lk.get("lake_name")
            lake_dist = lk.get("lake_distance_miles")
            lake_area = lk.get("lake_area_sq_mi")
            if pd.notna(lake_name) and pd.notna(lake_dist):
                area_str = f" ({lake_area:.0f} sq mi)" if pd.notna(lake_area) else ""
                parts.append(
                    f"Nearest significant lake: {lake_name}{area_str}, "
                    f"{lake_dist:.1f} miles."
                )

        if len(parts) > 1:
            docs.append(
                RAGDocument(
                    content="\n".join(parts),
                    canonical_city_id=cid,
                    source_type="geospatial",
                    metadata={"data_source": "communities+geodistance+lake"},
                )
            )

    logger.info("Built %d geospatial narrative documents", len(docs))
    return docs
