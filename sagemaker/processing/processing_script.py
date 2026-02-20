"""SageMaker Processing Job entry point for LME batch scoring.

Reads community Parquet from /opt/ml/processing/input/data/,
runs LME engine, writes enriched data to /opt/ml/processing/output/.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from move_to_happy.lme.engine import LMEEngine
from move_to_happy.lme.types import UserPreferences

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INPUT_DIR = Path("/opt/ml/processing/input/data")
OUTPUT_DIR = Path("/opt/ml/processing/output")


def main() -> None:
    """Run LME processing job."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load community data (handles Hive-partitioned directories and
    # files without .parquet extension from Athena/Redshift exports)
    logger.info("Loading parquet data from %s", INPUT_DIR)
    df = pd.read_parquet(INPUT_DIR)
    logger.info("Loaded %d communities", len(df))

    # Initialize engine (generates synthetic housing)
    engine = LMEEngine(df, seed=42)

    # Write enriched community data
    enriched = engine._df.copy()
    enriched.to_parquet(OUTPUT_DIR / "communities_enriched.parquet", index=False)
    logger.info("Wrote communities_enriched.parquet")

    engine._core_availability.to_parquet(
        OUTPUT_DIR / "core_availability.parquet", index=False
    )
    logger.info("Wrote core_availability.parquet")

    engine._attribute_overlay.to_parquet(
        OUTPUT_DIR / "attribute_overlay.parquet", index=False
    )
    logger.info("Wrote attribute_overlay.parquet")

    # Run sample scoring with default user
    sample_user = UserPreferences()
    result = engine.score(sample_user, top_n=50)

    # Write sample rankings
    rankings_data = [
        {
            "rank": i + 1,
            "canonical_id": r.canonical_id,
            "city_state": r.city_state,
            "state_name": r.state_name,
            "final_score": r.final_score,
            "housing_score": r.housing_score,
            "lifestyle_score": r.lifestyle_score,
            "spillover_score": r.spillover_score,
            "median_home_price": r.median_home_price,
            "pressure": r.pressure,
            "spillover_anchor": r.spillover_anchor,
        }
        for i, r in enumerate(result.rankings)
    ]
    pd.DataFrame(rankings_data).to_parquet(
        OUTPUT_DIR / "sample_rankings.parquet", index=False
    )

    # Write metadata
    metadata = {
        "total_candidates": result.total_candidates,
        "eliminated_count": result.eliminated_count,
        "max_purchase_price": result.max_purchase_price,
        "affordability_window": list(result.affordability_window),
        "top_community": result.rankings[0].city_state if result.rankings else "",
        "top_score": result.rankings[0].final_score if result.rankings else 0,
    }
    (OUTPUT_DIR / "metadata.json").write_text(json.dumps(metadata, indent=2))
    logger.info("Processing complete. Output at %s", OUTPUT_DIR)


if __name__ == "__main__":
    main()
