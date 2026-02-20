"""Final scoring — constraint pressure, housing score, weighted combination."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from move_to_happy.lme.constants import W_HOUSING, W_LIFESTYLE, W_SPILLOVER

logger = logging.getLogger(__name__)


def compute_constraint_pressure(residential: pd.DataFrame) -> pd.DataFrame:
    """Compute constraint pressure for residential candidates.

    Retention = matches_all_constraints / matches_price_only.
    Thresholds: <10% High, 10-25% Medium, >25% Low pressure.

    Returns residential with added retention and pressure columns.
    """
    residential = residential.copy()

    residential["retention"] = np.where(
        residential["matches_price_only"] > 0,
        residential["matches_all_constraints"] / residential["matches_price_only"],
        0,
    )

    def _pressure_label(r: float) -> str:
        if r < 0.10:
            return "High"
        if r < 0.25:
            return "Medium"
        return "Low"

    residential["pressure"] = residential["retention"].apply(_pressure_label)

    logger.info(
        "Constraint pressure: %s",
        residential["pressure"].value_counts().to_dict(),
    )
    return residential


def compute_housing_score(residential: pd.DataFrame) -> pd.DataFrame:
    """Compute normalized housing availability score.

    HousingScore = matches_bb / max(matches_bb).

    Returns residential with added HousingScore column.
    """
    residential = residential.copy()
    max_matches = residential["matches_bb"].max()
    residential["HousingScore"] = (
        residential["matches_bb"] / max_matches if max_matches > 0 else 0.0
    )
    logger.info("Housing score: max match count = %d", max_matches)
    return residential


def compute_final_score(
    residential: pd.DataFrame,
    w_housing: float = W_HOUSING,
    w_lifestyle: float = W_LIFESTYLE,
    w_spillover: float = W_SPILLOVER,
) -> pd.DataFrame:
    """Compute final weighted LME score.

    FinalScore = (w_housing * HousingScore + w_lifestyle * LifestyleMatch
    + w_spillover * SpilloverScore)

    Returns residential with added FinalScore column, sorted descending.
    """
    residential = residential.copy()
    residential["FinalScore"] = (
        w_housing * residential["HousingScore"]
        + w_lifestyle * residential["LifestyleMatch"]
        + w_spillover * residential["SpilloverScore"]
    )
    residential = residential.sort_values("FinalScore", ascending=False).reset_index(
        drop=True
    )
    logger.info(
        "Final scores: top=%.3f, mean=%.3f",
        residential["FinalScore"].iloc[0] if len(residential) > 0 else 0,
        residential["FinalScore"].mean() if len(residential) > 0 else 0,
    )
    return residential
