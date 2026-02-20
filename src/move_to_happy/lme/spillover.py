"""Spillover logic — lifestyle anchors boost nearby affordable communities."""

from __future__ import annotations

import logging

import pandas as pd

from move_to_happy.lme.constants import SPILLOVER_RANGE_MILES
from move_to_happy.lme.eliminators import haversine_miles

logger = logging.getLogger(__name__)


def identify_lifestyle_anchors(
    df: pd.DataFrame, threshold_quantile: float = 0.75
) -> pd.DataFrame:
    """Identify communities that are lifestyle anchors.

    Lifestyle anchors are communities that were eliminated as residential candidates
    but have top-quartile lifestyle scores.

    Args:
        df: Full DataFrame with elimination flags and LifestyleMatch scores.
        threshold_quantile: Quantile threshold for lifestyle score
            (default 75th pctile).

    Returns:
        DataFrame of lifestyle anchor communities.
    """
    threshold = df["LifestyleMatch"].quantile(threshold_quantile)
    anchors = df[(df["eliminated"]) & (df["LifestyleMatch"] >= threshold)].copy()
    logger.info("Lifestyle anchors: %d (threshold=%.3f)", len(anchors), threshold)
    return anchors


def compute_spillover_scores(
    residential: pd.DataFrame,
    anchors: pd.DataFrame,
) -> pd.DataFrame:
    """Compute spillover scores for residential candidates from lifestyle anchors.

    Spillover(r, a) = LifestyleAffinity(a) × Proximity(r, a)
    Proximity = max(0, 1 - dist / SPILLOVER_RANGE_MILES)

    Args:
        residential: Residential candidates DataFrame.
        anchors: Lifestyle anchor communities DataFrame.

    Returns:
        residential DataFrame with added SpilloverScore and SpilloverAnchor columns.
    """
    residential = residential.copy()

    if anchors.empty:
        residential["SpilloverScore_raw"] = 0.0
        residential["SpilloverScore"] = 0.0
        residential["SpilloverAnchor"] = ""
        return residential

    anchor_lats = anchors["latitude"].values
    anchor_lons = anchors["longitude"].values
    anchor_lifestyle = anchors["LifestyleMatch"].values
    anchor_names = anchors["city_state"].values

    spillover_scores = []
    spillover_anchors = []

    for _, r in residential.iterrows():
        r_lat, r_lon = r["latitude"], r["longitude"]
        max_spill = 0.0
        best_anchor_name = ""

        for j in range(len(anchors)):
            dist = haversine_miles(r_lat, r_lon, anchor_lats[j], anchor_lons[j])
            if dist < SPILLOVER_RANGE_MILES and dist > 0.5:
                proximity = max(0, 1 - dist / SPILLOVER_RANGE_MILES)
                spill = anchor_lifestyle[j] * proximity
                if spill > max_spill:
                    max_spill = spill
                    best_anchor_name = anchor_names[j]

        spillover_scores.append(max_spill)
        spillover_anchors.append(best_anchor_name)

    residential["SpilloverScore_raw"] = spillover_scores
    residential["SpilloverAnchor"] = spillover_anchors

    # Normalize to 0-1
    sp_max = residential["SpilloverScore_raw"].max()
    residential["SpilloverScore"] = (
        residential["SpilloverScore_raw"] / sp_max if sp_max > 0 else 0.0
    )

    logger.info(
        "Spillover: %d communities receiving, %d with no nearby anchor",
        (residential["SpilloverScore"] > 0).sum(),
        (residential["SpilloverScore"] == 0).sum(),
    )
    return residential


def generate_spillover_explanation(row: pd.Series, anchor_info: pd.Series) -> str:
    """Generate mandatory spillover explanation per Spec Sec 12.5.

    Returns string like: "You can't live in X (reason), but Y gives you
    the closest access to the X lifestyle (N miles away)."
    """
    anchor_name = row.get("SpilloverAnchor", "")
    if not anchor_name:
        return ""

    reasons = []
    if anchor_info.get("elim_affordability", False):
        reasons.append("unaffordable at your budget")
    if anchor_info.get("elim_distance", False):
        reasons.append("outside your radius")

    if not reasons:
        reasons.append("eliminated")

    dist = haversine_miles(
        row["latitude"],
        row["longitude"],
        anchor_info["latitude"],
        anchor_info["longitude"],
    )

    return (
        f"You can't live in {anchor_name} ({', '.join(reasons)}), "
        f"but {row['city_state']} gives you the closest access "
        f"to the {anchor_name} lifestyle ({dist:.0f} miles away)."
    )
