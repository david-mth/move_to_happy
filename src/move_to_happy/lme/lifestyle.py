"""Lifestyle dimension scoring — 7-dimension weighted lifestyle matching."""

from __future__ import annotations

import logging

import pandas as pd

from move_to_happy.lme.types import UserPreferences

logger = logging.getLogger(__name__)


def _normalize_inverse(series: pd.Series) -> pd.Series:
    """Lower values = better → invert and normalize to [0,1]."""
    s = series.fillna(series.max())
    if s.max() == s.min():
        return pd.Series(0.5, index=s.index)
    return 1 - (s - s.min()) / (s.max() - s.min())


def _normalize_direct(series: pd.Series) -> pd.Series:
    """Higher values = better → normalize to [0,1]."""
    s = series.fillna(series.min())
    if s.max() == s.min():
        return pd.Series(0.5, index=s.index)
    return (s - s.min()) / (s.max() - s.min())


def score_lifestyle_dimensions(df: pd.DataFrame, user: UserPreferences) -> pd.DataFrame:
    """Score ALL communities on 7 lifestyle dimensions and compute weighted
    LifestyleMatch.

    This scores ALL communities (not just residential candidates) because
    eliminated communities can still become Lifestyle Anchors for spillover.

    Args:
        df: Full communities DataFrame with normalized column names.
        user: User preferences with dimension weights and preferred values.

    Returns:
        DataFrame with added columns: ls_mountains, ls_beach, ls_lake, ls_airport,
        ls_climate, ls_terrain, ls_cost, LifestyleMatch.
    """
    df = df.copy()

    # Mountain access: closer = better
    df["ls_mountains"] = _normalize_inverse(df["miles_to_mountains"])

    # Beach access: closer = better
    df["ls_beach"] = _normalize_inverse(df["miles_to_beach"])

    # Lake access: closer = better
    df["ls_lake"] = _normalize_inverse(df["miles_to_lake"])

    # Airport access: closer to international airport = better
    df["ls_airport"] = _normalize_inverse(df["closest_intl_airport_miles"])

    # Climate match: binary (1 if preferred, 0.3 otherwise)
    df["ls_climate"] = df["climate"].apply(
        lambda c: 1.0 if c == user.preferred_climate else 0.3
    )

    # Terrain match: graded
    def _terrain_score(t: str) -> float:
        if t == user.preferred_terrain:
            return 1.0
        if t == "Hills":
            return 0.6
        if t == "Plains":
            return 0.3
        return 0.2

    df["ls_terrain"] = df["terrain"].apply(_terrain_score)

    # Cost favorability: lower COL = better
    df["ls_cost"] = _normalize_inverse(df["cost_of_living"])

    # Weighted lifestyle score
    lifestyle_dims = {
        "ls_mountains": user.pref_mountains,
        "ls_beach": user.pref_beach,
        "ls_lake": user.pref_lake,
        "ls_airport": user.pref_airport,
        "ls_climate": user.pref_climate,
        "ls_terrain": user.pref_terrain,
        "ls_cost": user.pref_cost,
    }

    df["LifestyleMatch_raw"] = sum(df[dim] * wt for dim, wt in lifestyle_dims.items())

    # Normalize to 0-1
    ls_min = df["LifestyleMatch_raw"].min()
    ls_max = df["LifestyleMatch_raw"].max()
    if ls_max > ls_min:
        df["LifestyleMatch"] = (df["LifestyleMatch_raw"] - ls_min) / (ls_max - ls_min)
    else:
        df["LifestyleMatch"] = 0.5

    logger.info(
        "Lifestyle scoring: mean=%.3f, std=%.3f",
        df["LifestyleMatch"].mean(),
        df["LifestyleMatch"].std(),
    )
    return df
