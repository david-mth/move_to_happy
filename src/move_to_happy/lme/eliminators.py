"""Eliminator pipeline — distance, affordability, and household fit gates."""

from __future__ import annotations

import logging
from math import atan2, cos, radians, sin, sqrt

import pandas as pd

from move_to_happy.lme.constants import BANDS_ABOVE, BANDS_BELOW, PRICE_BAND_WIDTH
from move_to_happy.lme.types import UserPreferences

logger = logging.getLogger(__name__)


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles between two lat/lon points."""
    earth_radius_miles = 3958.8
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return earth_radius_miles * 2 * atan2(sqrt(a), sqrt(1 - a))


def apply_distance_gate(
    df: pd.DataFrame,
    anchor_lat: float,
    anchor_lon: float,
    max_radius: float,
) -> tuple[pd.Series, pd.Series]:
    """Compute distance from anchor and return (distances, within_radius_mask).

    Uses columns: latitude, longitude.
    """
    distances = df.apply(
        lambda r: haversine_miles(
            anchor_lat, anchor_lon, r["latitude"], r["longitude"]
        ),
        axis=1,
    )
    within = distances <= max_radius
    logger.info(
        "Distance gate (%s mi): %d pass, %d eliminated",
        max_radius,
        within.sum(),
        (~within).sum(),
    )
    return distances, within


def apply_affordability_gate(
    max_band: int,
    core_availability: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, int, int]:
    """Filter by affordability window.

    Returns (avail_window, counts_df, band_min, band_max).

    Args:
        max_band: Max price band from ATL.
        core_availability: Core availability table with
            community_id, price_band, listing_count.

    Returns:
        Tuple of (filtered_availability, price_only_counts_per_community,
        band_min, band_max).
    """
    band_min = max_band - BANDS_BELOW * PRICE_BAND_WIDTH
    band_max = max_band + BANDS_ABOVE * PRICE_BAND_WIDTH

    avail_window = core_availability[
        (core_availability["price_band"] >= band_min)
        & (core_availability["price_band"] <= band_max)
    ].copy()

    price_counts = (
        avail_window.groupby("community_id")["listing_count"]
        .sum()
        .reset_index()
        .rename(columns={"listing_count": "matches_price_only"})
    )

    logger.info(
        "Affordability window: $%s – $%s (%d bands below, %d above)",
        f"{band_min:,}",
        f"{band_max:,}",
        BANDS_BELOW,
        BANDS_ABOVE,
    )
    return avail_window, price_counts, band_min, band_max


def apply_household_fit_gate(
    avail_window: pd.DataFrame,
    bb_target: str,
) -> pd.DataFrame:
    """Filter availability window to target bed/bath bucket.

    Returns DataFrame with community_id and matches_bb count.
    """
    avail_bb = avail_window[avail_window["bedbath_bucket"] == bb_target]
    bb_counts = (
        avail_bb.groupby("community_id")["listing_count"]
        .sum()
        .reset_index()
        .rename(columns={"listing_count": "matches_bb"})
    )
    logger.info(
        "Household fit gate (%s): %d communities pass", bb_target, len(bb_counts)
    )
    return bb_counts


def count_sfh_matches(
    attribute_overlay: pd.DataFrame,
    band_min: int,
    band_max: int,
) -> pd.DataFrame:
    """Count SFH attribute matches in the affordability window.

    Returns DataFrame with community_id and matches_sfh count.
    """
    sfh_overlay = attribute_overlay[
        (attribute_overlay["attribute_type"] == "SFH")
        & (attribute_overlay["price_band"] >= band_min)
        & (attribute_overlay["price_band"] <= band_max)
    ]
    sfh_counts = (
        sfh_overlay.groupby("community_id")["listing_count"]
        .sum()
        .reset_index()
        .rename(columns={"listing_count": "matches_sfh"})
    )
    return sfh_counts


def run_eliminator_pipeline(
    df: pd.DataFrame,
    user: UserPreferences,
    max_band: int,
    core_availability: pd.DataFrame,
    attribute_overlay: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full eliminator pipeline.

    Args:
        df: Communities DataFrame with normalized columns.
        user: User preferences.
        max_band: Max price band from ATL.
        core_availability: Core availability table.
        attribute_overlay: Attribute overlay table.

    Returns:
        Tuple of (full_df_with_flags, residential_candidates).
        full_df has columns: dist_to_anchor, elim_distance, matches_price_only,
        elim_affordability, matches_bb, elim_household_fit, matches_sfh,
        matches_all_constraints, eliminated.
    """
    df = df.copy()

    # 1. Distance gate
    df["dist_to_anchor"], within_radius = apply_distance_gate(
        df, user.anchor_lat, user.anchor_lon, user.max_radius_miles
    )
    df["elim_distance"] = ~within_radius

    # 2. Affordability gate
    avail_window, price_counts, band_min, band_max = apply_affordability_gate(
        max_band, core_availability
    )
    df = df.merge(
        price_counts, left_on="canonical_id", right_on="community_id", how="left"
    )
    if "community_id" in df.columns:
        df = df.drop(columns=["community_id"])
    df["matches_price_only"] = df["matches_price_only"].fillna(0).astype(int)
    df["elim_affordability"] = df["matches_price_only"] == 0

    # 3. Household fit gate
    bb_counts = apply_household_fit_gate(avail_window, user.bedbath_bucket)
    df = df.merge(
        bb_counts, left_on="canonical_id", right_on="community_id", how="left"
    )
    if "community_id" in df.columns:
        df = df.drop(columns=["community_id"])
    df["matches_bb"] = df["matches_bb"].fillna(0).astype(int)
    df["elim_household_fit"] = df["matches_bb"] == 0

    # 4. SFH soft filter
    sfh_counts = count_sfh_matches(attribute_overlay, band_min, band_max)
    df = df.merge(
        sfh_counts, left_on="canonical_id", right_on="community_id", how="left"
    )
    if "community_id" in df.columns:
        df = df.drop(columns=["community_id"])
    df["matches_sfh"] = df["matches_sfh"].fillna(0).astype(int)

    # Combined constraint count
    df["matches_all_constraints"] = df[["matches_bb", "matches_sfh"]].min(axis=1)

    # Final elimination flag
    df["eliminated"] = (
        df["elim_distance"] | df["elim_affordability"] | df["elim_household_fit"]
    )

    residential = df[~df["eliminated"]].copy()

    logger.info(
        "Eliminator pipeline: %d total → %d residential candidates (%d eliminated)",
        len(df),
        len(residential),
        df["eliminated"].sum(),
    )
    return df, residential
