"""Synthetic housing data generation.

Generates median prices, core availability, and attribute overlay.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from move_to_happy.lme.constants import (
    BASE_PRICE_AT_100,
    BB_SHARES,
    COL_SENSITIVITY,
    PRICE_BAND_WIDTH,
)

logger = logging.getLogger(__name__)


def generate_median_prices(df: pd.DataFrame, seed: int = 42) -> pd.Series:
    """Generate synthetic median home price for each community.

    Derived from cost_of_living (61-102 range). National median ~$350k.
    Scale so COL=100 ≈ $350k, lower COL = cheaper.

    Args:
        df: Communities DataFrame with cost_of_living, terrain, population columns.
        seed: Random seed for reproducibility.

    Returns:
        Series of median home prices (int), indexed same as df.
    """
    rng = np.random.default_rng(seed)

    def _price(row: pd.Series) -> int:
        col = row["cost_of_living"]
        pop = row["population"]
        if pd.isna(col) or pd.isna(pop):
            return BASE_PRICE_AT_100
        base = BASE_PRICE_AT_100 + (col - 100) * COL_SENSITIVITY
        # Coastal premium
        if row["terrain"] == "Coastal":
            base *= 1.15
        # Population density premium (log-scaled)
        pop_factor = 1 + 0.05 * np.log10(max(pop, 100)) / np.log10(1_000_000)
        base *= pop_factor
        # Random noise ±8%
        noise = rng.uniform(0.92, 1.08)
        return max(int(round(base * noise / 1000) * 1000), 60_000)

    prices = df.apply(_price, axis=1)
    logger.info(
        "Median prices: $%s – $%s (mean $%s)",
        f"{prices.min():,}",
        f"{prices.max():,}",
        f"{prices.mean():,.0f}",
    )
    return prices


def generate_core_availability(df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """Generate listing counts by price_band x bedbath_bucket for each community.

    Returns DataFrame with columns: community_id, price_band,
    bedbath_bucket, listing_count. Uses canonical_id as community_id.
    """
    rng = np.random.default_rng(seed)
    all_records: list[dict] = []

    for _, row in df.iterrows():
        cid = row["canonical_id"]
        median = row["median_home_price"]
        pop = row["population"]

        # Total active listings proportional to population
        total_listings = max(5, int(pop * 0.003 + rng.poisson(10)))

        # Price band center and spread
        band_center = int(median / PRICE_BAND_WIDTH) * PRICE_BAND_WIDTH
        spread_bands = max(3, int(total_listings**0.3))

        bands = [
            band_center + i * PRICE_BAND_WIDTH
            for i in range(-spread_bands, spread_bands + 1)
            if band_center + i * PRICE_BAND_WIDTH >= 60_000
        ]

        for band in bands:
            dist = abs(band - median) / PRICE_BAND_WIDTH
            weight = np.exp(-0.5 * (dist / (spread_bands * 0.5)) ** 2)
            band_total = max(
                0,
                int(total_listings * weight / len(bands) * 3 + rng.poisson(1)),
            )

            for bb, share in BB_SHARES.items():
                count = max(0, int(round(band_total * share + rng.uniform(-0.5, 0.5))))
                if count > 0:
                    all_records.append(
                        {
                            "community_id": cid,
                            "price_band": band,
                            "bedbath_bucket": bb,
                            "listing_count": count,
                        }
                    )

    result = pd.DataFrame(all_records)
    logger.info(
        "Core availability: %s rows, %s communities",
        f"{len(result):,}",
        result["community_id"].nunique(),
    )
    return result


def generate_attribute_overlay(
    df: pd.DataFrame,
    core_band_totals: pd.DataFrame,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate structural attribute overlay counts for communities.

    Args:
        df: Communities DataFrame with canonical_id, population, terrain,
            miles_to_lake.
        core_band_totals: Aggregated core availability
            (community_id, price_band, listing_count).
        seed: Random seed.

    Returns:
        DataFrame with columns: community_id, price_band, attribute_type, listing_count.
    """
    rng = np.random.default_rng(seed)
    all_records: list[dict] = []

    for _, row in df.iterrows():
        cid = row["canonical_id"]
        pop = row["population"]
        terrain = row["terrain"]
        min_lake = row["miles_to_lake"] if pd.notna(row.get("miles_to_lake")) else 50

        comm_rows = core_band_totals[core_band_totals["community_id"] == cid]
        if comm_rows.empty:
            continue

        # Property type mix: smaller towns → more SFH
        sfh_share = 0.85 if pop < 5_000 else (0.70 if pop < 25_000 else 0.55)
        condo_share = (1 - sfh_share) * 0.55
        townhome_share = 1 - sfh_share - condo_share

        # Structural attribute rates
        single_level_rate = (
            0.40 if terrain == "Coastal" else (0.25 if terrain == "Mountains" else 0.35)
        )
        basement_rate = (
            0.45 if terrain == "Mountains" else (0.15 if terrain == "Coastal" else 0.25)
        )
        lakefront_rate = min(0.15, max(0.01, 5.0 / (min_lake + 1)))
        new_construction_rate = 0.08 + 0.05 * (pop / 100_000)

        attr_rates = {
            "SFH": sfh_share,
            "Condo": condo_share,
            "Townhome": townhome_share,
            "SingleLevel": single_level_rate,
            "Basement": basement_rate,
            "Lakefront": lakefront_rate,
            "NewConstruction": min(new_construction_rate, 0.25),
        }

        for _, cr in comm_rows.iterrows():
            band = cr["price_band"]
            total = cr["listing_count"]
            for attr, rate in attr_rates.items():
                count = max(0, int(round(total * rate + rng.uniform(-0.3, 0.3))))
                if count > 0:
                    all_records.append(
                        {
                            "community_id": cid,
                            "price_band": band,
                            "attribute_type": attr,
                            "listing_count": count,
                        }
                    )

    result = pd.DataFrame(all_records)
    logger.info(
        "Attribute overlay: %s rows, types: %s",
        f"{len(result):,}",
        sorted(result["attribute_type"].unique()) if len(result) > 0 else [],
    )
    return result
