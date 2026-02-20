"""LME Engine — orchestrates the full scoring pipeline."""

from __future__ import annotations

import logging
from dataclasses import asdict
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import boto3

from move_to_happy.lme.atl import compute_max_price
from move_to_happy.lme.constants import (
    PRICE_BAND_WIDTH,
    W_HOUSING,
    W_LIFESTYLE,
    W_SPILLOVER,
)
from move_to_happy.lme.eliminators import run_eliminator_pipeline
from move_to_happy.lme.lifestyle import score_lifestyle_dimensions
from move_to_happy.lme.scoring import (
    compute_constraint_pressure,
    compute_final_score,
    compute_housing_score,
)
from move_to_happy.lme.spillover import (
    compute_spillover_scores,
    generate_spillover_explanation,
    identify_lifestyle_anchors,
)
from move_to_happy.lme.synthetic_housing import (
    generate_attribute_overlay,
    generate_core_availability,
    generate_median_prices,
)
from move_to_happy.lme.types import CommunityScore, LMEResult, UserPreferences

logger = logging.getLogger(__name__)


class LMEEngine:
    """Lifestyle Matching Engine — deterministic community scoring.

    Workflow: ATL → Eliminators → Lifestyle → Spillover → Final Score.

    The engine generates synthetic housing data once at init time,
    then score() can be called repeatedly per user (fast, no regeneration).
    """

    def __init__(self, communities_df: pd.DataFrame, seed: int = 42) -> None:
        """Initialize the engine with community data.

        Args:
            communities_df: DataFrame with normalized column names.
                Must include: canonical_id, city_state, state_name, latitude,
                longitude, terrain, climate, population, cost_of_living,
                miles_to_mountains, miles_to_beach, miles_to_lake,
                closest_intl_airport_miles.
            seed: Random seed for synthetic housing generation.
        """
        # Filter out communities marked as needing updates
        if "needs_updating" in communities_df.columns:
            df = communities_df[
                communities_df["needs_updating"].str.upper() != "YES"
            ].copy()
        else:
            df = communities_df.copy()
        df = df.reset_index(drop=True)

        # Generate synthetic housing data once
        logger.info("Generating synthetic housing for %d communities...", len(df))
        df["median_home_price"] = generate_median_prices(df, seed=seed)
        self._core_availability = generate_core_availability(df, seed=seed)

        # Aggregate core availability by community_id + price_band for overlay
        core_band_totals = (
            self._core_availability.groupby(["community_id", "price_band"])[
                "listing_count"
            ]
            .sum()
            .reset_index()
        )
        self._attribute_overlay = generate_attribute_overlay(
            df, core_band_totals, seed=seed
        )

        self._df = df
        self._seed = seed
        logger.info("LMEEngine initialized: %d communities", len(df))

    def score(
        self,
        user: UserPreferences,
        top_n: int = 25,
        w_housing: float = W_HOUSING,
        w_lifestyle: float = W_LIFESTYLE,
        w_spillover: float = W_SPILLOVER,
    ) -> LMEResult:
        """Score communities for a given user and return ranked results.

        Args:
            user: User preferences.
            top_n: Number of top results to return.
            w_housing: Weight for housing score.
            w_lifestyle: Weight for lifestyle score.
            w_spillover: Weight for spillover score.

        Returns:
            LMEResult with ranked communities and metadata.
        """
        # 1. ATL: compute max purchase price
        max_price = compute_max_price(
            user.monthly_payment,
            user.loan_term_years,
            user.down_payment_pct,
            user.anchor_state,
        )
        max_band = int(max_price / PRICE_BAND_WIDTH) * PRICE_BAND_WIDTH
        band_min = max_band - 7 * PRICE_BAND_WIDTH  # BANDS_BELOW
        band_max = max_band + 2 * PRICE_BAND_WIDTH  # BANDS_ABOVE
        logger.info("ATL: max_price=$%s, band=$%s", f"{max_price:,}", f"{max_band:,}")

        # 2. Lifestyle scoring on ALL communities (needed for spillover)
        df_scored = score_lifestyle_dimensions(self._df, user)

        # 3. Eliminator pipeline
        full_df, residential = run_eliminator_pipeline(
            df_scored,
            user,
            max_band,
            self._core_availability,
            self._attribute_overlay,
        )

        if residential.empty:
            logger.warning("No residential candidates after eliminators")
            return LMEResult(
                rankings=[],
                total_candidates=0,
                eliminated_count=len(full_df),
                max_purchase_price=max_price,
                affordability_window=(band_min, band_max),
            )

        # 4. Constraint pressure + Housing score
        residential = compute_constraint_pressure(residential)
        residential = compute_housing_score(residential)

        # 5. Spillover
        anchors = identify_lifestyle_anchors(full_df)
        residential = compute_spillover_scores(residential, anchors)

        # 6. Final weighted score
        residential = compute_final_score(
            residential, w_housing, w_lifestyle, w_spillover
        )

        # 7. Build results
        top = residential.head(top_n)
        rankings = []
        for _, row in top.iterrows():
            # Generate spillover explanation
            explanation = ""
            anchor_name = row.get("SpilloverAnchor", "")
            if anchor_name:
                anchor_data = full_df[full_df["city_state"] == anchor_name]
                if not anchor_data.empty:
                    explanation = generate_spillover_explanation(
                        row, anchor_data.iloc[0]
                    )

            rankings.append(
                CommunityScore(
                    canonical_id=row.get("canonical_id", ""),
                    city_state=row.get("city_state", ""),
                    state_name=row.get("state_name", ""),
                    latitude=float(row.get("latitude", 0)),
                    longitude=float(row.get("longitude", 0)),
                    terrain=row.get("terrain", ""),
                    climate=row.get("climate", ""),
                    population=int(row.get("population", 0)),
                    cost_of_living=float(row.get("cost_of_living", 0)),
                    median_home_price=int(row.get("median_home_price", 0)),
                    housing_score=float(row.get("HousingScore", 0)),
                    lifestyle_score=float(row.get("LifestyleMatch", 0)),
                    spillover_score=float(row.get("SpilloverScore", 0)),
                    final_score=float(row.get("FinalScore", 0)),
                    matches_bb=int(row.get("matches_bb", 0)),
                    matches_sfh=int(row.get("matches_sfh", 0)),
                    pressure=row.get("pressure", "Low"),
                    spillover_anchor=anchor_name,
                    spillover_explanation=explanation,
                    dist_to_anchor=float(row.get("dist_to_anchor", 0)),
                )
            )

        result = LMEResult(
            rankings=rankings,
            total_candidates=len(residential),
            eliminated_count=int(full_df["eliminated"].sum()),
            max_purchase_price=max_price,
            affordability_window=(band_min, band_max),
        )
        logger.info(
            "Scoring complete: %d candidates, top score=%.3f",
            result.total_candidates,
            rankings[0].final_score if rankings else 0,
        )
        return result

    @classmethod
    def from_s3_parquet(
        cls,
        s3_path: str,
        boto_session: boto3.Session | None = None,
        seed: int = 42,
    ) -> LMEEngine:
        """Load communities from S3 Parquet and initialize engine.

        Args:
            s3_path: S3 URI to Parquet file/directory.
            boto_session: Optional boto3.Session for AWS credentials.
            seed: Random seed for synthetic housing.

        Returns:
            Initialized LMEEngine.
        """
        import awswrangler as wr

        logger.info("Loading communities from %s", s3_path)
        df = wr.s3.read_parquet(
            path=s3_path,
            boto3_session=boto_session,
        )
        logger.info("Loaded %d communities from S3", len(df))
        return cls(df, seed=seed)

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, seed: int = 42) -> LMEEngine:
        """Create engine from an existing DataFrame (for testing).

        Args:
            df: Communities DataFrame with normalized column names.
            seed: Random seed.

        Returns:
            Initialized LMEEngine.
        """
        return cls(df, seed=seed)

    def to_dict(self, result: LMEResult) -> dict:
        """Convert LMEResult to a JSON-serializable dictionary."""
        return asdict(result)
