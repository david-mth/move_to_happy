"""LME data types — UserPreferences, CommunityScore, LMEResult.

UserPreferences uses Pydantic BaseModel for runtime validation at the
human→LME boundary. Invalid LLM extractions are caught here before they
corrupt any scoring math.

CommunityScore and LMEResult are output types — they use BaseModel for
clean .model_dump() serialization (replaces asdict()).
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

_BedbathBucket = Literal["BB1", "BB2", "BB3"]
_PropertyType = Literal["SFH", "Any"]
_AnchorState = Literal["Georgia", "Alabama", "Florida"]
_LoanTerm = Literal[15, 30]
_ClimateType = Literal[
    "Temperate", "Subtropical", "Tropical", "Arid", "Semi-Arid", "Continental"
]
_TerrainType = Literal[
    "Mountains", "Hills", "Piedmont", "Plains", "Coastal", "Swamp/Marsh"
]
_PressureLevel = Literal["Low", "Medium", "High"]

_Weight = Annotated[float, Field(ge=0.0, le=1.0)]


class UserPreferences(BaseModel):
    """User input for LME scoring — validated at construction time.

    All seven lifestyle weights must sum to within 5% of 1.0.
    Invalid values raise ValidationError before reaching LME math.
    """

    monthly_payment: Annotated[float, Field(gt=0)] = 2_500.0
    loan_term_years: _LoanTerm = 30
    down_payment_pct: Annotated[float, Field(ge=0.01, le=1.0)] = 0.10
    bedbath_bucket: _BedbathBucket = "BB2"
    property_type_pref: _PropertyType = "SFH"

    anchor_lat: float = 33.749
    anchor_lon: float = -84.388
    anchor_state: _AnchorState = "Georgia"
    max_radius_miles: Annotated[float, Field(gt=0)] = 120.0

    pref_mountains: _Weight = 0.30
    pref_beach: _Weight = 0.15
    pref_lake: _Weight = 0.10
    pref_airport: _Weight = 0.10
    pref_climate: _Weight = 0.15
    pref_terrain: _Weight = 0.10
    pref_cost: _Weight = 0.10

    preferred_climate: _ClimateType = "Temperate"
    preferred_terrain: _TerrainType = "Mountains"

    @model_validator(mode="after")
    def _validate_weight_sum(self) -> UserPreferences:
        total = (
            self.pref_mountains
            + self.pref_beach
            + self.pref_lake
            + self.pref_airport
            + self.pref_climate
            + self.pref_terrain
            + self.pref_cost
        )
        if not (0.90 <= total <= 1.10):
            msg = (
                f"Lifestyle weights must sum to ~1.0 (got {total:.3f}). "
                "Adjust pref_mountains, pref_beach, pref_lake, pref_airport, "
                "pref_climate, pref_terrain, pref_cost."
            )
            raise ValueError(msg)
        return self


class CommunityScore(BaseModel):
    """One ranked community in the LME result."""

    # Output type — no strict field validation, just typed serialization.
    model_config = ConfigDict(extra="ignore")

    canonical_id: str = ""
    city_state: str = ""
    state_name: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    terrain: str = ""
    climate: str = ""
    population: int = 0
    cost_of_living: float = 0.0
    median_home_price: int = 0

    housing_score: float = 0.0
    lifestyle_score: float = 0.0
    spillover_score: float = 0.0
    final_score: float = 0.0

    matches_bb: int = 0
    matches_sfh: int = 0
    pressure: _PressureLevel = "Low"
    spillover_anchor: str = ""
    spillover_explanation: str = ""
    dist_to_anchor: float = 0.0


class LMEResult(BaseModel):
    """Full LME scoring result."""

    rankings: list[CommunityScore] = Field(default_factory=list)
    total_candidates: int = 0
    eliminated_count: int = 0
    max_purchase_price: int = 0
    affordability_window: tuple[int, int] = (0, 0)
