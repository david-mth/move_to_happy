"""LME data types — UserPreferences, CommunityScore, LMEResult."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class UserPreferences:
    """User input for LME scoring.

    Replaces the hardcoded USER dict from the notebook.
    """

    monthly_payment: float = 2_500.0
    loan_term_years: int = 30
    down_payment_pct: float = 0.10
    bedbath_bucket: str = "BB2"
    property_type_pref: str = "SFH"

    # Anchor location (lat/lon directly — no city name lookup needed)
    anchor_lat: float = 33.749
    anchor_lon: float = -84.388
    anchor_state: str = "Georgia"
    max_radius_miles: float = 120.0

    # Lifestyle dimension weights (must sum to ~1.0)
    pref_mountains: float = 0.30
    pref_beach: float = 0.15
    pref_lake: float = 0.10
    pref_airport: float = 0.10
    pref_climate: float = 0.15
    pref_terrain: float = 0.10
    pref_cost: float = 0.10

    # Preferred categorical values
    preferred_climate: str = "Temperate"
    preferred_terrain: str = "Mountains"


@dataclass
class CommunityScore:
    """One ranked community in the LME result."""

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
    pressure: str = "Low"
    spillover_anchor: str = ""
    spillover_explanation: str = ""
    dist_to_anchor: float = 0.0


@dataclass
class LMEResult:
    """Full LME scoring result."""

    rankings: list[CommunityScore] = field(default_factory=list)
    total_candidates: int = 0
    eliminated_count: int = 0
    max_purchase_price: int = 0
    affordability_window: tuple[int, int] = (0, 0)
