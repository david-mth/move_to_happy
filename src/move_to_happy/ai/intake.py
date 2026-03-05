"""Intake/Interpreter — converts free-text to structured LME fields.

Uses Claude's tool use (function calling) for reliable JSON extraction.
"""

from __future__ import annotations

import logging
from typing import Any

from .claude_client import ClaudeClient
from .prompts import INTAKE_PROMPT_SUFFIX
from .schemas import LME_INPUT_SCHEMA

logger = logging.getLogger(__name__)

GEOCODE_ANCHORS: dict[str, tuple[float, float]] = {
    "atlanta": (33.749, -84.388),
    "birmingham": (33.521, -86.802),
    "montgomery": (32.377, -86.300),
    "huntsville": (34.730, -86.586),
    "mobile": (30.694, -88.043),
    "jacksonville": (30.332, -81.656),
    "tampa": (27.951, -82.459),
    "orlando": (28.538, -81.379),
    "miami": (25.762, -80.192),
    "savannah": (32.081, -81.091),
    "athens": (33.951, -83.357),
    "macon": (32.841, -83.632),
    "tallahassee": (30.438, -84.281),
    "pensacola": (30.443, -87.217),
    "gainesville": (29.652, -82.325),
    "chattanooga": (35.046, -85.309),
    "augusta": (33.474, -81.975),
    "columbus ga": (32.461, -84.988),
}


class IntakeInterpreter:
    """Converts natural-language user input into structured LME fields."""

    def __init__(self, claude: ClaudeClient) -> None:
        self._claude = claude

    def interpret(self, user_text: str) -> dict[str, Any]:
        """Extract structured preferences from free-text input."""
        result = self._claude.generate_structured(
            user_message=(f"{INTAKE_PROMPT_SUFFIX}\n\nUSER INPUT:\n{user_text}"),
            output_schema=LME_INPUT_SCHEMA,
        )
        return result

    def needs_clarification(self, extracted: dict[str, Any]) -> list[str]:
        """Return list of fields needing user clarification."""
        return extracted.get("clarification_needed", [])

    def to_lme_params(self, extracted: dict[str, Any]) -> dict[str, Any]:
        """Convert extracted preferences to LME UserPreferences kwargs.

        Maps the Claude-extracted schema to the UserPreferences dataclass
        fields used by LMEEngine.score().
        """
        params: dict[str, Any] = {}

        budget = extracted.get("budget", {})
        if budget.get("max_monthly_payment"):
            params["monthly_payment"] = budget["max_monthly_payment"]
        if budget.get("loan_term_years"):
            params["loan_term_years"] = budget["loan_term_years"]
        if budget.get("down_payment_pct"):
            params["down_payment_pct"] = budget["down_payment_pct"]

        household = extracted.get("household", {})
        if household.get("bedbath_bucket"):
            params["bedbath_bucket"] = household["bedbath_bucket"]
        if household.get("property_type"):
            params["property_type_pref"] = household["property_type"]

        geo = extracted.get("geographic_anchor", {})
        if geo.get("latitude") and geo.get("longitude"):
            params["anchor_lat"] = geo["latitude"]
            params["anchor_lon"] = geo["longitude"]
        elif geo.get("city_name"):
            coords = self._geocode_city(geo["city_name"])
            if coords:
                params["anchor_lat"], params["anchor_lon"] = coords
        if geo.get("state"):
            params["anchor_state"] = geo["state"]
        if geo.get("radius_miles"):
            params["max_radius_miles"] = geo["radius_miles"]

        weights = extracted.get("lifestyle_weights", {})
        weight_map = {
            "mountains": "pref_mountains",
            "beach": "pref_beach",
            "lake": "pref_lake",
            "airport": "pref_airport",
            "climate": "pref_climate",
            "terrain": "pref_terrain",
            "cost": "pref_cost",
        }
        for src, dst in weight_map.items():
            if src in weights and weights[src] is not None:
                params[dst] = weights[src]

        if extracted.get("preferred_climate"):
            params["preferred_climate"] = extracted["preferred_climate"]
        if extracted.get("preferred_terrain"):
            params["preferred_terrain"] = extracted["preferred_terrain"]

        return params

    def _geocode_city(self, city_name: str) -> tuple[float, float] | None:
        """Simple lookup for common anchor cities."""
        key = city_name.strip().lower()
        return GEOCODE_ANCHORS.get(key)
