"""Pydantic models for Claude tool-use structured extraction.

Each model is the authoritative definition of structured data.
JSON schemas for Claude are auto-generated via .model_json_schema()
so the schema is always in sync with the model — no hand-maintained dicts.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator


class BudgetExtraction(BaseModel):
    max_monthly_payment: float | None = None
    loan_term_years: Literal[15, 30] | None = None
    down_payment_pct: Annotated[float, Field(ge=0.01, le=1.0)] | None = None


class HouseholdExtraction(BaseModel):
    bedbath_bucket: Literal["BB1", "BB2", "BB3"] | None = None
    property_type: Literal["SFH", "Any"] | None = None


class GeographicAnchorExtraction(BaseModel):
    latitude: float | None = None
    longitude: float | None = None
    city_name: str | None = None
    state: Literal["Georgia", "Alabama", "Florida"] | None = None
    radius_miles: float | None = None


class LifestyleWeightsExtraction(BaseModel):
    mountains: Annotated[float, Field(ge=0.0, le=1.0)] | None = None
    beach: Annotated[float, Field(ge=0.0, le=1.0)] | None = None
    lake: Annotated[float, Field(ge=0.0, le=1.0)] | None = None
    airport: Annotated[float, Field(ge=0.0, le=1.0)] | None = None
    climate: Annotated[float, Field(ge=0.0, le=1.0)] | None = None
    terrain: Annotated[float, Field(ge=0.0, le=1.0)] | None = None
    cost: Annotated[float, Field(ge=0.0, le=1.0)] | None = None

    @model_validator(mode="after")
    def warn_if_weights_out_of_range(self) -> LifestyleWeightsExtraction:
        provided = [
            v
            for v in [
                self.mountains,
                self.beach,
                self.lake,
                self.airport,
                self.climate,
                self.terrain,
                self.cost,
            ]
            if v is not None
        ]
        if provided:
            total = sum(provided)
            if total > 0 and not (0.8 <= total <= 1.2):
                # Normalize rather than reject — LLM outputs are best-effort
                scale = 1.0 / total
                for field_name in [
                    "mountains",
                    "beach",
                    "lake",
                    "airport",
                    "climate",
                    "terrain",
                    "cost",
                ]:
                    val = getattr(self, field_name)
                    if val is not None:
                        object.__setattr__(self, field_name, round(val * scale, 4))
        return self


_ClimateType = Literal[
    "Temperate", "Subtropical", "Tropical", "Arid", "Semi-Arid", "Continental"
]
_TerrainType = Literal[
    "Mountains", "Hills", "Piedmont", "Plains", "Coastal", "Swamp/Marsh"
]


class LMEInputExtraction(BaseModel):
    """Top-level model for Claude's structured extraction of user preferences."""

    budget: BudgetExtraction = Field(default_factory=BudgetExtraction)
    household: HouseholdExtraction = Field(default_factory=HouseholdExtraction)
    geographic_anchor: GeographicAnchorExtraction = Field(
        default_factory=GeographicAnchorExtraction
    )
    lifestyle_weights: LifestyleWeightsExtraction = Field(
        default_factory=LifestyleWeightsExtraction
    )
    preferred_climate: _ClimateType | None = None
    preferred_terrain: _TerrainType | None = None
    extraction_confidence: dict[str, float] = Field(default_factory=dict)
    clarification_needed: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Lead summary models
# ---------------------------------------------------------------------------


class UserProfileSummary(BaseModel):
    budget_range: str | None = None
    household_type: str | None = None
    top_priorities: list[str] = Field(default_factory=list)
    geographic_preference: str | None = None
    dealbreakers: list[str] = Field(default_factory=list)


class TopMatch(BaseModel):
    canonical_city_id: str = ""
    city_state: str = ""
    rank: int = 0
    match_rationale: str | None = None
    key_strengths: list[str] = Field(default_factory=list)
    key_tradeoffs: list[str] = Field(default_factory=list)


class SessionMetadata(BaseModel):
    turns: int = 0
    refinements: int = 0
    timestamp: str = ""


class LeadSummaryOutput(BaseModel):
    user_profile: UserProfileSummary = Field(default_factory=UserProfileSummary)
    top_matches: list[TopMatch] = Field(default_factory=list)
    session_metadata: SessionMetadata = Field(default_factory=SessionMetadata)


# ---------------------------------------------------------------------------
# Auto-generated JSON schemas for Claude tool-use (always in sync with models)
# ---------------------------------------------------------------------------


def _simplify_schema(node: object) -> object:
    """Recursively flatten anyOf[<type>, {"type":"null"}] → <type>.

    Pydantic v2 generates anyOf patterns for Optional fields. Claude works
    best with simple {"type": "string", "enum": [...]} rather than
    {"anyOf": [{"type": "string", "enum": [...]}, {"type": "null"}]},
    and can produce malformed values like '{"Coastal"}' when it sees the
    latter pattern.
    """
    if isinstance(node, dict):
        if "anyOf" in node:
            non_null = [s for s in node["anyOf"] if s != {"type": "null"}]
            if len(non_null) == 1:
                simplified: dict = {**non_null[0]}
                for key in ("default", "title", "description"):
                    if key in node:
                        simplified[key] = node[key]
                return _simplify_schema(simplified)
        return {k: _simplify_schema(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_simplify_schema(item) for item in node]
    return node


LME_INPUT_SCHEMA: dict = _simplify_schema(LMEInputExtraction.model_json_schema())  # type: ignore[assignment]
LEAD_SUMMARY_SCHEMA: dict = _simplify_schema(LeadSummaryOutput.model_json_schema())  # type: ignore[assignment]
