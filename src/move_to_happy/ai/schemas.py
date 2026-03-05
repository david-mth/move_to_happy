"""JSON schemas for Claude tool-use structured extraction."""

from __future__ import annotations

LME_INPUT_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "budget": {
            "type": "object",
            "properties": {
                "max_monthly_payment": {
                    "type": "number",
                    "description": "Maximum monthly housing payment in USD",
                },
                "loan_term_years": {
                    "type": "integer",
                    "enum": [15, 30],
                },
                "down_payment_pct": {
                    "type": "number",
                    "description": ("Down payment as decimal (0.10 = 10%)"),
                },
            },
        },
        "household": {
            "type": "object",
            "properties": {
                "bedbath_bucket": {
                    "type": "string",
                    "enum": ["BB1", "BB2", "BB3"],
                    "description": ("BB1=1-2 bed, BB2=3 bed/2 bath, BB3=4+ bed"),
                },
                "property_type": {
                    "type": "string",
                    "enum": ["SFH", "Any"],
                },
            },
        },
        "geographic_anchor": {
            "type": "object",
            "properties": {
                "latitude": {"type": "number"},
                "longitude": {"type": "number"},
                "city_name": {
                    "type": "string",
                    "description": "City name for the anchor point",
                },
                "state": {
                    "type": "string",
                    "enum": ["Georgia", "Alabama", "Florida"],
                },
                "radius_miles": {"type": "number"},
            },
        },
        "lifestyle_weights": {
            "type": "object",
            "description": (
                "User importance weights (0.0-1.0) for each "
                "lifestyle dimension. Should sum to ~1.0."
            ),
            "properties": {
                "mountains": {"type": "number"},
                "beach": {"type": "number"},
                "lake": {"type": "number"},
                "airport": {"type": "number"},
                "climate": {"type": "number"},
                "terrain": {"type": "number"},
                "cost": {"type": "number"},
            },
        },
        "preferred_climate": {
            "type": "string",
            "enum": [
                "Temperate",
                "Subtropical",
                "Tropical",
                "Arid",
                "Semi-Arid",
                "Continental",
            ],
        },
        "preferred_terrain": {
            "type": "string",
            "enum": [
                "Mountains",
                "Hills",
                "Piedmont",
                "Plains",
                "Coastal",
                "Swamp/Marsh",
            ],
        },
        "extraction_confidence": {
            "type": "object",
            "description": ("Confidence scores (0.0-1.0) for each extracted field"),
            "additionalProperties": {"type": "number"},
        },
        "clarification_needed": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
                "Fields where confidence is low and clarification should be requested"
            ),
        },
    },
    "required": ["extraction_confidence"],
}

LEAD_SUMMARY_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "user_profile": {
            "type": "object",
            "properties": {
                "budget_range": {"type": "string"},
                "household_type": {"type": "string"},
                "top_priorities": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "geographic_preference": {"type": "string"},
                "dealbreakers": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
        },
        "top_matches": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "canonical_city_id": {"type": "string"},
                    "city_state": {"type": "string"},
                    "rank": {"type": "integer"},
                    "match_rationale": {"type": "string"},
                    "key_strengths": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "key_tradeoffs": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
        },
        "session_metadata": {
            "type": "object",
            "properties": {
                "turns": {"type": "integer"},
                "refinements": {"type": "integer"},
                "timestamp": {"type": "string"},
            },
        },
    },
}
