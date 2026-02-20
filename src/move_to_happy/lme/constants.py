"""LME constants — ATL defaults, tax rates, weights, band parameters."""

from __future__ import annotations

# Affordability Translation Layer defaults
ATL_DEFAULTS: dict[str, float | int] = {
    "interest_rate_30yr": 0.0699,
    "interest_rate_15yr": 0.0625,
    "property_tax_rate": 0.0105,
    "homeowners_insurance_annual": 1_800,
    "pmi_annual_rate": 0.005,
}

# State-level property-tax overrides (effective rates)
STATE_TAX_RATES: dict[str, float] = {
    "Georgia": 0.0092,
    "Florida": 0.0089,
    "Alabama": 0.0040,
}

# Price band parameters
PRICE_BAND_WIDTH: int = 10_000
BANDS_ABOVE: int = 2
BANDS_BELOW: int = 7

# Bed/bath buckets
BB_BUCKETS: list[str] = ["BB1", "BB2", "BB3"]
BB_SHARES: dict[str, float] = {"BB1": 0.30, "BB2": 0.45, "BB3": 0.25}

# Synthetic housing parameters
BASE_PRICE_AT_100: int = 350_000
COL_SENSITIVITY: int = 5_000

# Spillover parameters
SPILLOVER_RANGE_MILES: int = 60

# Final score weights
W_HOUSING: float = 0.40
W_LIFESTYLE: float = 0.40
W_SPILLOVER: float = 0.20
