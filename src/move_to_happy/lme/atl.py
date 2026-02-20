"""Affordability Translation Layer — monthly payment to max purchase price."""

from __future__ import annotations

from move_to_happy.lme.constants import ATL_DEFAULTS, STATE_TAX_RATES


def compute_max_price(
    monthly_payment: float,
    loan_term_years: int = 30,
    down_payment_pct: float = 0.10,
    state: str = "Georgia",
    rate: float | None = None,
    tax_rate: float | None = None,
    insurance_annual: float | None = None,
    pmi_rate: float | None = None,
) -> int:
    """Convert monthly housing payment to maximum purchase price.

    Uses the ATL formula from Spec Sec 5.3:
    MaxPrice = (MonthlyPayment - insurance/12) / (P&I_factor + tax/12 + PMI/12)
    """
    if rate is None:
        rate = (
            ATL_DEFAULTS["interest_rate_30yr"]
            if loan_term_years == 30
            else ATL_DEFAULTS["interest_rate_15yr"]
        )
    if tax_rate is None:
        tax_rate = STATE_TAX_RATES.get(state, ATL_DEFAULTS["property_tax_rate"])
    if insurance_annual is None:
        insurance_annual = ATL_DEFAULTS["homeowners_insurance_annual"]
    if pmi_rate is None:
        pmi_rate = ATL_DEFAULTS["pmi_annual_rate"]

    n = loan_term_years * 12
    r = rate / 12  # monthly rate
    financed_pct = 1.0 - down_payment_pct

    # P&I factor per dollar of home price (financed portion)
    if r > 0:
        pi_factor = financed_pct * (r * (1 + r) ** n) / ((1 + r) ** n - 1)
    else:
        pi_factor = financed_pct / n

    # Monthly costs per dollar of home price
    tax_monthly = tax_rate / 12
    insurance_monthly = insurance_annual / 12  # flat dollar amount
    pmi_monthly = (pmi_rate / 12) * financed_pct if down_payment_pct < 0.20 else 0.0

    # Solve: monthly_payment = price * (pi_factor + tax_monthly + pmi_monthly)
    #        + insurance_monthly
    price_factor = pi_factor + tax_monthly + pmi_monthly
    max_price = (monthly_payment - insurance_monthly) / price_factor

    return max(0, int(round(max_price / 1000) * 1000))
