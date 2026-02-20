"""Test the deployed LME SageMaker endpoint with a sample user."""

from __future__ import annotations

import json
import logging

from _config import get_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ENDPOINT_NAME = "mth-lme-endpoint"


def main() -> None:
    """Invoke endpoint with sample user preferences."""
    session = get_session()
    runtime = session.client("sagemaker-runtime")

    sample_user = {
        "monthly_payment": 2500,
        "loan_term_years": 30,
        "down_payment_pct": 0.10,
        "bedbath_bucket": "BB2",
        "property_type_pref": "SFH",
        "anchor_lat": 33.749,
        "anchor_lon": -84.388,
        "anchor_state": "Georgia",
        "max_radius_miles": 120,
        "pref_mountains": 0.30,
        "pref_beach": 0.15,
        "pref_lake": 0.10,
        "pref_airport": 0.10,
        "pref_climate": 0.15,
        "pref_terrain": 0.10,
        "pref_cost": 0.10,
        "preferred_climate": "Temperate",
        "preferred_terrain": "Mountains",
    }

    logger.info("Invoking endpoint: %s", ENDPOINT_NAME)
    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType="application/json",
        Accept="application/json",
        Body=json.dumps(sample_user),
    )

    result = json.loads(response["Body"].read().decode())

    # Display results
    print("\n" + "=" * 70)
    print("  LME ENDPOINT RESULTS")
    print("=" * 70)
    print(f"  Total candidates:    {result.get('total_candidates', 'N/A')}")
    print(f"  Eliminated:          {result.get('eliminated_count', 'N/A')}")
    print(f"  Max purchase price:  ${result.get('max_purchase_price', 0):,}")
    print(f"  Affordability window: {result.get('affordability_window', 'N/A')}")
    print()

    rankings = result.get("rankings", [])
    print(f"  Top {len(rankings)} Communities:")
    print(
        f"  {'Rank':<5} {'Community':<30} {'Score':<8}"
        f" {'Housing':<9} {'Lifestyle':<10} {'Spillover':<10}"
    )
    print("  " + "-" * 72)

    for i, r in enumerate(rankings[:25], 1):
        print(
            f"  {i:<5} {r['city_state']:<30} "
            f"{r['final_score']:<8.3f} {r['housing_score']:<9.3f} "
            f"{r['lifestyle_score']:<10.3f} {r['spillover_score']:<10.3f}"
        )

    print("=" * 70)


if __name__ == "__main__":
    main()
