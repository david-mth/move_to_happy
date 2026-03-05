"""Load canonical LME reference documents into RAG.

These allow the Explanation Generator to reference the system's own
architectural principles when explaining results.

NOTE: This loader requires the two canonical documents to be placed in
data/reference/. If the files are not found, stub documents are generated
from the system prompt and known LME logic.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ..types import RAGDocument

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "data"
REFERENCE_DIR = DATA_DIR / "reference"

FOUNDATIONAL_PRINCIPLES_STUB = """
Move to Happy — Foundational Principles for the Lifestyle Matching Engine

Move to Happy is a dynamic, explainable comparison engine for relocation \
intelligence. It is NOT a listings portal, NOT a static rankings site, \
NOT a marketing dashboard.

Core Principles:
1. Eliminators precede optimization — distance, housing viability, and \
structural constraints are gates, not preferences.
2. Every core metric must be normalizable and weightable.
3. Every composite score must be explainable — decomposable into \
contributing metrics, weight impact, directionality, percentile context.
4. Spillover is a core system feature — communities eliminated in one \
dimension may remain anchors in another.
5. Structure before narrative — data models stability, durability, \
viability, momentum; narrative flows FROM structure.
6. No static rankings — all comparisons are contextual, user-weighted, \
persona-driven, adjustable in real time.
7. Dual-layer design: Consumer and EDO are separate views of the SAME engine.

Canonical LME Flow:
User Entry → Distance Gate → Housing Affordability Gate → Health Module → \
Lifestyle Matching → Structural/Economic Signals → Spillover Adjustments → \
Weighted Final Ranking → Diagnostic Explanation Output

The Moat: Cross-domain normalization, deterministic weighting, spillover \
modeling, tradeoff transparency, persona-based comparison, dual-layer \
application.
"""

TECHNICAL_LOGIC_SPEC_STUB = """
Move to Happy — Master Technical Logic Specification v1.1

Affordability Translation Layer (ATL):
Monthly payment → max purchase price via P&I factor + tax/insurance/PMI.
Interest rates: 6.99% (30yr), 6.25% (15yr).
State tax rates: GA 0.92%, FL 0.89%, AL 0.40%.

Inventory Banding:
Fixed $10k price bands, precomputed. Household fit via bed/bath buckets \
(BB1=1-2 bed, BB2=3 bed/2 bath, BB3=4+ bed).
Band Window: Above max +1 to +2 bands, below max -5 to -7 bands.

Constraint Pressure Detection:
Retention = Matches_with_constraint / Matches_price_only.
<10% = High pressure, 10-30% = Moderate, >30% = Low.

Housing Availability Score:
HousingScore(c) = MatchCount(c) / max(MatchCount across candidates).

Community Roles:
- Residential Candidate: passes all eliminators
- Lifestyle Anchor: may fail affordability, retained as reference

Spillover Formula:
Spillover(r, a) = LifestyleAffinity(a) × Proximity(r, a)
Range: 60 miles. Proximity decay: inverse distance.

Final Score Composition:
FinalScore = 0.40 × HousingScore + 0.40 × LifestyleMatch + 0.20 × SpilloverScore

7 Lifestyle Dimensions (weighted, sum to 1.0):
Mountains (0.30), Beach (0.15), Lake (0.10), Airport (0.10), \
Climate (0.15), Terrain (0.10), Cost (0.10).

Mandatory Explanation Pattern:
"You can't live in X at this budget, but Y gives you the closest access \
to the X lifestyle."
"""


def load_lme_reference_docs() -> list[RAGDocument]:
    """Load the canonical LME specification documents into RAG."""
    docs: list[RAGDocument] = []

    fp_path = REFERENCE_DIR / "foundational_principles.txt"
    if fp_path.exists():
        content = fp_path.read_text()
        logger.info("Loaded Foundational Principles from %s", fp_path)
    else:
        content = FOUNDATIONAL_PRINCIPLES_STUB
        logger.warning(
            "Foundational Principles not found at %s — using stub",
            fp_path,
        )

    docs.append(
        RAGDocument(
            content=content,
            canonical_city_id=None,
            source_type="lme_spec",
            metadata={
                "document": "foundational_principles",
                "version": "2026-02-16",
                "authority": "system_of_record",
            },
        )
    )

    tls_path = REFERENCE_DIR / "technical_logic_spec.txt"
    if tls_path.exists():
        content = tls_path.read_text()
        logger.info("Loaded Technical Logic Spec from %s", tls_path)
    else:
        content = TECHNICAL_LOGIC_SPEC_STUB
        logger.warning(
            "Technical Logic Spec not found at %s — using stub",
            tls_path,
        )

    docs.append(
        RAGDocument(
            content=content,
            canonical_city_id=None,
            source_type="lme_spec",
            metadata={
                "document": "technical_logic_spec",
                "version": "v1.1",
                "authority": "system_of_record",
            },
        )
    )

    logger.info("Loaded %d LME reference documents", len(docs))
    return docs
