"""RAG guardrails — enforce that RAG never enters the LME scoring path."""

from __future__ import annotations

import inspect
import logging

logger = logging.getLogger(__name__)

_SCORING_MODULES = frozenset(
    {
        "move_to_happy.lme.engine",
        "move_to_happy.lme.atl",
        "move_to_happy.lme.eliminators",
        "move_to_happy.lme.lifestyle",
        "move_to_happy.lme.spillover",
        "move_to_happy.lme.scoring",
        "move_to_happy.lme.synthetic_housing",
    }
)


def validate_rag_not_in_scoring() -> bool:
    """Check that RAG retrieval is not being called from the LME scoring path.

    Walks the call stack and raises if any frame belongs to an LME scoring
    module. This is a runtime guardrail to enforce the architectural
    constraint that RAG is read-only narrative context.
    """
    for frame_info in inspect.stack():
        module = frame_info.frame.f_globals.get("__name__", "")
        if module in _SCORING_MODULES:
            msg = (
                f"RAG retrieval called from LME scoring module: {module}. "
                "RAG content must NEVER enter the scoring pipeline."
            )
            logger.error(msg)
            raise RuntimeError(msg)
    return True


def validate_lme_determinism(
    result_a: dict,
    result_b: dict,
) -> bool:
    """Verify two LME results are identical (with/without AI layer).

    Used in testing to confirm the AI layer doesn't affect LME outputs.
    """
    rankings_a = result_a.get("rankings", [])
    rankings_b = result_b.get("rankings", [])

    if len(rankings_a) != len(rankings_b):
        return False

    for a, b in zip(rankings_a, rankings_b, strict=False):
        if a.get("canonical_id") != b.get("canonical_id"):
            return False
        if abs(a.get("final_score", 0) - b.get("final_score", 0)) > 1e-10:
            return False

    return True
