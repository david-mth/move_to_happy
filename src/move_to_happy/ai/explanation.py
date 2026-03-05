"""Explanation Generator — LME trace + RAG context to narrative.

Uses LME trace (read-only, authoritative) + RAG context (narrative color).
NEVER modifies or reorders LME rankings.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from ..rag.retriever import RAGRetriever
from .claude_client import ClaudeClient
from .prompts import EXPLANATION_PROMPT_SUFFIX, SPILLOVER_PROMPT_SUFFIX

logger = logging.getLogger(__name__)


class ExplanationGenerator:
    """Generates human-readable explanations of LME results."""

    def __init__(self, claude: ClaudeClient, rag: RAGRetriever) -> None:
        self._claude = claude
        self._rag = rag

    def explain_result(
        self,
        canonical_city_id: str,
        lme_trace: dict[str, Any],
        user_preferences: dict[str, Any],
        rank: int,
        total_results: int,
    ) -> str:
        """Generate explanation for a single community's ranking."""
        rag_results = self._rag.retrieve(
            query=(
                f"community profile "
                f"{lme_trace.get('city_state', '')} "
                f"{lme_trace.get('state_name', '')}"
            ),
            k=5,
            canonical_city_id=canonical_city_id,
        )
        rag_context = "\n".join(r.chunk.text for r in rag_results)

        user_msg = (
            f"Explain why {lme_trace.get('city_state', 'this community')} "
            f"ranked #{rank} out of {total_results} results for this user.\n\n"
            f"USER'S STATED PRIORITIES:\n"
            f"{json.dumps(user_preferences, indent=2)}\n\n"
            f"{EXPLANATION_PROMPT_SUFFIX}"
        )

        return self._claude.generate_with_rag(
            user_message=user_msg,
            rag_context=rag_context,
            lme_trace=lme_trace,
        )

    def explain_spillover(
        self,
        residential_id: str,
        anchor_id: str,
        spillover_trace: dict[str, Any],
    ) -> str:
        """Generate the mandatory spillover explanation."""
        anchor_results = self._rag.retrieve(
            query="lifestyle amenities recreation culture",
            k=3,
            canonical_city_id=anchor_id,
        )
        residential_results = self._rag.retrieve(
            query="housing affordability cost of living",
            k=3,
            canonical_city_id=residential_id,
        )

        context_parts = ["LIFESTYLE ANCHOR:"]
        context_parts.extend(r.chunk.text for r in anchor_results)
        context_parts.append("\nRESIDENTIAL CANDIDATE:")
        context_parts.extend(r.chunk.text for r in residential_results)

        user_msg = (
            f"{SPILLOVER_PROMPT_SUFFIX}\n\n"
            f"SPILLOVER TRACE:\n"
            f"{json.dumps(spillover_trace, indent=2)}"
        )

        return self._claude.generate_with_rag(
            user_message=user_msg,
            rag_context="\n".join(context_parts),
        )

    def explain_results_batch(
        self,
        rankings: list[dict[str, Any]],
        user_preferences: dict[str, Any],
        max_explanations: int = 5,
    ) -> list[dict[str, str]]:
        """Generate explanations for the top N results."""
        explanations: list[dict[str, str]] = []
        total = len(rankings)

        for i, community in enumerate(rankings[:max_explanations]):
            cid = community.get("canonical_id", "")
            try:
                text = self.explain_result(
                    canonical_city_id=cid,
                    lme_trace=community,
                    user_preferences=user_preferences,
                    rank=i + 1,
                    total_results=total,
                )
            except Exception:
                logger.exception("Failed to explain %s", cid)
                text = (
                    "Explanation unavailable for this community. "
                    "The LME ranking is based on your stated preferences."
                )

            explanation = {"canonical_city_id": cid, "explanation": text}

            anchor = community.get("spillover_anchor")
            if anchor and anchor != cid:
                try:
                    spillover_text = self.explain_spillover(
                        residential_id=cid,
                        anchor_id=anchor,
                        spillover_trace={
                            "residential": cid,
                            "anchor": anchor,
                            "spillover_score": community.get("spillover_score", 0),
                            "dist_to_anchor": community.get("dist_to_anchor", 0),
                        },
                    )
                    explanation["spillover_explanation"] = spillover_text
                except Exception:
                    logger.exception("Failed spillover for %s→%s", cid, anchor)

            explanations.append(explanation)

        return explanations
