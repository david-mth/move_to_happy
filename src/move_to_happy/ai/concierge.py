"""Concierge Orchestrator — multi-turn discovery conversation.

Workflow: Collect → Validate → Call LME → Present → Refine
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from typing import Any

from ..lme.engine import LMEEngine
from ..lme.types import UserPreferences
from ..rag.retriever import RAGRetriever
from .claude_client import ClaudeClient
from .explanation import ExplanationGenerator
from .intake import IntakeInterpreter

logger = logging.getLogger(__name__)

_REFINEMENT_KEYWORDS = frozenset(
    {
        "adjust",
        "change",
        "more",
        "less",
        "increase",
        "decrease",
        "what if",
        "instead",
        "higher",
        "lower",
        "different",
        "modify",
        "update",
        "prefer",
        "prioritize",
        "budget",
    }
)

_COMMUNITY_KEYWORDS = frozenset(
    {
        "tell me about",
        "what's",
        "how is",
        "does it have",
        "is there",
        "what about",
        "describe",
        "info on",
        "details about",
    }
)


class ConciergeOrchestrator:
    """Manages the multi-turn relocation discovery conversation."""

    def __init__(
        self,
        claude: ClaudeClient,
        intake: IntakeInterpreter,
        explainer: ExplanationGenerator,
        rag: RAGRetriever,
        lme: LMEEngine,
    ) -> None:
        self._claude = claude
        self._intake = intake
        self._explainer = explainer
        self._rag = rag
        self._lme = lme
        self.conversation_history: list[dict[str, str]] = []
        self.extracted_preferences: dict[str, Any] = {}
        self.lme_results: dict[str, Any] | None = None

    def handle_message(self, user_message: str) -> dict[str, Any]:
        """Process a single user message and return a response.

        Returns a dict with:
            - role: "assistant"
            - content: the response text
            - results: LME results if scoring was performed (or None)
            - explanations: per-community explanations (or None)
            - needs_clarification: list of fields needing input (or None)
        """
        self.conversation_history.append(
            {
                "role": "user",
                "content": user_message,
            }
        )

        response: dict[str, Any] = {
            "role": "assistant",
            "content": "",
            "results": None,
            "explanations": None,
            "needs_clarification": None,
        }

        if not self.extracted_preferences:
            response = self._handle_initial_intake(user_message, response)
        elif self._is_refinement(user_message):
            response = self._handle_refinement(user_message, response)
        elif self._is_community_question(user_message):
            response = self._handle_community_question(
                user_message,
                response,
            )
        else:
            text = self._claude.generate_conversation(
                self.conversation_history,
            )
            response["content"] = text

        self.conversation_history.append(
            {
                "role": "assistant",
                "content": response["content"],
            }
        )
        return response

    def _handle_initial_intake(
        self,
        user_message: str,
        response: dict[str, Any],
    ) -> dict[str, Any]:
        """Extract preferences and either ask for clarification or run LME."""
        try:
            self.extracted_preferences = self._intake.interpret(user_message)
        except Exception:
            logger.exception("Intake extraction failed")
            response["content"] = (
                "I'd love to help you find your ideal community. "
                "Could you tell me about your budget, where you'd like "
                "to be located, and what matters most to you in a community?"
            )
            return response

        clarifications = self._intake.needs_clarification(
            self.extracted_preferences,
        )

        if clarifications:
            response["needs_clarification"] = clarifications
            response["content"] = self._claude.generate(
                user_message=(
                    "The user has provided some preferences but I need "
                    f"clarification on: {clarifications}. Generate a "
                    "friendly, conversational follow-up question to "
                    "clarify these. Ask about the most important one "
                    "first. Keep it natural."
                ),
            )
        else:
            response = self._run_lme_and_present(response)

        return response

    def _run_lme_and_present(
        self,
        response: dict[str, Any],
    ) -> dict[str, Any]:
        """Call the LME deterministically and present results."""
        try:
            lme_params = self._intake.to_lme_params(
                self.extracted_preferences,
            )
            prefs = UserPreferences(**lme_params)
            result = self._lme.score(prefs, top_n=10)
            self.lme_results = asdict(result)

            response["results"] = self.lme_results

            explanations = self._explainer.explain_results_batch(
                rankings=self.lme_results["rankings"],
                user_preferences=self.extracted_preferences,
                max_explanations=5,
            )
            response["explanations"] = explanations

            top_communities = [
                r.get("city_state", r.get("canonical_id", ""))
                for r in self.lme_results["rankings"][:5]
            ]
            response["content"] = self._claude.generate(
                user_message=(
                    f"Present LME results to the user. "
                    f"Found {result.total_candidates} matching communities. "
                    f"Top matches: {', '.join(top_communities)}. "
                    f"Max purchase price: ${result.max_purchase_price:,}. "
                    f"User priorities: "
                    f"{json.dumps(self.extracted_preferences, indent=2)}\n\n"
                    f"Give a warm, conversational summary of the results. "
                    f"Mention the top 3-5 communities and why they match. "
                    f"Invite the user to ask about specific communities "
                    f"or adjust their preferences."
                ),
            )
        except Exception:
            logger.exception("LME scoring failed")
            response["content"] = (
                "I wasn't able to run the matching engine with the "
                "information provided. Could you tell me more about "
                "your budget and where you'd like to be located?"
            )
            self.extracted_preferences = {}

        return response

    def _handle_refinement(
        self,
        user_message: str,
        response: dict[str, Any],
    ) -> dict[str, Any]:
        """Handle preference adjustments and re-run LME."""
        try:
            new_prefs = self._intake.interpret(user_message)
            for key, val in new_prefs.items():
                if (
                    key not in ("extraction_confidence", "clarification_needed")
                    and val is not None
                ):
                    self.extracted_preferences[key] = val
        except Exception:
            logger.exception("Refinement extraction failed")

        return self._run_lme_and_present(response)

    def _handle_community_question(
        self,
        question: str,
        response: dict[str, Any],
    ) -> dict[str, Any]:
        """Answer a question about a specific community using RAG."""
        results = self._rag.retrieve(query=question, k=5)
        if not results:
            response["content"] = (
                "I don't have specific information about that community "
                "in my knowledge base yet. Would you like to know about "
                "one of your matched communities instead?"
            )
            return response

        context = "\n".join(r.chunk.text for r in results)
        response["content"] = self._claude.generate_with_rag(
            user_message=question,
            rag_context=context,
        )
        return response

    def _is_refinement(self, msg: str) -> bool:
        lower = msg.lower()
        return any(k in lower for k in _REFINEMENT_KEYWORDS)

    def _is_community_question(self, msg: str) -> bool:
        lower = msg.lower()
        return any(k in lower for k in _COMMUNITY_KEYWORDS)

    def get_session_data(self) -> dict[str, Any]:
        """Return session data for lead summary generation."""
        return {
            "conversation_history": self.conversation_history,
            "extracted_preferences": self.extracted_preferences,
            "lme_results": self.lme_results,
        }
