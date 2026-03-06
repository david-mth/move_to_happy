"""Concierge Orchestrator — multi-turn discovery conversation.

Workflow: Collect → Validate → Call LME → Present → Refine

handle_message       — sync interface (kept for scripts / tests)
handle_message_async — async interface for FastAPI; never blocks the event loop.
                       LME scoring is offloaded via asyncio.to_thread (CPU-bound).
                       Explanations run concurrently via asyncio.gather.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from langsmith import traceable

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

    # ------------------------------------------------------------------
    # Synchronous interface (backward compat — used by scripts / tests)
    # ------------------------------------------------------------------

    @traceable(run_type="chain", name="concierge_turn")
    def handle_message(self, user_message: str) -> dict[str, Any]:
        """Process a single user message and return a response.

        Returns a dict with:
            - role: "assistant"
            - content: the response text
            - results: LME results if scoring was performed (or None)
            - explanations: per-community explanations (or None)
            - needs_clarification: list of fields needing input (or None)
        """
        self.conversation_history.append({"role": "user", "content": user_message})

        response: dict[str, Any] = {
            "role": "assistant",
            "content": "",
            "results": None,
            "explanations": None,
            "needs_clarification": None,
        }

        if not self.extracted_preferences:
            response = self._handle_initial_intake(user_message, response)
        elif self._is_community_question(user_message):
            response = self._handle_community_question(user_message, response)
        else:
            response = self._handle_refinement(user_message, response)

        self.conversation_history.append(
            {"role": "assistant", "content": response["content"]}
        )
        return response

    @traceable(run_type="chain", name="intake_flow")
    def _handle_initial_intake(
        self, user_message: str, response: dict[str, Any]
    ) -> dict[str, Any]:
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

        clarifications = self._intake.needs_clarification(self.extracted_preferences)
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

    @traceable(run_type="chain", name="lme_and_present")
    def _run_lme_and_present(self, response: dict[str, Any]) -> dict[str, Any]:
        try:
            lme_params = self._intake.to_lme_params(self.extracted_preferences)
            prefs = UserPreferences(**lme_params)
            result = self._lme.score(prefs, top_n=10)
            self.lme_results = result.model_dump()
            response["results"] = self.lme_results

            explanations = self._explainer.explain_results_batch(
                rankings=self.lme_results["rankings"],
                user_preferences=self.extracted_preferences,
                max_explanations=5,
            )
            response["explanations"] = explanations
            response["content"] = self._claude.generate(
                user_message=self._build_present_prompt(result),
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

    @traceable(run_type="chain", name="refinement_flow")
    def _handle_refinement(
        self, user_message: str, response: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            new_prefs = self._intake.interpret(user_message)
            self._merge_preferences(new_prefs)
        except Exception:
            logger.exception("Refinement extraction failed")
        return self._run_lme_and_present(response)

    @traceable(run_type="chain", name="community_question")
    def _handle_community_question(
        self, question: str, response: dict[str, Any]
    ) -> dict[str, Any]:
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
            user_message=question, rag_context=context
        )
        return response

    # ------------------------------------------------------------------
    # Asynchronous interface — use from FastAPI async endpoints.
    #
    # LME scoring: asyncio.to_thread (CPU-bound pandas work)
    # Explanations: asyncio.gather via explain_results_batch_async
    # Claude calls:  await self._claude.agenerate* (non-blocking I/O)
    # FAISS search:  asyncio.to_thread inside explain_results_batch_async
    # ------------------------------------------------------------------

    @traceable(run_type="chain", name="concierge_turn_async")
    async def handle_message_async(self, user_message: str) -> dict[str, Any]:
        """Async — process a single user message without blocking the event loop."""
        self.conversation_history.append({"role": "user", "content": user_message})

        response: dict[str, Any] = {
            "role": "assistant",
            "content": "",
            "results": None,
            "explanations": None,
            "needs_clarification": None,
        }

        if not self.extracted_preferences:
            response = await self._handle_initial_intake_async(user_message, response)
        elif self._is_community_question(user_message):
            response = await self._handle_community_question_async(
                user_message, response
            )
        else:
            # Once preferences are set, treat every non-community message as a
            # refinement so the LME always re-runs and results always come back.
            response = await self._handle_refinement_async(user_message, response)

        self.conversation_history.append(
            {"role": "assistant", "content": response["content"]}
        )
        return response

    async def _handle_initial_intake_async(
        self, user_message: str, response: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            self.extracted_preferences = await self._intake.interpret_async(
                user_message
            )
        except Exception:
            logger.exception("Async intake extraction failed")
            response["content"] = (
                "I'd love to help you find your ideal community. "
                "Could you tell me about your budget, where you'd like "
                "to be located, and what matters most to you in a community?"
            )
            return response

        clarifications = self._intake.needs_clarification(self.extracted_preferences)
        if clarifications:
            response["needs_clarification"] = clarifications
            response["content"] = await self._claude.agenerate(
                user_message=(
                    "The user has provided some preferences but I need "
                    f"clarification on: {clarifications}. Generate a "
                    "friendly, conversational follow-up question to "
                    "clarify these. Ask about the most important one "
                    "first. Keep it natural."
                ),
            )
        else:
            response = await self._run_lme_and_present_async(response)
        return response

    async def _run_lme_and_present_async(
        self, response: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            lme_params = self._intake.to_lme_params(self.extracted_preferences)
            prefs = UserPreferences(**lme_params)

            # CPU-bound — offload to thread pool so the event loop stays free
            result = await asyncio.to_thread(self._lme.score, prefs, top_n=10)
            self.lme_results = result.model_dump()
            response["results"] = self.lme_results

            # Concurrent explanations via asyncio.gather (see explanation.py)
            explanations = await self._explainer.explain_results_batch_async(
                rankings=self.lme_results["rankings"],
                user_preferences=self.extracted_preferences,
                max_explanations=5,
            )
            response["explanations"] = explanations
            response["content"] = await self._claude.agenerate(
                user_message=self._build_present_prompt(result),
            )
        except Exception:
            logger.exception("Async LME scoring failed")
            response["content"] = (
                "I wasn't able to run the matching engine with the "
                "information provided. Could you tell me more about "
                "your budget and where you'd like to be located?"
            )
            self.extracted_preferences = {}
        return response

    async def _handle_refinement_async(
        self, user_message: str, response: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            new_prefs = await self._intake.interpret_async(user_message)
            self._merge_preferences(new_prefs)
        except Exception:
            logger.exception("Async refinement extraction failed")
        return await self._run_lme_and_present_async(response)

    async def _handle_community_question_async(
        self, question: str, response: dict[str, Any]
    ) -> dict[str, Any]:
        # FAISS retrieval — CPU-bound, offload to thread
        results = await asyncio.to_thread(self._rag.retrieve, query=question, k=5)
        if not results:
            response["content"] = (
                "I don't have specific information about that community "
                "in my knowledge base yet. Would you like to know about "
                "one of your matched communities instead?"
            )
            return response
        context = "\n".join(r.chunk.text for r in results)
        response["content"] = await self._claude.agenerate_with_rag(
            user_message=question, rag_context=context
        )
        return response

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    # Nested keys whose sub-values must be merged individually.
    # A top-level replacement would wipe valid sub-fields (e.g. state="Alabama")
    # whenever Claude returns a partial update with null sub-values.
    _NESTED_KEYS = frozenset(
        {"budget", "household", "geographic_anchor", "lifestyle_weights"}
    )
    _SKIP_KEYS = frozenset({"extraction_confidence", "clarification_needed"})

    def _merge_preferences(self, new_prefs: dict[str, Any]) -> None:
        """Deep-merge new extracted preferences into the current session state.

        For top-level scalar fields (preferred_climate, preferred_terrain, …):
          overwrite only when the new value is non-None.

        For nested dicts (budget, household, geographic_anchor, lifestyle_weights):
          merge at the sub-key level — only sub-values that are explicitly
          non-None overwrite existing values.  This prevents a partial
          refinement (e.g. "close to nice dining") from wiping the
          geographic_anchor.state that was set in the initial intake.
        """
        for key, val in new_prefs.items():
            if key in self._SKIP_KEYS or val is None:
                continue
            if key in self._NESTED_KEYS and isinstance(val, dict):
                existing = self.extracted_preferences.setdefault(key, {})
                for sub_key, sub_val in val.items():
                    if sub_val is not None:
                        existing[sub_key] = sub_val
            else:
                self.extracted_preferences[key] = val

    def _build_present_prompt(self, result: Any) -> str:
        top_communities = [
            r.get("city_state", r.get("canonical_id", ""))
            if isinstance(r, dict)
            else (r.city_state or r.canonical_id)
            for r in (self.lme_results["rankings"][:5] if self.lme_results else [])
        ]
        return (
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
        )

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
