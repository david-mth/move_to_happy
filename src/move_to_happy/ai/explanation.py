"""Explanation Generator — LME trace + RAG context to narrative.

Uses LME trace (read-only, authoritative) + RAG context (narrative color).
NEVER modifies or reorders LME rankings.

explain_results_batch       — sync, uses ThreadPoolExecutor (backward compat)
explain_results_batch_async — async, uses asyncio.gather + asyncio.to_thread
                               for FAISS retrieval; preferred in FastAPI context.
"""

from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from langsmith import traceable

from ..rag.retriever import RAGRetriever
from .claude_client import ClaudeClient
from .prompts import EXPLANATION_PROMPT_SUFFIX, SPILLOVER_PROMPT_SUFFIX

logger = logging.getLogger(__name__)


class ExplanationGenerator:
    """Generates human-readable explanations of LME results."""

    def __init__(self, claude: ClaudeClient, rag: RAGRetriever) -> None:
        self._claude = claude
        self._rag = rag
        self._explanation_model = claude._config.explanation_model

    # ------------------------------------------------------------------
    # Synchronous interface
    # ------------------------------------------------------------------

    @traceable(run_type="chain", name="explain_result")
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
        user_msg = self._build_explain_message(
            lme_trace, user_preferences, rank, total_results
        )
        return self._claude.generate_with_rag(
            user_message=user_msg,
            rag_context=rag_context,
            lme_trace=lme_trace,
            model=self._explanation_model,
        )

    @traceable(run_type="chain", name="explain_spillover")
    def explain_spillover(
        self,
        residential_id: str,
        anchor_id: str,
        spillover_trace: dict[str, Any],
    ) -> str:
        """Generate the mandatory spillover explanation."""
        context = self._build_spillover_context(anchor_id, residential_id)
        user_msg = (
            f"{SPILLOVER_PROMPT_SUFFIX}\n\n"
            f"SPILLOVER TRACE:\n{json.dumps(spillover_trace, indent=2)}"
        )
        return self._claude.generate_with_rag(
            user_message=user_msg,
            rag_context=context,
            model=self._explanation_model,
        )

    def _explain_one(
        self,
        index: int,
        community: dict[str, Any],
        user_preferences: dict[str, Any],
        total: int,
    ) -> dict[str, str]:
        """Build a single community explanation (main + optional spillover)."""
        cid = community.get("canonical_id", "")
        try:
            text = self.explain_result(
                canonical_city_id=cid,
                lme_trace=community,
                user_preferences=user_preferences,
                rank=index + 1,
                total_results=total,
            )
        except Exception:
            logger.exception("Failed to explain %s", cid)
            text = (
                "Explanation unavailable for this community. "
                "The LME ranking is based on your stated preferences."
            )

        explanation: dict[str, str] = {"canonical_city_id": cid, "explanation": text}

        anchor = community.get("spillover_anchor")
        if anchor and anchor != cid:
            try:
                explanation["spillover_explanation"] = self.explain_spillover(
                    residential_id=cid,
                    anchor_id=anchor,
                    spillover_trace={
                        "residential": cid,
                        "anchor": anchor,
                        "spillover_score": community.get("spillover_score", 0),
                        "dist_to_anchor": community.get("dist_to_anchor", 0),
                    },
                )
            except Exception:
                logger.exception("Failed spillover for %s→%s", cid, anchor)

        return explanation

    @traceable(run_type="chain", name="explain_batch")
    def explain_results_batch(
        self,
        rankings: list[dict[str, Any]],
        user_preferences: dict[str, Any],
        max_explanations: int = 5,
        max_workers: int = 5,
    ) -> list[dict[str, str]]:
        """Generate explanations for the top N results in parallel (sync)."""
        candidates = rankings[:max_explanations]
        total = len(rankings)

        futures: dict[Any, int] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for i, community in enumerate(candidates):
                future = pool.submit(
                    self._explain_one, i, community, user_preferences, total
                )
                futures[future] = i

        results: dict[int, dict[str, str]] = {}
        for future in as_completed(futures):
            i = futures[future]
            try:
                results[i] = future.result()
            except Exception:
                cid = candidates[i].get("canonical_id", "")
                logger.exception("Explanation worker failed for %s", cid)
                results[i] = {
                    "canonical_city_id": cid,
                    "explanation": (
                        "Explanation unavailable for this community. "
                        "The LME ranking is based on your stated preferences."
                    ),
                }

        return [results[i] for i in range(len(candidates))]

    # ------------------------------------------------------------------
    # Asynchronous interface — preferred in FastAPI context.
    # FAISS retrieval is offloaded via asyncio.to_thread (CPU-bound).
    # Claude calls use the async client via agenerate_with_rag.
    # asyncio.gather runs all per-community tasks concurrently.
    # ------------------------------------------------------------------

    async def _explain_one_async(
        self,
        index: int,
        community: dict[str, Any],
        user_preferences: dict[str, Any],
        total: int,
    ) -> dict[str, str]:
        """Async — build explanation for a single community."""
        cid = community.get("canonical_id", "")
        try:
            # FAISS search is CPU-bound — offload to thread pool
            rag_results = await asyncio.to_thread(
                self._rag.retrieve,
                query=(
                    f"community profile "
                    f"{community.get('city_state', '')} "
                    f"{community.get('state_name', '')}"
                ),
                k=5,
                canonical_city_id=cid,
            )
            rag_context = "\n".join(r.chunk.text for r in rag_results)
            user_msg = self._build_explain_message(
                community, user_preferences, index + 1, total
            )
            text = await self._claude.agenerate_with_rag(
                user_message=user_msg,
                rag_context=rag_context,
                lme_trace=community,
                model=self._explanation_model,
            )
        except Exception:
            logger.exception("Async explain failed for %s", cid)
            text = (
                "Explanation unavailable for this community. "
                "The LME ranking is based on your stated preferences."
            )

        explanation: dict[str, str] = {"canonical_city_id": cid, "explanation": text}

        anchor = community.get("spillover_anchor")
        if anchor and anchor != cid:
            try:
                explanation[
                    "spillover_explanation"
                ] = await self._explain_spillover_async(
                    residential_id=cid,
                    anchor_id=anchor,
                    spillover_trace={
                        "residential": cid,
                        "anchor": anchor,
                        "spillover_score": community.get("spillover_score", 0),
                        "dist_to_anchor": community.get("dist_to_anchor", 0),
                    },
                )
            except Exception:
                logger.exception("Async spillover failed for %s→%s", cid, anchor)

        return explanation

    async def _explain_spillover_async(
        self,
        residential_id: str,
        anchor_id: str,
        spillover_trace: dict[str, Any],
    ) -> str:
        """Async spillover explanation — FAISS on thread pool, Claude async."""
        anchor_task = asyncio.to_thread(
            self._rag.retrieve,
            query="lifestyle amenities recreation culture",
            k=3,
            canonical_city_id=anchor_id,
        )
        residential_task = asyncio.to_thread(
            self._rag.retrieve,
            query="housing affordability cost of living",
            k=3,
            canonical_city_id=residential_id,
        )
        anchor_results, residential_results = await asyncio.gather(
            anchor_task, residential_task
        )

        context_parts = ["LIFESTYLE ANCHOR:"]
        context_parts.extend(r.chunk.text for r in anchor_results)
        context_parts.append("\nRESIDENTIAL CANDIDATE:")
        context_parts.extend(r.chunk.text for r in residential_results)

        user_msg = (
            f"{SPILLOVER_PROMPT_SUFFIX}\n\n"
            f"SPILLOVER TRACE:\n{json.dumps(spillover_trace, indent=2)}"
        )
        return await self._claude.agenerate_with_rag(
            user_message=user_msg,
            rag_context="\n".join(context_parts),
            model=self._explanation_model,
        )

    @traceable(run_type="chain", name="explain_batch_async")
    async def explain_results_batch_async(
        self,
        rankings: list[dict[str, Any]],
        user_preferences: dict[str, Any],
        max_explanations: int = 5,
    ) -> list[dict[str, str]]:
        """Async — explain top N results concurrently with asyncio.gather.

        All explanation tasks run concurrently. Each task independently
        offloads FAISS retrieval to a thread and awaits Claude.
        """
        candidates = rankings[:max_explanations]
        total = len(rankings)

        tasks = [
            self._explain_one_async(i, community, user_preferences, total)
            for i, community in enumerate(candidates)
        ]
        raw = await asyncio.gather(*tasks, return_exceptions=True)

        results: list[dict[str, str]] = []
        for i, outcome in enumerate(raw):
            if isinstance(outcome, BaseException):
                cid = candidates[i].get("canonical_id", "")
                logger.exception("Explanation gather failed for %s: %s", cid, outcome)
                results.append(
                    {
                        "canonical_city_id": cid,
                        "explanation": (
                            "Explanation unavailable for this community. "
                            "The LME ranking is based on your stated preferences."
                        ),
                    }
                )
            else:
                results.append(outcome)  # type: ignore[arg-type]

        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_explain_message(
        lme_trace: dict[str, Any],
        user_preferences: dict[str, Any],
        rank: int,
        total_results: int,
    ) -> str:
        return (
            f"Explain why {lme_trace.get('city_state', 'this community')} "
            f"ranked #{rank} out of {total_results} results for this user.\n\n"
            f"USER'S STATED PRIORITIES:\n"
            f"{json.dumps(user_preferences, indent=2)}\n\n"
            f"{EXPLANATION_PROMPT_SUFFIX}"
        )

    def _build_spillover_context(self, anchor_id: str, residential_id: str) -> str:
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
        parts = ["LIFESTYLE ANCHOR:"]
        parts.extend(r.chunk.text for r in anchor_results)
        parts.append("\nRESIDENTIAL CANDIDATE:")
        parts.extend(r.chunk.text for r in residential_results)
        return "\n".join(parts)
