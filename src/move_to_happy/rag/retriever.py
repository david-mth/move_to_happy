"""Public RAG retrieval interface with filtering."""

from __future__ import annotations

import logging

from langsmith import traceable

from .config import RAGConfig
from .indexer import FAISSIndex
from .types import RAGResult

logger = logging.getLogger(__name__)


class RAGRetriever:
    """Query interface for the RAG knowledge base.

    Supports filtering by canonical_city_id and source_type to scope
    retrieval to relevant documents.
    """

    def __init__(
        self,
        index: FAISSIndex | None = None,
        config: RAGConfig | None = None,
    ) -> None:
        self._config = config or RAGConfig()
        self._index = index or FAISSIndex(self._config)

    @property
    def is_loaded(self) -> bool:
        return self._index.size > 0

    def load_index(self, directory: str | None = None) -> None:
        """Load a persisted FAISS index from disk."""
        self._index.load(directory)

    @traceable(run_type="retriever", name="rag_retrieve")
    def retrieve(
        self,
        query: str,
        k: int | None = None,
        *,
        canonical_city_id: str | None = None,
        source_type: str | None = None,
    ) -> list[RAGResult]:
        """Retrieve relevant chunks, optionally filtered.

        Args:
            query: Natural language query.
            k: Number of results to return.
            canonical_city_id: Filter to a specific community.
            source_type: Filter to a source type (e.g. "community",
                "lme_spec", "health", "economic").

        Returns:
            Filtered and ranked RAGResults.
        """
        if not self.is_loaded:
            logger.warning("RAG index not loaded — returning empty results")
            return []

        fetch_k = min((k or self._config.default_k) * 3, self._index.size)
        raw_results = self._index.search(query, k=fetch_k)

        filtered: list[RAGResult] = []
        target_k = k or self._config.default_k

        for result in raw_results:
            if (
                canonical_city_id is not None
                and result.chunk.canonical_city_id != canonical_city_id
            ):
                continue
            if source_type is not None and result.chunk.source_type != source_type:
                continue
            filtered.append(result)
            if len(filtered) >= target_k:
                break

        return filtered

    def retrieve_for_community(
        self,
        canonical_city_id: str,
        query: str = "community profile",
        k: int = 5,
    ) -> list[RAGResult]:
        """Convenience method: retrieve context for a specific community."""
        return self.retrieve(
            query=query,
            k=k,
            canonical_city_id=canonical_city_id,
        )

    def retrieve_lme_spec(
        self,
        query: str,
        k: int = 3,
    ) -> list[RAGResult]:
        """Retrieve from the LME specification documents."""
        return self.retrieve(
            query=query,
            k=k,
            source_type="lme_spec",
        )
