"""Sentence-transformers embedding wrapper."""

from __future__ import annotations

import logging

import numpy as np
from sentence_transformers import SentenceTransformer

from .config import RAGConfig

logger = logging.getLogger(__name__)


class EmbeddingModel:
    """Wraps sentence-transformers for consistent embedding generation."""

    def __init__(self, config: RAGConfig | None = None) -> None:
        self._config = config or RAGConfig()
        logger.info("Loading embedding model: %s", self._config.embedding_model)
        self._model = SentenceTransformer(self._config.embedding_model)

    @property
    def dim(self) -> int:
        return self._config.embedding_dim

    def embed(self, texts: list[str]) -> np.ndarray:
        """Embed a list of texts into float32 vectors."""
        if not texts:
            return np.empty((0, self.dim), dtype=np.float32)
        vectors = self._model.encode(
            texts,
            show_progress_bar=len(texts) > 100,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return vectors.astype(np.float32)

    def embed_query(self, query: str) -> np.ndarray:
        """Embed a single query string."""
        return self.embed([query])[0]
