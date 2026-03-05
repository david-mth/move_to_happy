"""FAISS vector index for RAG retrieval."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import faiss

from .config import RAGConfig
from .embeddings import EmbeddingModel
from .types import RAGChunk, RAGResult

logger = logging.getLogger(__name__)

CHUNKS_FILE = "chunks.json"
INDEX_FILE = "faiss.index"


class FAISSIndex:
    """FAISS flat-IP index with chunk metadata storage."""

    def __init__(
        self,
        config: RAGConfig | None = None,
        embedder: EmbeddingModel | None = None,
    ) -> None:
        self._config = config or RAGConfig()
        self._embedder = embedder
        self._index: faiss.IndexFlatIP | None = None
        self._chunks: list[RAGChunk] = []

    @property
    def size(self) -> int:
        return len(self._chunks)

    def _get_embedder(self) -> EmbeddingModel:
        if self._embedder is None:
            self._embedder = EmbeddingModel(self._config)
        return self._embedder

    def build(self, chunks: list[RAGChunk]) -> None:
        """Build the FAISS index from a list of chunks."""
        if not chunks:
            logger.warning("No chunks to index")
            return

        embedder = self._get_embedder()
        texts = [c.text for c in chunks]
        logger.info("Embedding %d chunks...", len(texts))
        vectors = embedder.embed(texts)

        self._index = faiss.IndexFlatIP(embedder.dim)
        self._index.add(vectors)
        self._chunks = list(chunks)
        logger.info(
            "FAISS index built: %d vectors, %d dims",
            self._index.ntotal,
            embedder.dim,
        )

    def search(self, query: str, k: int = 5) -> list[RAGResult]:
        """Search the index for the k nearest chunks."""
        if self._index is None or not self._chunks:
            return []

        embedder = self._get_embedder()
        query_vec = embedder.embed_query(query).reshape(1, -1)
        k = min(k, self._index.ntotal)
        scores, indices = self._index.search(query_vec, k)

        results: list[RAGResult] = []
        for score, idx in zip(scores[0], indices[0], strict=False):
            if idx < 0:
                continue
            results.append(
                RAGResult(
                    chunk=self._chunks[idx],
                    score=float(score),
                )
            )
        return results

    def save(self, directory: str | Path | None = None) -> None:
        """Persist the index and chunk metadata to disk."""
        if self._index is None:
            logger.warning("No index to save")
            return

        path = Path(directory or self._config.index_dir)
        path.mkdir(parents=True, exist_ok=True)

        faiss.write_index(self._index, str(path / INDEX_FILE))

        chunk_dicts = [
            {
                "text": c.text,
                "chunk_index": c.chunk_index,
                "canonical_city_id": c.canonical_city_id,
                "source_type": c.source_type,
                "metadata": c.metadata,
            }
            for c in self._chunks
        ]
        with open(path / CHUNKS_FILE, "w") as f:
            json.dump(chunk_dicts, f)

        logger.info("Saved index to %s (%d chunks)", path, len(self._chunks))

    def load(self, directory: str | Path | None = None) -> None:
        """Load a previously saved index from disk."""
        path = Path(directory or self._config.index_dir)

        index_path = path / INDEX_FILE
        chunks_path = path / CHUNKS_FILE

        if not index_path.exists() or not chunks_path.exists():
            msg = f"Index files not found in {path}"
            raise FileNotFoundError(msg)

        self._index = faiss.read_index(str(index_path))

        with open(chunks_path) as f:
            chunk_dicts = json.load(f)

        self._chunks = [
            RAGChunk(
                text=d["text"],
                chunk_index=d["chunk_index"],
                canonical_city_id=d.get("canonical_city_id"),
                source_type=d.get("source_type", "community"),
                metadata=d.get("metadata", {}),
            )
            for d in chunk_dicts
        ]
        logger.info(
            "Loaded index from %s (%d chunks)",
            path,
            len(self._chunks),
        )
