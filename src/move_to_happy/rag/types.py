"""RAG data types — documents, chunks, and retrieval results."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RAGDocument:
    """A source document before chunking."""

    content: str
    canonical_city_id: str | None = None
    source_type: str = "community"
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class RAGChunk:
    """A chunked segment of a RAGDocument, ready for embedding."""

    text: str
    chunk_index: int = 0
    canonical_city_id: str | None = None
    source_type: str = "community"
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class RAGResult:
    """A single retrieval result with similarity score."""

    chunk: RAGChunk
    score: float = 0.0
