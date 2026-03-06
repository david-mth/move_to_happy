"""RAG data types — documents, chunks, and retrieval results.

Using Pydantic BaseModel gives consistent .model_dump() serialization
and a clean interface aligned with the rest of the AI layer.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class RAGDocument(BaseModel):
    """A source document before chunking."""

    content: str
    canonical_city_id: str | None = None
    source_type: str = "community"
    metadata: dict[str, str] = Field(default_factory=dict)


class RAGChunk(BaseModel):
    """A chunked segment of a RAGDocument, ready for embedding."""

    text: str
    chunk_index: int = 0
    canonical_city_id: str | None = None
    source_type: str = "community"
    metadata: dict[str, str] = Field(default_factory=dict)


class RAGResult(BaseModel):
    """A single retrieval result with similarity score."""

    chunk: RAGChunk
    score: float = 0.0
