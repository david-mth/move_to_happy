"""Test document chunker — sentence boundaries and token limits."""

from move_to_happy.rag.chunker import DocumentChunker
from move_to_happy.rag.config import RAGConfig
from move_to_happy.rag.types import RAGDocument


def test_chunk_short_document():
    config = RAGConfig(chunk_size_tokens=512, chunk_overlap_tokens=64)
    chunker = DocumentChunker(config)
    doc = RAGDocument(
        content="This is a short document. It has two sentences.",
        canonical_city_id="mth_ga_0001",
        source_type="community",
    )
    chunks = chunker.chunk_document(doc)
    assert len(chunks) == 1
    assert chunks[0].canonical_city_id == "mth_ga_0001"
    assert chunks[0].source_type == "community"
    assert "short document" in chunks[0].text


def test_chunk_preserves_metadata():
    chunker = DocumentChunker()
    doc = RAGDocument(
        content="Test content here.",
        canonical_city_id="mth_fl_0042",
        source_type="health",
        metadata={"hospital_name": "Test Hospital"},
    )
    chunks = chunker.chunk_document(doc)
    assert len(chunks) >= 1
    assert chunks[0].metadata["hospital_name"] == "Test Hospital"


def test_chunk_long_document_produces_multiple_chunks():
    config = RAGConfig(chunk_size_tokens=20, chunk_overlap_tokens=5)
    chunker = DocumentChunker(config)
    sentences = [f"Sentence number {i} with some words." for i in range(50)]
    doc = RAGDocument(content=" ".join(sentences))
    chunks = chunker.chunk_document(doc)
    assert len(chunks) > 1


def test_chunk_empty_document():
    chunker = DocumentChunker()
    doc = RAGDocument(content="")
    chunks = chunker.chunk_document(doc)
    assert len(chunks) == 0


def test_chunk_documents_batch():
    chunker = DocumentChunker()
    docs = [
        RAGDocument(content="First document.", canonical_city_id="a"),
        RAGDocument(content="Second document.", canonical_city_id="b"),
    ]
    chunks = chunker.chunk_documents(docs)
    assert len(chunks) == 2
    ids = {c.canonical_city_id for c in chunks}
    assert ids == {"a", "b"}
