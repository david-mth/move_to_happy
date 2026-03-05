"""Test RAG retriever — filtering by canonical_city_id and source_type."""

import pytest

from move_to_happy.rag.indexer import FAISSIndex
from move_to_happy.rag.retriever import RAGRetriever
from move_to_happy.rag.types import RAGChunk


@pytest.fixture()
def loaded_retriever() -> RAGRetriever:
    chunks = [
        RAGChunk(
            text="Dahlonega Georgia is a mountain community.",
            canonical_city_id="mth_ga_0100",
            source_type="community",
        ),
        RAGChunk(
            text="Healthcare in Dahlonega includes a regional hospital.",
            canonical_city_id="mth_ga_0100",
            source_type="health",
        ),
        RAGChunk(
            text="Tampa Florida is a large coastal city.",
            canonical_city_id="mth_fl_0050",
            source_type="community",
        ),
        RAGChunk(
            text="The LME scoring flow starts with eliminators.",
            canonical_city_id=None,
            source_type="lme_spec",
        ),
    ]
    index = FAISSIndex()
    index.build(chunks)
    return RAGRetriever(index=index)


def test_retrieve_unfiltered(loaded_retriever: RAGRetriever):
    results = loaded_retriever.retrieve("mountain community", k=4)
    assert len(results) >= 1


def test_retrieve_by_canonical_id(loaded_retriever: RAGRetriever):
    results = loaded_retriever.retrieve(
        "community profile",
        k=5,
        canonical_city_id="mth_ga_0100",
    )
    for r in results:
        assert r.chunk.canonical_city_id == "mth_ga_0100"


def test_retrieve_by_source_type(loaded_retriever: RAGRetriever):
    results = loaded_retriever.retrieve(
        "scoring",
        k=5,
        source_type="lme_spec",
    )
    for r in results:
        assert r.chunk.source_type == "lme_spec"


def test_retrieve_for_community(loaded_retriever: RAGRetriever):
    results = loaded_retriever.retrieve_for_community("mth_fl_0050")
    for r in results:
        assert r.chunk.canonical_city_id == "mth_fl_0050"


def test_retrieve_lme_spec(loaded_retriever: RAGRetriever):
    results = loaded_retriever.retrieve_lme_spec("eliminators")
    assert len(results) >= 1
    assert results[0].chunk.source_type == "lme_spec"


def test_not_loaded_returns_empty():
    retriever = RAGRetriever()
    results = retriever.retrieve("anything")
    assert results == []
