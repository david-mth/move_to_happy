"""Test FAISS index — build, save, load, search roundtrip."""

import tempfile
from pathlib import Path

import pytest

from move_to_happy.rag.indexer import FAISSIndex
from move_to_happy.rag.types import RAGChunk


@pytest.fixture()
def sample_chunks() -> list[RAGChunk]:
    return [
        RAGChunk(
            text="Atlanta Georgia is a major city in the southeast.",
            chunk_index=0,
            canonical_city_id="mth_ga_0001",
            source_type="community",
        ),
        RAGChunk(
            text="Birmingham Alabama has a growing economy.",
            chunk_index=0,
            canonical_city_id="mth_al_0001",
            source_type="community",
        ),
        RAGChunk(
            text="Jacksonville Florida is near the Atlantic coast.",
            chunk_index=0,
            canonical_city_id="mth_fl_0001",
            source_type="community",
        ),
        RAGChunk(
            text="The LME uses eliminators before optimization.",
            chunk_index=0,
            canonical_city_id=None,
            source_type="lme_spec",
        ),
    ]


def test_build_and_search(sample_chunks: list[RAGChunk]):
    index = FAISSIndex()
    index.build(sample_chunks)
    assert index.size == 4

    results = index.search("city in Georgia", k=2)
    assert len(results) == 2
    assert results[0].chunk.text is not None
    assert results[0].score > 0


def test_save_and_load(sample_chunks: list[RAGChunk]):
    with tempfile.TemporaryDirectory() as tmpdir:
        index = FAISSIndex()
        index.build(sample_chunks)
        index.save(tmpdir)

        assert (Path(tmpdir) / "faiss.index").exists()
        assert (Path(tmpdir) / "chunks.json").exists()

        loaded = FAISSIndex()
        loaded.load(tmpdir)
        assert loaded.size == 4

        results = loaded.search("Florida coast", k=1)
        assert len(results) == 1
        assert "Jacksonville" in results[0].chunk.text


def test_search_empty_index():
    index = FAISSIndex()
    results = index.search("anything", k=5)
    assert results == []


def test_build_empty_chunks():
    index = FAISSIndex()
    index.build([])
    assert index.size == 0
