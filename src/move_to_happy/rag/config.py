"""RAG configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RAGConfig:
    """Configuration for the RAG layer."""

    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_dim: int = 384
    chunk_size_tokens: int = 512
    chunk_overlap_tokens: int = 64
    index_dir: str = "data/rag_index"
    s3_prefix: str = "rag-index"
    default_k: int = 5

    @classmethod
    def from_env(cls) -> RAGConfig:
        return cls(
            index_dir=os.environ.get("MTH_RAG_INDEX_DIR", "data/rag_index"),
        )

    @property
    def index_path(self) -> Path:
        return Path(self.index_dir)
