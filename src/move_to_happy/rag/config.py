"""RAG configuration — backed by pydantic-settings."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RAGConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    embedding_model: str = "all-MiniLM-L6-v2"
    embedding_dim: int = Field(default=384, gt=0)
    chunk_size_tokens: int = Field(default=512, gt=0)
    chunk_overlap_tokens: int = Field(default=64, ge=0)
    index_dir: str = Field(default="data/rag_index", alias="MTH_RAG_INDEX_DIR")
    s3_prefix: str = "rag-index"
    default_k: int = Field(default=5, gt=0)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def index_path(self) -> Path:
        return Path(self.index_dir)

    @classmethod
    def from_env(cls) -> RAGConfig:
        """Convenience alias — BaseSettings loads env automatically on construction."""
        return cls()
