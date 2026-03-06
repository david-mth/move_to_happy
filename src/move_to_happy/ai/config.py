"""Configuration for the Claude AI layer — backed by pydantic-settings.

Env vars are loaded automatically. Field aliases map to the actual env var names.
Use populate_by_name=True so programmatic construction still uses Python names.
"""

from __future__ import annotations

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AIConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    anthropic_api_key: str = Field(default="", alias="ANTHROPIC_API_KEY")
    model: str = Field(default="claude-sonnet-4-20250514", alias="MTH_CLAUDE_MODEL")
    # Falls back to model if MTH_CLAUDE_EXPLANATION_MODEL is not set.
    explanation_model: str = Field(default="", alias="MTH_CLAUDE_EXPLANATION_MODEL")
    max_tokens: int = Field(default=4096, gt=0)
    temperature: float = Field(default=0.3, ge=0.0, le=1.0)
    extraction_temperature: float = Field(default=0.1, ge=0.0, le=1.0)
    rag_index_dir: str = Field(default="data/rag_index", alias="MTH_RAG_INDEX_DIR")
    rag_s3_prefix: str = "rag-index"

    @model_validator(mode="after")
    def _explanation_model_fallback(self) -> AIConfig:
        if not self.explanation_model:
            self.explanation_model = self.model
        return self

    @classmethod
    def from_env(cls) -> AIConfig:
        """Convenience alias — BaseSettings loads env automatically on construction."""
        return cls()
