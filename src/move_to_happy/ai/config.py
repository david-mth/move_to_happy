"""Configuration for the Claude AI layer."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class AIConfig:
    """Configuration for the Claude AI layer."""

    anthropic_api_key: str = ""
    model: str = "claude-sonnet-4-20250514"
    max_tokens: int = 4096
    temperature: float = 0.3
    extraction_temperature: float = 0.1
    rag_index_dir: str = "data/rag_index"
    rag_s3_prefix: str = "rag-index"

    @classmethod
    def from_env(cls) -> AIConfig:
        return cls(
            anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
            model=os.environ.get("MTH_CLAUDE_MODEL", "claude-sonnet-4-20250514"),
            rag_index_dir=os.environ.get("MTH_RAG_INDEX_DIR", "data/rag_index"),
        )
