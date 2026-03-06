"""Anthropic Claude API wrapper for the MTH AI layer.

Provides both synchronous and asynchronous interfaces.
Sync methods are kept for scripts and tests.
Async methods (prefixed with 'a') are used by the FastAPI concierge
so blocking I/O never stalls the event loop.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import anthropic
from langsmith import traceable
from langsmith.wrappers import wrap_anthropic

from .config import AIConfig
from .prompts import SYSTEM_PROMPT

logger = logging.getLogger(__name__)


class ClaudeClient:
    """Wrapper for Anthropic Claude API — the LLM endpoint for MTH."""

    def __init__(self, config: AIConfig | None = None) -> None:
        self._config = config or AIConfig.from_env()
        self._client = wrap_anthropic(
            anthropic.Anthropic(api_key=self._config.anthropic_api_key or None)
        )
        self._async_client = wrap_anthropic(
            anthropic.AsyncAnthropic(api_key=self._config.anthropic_api_key or None)
        )

    @property
    def model(self) -> str:
        return self._config.model

    # ------------------------------------------------------------------
    # Synchronous interface
    # ------------------------------------------------------------------

    @traceable(run_type="llm", name="llm_generate")
    def generate(
        self,
        user_message: str,
        *,
        system_override: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        model: str | None = None,
    ) -> str:
        """Generate a single response from Claude."""
        response = self._client.messages.create(
            model=model or self._config.model,
            max_tokens=max_tokens or self._config.max_tokens,
            temperature=(
                temperature if temperature is not None else self._config.temperature
            ),
            system=system_override or SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text

    @traceable(run_type="llm", name="llm_generate_structured")
    def generate_structured(
        self,
        user_message: str,
        output_schema: dict[str, Any],
        *,
        tool_name: str = "extract_preferences",
        tool_description: str = (
            "Extract structured user preferences from natural language"
        ),
        system_override: str | None = None,
    ) -> dict[str, Any]:
        """Generate structured JSON output using Claude's tool use."""
        response = self._client.messages.create(
            model=self._config.model,
            max_tokens=self._config.max_tokens,
            temperature=self._config.extraction_temperature,
            system=system_override or SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
            tools=[
                {
                    "name": tool_name,
                    "description": tool_description,
                    "input_schema": output_schema,
                }
            ],
            tool_choice={"type": "tool", "name": tool_name},
        )
        for block in response.content:
            if block.type == "tool_use":
                return dict(block.input)
        msg = "Claude did not return structured output"
        raise ValueError(msg)

    @traceable(run_type="llm", name="llm_generate_with_rag")
    def generate_with_rag(
        self,
        user_message: str,
        rag_context: str,
        lme_trace: dict[str, Any] | None = None,
        model: str | None = None,
    ) -> str:
        """Generate a response with RAG context injected into the user message."""
        return self.generate(
            user_message=self._build_rag_message(user_message, rag_context, lme_trace),
            model=model,
        )

    @traceable(run_type="llm", name="llm_generate_conversation")
    def generate_conversation(
        self,
        messages: list[dict[str, str]],
        *,
        system_override: str | None = None,
    ) -> str:
        """Multi-turn conversation for the Concierge Orchestrator."""
        response = self._client.messages.create(
            model=self._config.model,
            max_tokens=self._config.max_tokens,
            temperature=self._config.temperature,
            system=system_override or SYSTEM_PROMPT,
            messages=messages,
        )
        return response.content[0].text

    # ------------------------------------------------------------------
    # Asynchronous interface — use these from FastAPI async endpoints
    # so the event loop is never blocked by Anthropic I/O.
    # ------------------------------------------------------------------

    async def agenerate(
        self,
        user_message: str,
        *,
        system_override: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        model: str | None = None,
    ) -> str:
        """Async — generate a single response from Claude."""
        response = await self._async_client.messages.create(
            model=model or self._config.model,
            max_tokens=max_tokens or self._config.max_tokens,
            temperature=(
                temperature if temperature is not None else self._config.temperature
            ),
            system=system_override or SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text

    async def agenerate_structured(
        self,
        user_message: str,
        output_schema: dict[str, Any],
        *,
        tool_name: str = "extract_preferences",
        tool_description: str = (
            "Extract structured user preferences from natural language"
        ),
        system_override: str | None = None,
    ) -> dict[str, Any]:
        """Async — generate structured JSON output using Claude's tool use."""
        response = await self._async_client.messages.create(
            model=self._config.model,
            max_tokens=self._config.max_tokens,
            temperature=self._config.extraction_temperature,
            system=system_override or SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
            tools=[
                {
                    "name": tool_name,
                    "description": tool_description,
                    "input_schema": output_schema,
                }
            ],
            tool_choice={"type": "tool", "name": tool_name},
        )
        for block in response.content:
            if block.type == "tool_use":
                return dict(block.input)
        msg = "Claude did not return structured output"
        raise ValueError(msg)

    async def agenerate_with_rag(
        self,
        user_message: str,
        rag_context: str,
        lme_trace: dict[str, Any] | None = None,
        model: str | None = None,
    ) -> str:
        """Async — generate a response with RAG context."""
        return await self.agenerate(
            user_message=self._build_rag_message(user_message, rag_context, lme_trace),
            model=model,
        )

    async def agenerate_conversation(
        self,
        messages: list[dict[str, str]],
        *,
        system_override: str | None = None,
    ) -> str:
        """Async — multi-turn conversation."""
        response = await self._async_client.messages.create(
            model=self._config.model,
            max_tokens=self._config.max_tokens,
            temperature=self._config.temperature,
            system=system_override or SYSTEM_PROMPT,
            messages=messages,
        )
        return response.content[0].text

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_rag_message(
        user_message: str,
        rag_context: str,
        lme_trace: dict[str, Any] | None,
    ) -> str:
        """Assemble the user message with RAG context and optional LME trace."""
        parts = [
            "COMMUNITY CONTEXT (from Move to Happy knowledge base "
            "— use for narrative only):",
            rag_context,
            "",
        ]
        if lme_trace:
            parts.extend(
                [
                    "LME SCORING TRACE (authoritative — this is what "
                    "the engine computed):",
                    json.dumps(lme_trace, indent=2),
                    "",
                ]
            )
        parts.extend(["USER MESSAGE:", user_message])
        return "\n".join(parts)
