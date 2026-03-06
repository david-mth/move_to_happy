"""Lead Summary Agent — generates CRM-ready session summaries."""

from __future__ import annotations

import json
import logging
from typing import Any

from langsmith import traceable

from .claude_client import ClaudeClient
from .schemas import LEAD_SUMMARY_SCHEMA

logger = logging.getLogger(__name__)


class LeadSummaryAgent:
    """Generates CRM-ready session summaries at conversation end."""

    def __init__(self, claude: ClaudeClient) -> None:
        self._claude = claude

    @traceable(run_type="chain", name="lead_summary")
    def generate_summary(
        self,
        conversation_history: list[dict[str, str]],
        extracted_preferences: dict[str, Any],
        lme_results: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Generate CRM-ready lead summary from session."""
        recent_history = conversation_history[-10:]
        top_results = {}
        if lme_results and lme_results.get("rankings"):
            top_results = {
                "rankings": lme_results["rankings"][:5],
                "total_candidates": lme_results.get("total_candidates", 0),
                "max_purchase_price": lme_results.get("max_purchase_price", 0),
            }

        return self._claude.generate_structured(
            user_message=(
                "Generate a CRM-ready lead summary from this "
                "Move to Happy session.\n\n"
                f"CONVERSATION HISTORY:\n"
                f"{json.dumps(recent_history, indent=2)}\n\n"
                f"EXTRACTED PREFERENCES:\n"
                f"{json.dumps(extracted_preferences, indent=2)}\n\n"
                f"LME RESULTS (top 5):\n"
                f"{json.dumps(top_results, indent=2)}\n\n"
                "Summarize: who this person is, what they're looking "
                "for, what matched, and what their key tradeoffs are. "
                "This goes to a sales team."
            ),
            output_schema=LEAD_SUMMARY_SCHEMA,
            tool_name="generate_lead_summary",
            tool_description=(
                "Generate a structured CRM lead summary from a Move to Happy session"
            ),
        )
