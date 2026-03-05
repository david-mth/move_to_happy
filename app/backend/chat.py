"""Chat with Data — Claude-powered natural language data analysis.

Uses the Anthropic tool-use API to let Claude run pandas queries against
the in-memory community DataFrames and return formatted answers.
"""

from __future__ import annotations

import io
import os
import re
from typing import Any

import anthropic
import numpy as np
import pandas as pd

BLOCKED_TOKENS = frozenset(
    {
        "import ",
        "__import__",
        "exec(",
        "eval(",
        "compile(",
        "open(",
        "os.",
        "sys.",
        "subprocess",
        "shutil",
        "pathlib",
        "glob",
        "socket",
        "requests",
        "urllib",
        "http.",
        "__builtins__",
        "__class__",
        "__subclasses__",
        "breakpoint",
        "getattr(",
        "setattr(",
        "delattr(",
    }
)

TOOL_DEFINITION = {
    "name": "run_pandas",
    "description": (
        "Execute a Python/pandas expression against the available DataFrames. "
        "The code has access to: pd (pandas), np (numpy), and all named "
        "DataFrames listed in the system prompt. The last expression's value "
        "is captured and returned as a string. Use .to_string() or "
        ".to_markdown() for readable table output."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "code": {
                "type": "string",
                "description": (
                    "Python code to execute. Must use only pd, np, and the "
                    "named DataFrames. The result of the last expression is "
                    "returned. For multi-line code, assign the final result "
                    "to a variable named `result`."
                ),
            }
        },
        "required": ["code"],
    },
}

MAX_TOOL_ROUNDS = 6
MAX_OUTPUT_CHARS = 8_000


def _build_schema_description(dataframes: dict[str, pd.DataFrame]) -> str:
    """Generate a compact schema summary for each DataFrame."""
    parts: list[str] = []
    for name, df in dataframes.items():
        cols = ", ".join(f"{c}({df[c].dtype})" for c in df.columns)
        parts.append(f"**{name}** ({len(df):,} rows): {cols}")
    return "\n\n".join(parts)


def _build_system_prompt(dataframes: dict[str, pd.DataFrame]) -> str:
    schema = _build_schema_description(dataframes)
    return f"""\
You are a data analyst for "Move to Happy" — ~1,305 communities across \
Georgia, Alabama, and Florida scored to help people find the best place to live.

Available DataFrames (join on `canonical_id`):

{schema}

Rules:
- Use `run_pandas` to query data. Be efficient — try to answer in 1-2 tool calls.
- Column names are listed above. Use them directly, don't waste a call to check.
- For tables, use `.head(10).to_markdown()`. Keep results to top 10-15 rows.
- Format numbers nicely. Be concise. If data doesn't exist, say so immediately.
- Key columns: communities has city, state_name, lat, lon; crime has \
violent_crime_rate, property_crime_rate; census has median_household_income, \
unemployment_rate; tax_rates has effective_property_tax_rate, \
combined_sales_tax_rate; education has hs_graduation_rate, employment_rate."""


def _safe_exec(code: str, dataframes: dict[str, pd.DataFrame]) -> str:
    """Execute pandas code in a restricted namespace."""
    code_lower = code.lower()
    for token in BLOCKED_TOKENS:
        if token.lower() in code_lower:
            return f"Error: blocked operation — '{token.strip()}' is not allowed."

    # Allow safe builtins needed for data analysis while blocking dangerous ones
    safe_builtins = {
        k: __builtins__[k]
        if isinstance(__builtins__, dict)
        else getattr(__builtins__, k)
        for k in (
            "abs",
            "all",
            "any",
            "bool",
            "dict",
            "enumerate",
            "filter",
            "float",
            "format",
            "frozenset",
            "hasattr",
            "hash",
            "int",
            "isinstance",
            "issubclass",
            "iter",
            "len",
            "list",
            "map",
            "max",
            "min",
            "next",
            "print",
            "range",
            "repr",
            "reversed",
            "round",
            "set",
            "slice",
            "sorted",
            "str",
            "sum",
            "tuple",
            "type",
            "zip",
            "True",
            "False",
            "None",
        )
        if (isinstance(__builtins__, dict) and k in __builtins__)
        or (not isinstance(__builtins__, dict) and hasattr(__builtins__, k))
    }

    namespace: dict[str, Any] = {"pd": pd, "np": np}
    namespace.update(dataframes)

    buf = io.StringIO()
    try:
        stmts = code.strip().split("\n")
        if len(stmts) == 1:
            result = eval(code, {"__builtins__": safe_builtins}, namespace)  # noqa: S307
        else:
            exec_code = "\n".join(stmts)
            exec(exec_code, {"__builtins__": safe_builtins}, namespace)  # noqa: S102
            result = namespace.get("result")

        output = str(result) if result is not None else buf.getvalue() or "(no output)"
    except Exception as exc:
        output = f"Error: {type(exc).__name__}: {exc}"
    finally:
        buf.close()

    if len(output) > MAX_OUTPUT_CHARS:
        output = output[:MAX_OUTPUT_CHARS] + "\n... (truncated)"
    return output


class DataChat:
    """Manages conversations with Claude about the community data."""

    def __init__(self, dataframes: dict[str, pd.DataFrame]) -> None:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY environment variable is not set. "
                "Set it in Replit Secrets or a .env file."
            )
        self._client = anthropic.Anthropic(api_key=api_key)
        self._dataframes = dataframes
        self._system_prompt = _build_system_prompt(dataframes)

    def chat(
        self,
        user_message: str,
        history: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Send a message and return Claude's response, handling tool calls.

        Returns {"role": "assistant", "content": str, "table": list | None}
        """
        messages = list(history)
        messages.append({"role": "user", "content": user_message})

        for _ in range(MAX_TOOL_ROUNDS):
            response = self._client.messages.create(
                model="claude-3-5-haiku-20241022",
                max_tokens=4096,
                system=self._system_prompt,
                tools=[TOOL_DEFINITION],
                messages=messages,
            )

            if response.stop_reason == "tool_use":
                assistant_content = []
                tool_results = []

                for block in response.content:
                    if block.type == "text":
                        assistant_content.append({"type": "text", "text": block.text})
                    elif block.type == "tool_use":
                        assistant_content.append(
                            {
                                "type": "tool_use",
                                "id": block.id,
                                "name": block.name,
                                "input": block.input,
                            }
                        )
                        code = block.input.get("code", "")
                        output = _safe_exec(code, self._dataframes)
                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": output,
                            }
                        )

                messages.append({"role": "assistant", "content": assistant_content})
                messages.append({"role": "user", "content": tool_results})
            else:
                text_parts = []
                for block in response.content:
                    if block.type == "text":
                        text_parts.append(block.text)

                full_text = "\n".join(text_parts)
                table = _extract_table(full_text)

                return {
                    "role": "assistant",
                    "content": full_text,
                    "table": table,
                }

        return {
            "role": "assistant",
            "content": "I ran out of analysis steps. Try a simpler question.",
            "table": None,
        }


def _extract_table(text: str) -> list[dict[str, str]] | None:
    """Extract the first markdown table from text into list-of-dicts."""
    table_pattern = re.compile(
        r"(\|.+\|)\n(\|[-| :]+\|)\n((?:\|.+\|\n?)+)", re.MULTILINE
    )
    match = table_pattern.search(text)
    if not match:
        return None

    header_line = match.group(1)
    body_lines = match.group(3).strip().split("\n")

    headers = [h.strip() for h in header_line.strip("|").split("|")]
    rows = []
    for line in body_lines:
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) == len(headers):
            rows.append(dict(zip(headers, cells, strict=False)))

    return rows if rows else None
