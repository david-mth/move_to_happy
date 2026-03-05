#!/usr/bin/env python3
"""End-to-end smoke test: Claude API + RAG + LME integration.

Usage:
    poetry run python scripts/test_claude_integration.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def main() -> None:
    print("=" * 60)
    print("MTH AI Layer — Integration Smoke Test")
    print("=" * 60)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("\nSKIPPED: ANTHROPIC_API_KEY not set")
        print("Set the env var and re-run to test Claude integration.")
        return

    from move_to_happy.ai.claude_client import ClaudeClient
    from move_to_happy.ai.config import AIConfig

    config = AIConfig.from_env()
    print(f"\nModel: {config.model}")

    print("\n[1/4] Testing basic generation...")
    client = ClaudeClient(config)
    response = client.generate(
        "Say 'MTH AI layer is operational' and nothing else.",
        max_tokens=50,
    )
    print(f"  Response: {response.strip()}")
    assert "operational" in response.lower() or "mth" in response.lower()
    print("  PASS")

    print("\n[2/4] Testing structured extraction...")
    from move_to_happy.ai.intake import IntakeInterpreter

    intake = IntakeInterpreter(client)
    result = intake.interpret(
        "I'm looking for a 3-bedroom home near Atlanta with a "
        "budget of $2,500/month. I love hiking and being near mountains."
    )
    print(f"  Extracted: {list(result.keys())}")
    assert "extraction_confidence" in result
    print("  PASS")

    print("\n[3/4] Testing RAG index load...")
    rag_dir = Path("data/rag_index")
    if rag_dir.exists():
        from move_to_happy.rag.indexer import FAISSIndex

        index = FAISSIndex()
        index.load(rag_dir)
        print(f"  Index loaded: {index.size} chunks")

        results = index.search("Atlanta Georgia community", k=3)
        for r in results:
            print(f"  → {r.chunk.canonical_city_id}: {r.chunk.text[:60]}...")
        print("  PASS")
    else:
        print("  SKIPPED: No RAG index found. Run build_rag_index.py first.")

    print("\n[4/4] Testing RAG-augmented generation...")
    response = client.generate_with_rag(
        user_message="What makes this community attractive?",
        rag_context=(
            "Dahlonega, GA (mth_ga_0100) is a mountain community "
            "with population 7,000. Located in the foothills of the "
            "Blue Ridge Mountains. Low cost of living."
        ),
    )
    print(f"  Response: {response[:200]}...")
    print("  PASS")

    print(f"\n{'=' * 60}")
    print("All smoke tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
