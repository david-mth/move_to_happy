"""Validate system prompt contains all non-negotiable constraints."""

from move_to_happy.ai.prompts import SYSTEM_PROMPT


def test_system_prompt_contains_role_definition():
    assert "You guide, translate, and explain" in SYSTEM_PROMPT
    assert "You do NOT score, rank, or make matching decisions" in SYSTEM_PROMPT


def test_system_prompt_contains_eliminators_precede_optimization():
    assert "ELIMINATORS PRECEDE OPTIMIZATION" in SYSTEM_PROMPT


def test_system_prompt_contains_deterministic_engine():
    assert "DETERMINISTIC ENGINE" in SYSTEM_PROMPT
    assert "Identical inputs must ALWAYS produce identical outputs" in SYSTEM_PROMPT


def test_system_prompt_contains_canonical_id_spine():
    assert "CANONICAL CITY/STATE ID SPINE" in SYSTEM_PROMPT
    assert "mth_{state}_{sequence}" in SYSTEM_PROMPT


def test_system_prompt_contains_explainability():
    assert "EXPLAINABILITY IS FOUNDATIONAL" in SYSTEM_PROMPT


def test_system_prompt_contains_spillover():
    assert "SPILLOVER IS CORE" in SYSTEM_PROMPT
    assert "You can't live in X at this budget" in SYSTEM_PROMPT


def test_system_prompt_contains_no_static_rankings():
    assert "NO STATIC RANKINGS" in SYSTEM_PROMPT
    assert 'Never produce "Top 10" lists' in SYSTEM_PROMPT


def test_system_prompt_contains_structure_before_narrative():
    assert "STRUCTURE BEFORE NARRATIVE" in SYSTEM_PROMPT


def test_system_prompt_contains_dual_layer():
    assert "DUAL-LAYER DESIGN" in SYSTEM_PROMPT
    assert "Consumer and EDO" in SYSTEM_PROMPT


def test_system_prompt_contains_lme_flow():
    assert "Distance Gate" in SYSTEM_PROMPT
    assert "Housing Affordability" in SYSTEM_PROMPT
    assert "Spillover Adjustments" in SYSTEM_PROMPT
