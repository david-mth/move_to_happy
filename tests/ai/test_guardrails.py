"""Test RAG guardrails — RAG never in scoring path."""

from move_to_happy.rag.guardrails import (
    validate_lme_determinism,
    validate_rag_not_in_scoring,
)


def test_validate_rag_not_in_scoring_passes_outside_lme():
    assert validate_rag_not_in_scoring() is True


def test_validate_lme_determinism_identical_results():
    result_a = {
        "rankings": [
            {"canonical_id": "mth_ga_0001", "final_score": 0.85},
            {"canonical_id": "mth_ga_0002", "final_score": 0.72},
        ],
    }
    result_b = {
        "rankings": [
            {"canonical_id": "mth_ga_0001", "final_score": 0.85},
            {"canonical_id": "mth_ga_0002", "final_score": 0.72},
        ],
    }
    assert validate_lme_determinism(result_a, result_b) is True


def test_validate_lme_determinism_different_order():
    result_a = {
        "rankings": [
            {"canonical_id": "mth_ga_0001", "final_score": 0.85},
            {"canonical_id": "mth_ga_0002", "final_score": 0.72},
        ],
    }
    result_b = {
        "rankings": [
            {"canonical_id": "mth_ga_0002", "final_score": 0.72},
            {"canonical_id": "mth_ga_0001", "final_score": 0.85},
        ],
    }
    assert validate_lme_determinism(result_a, result_b) is False


def test_validate_lme_determinism_different_scores():
    result_a = {
        "rankings": [
            {"canonical_id": "mth_ga_0001", "final_score": 0.85},
        ],
    }
    result_b = {
        "rankings": [
            {"canonical_id": "mth_ga_0001", "final_score": 0.80},
        ],
    }
    assert validate_lme_determinism(result_a, result_b) is False


def test_validate_lme_determinism_different_lengths():
    result_a = {"rankings": [{"canonical_id": "mth_ga_0001", "final_score": 0.85}]}
    result_b = {"rankings": []}
    assert validate_lme_determinism(result_a, result_b) is False
