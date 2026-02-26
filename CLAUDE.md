# Move to Happy

## Project Overview

A data pipeline and ML system that scores ~1,305 communities across Georgia, Alabama, and Florida to help find the best place to live. The core **Lifestyle Matching Engine (LME)** evaluates communities against user financial constraints and lifestyle preferences.

## Tech Stack

- **Language:** Python 3.13
- **Package Manager:** Poetry (virtualenv in-project)
- **AWS Services:** S3, Athena/Glue, Redshift, SageMaker, Secrets Manager
- **AWS Auth:** SSO profile `move-to-happy`
- **Key Libraries:** pandas, numpy, boto3, awswrangler, pyathena, sagemaker
- **Dev Tools:** ruff (lint + format), mypy (strict mode), pytest, pre-commit

## Commands

```bash
# Install dependencies
poetry install

# Run tests
poetry run pytest

# Lint
poetry run ruff check src/ scripts/

# Format
poetry run ruff format src/ scripts/

# Type check
poetry run mypy src/

# Run LME locally (no AWS needed)
poetry run python scripts/run_lme_example.py
```

## Project Structure

```
src/move_to_happy/lme/    # Core scoring engine (runs standalone, no AWS needed)
  engine.py               # LMEEngine orchestrator - init generates housing, score() runs pipeline
  atl.py                  # Affordability Translation Layer (monthly payment -> max price)
  eliminators.py          # Three-gate filter: distance, affordability, household fit
  lifestyle.py            # 7-dimension weighted scoring (mountains, beach, lake, airport, climate, terrain, COL)
  spillover.py            # Proximity-weighted boost from nearby desirable-but-unaffordable communities
  scoring.py              # Final score: 0.40 housing + 0.40 lifestyle + 0.20 spillover
  synthetic_housing.py    # Deterministic synthetic housing data from community attributes
  types.py                # UserPreferences, CommunityScore, LMEResult dataclasses
  constants.py            # Weights, tax rates, band parameters, ATL defaults

scripts/                  # Numbered pipeline scripts (01-13, run sequentially)
  _config.py              # Shared AWS session factory and pipeline config loader

sagemaker/
  processing/             # SageMaker batch scoring job
  endpoint/               # Real-time inference endpoint (model_fn/input_fn/predict_fn/output_fn)
  iam.py                  # IAM role setup

data/
  prepared/               # Prepared community CSV/TSV (~1,305 rows, 33 columns)
  pipeline_config.json    # Accumulating AWS resource ARNs/paths (created by script 02)
```

## Architecture

**Scoring Pipeline (LME):**
1. `ATL` converts monthly mortgage budget to max purchase price
2. `Eliminators` apply distance gate -> affordability gate -> household fit gate
3. `Lifestyle` scores all communities on 7 weighted dimensions
4. `Spillover` identifies eliminated-but-desirable "lifestyle anchors" and boosts nearby affordable communities
5. `Scoring` combines: 40% housing + 40% lifestyle + 20% spillover

**Data Pipeline (scripts 01-13):**
Raw CSV -> Normalize (01) -> S3 (02) -> Athena/Glue (03-06) -> Redshift (07-10) -> SageMaker (11-13)

`pipeline_config.json` is the shared state store across all scripts.

## Coding Conventions

- **Style:** ruff enforces pycodestyle, pyflakes, isort, pep8-naming, pyupgrade, bugbear, simplify
- **Line length:** 88 characters
- **Type checking:** mypy strict mode enabled
- **Imports:** isort ordering enforced by ruff
- **Target Python:** 3.13 (do not use features from older versions when newer equivalents exist)
- **Pre-commit hooks:** trailing-whitespace, end-of-file-fixer, check-yaml, check-added-large-files, ruff lint+format

## Key Design Decisions

- LME engine generates **synthetic housing data** at init time (no real MLS data needed), making it fully self-contained
- `LMEEngine` is initialized once with community data, then `score()` is called per-user (fast repeated scoring)
- `UserPreferences` and `CommunityScore` are immutable dataclasses
- Scripts use `sys.path.insert` to import shared `_config.py` (not ideal but intentional for standalone script execution)
- State-specific tax rates live in `constants.py` — adding a new state requires updating there and in script 01's `STATE_ABBREV`

## Important Warnings

- **Do not commit** `data/pipeline_config.json` with real AWS account IDs or resource ARNs
- **Redshift costs money** (~$0.25/hr) — always run `99_cleanup.py` when done
- `sagemaker/processing/processing_script.py` accesses private `engine._df`, `engine._core_availability`, `engine._attribute_overlay` — be careful when refactoring engine internals
- Test coverage is minimal (only a smoke test exists) — add tests before modifying scoring logic
