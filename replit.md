# Move to Happy

## Project Overview

A data pipeline and ML system that scores ~1,305 communities across Georgia, Alabama, and Florida to help find the best place to live. The core **Lifestyle Matching Engine (LME)** evaluates communities against user financial constraints and lifestyle preferences.

## Tech Stack

- **Language:** Python 3.13
- **Package Manager:** Poetry (virtualenv in `.venv/`)
- **AWS Services:** S3, Athena/Glue, Redshift, SageMaker, Secrets Manager
- **Key Libraries:** pandas, numpy, boto3, awswrangler, pyathena, sagemaker
- **Dev Tools:** ruff, mypy (strict), pytest, pre-commit

## Project Structure

```
src/move_to_happy/lme/    # Core scoring engine (runs standalone, no AWS needed)
  engine.py               # LMEEngine orchestrator
  atl.py                  # Affordability Translation Layer
  eliminators.py          # Three-gate filter
  lifestyle.py            # 7-dimension weighted scoring
  spillover.py            # Proximity boost from nearby anchors
  scoring.py              # Final score combination
  synthetic_housing.py    # Deterministic synthetic housing data
  types.py                # UserPreferences, CommunityScore dataclasses
  constants.py            # Weights, tax rates, parameters

scripts/                  # Numbered pipeline scripts (01-13, run sequentially)
  run_lme_example.py      # Local LME demo (requires data/prepared/mth_communities.csv)
  _config.py              # Shared AWS session factory

sagemaker/                # AWS SageMaker integration code
tests/                    # pytest tests
data/                     # Local data (gitignored)
```

## Commands

```bash
# Install dependencies
poetry install

# Run tests
poetry run pytest

# Run LME locally (requires data/prepared/mth_communities.csv)
poetry run python scripts/run_lme_example.py

# Lint
poetry run ruff check src/ scripts/

# Format
poetry run ruff format src/ scripts/

# Type check
poetry run mypy src/
```

## Workflow

The "Start application" workflow runs `poetry run pytest tests/ -v` as a console workflow.

## Notes

- The LME engine is self-contained and requires no AWS credentials to run locally
- AWS scripts (01-13) require AWS SSO profile `move-to-happy`
- `data/pipeline_config.json` stores AWS resource ARNs across script runs (gitignored)
- Redshift costs money — always run `99_cleanup.py` when done
