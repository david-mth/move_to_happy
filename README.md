# Move to Happy

A data pipeline and ML system that scores communities based on user preferences to help find the best place to live. The core **Lifestyle Matching Engine (LME)** evaluates communities across affordability, lifestyle fit, and spillover effects from nearby desirable areas.

## Architecture

```
S3 (Parquet) → Athena/Redshift → LME Engine → SageMaker Endpoint
```

The pipeline flows through three layers:

1. **Data Lake** — Community data prepared, uploaded to S3, registered in Athena, and optionally loaded into Redshift for Spectrum queries.
2. **LME Engine** — Deterministic scoring pipeline: ATL (affordability) → Eliminators → Lifestyle matching → Spillover analysis → Final weighted score.
3. **SageMaker** — Batch processing jobs for bulk scoring and a real-time endpoint for on-demand queries.

## LME Engine

The engine lives in `src/move_to_happy/lme/` and can be used standalone:

```python
from move_to_happy.lme import LMEEngine, UserPreferences

engine = LMEEngine(communities_df)
result = engine.score(UserPreferences(
    monthly_payment=2500,
    loan_term_years=30,
    down_payment_pct=0.10,
))

for r in result.rankings[:10]:
    print(f"{r.city_state} — {r.final_score:.3f}")
```

### Scoring pipeline

| Stage | Module | What it does |
|-------|--------|-------------|
| ATL | `atl.py` | Converts monthly budget to max purchase price |
| Eliminators | `eliminators.py` | Filters by distance, affordability, household fit |
| Lifestyle | `lifestyle.py` | Scores climate, terrain, amenity preferences |
| Spillover | `spillover.py` | Boosts communities near desirable anchor cities |
| Scoring | `scoring.py` | Combines housing, lifestyle, spillover into final score |

Synthetic housing data (`synthetic_housing.py`) is generated once at engine init and reused across user queries.

## Pipeline Scripts

Scripts are numbered in execution order. Run from the project root.

| Script | Purpose |
|--------|---------|
| `01_prepare_data.py` | Normalize CSV, add canonical IDs, output CSV + TSV |
| `02_upload_to_s3.py` | Create S3 bucket and upload prepared data |
| `03_create_athena_database.py` | Create Athena database in Glue Data Catalog |
| `04_register_csv_with_athena.py` | Register CSV as Athena external table |
| `05_convert_to_parquet.py` | Convert to Parquet via Athena CTAS, partitioned by state |
| `06_query_with_wrangler.py` | Query data with AWS Data Wrangler |
| `07_create_redshift_cluster.py` | Create Redshift cluster (~$0.25/hr) |
| `08_load_into_redshift.py` | Load data into Redshift via Spectrum |
| `09_query_with_spectrum.py` | Cross-query Redshift + S3 via Spectrum |
| `10_export_to_s3_parquet.py` | UNLOAD Redshift data back to S3 as Parquet |
| `11_run_processing_job.py` | Run LME batch scoring on SageMaker |
| `12_deploy_endpoint.py` | Deploy LME as a real-time SageMaker endpoint |
| `13_test_endpoint.py` | Test the deployed endpoint |
| `99_cleanup.py` | Tear down billable resources (preserves S3 + Glue) |

## Setup

**Prerequisites:** Python 3.13, [Poetry](https://python-poetry.org/), AWS CLI configured with SSO.

```bash
# Install dependencies
poetry install

# Configure AWS (uses the "move-to-happy" SSO profile)
aws configure sso

# Run the data pipeline
python scripts/01_prepare_data.py
python scripts/02_upload_to_s3.py
# ... continue through the numbered scripts
```

## Project Structure

```
src/move_to_happy/
  lme/                  # Lifestyle Matching Engine
    engine.py           # Main LMEEngine class
    atl.py              # Affordability Translation Layer
    eliminators.py      # Distance, affordability, household filters
    lifestyle.py        # Lifestyle dimension scoring
    spillover.py        # Spillover from nearby anchor cities
    scoring.py          # Final score composition
    synthetic_housing.py # Synthetic housing data generation
    types.py            # UserPreferences, CommunityScore, LMEResult
    constants.py        # Weights, tax rates, band parameters

scripts/                # Numbered pipeline scripts
sagemaker/
  processing/           # Batch scoring job
  endpoint/             # Real-time inference endpoint
  iam.py                # IAM role setup

data/
  prepared/             # Prepared community CSV/TSV
  pipeline_config.json  # AWS resource ARNs and paths
```

## Development

```bash
# Run tests
poetry run pytest

# Lint
poetry run ruff check src/ scripts/

# Type check
poetry run mypy src/
```

Pre-commit hooks enforce trailing whitespace, file size limits, ruff linting, and ruff formatting.
