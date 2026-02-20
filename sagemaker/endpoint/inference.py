"""SageMaker real-time endpoint inference handler for LME.

Implements the standard SageMaker inference contract:
model_fn, input_fn, predict_fn, output_fn.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from move_to_happy.lme.engine import LMEEngine
from move_to_happy.lme.types import UserPreferences

logger = logging.getLogger(__name__)


def model_fn(model_dir: str) -> LMEEngine:
    """Load community data and initialize LME engine.

    The model artifact (model.tar.gz) should contain communities.parquet.
    """
    model_path = Path(model_dir)
    parquet_path = model_path / "communities.parquet"

    if not parquet_path.exists():
        # Try loading any parquet file in the directory
        parquet_files = list(model_path.glob("*.parquet"))
        if not parquet_files:
            msg = f"No parquet files found in {model_dir}"
            raise FileNotFoundError(msg)
        parquet_path = parquet_files[0]

    logger.info("Loading communities from %s", parquet_path)
    df = pd.read_parquet(parquet_path)
    logger.info("Loaded %d communities, initializing engine...", len(df))

    engine = LMEEngine(df, seed=42)
    logger.info("LME Engine ready")
    return engine


def input_fn(request_body: str, content_type: str) -> UserPreferences:
    """Parse incoming request into UserPreferences.

    Accepts JSON with user preference fields.
    """
    if content_type != "application/json":
        msg = f"Unsupported content type: {content_type}. Use application/json."
        raise ValueError(msg)

    data = json.loads(request_body)
    filtered = {k: v for k, v in data.items() if hasattr(UserPreferences, k)}
    return UserPreferences(**filtered)


def predict_fn(user: UserPreferences, engine: LMEEngine) -> dict:
    """Run LME scoring for the given user preferences."""
    result = engine.score(user)
    return asdict(result)


def output_fn(prediction: dict, accept: str) -> str:
    """Serialize prediction to JSON response."""
    if accept != "application/json":
        logger.warning("Unsupported accept type %s, defaulting to JSON", accept)
    return json.dumps(prediction, default=str)
