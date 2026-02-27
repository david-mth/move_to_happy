"""Shared utilities for Tier 1 data capture pipeline.

Provides caching, rate-limited HTTP, S3 upload, Athena registration,
and data loading used by all tier1 scripts.
"""

from __future__ import annotations

import json
import os
import sys
import time
from math import atan2, cos, radians, sin, sqrt
from pathlib import Path

import pandas as pd
import requests
from pyathena import connect

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PREPARED_DIR = DATA_DIR / "prepared"
CACHE_DIR = DATA_DIR / "cache" / "tier1"
CROSSWALK_DIR = PREPARED_DIR / "crosswalks"
TIER1_DIR = PREPARED_DIR / "tier1"

# Import shared AWS config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _config import get_session, load_pipeline_config  # noqa: E402

DATABASE_NAME = "mth_lme"

# State FIPS codes for GA, AL, FL
STATE_FIPS = {"Georgia": "13", "Alabama": "01", "Florida": "12"}


# ---------------------------------------------------------------------------
# Directory setup
# ---------------------------------------------------------------------------
def ensure_dirs() -> None:
    """Create all required cache and output directories."""
    for d in [
        CACHE_DIR / "geocoder",
        CACHE_DIR / "census_acs",
        CACHE_DIR / "noaa",
        CACHE_DIR / "epa",
        CACHE_DIR / "bls",
        CACHE_DIR / "fcc",
        CACHE_DIR / "fbi",
        CACHE_DIR / "cms_hospitals",
        CACHE_DIR / "cms_physicians",
        CROSSWALK_DIR,
        TIER1_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------
def load_communities() -> pd.DataFrame:
    """Load the master communities CSV (1,307 rows)."""
    path = PREPARED_DIR / "mth_communities.csv"
    return pd.read_csv(path)


def load_crosswalk() -> pd.DataFrame:
    """Load the county FIPS crosswalk CSV."""
    path = CROSSWALK_DIR / "county_fips_crosswalk.csv"
    return pd.read_csv(
        path,
        dtype={
            "state_fips": str,
            "county_fips": str,
            "fips_5digit": str,
        },
    )


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------
def load_cached(source: str, key: str) -> dict | list | None:
    """Load cached API response from data/cache/tier1/{source}/{key}.json."""
    path = CACHE_DIR / source / f"{key}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def save_cache(source: str, key: str, data: dict | list) -> None:
    """Save API response to data/cache/tier1/{source}/{key}.json."""
    path = CACHE_DIR / source / f"{key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


# ---------------------------------------------------------------------------
# Rate-limited HTTP
# ---------------------------------------------------------------------------
def api_get(
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    rate_limit: float = 1.0,
    max_retries: int = 3,
    timeout: int = 30,
) -> requests.Response:
    """HTTP GET with rate limiting and exponential backoff on 429/5xx."""
    time.sleep(rate_limit)
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = 2 ** (attempt + 1)
                print(
                    f"  Retry {attempt + 1}/{max_retries} after {wait}s "
                    f"(HTTP {resp.status_code})"
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  Retry {attempt + 1}/{max_retries} after {wait}s ({e})")
                time.sleep(wait)
            else:
                raise
    # Should not reach here, but satisfy type checker
    msg = f"All {max_retries} retries exhausted"
    raise requests.exceptions.RequestException(msg) from last_exc


# ---------------------------------------------------------------------------
# S3 upload
# ---------------------------------------------------------------------------
def upload_to_s3(local_path: Path | str, source_name: str) -> str:
    """Upload file to s3://{bucket}/tier1-data/{source}/csv/{file}.

    Returns the full S3 URI.
    """
    local_path = Path(local_path)
    config = load_pipeline_config()
    session = get_session()
    s3 = session.client("s3")
    bucket = config["bucket"]
    s3_key = f"tier1-data/{source_name}/csv/{local_path.name}"

    s3.upload_file(str(local_path), bucket, s3_key)
    s3_uri = f"s3://{bucket}/{s3_key}"
    print(f"  Uploaded: {s3_uri}")
    return s3_uri


# ---------------------------------------------------------------------------
# Athena registration
# ---------------------------------------------------------------------------
def register_athena_table(
    table_name: str,
    columns: list[tuple[str, str]],
    s3_location: str,
) -> None:
    """Register a CSV as an Athena external table (DROP + CREATE)."""
    config = load_pipeline_config()
    session = get_session()
    bucket = config["bucket"]
    s3_staging = f"s3://{bucket}/athena/staging"

    conn = connect(
        profile_name=session.profile_name,
        region_name=session.region_name,
        s3_staging_dir=s3_staging,
    )

    # Drop for idempotency
    pd.read_sql(f"DROP TABLE IF EXISTS {DATABASE_NAME}.{table_name}", conn)

    # Build column definitions
    col_defs = ",\n        ".join(f"{name} {dtype}" for name, dtype in columns)

    # S3 location must be the directory containing the CSV
    s3_dir = s3_location.rsplit("/", 1)[0] + "/"

    stmt = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name}(
        {col_defs}
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    LOCATION '{s3_dir}'
    TBLPROPERTIES ('skip.header.line.count'='1')"""

    pd.read_sql(stmt, conn)

    # Verify with COUNT
    count_df = pd.read_sql(
        f"SELECT COUNT(*) as cnt FROM {DATABASE_NAME}.{table_name}",
        conn,
    )
    cnt = count_df["cnt"].iloc[0]
    print(f"  Athena table {DATABASE_NAME}.{table_name}: {cnt} rows")


# ---------------------------------------------------------------------------
# Haversine (copied from src/move_to_happy/lme/eliminators.py)
# ---------------------------------------------------------------------------
def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles between two lat/lon points."""
    earth_radius_miles = 3958.8
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return earth_radius_miles * 2 * atan2(sqrt(a), sqrt(1 - a))


# ---------------------------------------------------------------------------
# Env var loader
# ---------------------------------------------------------------------------
def require_env(name: str) -> str:
    """Get required environment variable or raise with helpful message."""
    val = os.environ.get(name)
    if not val:
        msg = f"Environment variable {name} is required. Add it to your .env file."
        raise RuntimeError(msg)
    return val
