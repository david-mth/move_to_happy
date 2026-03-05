"""Persist and load the FAISS index from S3."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from .config import RAGConfig
from .indexer import CHUNKS_FILE, INDEX_FILE

logger = logging.getLogger(__name__)

_scripts = Path(__file__).resolve().parent.parent.parent.parent / "scripts"
sys.path.insert(0, str(_scripts))


def upload_index_to_s3(
    config: RAGConfig | None = None,
) -> str:
    """Upload the local FAISS index to S3."""
    from _config import get_session, load_pipeline_config

    config = config or RAGConfig()
    pipeline = load_pipeline_config()
    session = get_session()
    s3 = session.client("s3")
    bucket = pipeline["bucket"]

    local_dir = config.index_path
    uploaded: list[str] = []

    for filename in [INDEX_FILE, CHUNKS_FILE]:
        local_path = local_dir / filename
        if not local_path.exists():
            logger.warning("Skipping %s — not found", local_path)
            continue
        s3_key = f"{config.s3_prefix}/{filename}"
        s3.upload_file(str(local_path), bucket, s3_key)
        uri = f"s3://{bucket}/{s3_key}"
        uploaded.append(uri)
        logger.info("Uploaded: %s", uri)

    return f"s3://{bucket}/{config.s3_prefix}/"


def download_index_from_s3(
    config: RAGConfig | None = None,
) -> Path:
    """Download the FAISS index from S3 to local disk."""
    from _config import get_session, load_pipeline_config

    config = config or RAGConfig()
    pipeline = load_pipeline_config()
    session = get_session()
    s3 = session.client("s3")
    bucket = pipeline["bucket"]

    local_dir = config.index_path
    local_dir.mkdir(parents=True, exist_ok=True)

    for filename in [INDEX_FILE, CHUNKS_FILE]:
        s3_key = f"{config.s3_prefix}/{filename}"
        local_path = local_dir / filename
        s3.download_file(bucket, s3_key, str(local_path))
        logger.info("Downloaded: s3://%s/%s → %s", bucket, s3_key, local_path)

    return local_dir
