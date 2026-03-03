"""Download community and tier-1 CSV files from S3 into app/data/.

Intended to run on Replit before the app starts.  Requires
AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
(set via Replit Secrets).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

BUCKET = "mth-lme-data-284503683798-us-east-1"
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# S3 key  ->  local path relative to DATA_DIR
FILE_MAP: dict[str, str] = {
    "mth-communities/csv/mth_communities.csv": "mth_communities.csv",
    "tier1-data/census_acs/csv/census_acs.csv": "tier1/census_acs.csv",
    "tier1-data/fbi_crime/csv/fbi_crime.csv": "tier1/fbi_crime.csv",
    "tier1-data/fcc_broadband/csv/fcc_broadband.csv": "tier1/fcc_broadband.csv",
    "tier1-data/epa_air_quality/csv/epa_air_quality.csv": "tier1/epa_air_quality.csv",
    "tier1-data/bls_employment/csv/bls_employment.csv": "tier1/bls_employment.csv",
    "tier1-data/cms_hospitals/csv/cms_hospitals.csv": "tier1/cms_hospitals.csv",
    "tier1-data/cms_physicians/csv/cms_physicians.csv": "tier1/cms_physicians.csv",
}


def sync(*, force: bool = False) -> None:
    try:
        s3 = boto3.client("s3", region_name=REGION)
    except NoCredentialsError:
        print(
            "[sync_data] AWS credentials not found. "
            "Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.",
            file=sys.stderr,
        )
        sys.exit(1)

    downloaded = 0
    skipped = 0

    for s3_key, local_rel in FILE_MAP.items():
        local_path = DATA_DIR / local_rel
        if local_path.exists() and not force:
            skipped += 1
            continue

        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            s3.download_file(BUCKET, s3_key, str(local_path))
            print(f"  [OK] {local_rel}")
            downloaded += 1
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            print(f"  [FAIL] {local_rel}  ({code})", file=sys.stderr)

    print(f"[sync_data] Done — {downloaded} downloaded, {skipped} already present.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync data from S3")
    parser.add_argument(
        "--force", action="store_true", help="Re-download even if files exist"
    )
    args = parser.parse_args()

    print(f"[sync_data] Syncing {len(FILE_MAP)} files from s3://{BUCKET} ...")
    sync(force=args.force)
