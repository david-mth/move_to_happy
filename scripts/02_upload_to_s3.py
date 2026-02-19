"""
Create S3 bucket for MTH LME data and upload prepared community data.
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _config import get_session


def create_bucket(s3_client, bucket_name, region):
    """Create S3 bucket if it doesn't exist."""
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"Created bucket: {bucket_name}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket already exists: {bucket_name}")


def upload_file(s3_client, local_path, bucket, s3_key):
    """Upload a file to S3."""
    s3_client.upload_file(str(local_path), bucket, s3_key)
    print(f"Uploaded: s3://{bucket}/{s3_key}")


def main():
    session = get_session()
    s3 = session.client("s3")
    sts = session.client("sts")

    account_id = sts.get_caller_identity()["Account"]
    region = session.region_name
    bucket_name = f"mth-lme-data-{account_id}-{region}"

    s3_prefix_csv = "mth-communities/csv"
    s3_prefix_tsv = "mth-communities/tsv"

    project_root = Path(__file__).resolve().parent.parent
    local_csv = project_root / "data" / "prepared" / "mth_communities.csv"
    local_tsv = project_root / "data" / "prepared" / "mth_communities.tsv"

    # Create bucket
    create_bucket(s3, bucket_name, region)

    # Upload CSV
    upload_file(s3, local_csv, bucket_name, f"{s3_prefix_csv}/mth_communities.csv")

    # Upload TSV
    upload_file(s3, local_tsv, bucket_name, f"{s3_prefix_tsv}/mth_communities.tsv")

    # Store config for downstream scripts
    config = {
        "bucket": bucket_name,
        "region": region,
        "account_id": account_id,
        "s3_csv_path": f"s3://{bucket_name}/{s3_prefix_csv}",
        "s3_tsv_path": f"s3://{bucket_name}/{s3_prefix_tsv}",
    }

    config_path = project_root / "data" / "pipeline_config.json"
    os.makedirs(config_path.parent, exist_ok=True)
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    print(f"\nPipeline config saved to {config_path}")
    print(f"Bucket: {bucket_name}")
    print(f"CSV path: s3://{bucket_name}/{s3_prefix_csv}")


if __name__ == "__main__":
    main()
