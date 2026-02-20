"""Deploy LME as a SageMaker real-time endpoint.

1. Packages community Parquet as model.tar.gz
2. Uploads to S3
3. Deploys sklearn endpoint with inference.py
"""

from __future__ import annotations

import logging
import subprocess
import sys
import tempfile
from pathlib import Path

from _config import get_session, load_pipeline_config
from sagemaker.sklearn import SKLearnModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENDPOINT_NAME = "mth-lme-endpoint"


def main() -> None:
    """Deploy LME endpoint."""
    session = get_session()
    config = load_pipeline_config()

    bucket = config["bucket"]
    role_arn = config.get("sagemaker_role_arn")
    if not role_arn:
        logger.error("sagemaker_role_arn not found in pipeline_config.json")
        logger.error("Run: python sagemaker/iam.py  to create the role first")
        sys.exit(1)

    import sagemaker

    sm_session = sagemaker.Session(boto_session=session)

    # 1. Download community parquet from S3 and package as model artifact
    s3_parquet = config["s3_parquet_path"]
    s3_model_path = f"s3://{bucket}/lme/model/model.tar.gz"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Download parquet files from S3
        logger.info("Downloading community data from %s", s3_parquet)
        import awswrangler as wr

        df = wr.s3.read_parquet(path=s3_parquet, boto3_session=session)
        parquet_path = tmpdir_path / "communities.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info("Wrote %d communities to %s", len(df), parquet_path)

        # Create model.tar.gz
        tar_path = tmpdir_path / "model.tar.gz"
        subprocess.run(
            [
                "tar",
                "czf",
                str(tar_path),
                "-C",
                str(tmpdir_path),
                "communities.parquet",
            ],
            check=True,
        )

        # Upload to S3
        s3 = session.client("s3")
        model_key = "lme/model/model.tar.gz"
        s3.upload_file(str(tar_path), bucket, model_key)
        logger.info("Uploaded model artifact to %s", s3_model_path)

    # 2. Deploy endpoint (bundle move_to_happy package via dependencies)
    model = SKLearnModel(
        model_data=s3_model_path,
        role=role_arn,
        entry_point="inference.py",
        source_dir=str(PROJECT_ROOT / "sagemaker" / "endpoint"),
        dependencies=[str(PROJECT_ROOT / "src" / "move_to_happy")],
        framework_version="1.2-1",
        sagemaker_session=sm_session,
    )

    logger.info("Deploying endpoint: %s", ENDPOINT_NAME)
    model.deploy(
        initial_instance_count=1,
        instance_type="ml.t2.medium",
        endpoint_name=ENDPOINT_NAME,
    )

    logger.info("Endpoint deployed: %s", ENDPOINT_NAME)
    logger.info("Test with: python scripts/13_test_endpoint.py")


if __name__ == "__main__":
    main()
