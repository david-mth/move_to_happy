"""Launch SageMaker Processing Job for LME batch scoring.

Uses SKLearnProcessor to run the LME engine on community data from S3.
Bundles the move_to_happy package via dependencies so it's available
inside the container.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from _config import get_session, load_pipeline_config
from sagemaker.processing import FrameworkProcessor, ProcessingInput, ProcessingOutput
from sagemaker.sklearn.estimator import SKLearn

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    """Run LME processing job on SageMaker."""
    session = get_session()
    config = load_pipeline_config()

    bucket = config["bucket"]
    role_arn = config.get("sagemaker_role_arn")
    if not role_arn:
        logger.error("sagemaker_role_arn not found in pipeline_config.json")
        logger.error("Run: python sagemaker/iam.py  to create the role first")
        sys.exit(1)

    s3_input = config["s3_parquet_path"]
    s3_output = f"s3://{bucket}/lme/results/latest"

    import sagemaker

    sm_session = sagemaker.Session(boto_session=session)

    processor = FrameworkProcessor(
        estimator_cls=SKLearn,
        framework_version="1.2-1",
        role=role_arn,
        instance_count=1,
        instance_type="ml.t3.medium",
        sagemaker_session=sm_session,
    )

    logger.info("Launching processing job...")
    logger.info("  Input:  %s", s3_input)
    logger.info("  Output: %s", s3_output)

    processor.run(
        code="processing_script.py",
        source_dir=str(PROJECT_ROOT / "sagemaker" / "processing"),
        dependencies=[str(PROJECT_ROOT / "src" / "move_to_happy")],
        inputs=[
            ProcessingInput(
                source=s3_input,
                destination="/opt/ml/processing/input/data",
                input_name="communities",
            ),
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output",
                destination=s3_output,
                output_name="results",
            ),
        ],
    )
    logger.info("Processing job complete. Results at: %s", s3_output)


if __name__ == "__main__":
    main()
