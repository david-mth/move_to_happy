"""Create SageMaker IAM role for MTH LME processing and endpoints."""

from __future__ import annotations

import json
import logging

import boto3

logger = logging.getLogger(__name__)

ROLE_NAME = "MTH_SageMakerRole"

TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "sagemaker.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}


def create_sagemaker_role(session: boto3.Session, bucket: str) -> str:
    """Create or retrieve the MTH SageMaker IAM role.

    Args:
        session: boto3 session with IAM permissions.
        bucket: S3 bucket name for data access.

    Returns:
        ARN of the created/existing role.
    """
    iam = session.client("iam")

    # Check if role already exists
    try:
        response = iam.get_role(RoleName=ROLE_NAME)
        arn = response["Role"]["Arn"]
        logger.info("Role %s already exists: %s", ROLE_NAME, arn)
        return arn
    except iam.exceptions.NoSuchEntityException:
        pass

    # Create the role
    logger.info("Creating IAM role: %s", ROLE_NAME)
    response = iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
        Description="SageMaker execution role for MTH LME pipeline",
    )
    arn = response["Role"]["Arn"]

    # Attach SageMaker managed policy
    iam.attach_role_policy(
        RoleName=ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonSageMakerFullAccess",
    )

    # Inline policy for S3 + CloudWatch access
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket",
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket}",
                    f"arn:aws:s3:::{bucket}/*",
                ],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ],
    }
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="MTH_S3_CloudWatch_Access",
        PolicyDocument=json.dumps(s3_policy),
    )

    logger.info("Created role %s: %s", ROLE_NAME, arn)
    return arn


if __name__ == "__main__":
    import pathlib
    import sys

    scripts_dir = pathlib.Path(__file__).resolve().parent.parent / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from _config import get_session, load_pipeline_config

    logging.basicConfig(level=logging.INFO)
    session = get_session()
    config = load_pipeline_config()
    arn = create_sagemaker_role(session, config["bucket"])
    print(f"Role ARN: {arn}")
