"""
Create Redshift cluster for MTH LME with required IAM roles and policies.

IMPORTANT: This creates billable AWS resources (~$0.25/hr).
Remember to run 99_cleanup.py when done.
"""

import json
import secrets
import string
import sys
import time
from pathlib import Path

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _config import get_session, load_pipeline_config

# Redshift configuration
CLUSTER_ID = "mth-lme"
DATABASE_NAME = "mth_lme"
MASTER_USER = "mth_admin"
NODE_TYPE = "ra3.large"
NUM_NODES = 1  # Single node for dev
IAM_ROLE_NAME = "MTH_RedshiftRole"
PORT = 5439


def create_iam_role(iam):
    """Create IAM role for Redshift with S3, Athena, and Glue access."""
    assume_role_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "redshift.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        iam.create_role(
            RoleName=IAM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(assume_role_doc),
            Description="MTH Redshift role for S3/Athena/Glue access",
        )
        print(f"Created IAM role: {IAM_ROLE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"IAM role already exists: {IAM_ROLE_NAME}")
        else:
            raise

    # Attach managed policies
    managed_policies = [
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        "arn:aws:iam::aws:policy/AmazonAthenaFullAccess",
        "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess",
    ]

    for policy_arn in managed_policies:
        try:
            iam.attach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn=policy_arn)
            print(f"  Attached: {policy_arn.split('/')[-1]}")
        except ClientError:
            pass

    # Get role ARN
    role_info = iam.get_role(RoleName=IAM_ROLE_NAME)
    return role_info["Role"]["Arn"]


def store_credentials(secretsmanager, master_user, master_pw):
    """Store Redshift credentials in Secrets Manager."""
    secret_name = "mth_redshift_login"
    secret_value = json.dumps([{"username": master_user}, {"password": master_pw}])

    try:
        secretsmanager.create_secret(
            Name=secret_name,
            SecretString=secret_value,
            Description="MTH Redshift master credentials",
        )
        print(f"Stored credentials in Secrets Manager: {secret_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceExistsException":
            secretsmanager.update_secret(
                SecretId=secret_name, SecretString=secret_value
            )
            print(f"Updated credentials in Secrets Manager: {secret_name}")
        else:
            raise


def create_cluster(redshift, secretsmanager, iam_role_arn):
    """Create Redshift cluster."""
    # Generate a secure password
    alphabet = string.ascii_letters + string.digits + "!#$%&()*+,-.:;<=>?[]^_{|}~"
    password = (
        secrets.choice(string.ascii_uppercase)
        + secrets.choice(string.ascii_lowercase)
        + secrets.choice(string.digits)
        + "".join(secrets.choice(alphabet) for _ in range(13))
    )

    store_credentials(secretsmanager, MASTER_USER, password)

    try:
        redshift.create_cluster(
            ClusterIdentifier=CLUSTER_ID,
            ClusterType="single-node",
            NodeType=NODE_TYPE,
            MasterUsername=MASTER_USER,
            MasterUserPassword=password,
            DBName=DATABASE_NAME,
            Port=PORT,
            IamRoles=[iam_role_arn],
            PubliclyAccessible=True,
            Tags=[
                {"Key": "Project", "Value": "MTH-LME"},
                {"Key": "Environment", "Value": "development"},
            ],
        )
        print(f"Creating Redshift cluster: {CLUSTER_ID}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ClusterAlreadyExists":
            print(f"Cluster already exists: {CLUSTER_ID}")
        else:
            raise


def wait_for_cluster(redshift):
    """Wait until cluster is available."""
    print("Waiting for cluster to become available...")
    while True:
        response = redshift.describe_clusters(ClusterIdentifier=CLUSTER_ID)
        status = response["Clusters"][0]["ClusterStatus"]
        print(f"  Status: {status}")
        if status == "available":
            endpoint = response["Clusters"][0]["Endpoint"]["Address"]
            iam_role = response["Clusters"][0]["IamRoles"][0]["IamRoleArn"]
            print("\n[OK] Cluster available!")
            print(f"  Endpoint: {endpoint}")
            print(f"  IAM Role: {iam_role}")
            return endpoint, iam_role
        time.sleep(30)


def main():
    config = load_pipeline_config()
    session = get_session()

    iam = session.client("iam")
    redshift = session.client("redshift")
    secretsmanager = session.client("secretsmanager")

    # Step 1: Create IAM role
    iam_role_arn = create_iam_role(iam)
    print(f"IAM Role ARN: {iam_role_arn}")

    # Step 2: Wait for role propagation
    print("Waiting 10s for IAM role propagation...")
    time.sleep(10)

    # Step 3: Create cluster
    create_cluster(redshift, secretsmanager, iam_role_arn)

    # Step 4: Wait for cluster
    endpoint, iam_role = wait_for_cluster(redshift)

    # Step 5: Update pipeline config
    config["redshift_cluster_id"] = CLUSTER_ID
    config["redshift_endpoint"] = endpoint
    config["redshift_database"] = DATABASE_NAME
    config["redshift_iam_role"] = iam_role
    config["redshift_port"] = PORT
    config["redshift_master_user"] = MASTER_USER
    config["redshift_secret_name"] = "mth_redshift_login"

    config_path = (
        Path(__file__).resolve().parent.parent / "data" / "pipeline_config.json"
    )
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    print("\nPipeline config updated with Redshift details.")


if __name__ == "__main__":
    main()
