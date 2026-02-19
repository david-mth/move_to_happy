"""
Tear down billable resources. Run when done with development session.
Preserves: S3 data, Glue catalog, Secrets Manager entries.
Destroys: Redshift cluster (biggest cost item).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _config import get_session, load_pipeline_config


def main():
    config = load_pipeline_config()
    session = get_session()
    redshift = session.client("redshift")

    cluster_id = config.get("redshift_cluster_id", "mth-lme")

    print(f"Deleting Redshift cluster: {cluster_id}")
    print("  Skipping final snapshot (dev cluster)")

    try:
        redshift.delete_cluster(
            ClusterIdentifier=cluster_id, SkipFinalClusterSnapshot=True
        )
        print("  Cluster deletion initiated. This takes ~5 minutes.")
    except redshift.exceptions.ClusterNotFoundFault:
        print("  Cluster not found (already deleted)")
    except Exception as e:
        print(f"  Error: {e}")

    print("\nResources preserved:")
    print(f"  S3 bucket: {config['bucket']}")
    print("  Athena database: mth_lme")
    print("  Glue catalog tables")
    print("  Secrets Manager: mth_redshift_login")
    print("\nTo fully clean up S3:")
    print(f"  aws s3 rb s3://{config['bucket']} --force")


if __name__ == "__main__":
    main()
