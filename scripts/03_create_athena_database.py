"""
Create Athena database for MTH LME data.
Uses PyAthena to execute DDL against Athena, which registers in Glue Data Catalog.
"""

import sys
from pathlib import Path

import pandas as pd
from pyathena import connect

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _config import get_session, load_pipeline_config

DATABASE_NAME = "mth_lme"


def main():
    config = load_pipeline_config()
    session = get_session()
    bucket = config["bucket"]
    region = session.region_name

    s3_staging_dir = f"s3://{bucket}/athena/staging"

    conn = connect(
        profile_name=session.profile_name,
        region_name=region,
        s3_staging_dir=s3_staging_dir,
    )

    # Create database
    statement = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}"
    print(f"Executing: {statement}")
    pd.read_sql(statement, conn)

    # Verify
    df_show = pd.read_sql("SHOW DATABASES", conn)
    if DATABASE_NAME in df_show.values:
        print(f"[OK] Database '{DATABASE_NAME}' created successfully")
    else:
        print(f"[ERROR] Database '{DATABASE_NAME}' not found")
        raise RuntimeError("Database creation failed")


if __name__ == "__main__":
    main()
