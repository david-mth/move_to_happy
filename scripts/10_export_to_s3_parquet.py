"""
Export Redshift data to S3 as Parquet using UNLOAD, partitioned by state.
This creates a clean, query-optimized export in the data lake.
"""

import contextlib
import json
import sys
from pathlib import Path

import awswrangler as wr
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _config import get_session, load_pipeline_config


def main():
    config = load_pipeline_config()
    session = get_session()

    # Set default session for awswrangler
    boto3.setup_default_session(
        profile_name=session.profile_name,
        region_name=session.region_name,
    )

    cluster_id = config["redshift_cluster_id"]
    redshift_db = config["redshift_database"]
    master_user = config["redshift_master_user"]
    iam_role = config["redshift_iam_role"]
    bucket = config["bucket"]

    s3_export_path = f"s3://{bucket}/mth-communities/parquet-from-redshift"

    con = wr.data_api.redshift.connect(
        cluster_id=cluster_id,
        database=redshift_db,
        db_user=master_user,
    )

    # UNLOAD to S3 as Parquet, partitioned by state_name
    statement = f"""
    UNLOAD ('SELECT
        canonical_id, source_id, region, city, city_state, county_name,
        latitude, longitude, population, nlcd_code, land_use_description,
        terrain, climate, cost_of_living, miles_to_mountains,
        drive_time_mountains, miles_to_atlantic, miles_to_gulf,
        miles_to_beach, drive_time_beach, miles_to_lake, drive_time_lake,
        closest_intl_airport_iata, closest_intl_airport_miles,
        closest_intl_airport_hours, closest_natl_airport_iata,
        closest_natl_airport_miles, closest_natl_airport_hours,
        closest_regional_airport_iata, closest_regional_airport_hours,
        closest_regional_airport_miles, state_name
    FROM redshift.mth_communities')
    TO '{s3_export_path}/'
    IAM_ROLE '{iam_role}'
    PARQUET
    PARALLEL ON
    PARTITION BY (state_name)
    ALLOWOVERWRITE
    """

    print(f"Exporting Redshift data to: {s3_export_path}")
    print("This may take a moment...")

    # UNLOAD doesn't return rows
    with contextlib.suppress(Exception):
        wr.data_api.redshift.read_sql_query(sql=statement, con=con)

    print(f"\n[DONE] Data exported to {s3_export_path}")
    print("Verify with:")
    print(
        f"  aws s3 ls {s3_export_path}/ --recursive --profile move-to-happy | head -20"
    )

    # Update config
    config["s3_export_path"] = s3_export_path
    config_path = (
        Path(__file__).resolve().parent.parent / "data" / "pipeline_config.json"
    )
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)


if __name__ == "__main__":
    main()
