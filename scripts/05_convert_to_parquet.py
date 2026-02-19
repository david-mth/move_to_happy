"""
Convert CSV data to Parquet using Athena CTAS (Create Table As Select).
Partitioned by state_name for query performance.
"""

import json
import sys
from pathlib import Path

import pandas as pd
from pyathena import connect

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _config import get_session, load_pipeline_config

DATABASE_NAME = "mth_lme"
TABLE_NAME_CSV = "mth_communities_csv"
TABLE_NAME_PARQUET = "mth_communities_parquet"


def main():
    config = load_pipeline_config()
    session = get_session()
    bucket = config["bucket"]
    region = session.region_name
    s3_parquet_path = f"s3://{bucket}/mth-communities/parquet"
    s3_staging_dir = f"s3://{bucket}/athena/staging"

    conn = connect(
        profile_name=session.profile_name,
        region_name=region,
        s3_staging_dir=s3_staging_dir,
    )

    # Drop if exists (for idempotency)
    pd.read_sql(f"DROP TABLE IF EXISTS {DATABASE_NAME}.{TABLE_NAME_PARQUET}", conn)

    # CTAS: Create Parquet table partitioned by state_name
    # Note: partition column must be last in SELECT
    statement = f"""CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME_PARQUET}
    WITH (
        format = 'PARQUET',
        external_location = '{s3_parquet_path}',
        partitioned_by = ARRAY['state_name']
    ) AS
    SELECT
        canonical_id,
        source_id,
        region,
        city,
        city_state,
        county_name,
        latitude,
        longitude,
        population,
        nlcd_code,
        land_use_description,
        terrain,
        climate,
        cost_of_living,
        miles_to_mountains,
        drive_time_mountains,
        miles_to_atlantic,
        miles_to_gulf,
        miles_to_beach,
        drive_time_beach,
        miles_to_lake,
        drive_time_lake,
        closest_intl_airport_iata,
        closest_intl_airport_miles,
        closest_intl_airport_hours,
        closest_natl_airport_iata,
        closest_natl_airport_miles,
        closest_natl_airport_hours,
        closest_regional_airport_iata,
        closest_regional_airport_hours,
        closest_regional_airport_miles,
        state_name
    FROM {DATABASE_NAME}.{TABLE_NAME_CSV}"""

    print("Executing CTAS (CSV -> Parquet)... this may take a minute.")
    pd.read_sql(statement, conn)

    # Repair partitions
    repair = f"MSCK REPAIR TABLE {DATABASE_NAME}.{TABLE_NAME_PARQUET}"
    print("Repairing partitions...")
    pd.read_sql(repair, conn)

    # Show partitions
    df_partitions = pd.read_sql(
        f"SHOW PARTITIONS {DATABASE_NAME}.{TABLE_NAME_PARQUET}", conn
    )
    print("\nPartitions:")
    print(df_partitions.to_string(index=False))

    # Verify with sample query
    df_sample = pd.read_sql(
        f"SELECT canonical_id, city, state_name, population "
        f"FROM {DATABASE_NAME}.{TABLE_NAME_PARQUET} "
        f"WHERE state_name = 'Georgia' LIMIT 5",
        conn,
    )
    print("\nSample Parquet query (Georgia):")
    print(df_sample.to_string(index=False))

    row_count = pd.read_sql(
        f"SELECT COUNT(*) as cnt FROM {DATABASE_NAME}.{TABLE_NAME_PARQUET}", conn
    )
    print(f"\nTotal rows in Parquet: {row_count['cnt'].iloc[0]}")

    # Save parquet path to config
    config["s3_parquet_path"] = s3_parquet_path
    config_path = (
        Path(__file__).resolve().parent.parent / "data" / "pipeline_config.json"
    )
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)
    print("Updated pipeline_config.json with parquet path.")


if __name__ == "__main__":
    main()
