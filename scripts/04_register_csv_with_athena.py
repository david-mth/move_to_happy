"""
Register the MTH communities CSV as an Athena external table.
This creates the table definition in the Glue Data Catalog.
"""

import sys
from pathlib import Path

import pandas as pd
from pyathena import connect

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _config import get_session, load_pipeline_config

DATABASE_NAME = "mth_lme"
TABLE_NAME = "mth_communities_csv"


def main():
    config = load_pipeline_config()
    session = get_session()
    bucket = config["bucket"]
    region = session.region_name
    s3_csv_path = config["s3_csv_path"]
    s3_staging_dir = f"s3://{bucket}/athena/staging"

    conn = connect(
        profile_name=session.profile_name,
        region_name=region,
        s3_staging_dir=s3_staging_dir,
    )

    # Drop if exists (for idempotency during development)
    pd.read_sql(f"DROP TABLE IF EXISTS {DATABASE_NAME}.{TABLE_NAME}", conn)

    statement = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.{TABLE_NAME}(
        canonical_id string,
        source_id int,
        region string,
        needs_updating string,
        city string,
        state_name string,
        city_state string,
        county_name string,
        latitude double,
        longitude double,
        population int,
        nlcd_code int,
        land_use_description string,
        terrain string,
        climate string,
        cost_of_living double,
        miles_to_mountains double,
        drive_time_mountains double,
        miles_to_atlantic double,
        miles_to_gulf double,
        miles_to_beach double,
        drive_time_beach double,
        miles_to_lake double,
        drive_time_lake double,
        closest_intl_airport_iata string,
        closest_intl_airport_miles double,
        closest_intl_airport_hours double,
        closest_natl_airport_iata string,
        closest_natl_airport_miles double,
        closest_natl_airport_hours double,
        closest_regional_airport_iata string,
        closest_regional_airport_hours double,
        closest_regional_airport_miles double
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    LOCATION '{s3_csv_path}'
    TBLPROPERTIES ('skip.header.line.count'='1')"""

    print("Executing CREATE TABLE...")
    pd.read_sql(statement, conn)

    # Verify table exists
    df_tables = pd.read_sql(f"SHOW TABLES IN {DATABASE_NAME}", conn)
    if TABLE_NAME in df_tables.values:
        print(f"[OK] Table '{TABLE_NAME}' created")
    else:
        print("[ERROR] Table not found")
        raise RuntimeError("Table creation failed")

    # Run sample query
    df_sample = pd.read_sql(
        f"SELECT canonical_id, city, state_name, county_name, population "
        f"FROM {DATABASE_NAME}.{TABLE_NAME} LIMIT 10",
        conn,
    )
    print("\nSample query results:")
    print(df_sample.to_string(index=False))

    row_count = pd.read_sql(
        f"SELECT COUNT(*) as cnt FROM {DATABASE_NAME}.{TABLE_NAME}", conn
    )
    print(f"\nTotal rows: {row_count['cnt'].iloc[0]}")


if __name__ == "__main__":
    main()
