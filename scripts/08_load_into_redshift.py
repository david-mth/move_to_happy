"""
Load MTH community data into Redshift:
1. Create external schema pointing to Glue/Athena catalog (Spectrum)
2. Create internal Redshift schema and table
3. Load data from Athena into Redshift via INSERT INTO ... SELECT
"""

import sys
from pathlib import Path

import awswrangler as wr
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _config import get_session, load_pipeline_config

SCHEMA_ATHENA = "athena"
SCHEMA_REDSHIFT = "redshift"
ATHENA_DB = "mth_lme"


def execute_sql(con, sql, description=""):
    """Execute SQL and print result."""
    if description:
        print(f"\n--- {description} ---")
    print(f"SQL: {sql.strip()[:200]}...")
    try:
        result = wr.data_api.redshift.read_sql_query(sql=sql, con=con)
        if result is not None and not result.empty:
            print(result.to_string(index=False))
        return result
    except Exception as e:
        if "EmptyDataError" in type(e).__name__ or "No records" in str(e):
            print("  Done.")
        else:
            print(f"  Executed. ({e})")
        return None


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
    region = config["region"]

    # Connect via Redshift Data API
    con = wr.data_api.redshift.connect(
        cluster_id=cluster_id,
        database=redshift_db,
        db_user=master_user,
    )

    # 1. Create external schema (Athena/Glue Catalog -> Redshift Spectrum)
    execute_sql(
        con,
        f"""
        CREATE EXTERNAL SCHEMA IF NOT EXISTS {SCHEMA_ATHENA}
        FROM DATA CATALOG
        DATABASE '{ATHENA_DB}'
        IAM_ROLE '{iam_role}'
        REGION '{region}'
        CREATE EXTERNAL DATABASE IF NOT EXISTS
    """,
        "Create Athena external schema",
    )

    # 2. Verify: query Athena data through Spectrum
    execute_sql(
        con,
        f"""
        SELECT state_name, COUNT(*) as cnt
        FROM {SCHEMA_ATHENA}.mth_communities_csv
        GROUP BY state_name
        ORDER BY cnt DESC
    """,
        "Verify Spectrum access to Athena data",
    )

    # 3. Create internal Redshift schema
    execute_sql(
        con,
        f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_REDSHIFT}
    """,
        "Create Redshift internal schema",
    )

    # 4. Create internal Redshift table
    execute_sql(
        con,
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_REDSHIFT}.mth_communities (
            canonical_id VARCHAR(20) PRIMARY KEY,
            source_id INTEGER,
            region VARCHAR(50),
            city VARCHAR(100),
            city_state VARCHAR(200) DISTKEY,
            county_name VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            population INTEGER,
            nlcd_code INTEGER,
            land_use_description VARCHAR(100),
            terrain VARCHAR(50),
            climate VARCHAR(50),
            cost_of_living DOUBLE PRECISION,
            miles_to_mountains DOUBLE PRECISION,
            drive_time_mountains DOUBLE PRECISION,
            miles_to_atlantic DOUBLE PRECISION,
            miles_to_gulf DOUBLE PRECISION,
            miles_to_beach DOUBLE PRECISION,
            drive_time_beach DOUBLE PRECISION,
            miles_to_lake DOUBLE PRECISION,
            drive_time_lake DOUBLE PRECISION,
            closest_intl_airport_iata VARCHAR(10),
            closest_intl_airport_miles DOUBLE PRECISION,
            closest_intl_airport_hours DOUBLE PRECISION,
            closest_natl_airport_iata VARCHAR(10),
            closest_natl_airport_miles DOUBLE PRECISION,
            closest_natl_airport_hours DOUBLE PRECISION,
            closest_regional_airport_iata VARCHAR(10),
            closest_regional_airport_hours DOUBLE PRECISION,
            closest_regional_airport_miles DOUBLE PRECISION,
            state_name VARCHAR(50)
        )
        SORTKEY (state_name, canonical_id)
    """,
        "Create Redshift internal table",
    )

    # 5. Load data from Athena/Spectrum into Redshift
    execute_sql(
        con,
        f"""
        INSERT INTO {SCHEMA_REDSHIFT}.mth_communities
        SELECT
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
        FROM {SCHEMA_ATHENA}.mth_communities_parquet
    """,
        "Load data from Athena into Redshift",
    )

    # 6. Verify row count
    execute_sql(
        con,
        f"""
        SELECT COUNT(*) as total_rows FROM {SCHEMA_REDSHIFT}.mth_communities
    """,
        "Verify row count in Redshift",
    )

    # 7. Sample query
    execute_sql(
        con,
        f"""
        SELECT canonical_id, city, state_name, population, cost_of_living
        FROM {SCHEMA_REDSHIFT}.mth_communities
        ORDER BY population DESC
        LIMIT 10
    """,
        "Top 10 communities by population",
    )

    print("\n[DONE] Data loaded into Redshift successfully.")


if __name__ == "__main__":
    main()
