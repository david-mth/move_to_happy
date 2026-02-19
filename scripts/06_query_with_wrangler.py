"""
Query MTH community data using AWS Data Wrangler (awswrangler).
Demonstrates both direct S3 Parquet reads and Athena SQL queries.
"""

import sys
from pathlib import Path

import awswrangler as wr
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _config import get_session, load_pipeline_config

DATABASE_NAME = "mth_lme"


def main():
    config = load_pipeline_config()
    session = get_session()
    bucket = config["bucket"]

    # Set default session for awswrangler
    boto3.setup_default_session(
        profile_name=session.profile_name,
        region_name=session.region_name,
    )

    # --- 1. Query Parquet from S3 with push-down filter ---
    print("=" * 60)
    print("1. Direct S3 Parquet read (Georgia only, push-down filter)")
    print("=" * 60)

    path = f"s3://{bucket}/mth-communities/parquet/"

    def p_filter(x):
        return x["state_name"] == "Georgia"

    df_ga = wr.s3.read_parquet(
        path,
        columns=[
            "canonical_id",
            "city",
            "county_name",
            "population",
            "cost_of_living",
            "state_name",
        ],
        partition_filter=p_filter,
        dataset=True,
    )
    print(f"Georgia communities: {len(df_ga)}")
    print(df_ga.head(10).to_string(index=False))

    # --- 2. Query Glue Catalog tables ---
    print("\n" + "=" * 60)
    print("2. Glue Catalog tables in mth_lme")
    print("=" * 60)

    for table in wr.catalog.get_tables(database=DATABASE_NAME):
        print(f"  {table['Name']}")

    # --- 3. Query via Athena ---
    print("\n" + "=" * 60)
    print("3. Athena query: Top 15 communities by population")
    print("=" * 60)

    df_top = wr.athena.read_sql_query(
        sql="""
        SELECT canonical_id, city, state_name, county_name, population,
               cost_of_living, terrain, climate
        FROM mth_communities_parquet
        WHERE population IS NOT NULL
        ORDER BY population DESC
        LIMIT 15
        """,
        database=DATABASE_NAME,
    )
    print(df_top.to_string(index=False))

    # --- 4. Athena aggregation query ---
    print("\n" + "=" * 60)
    print("4. Athena: State-level summary statistics")
    print("=" * 60)

    df_summary = wr.athena.read_sql_query(
        sql="""
        SELECT
            state_name,
            COUNT(*) as community_count,
            ROUND(AVG(population), 0) as avg_population,
            ROUND(AVG(cost_of_living), 1) as avg_cost_of_living,
            ROUND(AVG(miles_to_beach), 1) as avg_miles_to_beach,
            ROUND(AVG(miles_to_mountains), 1) as avg_miles_to_mountains
        FROM mth_communities_parquet
        GROUP BY state_name
        ORDER BY community_count DESC
        """,
        database=DATABASE_NAME,
    )
    print(df_summary.to_string(index=False))


if __name__ == "__main__":
    main()
