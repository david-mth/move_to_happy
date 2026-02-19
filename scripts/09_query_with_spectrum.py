"""
Demonstrate Redshift Spectrum: query both Redshift internal tables and
S3-backed Athena tables in a single SQL statement.
"""

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

    con = wr.data_api.redshift.connect(
        cluster_id=cluster_id,
        database=redshift_db,
        db_user=master_user,
    )

    # --- Query 1: Redshift internal only ---
    print("=" * 60)
    print("1. Redshift internal: State summary")
    print("=" * 60)

    df = wr.data_api.redshift.read_sql_query(
        sql="""
        SELECT state_name,
               COUNT(*) as communities,
               ROUND(AVG(population), 0) as avg_pop,
               ROUND(AVG(cost_of_living), 1) as avg_col
        FROM redshift.mth_communities
        GROUP BY state_name
        ORDER BY communities DESC
        """,
        con=con,
    )
    print(df.to_string(index=False))

    # --- Query 2: Athena/S3 via Spectrum ---
    print("\n" + "=" * 60)
    print("2. Spectrum (Athena/S3): Same query on S3 data")
    print("=" * 60)

    df = wr.data_api.redshift.read_sql_query(
        sql="""
        SELECT state_name,
               COUNT(*) as communities,
               ROUND(AVG(population), 0) as avg_pop,
               ROUND(AVG(cost_of_living), 1) as avg_col
        FROM athena.mth_communities_parquet
        GROUP BY state_name
        ORDER BY communities DESC
        """,
        con=con,
    )
    print(df.to_string(index=False))

    # --- Query 3: Cross-system UNION (Redshift + Spectrum) ---
    print("\n" + "=" * 60)
    print("3. UNION across Redshift internal + Athena/S3 (proof of concept)")
    print("=" * 60)

    df = wr.data_api.redshift.read_sql_query(
        sql="""
        SELECT 'redshift' as source, canonical_id, city, state_name, population
        FROM redshift.mth_communities
        WHERE population > 100000
        UNION ALL
        SELECT 'athena_s3' as source, canonical_id, city, state_name, population
        FROM athena.mth_communities_parquet
        WHERE population > 100000
        ORDER BY population DESC
        """,
        con=con,
    )
    print(df.to_string(index=False))

    # --- Query 4: EXPLAIN plan showing both sources ---
    print("\n" + "=" * 60)
    print("4. EXPLAIN plan (shows S3 Seq Scan for Spectrum)")
    print("=" * 60)

    df = wr.data_api.redshift.read_sql_query(
        sql="""
        EXPLAIN
        SELECT r.canonical_id, r.city, r.state_name, r.population
        FROM redshift.mth_communities r
        JOIN athena.mth_communities_parquet a
            ON r.canonical_id = a.canonical_id
        WHERE r.population > 50000
        """,
        con=con,
    )
    print(df.to_string(index=False))

    print("\n[DONE] Spectrum queries completed.")


if __name__ == "__main__":
    main()
