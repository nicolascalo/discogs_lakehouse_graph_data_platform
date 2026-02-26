"""
uc_init.py - Unity Catalog initialization script
-------------------------------------------------
Scans MinIO for Delta tables and registers any missing ones in UC.
Run this once on startup, or whenever you add/remove tables manually in MinIO.

Usage:
    python3 uc_init.py
    # or from Jupyter:
    %run uc_init.py
"""

import boto3
from botocore.client import Config

from logs.logging_config import setup_logger


from unitycatalog.uc import (
    uc_list_catalogs,
    uc_list_schemas,
    uc_list_tables,
    uc_create_catalog,
    uc_create_schema,
    uc_register_table,
)
from unitycatalog.delta import infer_columns_from_delta, scan_minio_for_delta_tables
from helpers_spark.spark_session import create_spark_session
from unitycatalog.config import UCConfig
from helpers_minio.minio_client import get_minio_client

# ─── MinIO Client ─────────────────────────────────────────────────────────────


# ─── Main Init Logic ──────────────────────────────────────────────────────────


def init_unity_catalog(config, s3):
    print("🔍 Scanning MinIO for Delta tables...")
    delta_tables = scan_minio_for_delta_tables(config.minio_bucket, s3)

    if not delta_tables:
        print("  No Delta tables found in MinIO.")
        return

    print(f"  Found {len(delta_tables)} Delta table(s)\n")

    # Get current UC state
    existing_catalogs = uc_list_catalogs(config.headers, config.uc_url)

    for catalog, schema, table, s3a_path in delta_tables:
        print(f"📦 Processing: {catalog}.{schema}.{table}")
        print(f"   Location: {s3a_path}")

        # Ensure catalog exists
        if catalog not in existing_catalogs:
            uc_create_catalog(catalog, config.headers, config.uc_url)
            existing_catalogs.add(catalog)

        # Ensure schema exists
        existing_schemas = uc_list_schemas(catalog, config.headers, config.uc_url)
        if schema not in existing_schemas:
            uc_create_schema(catalog, schema, config.headers, config.uc_url)

        # Register table if not already in UC
        existing_tables = uc_list_tables(catalog, schema, config.headers, config.uc_url)
        if table in existing_tables:
            print(f"  ⏭️  Already registered: {catalog}.{schema}.{table}")
            continue

        # Infer schema from Delta log
        columns = infer_columns_from_delta(s3a_path, spark)
        if columns is None:
            print(f"  ❌ Skipping {table} — could not read schema")
            continue

        uc_register_table(
            catalog,
            schema,
            table,
            s3a_path,
            columns,
            config.headers,
            UC_URL=config.uc_url,
        )

    print("\n✅ UC initialization complete!")
    print("\n📋 Current UC state:")
    for catalog in uc_list_catalogs(config.headers, config.uc_url):
        print(f"  Catalog: {catalog}")
        for schema in uc_list_schemas(catalog, config.headers, config.uc_url):
            tables = uc_list_tables(catalog, schema, config.headers, config.uc_url)
            for t in tables:
                print(f"    └── {schema}.{t}")


# ─── Run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__" or "get_ipython" in dir():
    config = UCConfig.from_env()
    spark = create_spark_session(
        app_name="discogs-uc-init",
        MINIO_ENDPOINT=config.minio_endpoint,
        MINIO_ACCESS_KEY=config.minio_access_key,
        MINIO_SECRET_KEY=config.minio_secret_key,
    )
    logger = setup_logger(config.log_dir / "discogs_uc_init.log")

    s3 = get_minio_client(
        config.minio_endpoint,
        config.minio_access_key,
        config.minio_secret_key,
        config.region_name,
    )

    init_unity_catalog(config, s3)
