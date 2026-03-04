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
from minio.minio_client import create_s3_bucket
from logs.logging_config import setup_logger
import logging
from pathlib import Path

from unitycatalog.uc import (
    uc_list_catalogs,
    uc_list_schemas,
    uc_list_tables,
    uc_create_catalog,
    uc_create_schemas,
    uc_register_table,
)
from helpers_spark.delta_tables import infer_columns_from_delta, scan_minio_for_delta_tables
from helpers_spark.spark_session import create_spark_session

from settings import Settings
import sys


# ─── MinIO Client ─────────────────────────────────────────────────────────────


# ─── Main Init Logic ──────────────────────────────────────────────────────────


def init_unity_catalog(minio_bucket, headers, uc_url, s3):
    logger.info(f"Scanning MinIO for Delta tables in bucket {minio_bucket}...")
    delta_tables = scan_minio_for_delta_tables(minio_bucket, s3)

    if not delta_tables:
        logger.info("No Delta tables found in MinIO.")
        return

    logger.info(f"Found {len(delta_tables)} Delta table(s)")

    # Get current UC state
    existing_catalogs = uc_list_catalogs(headers, uc_url,logger)

    for catalog, schema, table, s3a_path in delta_tables:
        logger.info(f"Processing: {catalog}.{schema}.{table} in location {s3a_path}")

        # Ensure catalog exists
        if catalog not in existing_catalogs:
            uc_create_catalog(catalog, headers, uc_url,logger)
            existing_catalogs.add(catalog)

        # Ensure schema exists
        existing_schemas = uc_list_schemas(catalog, headers, uc_url,logger)
        if schema not in existing_schemas:
            uc_create_schemas(catalog, [schema], headers, uc_url,logger)

        # Register table if not already in UC
        existing_tables = uc_list_tables(catalog, schema, headers, uc_url,logger)
        if table in existing_tables:
            logger.info(f"Already registered: {catalog}.{schema}.{table}")
            continue

        # Infer schema from Delta log
        columns = infer_columns_from_delta(s3a_path, spark)
        if columns is None:
            logger.info(f"Skipping {table} — could not read schema")
            continue

        uc_register_table(
            catalog,
            schema,
            table,
            s3a_path,
            columns,
            headers,
            
            UC_URL=uc_url,
            logger=logger
        )

    logger.info("UC initialization complete!")
    logger.info("Current UC state:")
    for catalog in uc_list_catalogs(headers, uc_url,logger):
        for schema in uc_list_schemas(catalog, headers, uc_url,logger):
            tables = uc_list_tables(catalog, schema, headers, uc_url,logger)
            for t in tables:
                logger.info(f"Catalog - Schema: {catalog} - {schema}.{t}")


# ─── Run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__" or "get_ipython" in dir():
    settings = Settings.load()

    setup_logger(
        settings.log_dir / f"{settings.project_name}_{settings.env}_uc_init_log.json",
        settings.env,
        settings.project_name,
        settings.storage.type)
    

    logger = logging.getLogger(Path(__file__).stem)

    try:
        storage = settings.storage
        uc = settings.uc
        app_name = "discogs-uc-init"

        spark = create_spark_session(
            app_name, storage.endpoint, storage.access_key, storage.secret_key
        )
        
        
        if settings.storage.type == "minio":


            create_s3_bucket(storage.s3, settings.project_name, logger=logger)

            init_unity_catalog(storage.bucket, uc.headers, uc.url, storage.s3)
            
            
            

    except Exception:
        logger.exception("Fatal error during Unity catalog initialization/refresh")
        sys.exit(1)
