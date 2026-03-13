from bronze_ingest.archive import cleanup_old_raw_files_s3
from bronze_ingest.file_discovery import (
    get_s3_folder_contents,
    get_latest_dump_date_s3,
    get_latest_dump_files_info_s3,
)
from unitycatalog.uc import (
    uc_list_tables,
)
from helpers_spark.delta_metrics import (
    export_delta_table_history_s3,
    create_new_delta_history,
)

from helpers_spark.delta_tables import (
    infer_columns_from_delta,
)

from download_raw.validate import validate_downloads_s3

from settings import Settings
import logging
from pathlib import Path

from logs.logging_config import setup_logger
from helpers_spark.schemas import export_schemas_s3_and_head
from helpers_spark.spark_session import create_spark_session
from bronze_ingest.xml import read_xml_to_df
from helpers_spark.hash import apply_hash
from helpers_spark.spark_uc import (
    merge_into_uc_delta,
    uc_create_catalog,
    uc_create_schemas,
    uc_list_tables,
    uc_register_table,
)
from pyspark.sql import DataFrame

import pandas as pd
import os
from helpers_spark.serialization import serialize_for_merge, clean_nested_columns

import sys

import datetime


def process_single_dump(
    spark, dump, settings, logger, catalog, s3, input_layer, output_layer
):

    config_bronze = settings.bronze

    dump_type = dump["dump_type"]
    dump_date = dump["dump_date"]

    if dump_type not in config_bronze["PRIMARY_KEYS"]:
        return None

    primary_key = config_bronze["PRIMARY_KEYS"][dump_type]

    latest_dump_date_to_process = dump["dump_date"]

    input_file_s3_path = f"s3a://{storage.bucket}/{dump['file']}"

    logger.info(f"Processing {input_file_s3_path}")
    logger.info(f"{config_bronze = }")

    HEADERS = settings.uc.headers
    UC_URL = settings.uc.url
    output_layer_data_table_name = f"{dump_type}_data"  # output_layer_data_table_name in UC
    metrics_table_name = f"{dump_type}_metrics"  # output_layer_data_table_name in UC

    output_layer_history_deltatable_s3_path = (
        f"s3a://{storage.bucket}/{catalog}/{output_layer}/{metrics_table_name}"
    )
    latest_recorded_dump_date = get_latest_dump_date_s3(
        spark, dump_type, output_layer_history_deltatable_s3_path
    )

    logger.info(f"{latest_recorded_dump_date = }")

    try:
        if latest_recorded_dump_date:
            prev = datetime.datetime.strptime(latest_recorded_dump_date, "%Y%m%d")
            curr = datetime.datetime.strptime(latest_dump_date_to_process, "%Y%m%d")

            if prev >= curr:
                logger.info(
                    "Skipping dump %s (%s) because previous dump %s already processed",
                    dump_type,
                    latest_dump_date_to_process,
                    latest_recorded_dump_date,
                )
                return None

        start = datetime.datetime.now()
        df = read_xml_to_df(spark, dump_type, input_file_s3_path, logger=logger)
        num_partitions = spark.sparkContext.defaultParallelism * 2
        logger.info(
            f"Using {num_partitions} partitions to process {input_file_s3_path}"
        )


        df = clean_nested_columns(df)
        
        
        export_schemas_s3_and_head(
            df,
            catalog,
            f"bronze_{dump_type}_raw",
            logger=logger,
            metadata_dir=settings.metadata_dir,
            n = 500
        )

        
        df = df.select(*config_bronze["COLS_TO_KEEP"][dump_type])
        
        
        export_schemas_s3_and_head(
            df,
            catalog,
            f"bronze_{dump_type}_filtered",
            logger=logger,
            metadata_dir=settings.metadata_dir,
            n = 500
        )

        
        
        df = df.repartition(num_partitions).cache()


        df_hashed = apply_hash(
            df,
            primary_key=primary_key,
            latest_dump_date=latest_dump_date_to_process,
            hash_col="root_hash",
            logger=logger,
        )

        if config_bronze["COLS_TO_HASH"][dump_type]:
            for col in config_bronze["COLS_TO_HASH"][dump_type]:
                df_hashed = apply_hash(
                    df_hashed,
                    primary_key=primary_key,
                    latest_dump_date=latest_dump_date_to_process,
                    hash_col=f"{col}_hash",
                    col_to_hash=col,
                    logger=logger,
                )

        df_mergeable = df_hashed

        output_layer_data_deltatable_s3_path = (
            f"s3a://{settings.project_name}/{catalog}/{output_layer}/{output_layer_data_table_name}"
        )
        uc_table_list = uc_list_tables(
            catalog, output_layer, HEADERS, UC_URL, logger=logger
        )

        logger.info(f"{uc_table_list = }")

        df_mergeable = df_mergeable.repartition(16, primary_key)

        if output_layer_data_table_name in uc_table_list:
            
            
            
            merge_into_uc_delta(
                spark,
                df_mergeable,
                output_layer_data_deltatable_s3_path,
                primary_key,
                hash_col="root_hash",
                logger=logger,
            )

            new_table_flag = False

        else:
            new_table_flag = True

            df_mergeable.limit(0).write.format("delta").option(
                "delta.enableChangeDataFeed", "true"
            ).mode("overwrite").option(
                "path", output_layer_data_deltatable_s3_path
            ).option("delta.autoOptimize.optimizeWrite", "true").option(
                "delta.autoOptimize.autoCompact", "true"
            ).save()

            df_mergeable.write.format("delta").mode("append").save(
                output_layer_data_deltatable_s3_path
            )
            create_new_delta_history(spark)
            
            
            columns = infer_columns_from_delta(
                output_layer_data_deltatable_s3_path, spark
            )

            uc_register_table(
                catalog,
                output_layer,
                output_layer_data_table_name,
                output_layer_data_deltatable_s3_path,
                columns,
                HEADERS,
                UC_URL,
                logger,
            )



        
        input_data_s3_path = f"s3://{storage.bucket}/{catalog}/{input_layer}/data/"
        input_data_archive_s3_path = (
            f"s3://{storage.bucket}/{catalog}/{input_layer}/data_archive/"
        )

        cleanup_old_raw_files_s3(
            s3,
            latest_dump_date_to_process,
            dump_type,
            origin_s3_path=input_data_s3_path,
            destination_s3_path=input_data_archive_s3_path,
            logger=logger,
        )

        duration = (datetime.datetime.now() - start).total_seconds()
        logger.info(
            "dump_processed",
            extra={
                "dump_date": latest_dump_date_to_process,
                "dump_type": dump_type,
                "duration": duration,
            },
        )

        export_delta_table_history_s3(
            spark=spark,
            input_data_table=output_layer_data_deltatable_s3_path,
            output_history_table=output_layer_history_deltatable_s3_path,
            dump_type=dump_type,
            dump_date=latest_dump_date_to_process,
            input_layer=input_layer,
            output_layer=output_layer,
            logger=logger,
            LOG_DIR=settings.log_dir,
            csv_file_name=f"{catalog}_{output_layer}_{output_layer_data_table_name}",
            settings = settings,
            catalog = catalog,
            last_input_version_processed = None
        )

        columns = infer_columns_from_delta(
            output_layer_history_deltatable_s3_path, spark
        )

        uc_register_table(
            catalog,
            output_layer,
            metrics_table_name,
            output_layer_history_deltatable_s3_path,
            columns,
            HEADERS,
            UC_URL,
            logger,
        )

    except Exception:
        logger.exception(
            "Failed processing dump %s-%s",
            latest_dump_date_to_process,
            dump_type,
        )
        raise


def main(spark, settings,  logger, s3, input_layer, output_layer):

    catalog = settings.project_name + "_" + settings.storage.env

    if settings.storage.type == "minio":
        input_s3_folder_path = "/".join([catalog, input_layer, "data"])
        storage_root = f"s3a://{settings.project_name}/{catalog}/"

        bucket = settings.project_name
        catalog_info = uc_create_catalog(
            catalog=catalog,
            HEADERS=settings.uc.headers,
            UC_URL=settings.uc.url,
            logger=logger,
            managed_location=storage_root,
            comment=f"{settings.project_name} catalog",
        )

        uc_create_schemas(
            catalog,
            schema_list=[output_layer],
            HEADERS=settings.uc.headers,
            UC_URL=settings.uc.url,
            logger=logger,
        )

        validate_downloads_s3(
            bucket=bucket,
            s3_folder_path=input_s3_folder_path,
            logger=logger,
            s3=s3,
            dump_types_to_process=settings.raw["DUMP_TYPE_TO_PROCESS"][settings.env],
        )

        input_dump_list = get_s3_folder_contents(
            bucket=bucket, s3_folder_path=input_s3_folder_path, logger=logger, s3=s3
        )

        logger.info(f"{input_dump_list = }")

        latest_input_dumps = get_latest_dump_files_info_s3(input_dump_list, logger)
        logger.info(f"{latest_input_dumps = }")

        results = []
        for dump in latest_input_dumps:
            logger.info(f"{dump = }")
            result = process_single_dump(
                spark,
                dump,
                settings,
                logger,
                catalog,
                s3,
                input_layer,
                output_layer,
            )
            if result:
                results.append(result)


if __name__ == "__main__":
    settings = Settings.load()
    storage = settings.storage
    uc = settings.uc
    catalog = settings.project_name + "_" + settings.storage.env
    input_layer = "raw"
    output_layer = "bronze"

    try:
        s3 = storage.s3

        spark = create_spark_session(
            app_name=f"{settings.project_name}-{output_layer}",
            endpoint=storage.endpoint,
            access_key=storage.access_key,
            secret_key=storage.secret_key,
            uc_uri=uc.url,
            uc_token=uc.token,
            bucket=settings.project_name,
            catalog=catalog,
        )

        setup_logger(
            settings.log_dir
            / f"{settings.project_name}_{settings.env}_{output_layer}_log.json",
            settings.env,
            settings.project_name,
            storage_type=settings.storage.type,
        )

        logger = logging.getLogger(Path(__file__).stem)

        try:
            main(
                spark,
                settings,
                logger=logger,
                s3=s3,
                input_layer=input_layer,
                output_layer=output_layer,
            )
        finally:
            spark.stop()
    except Exception:
        logger.exception(f"Fatal error during {output_layer} job")
        sys.exit(1)
