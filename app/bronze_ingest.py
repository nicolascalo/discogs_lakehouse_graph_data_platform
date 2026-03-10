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
from helpers_spark.schemas import export_schemas_s3
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


from helpers_spark.serialization import serialize_for_merge

import sys

import datetime


def process_single_dump(spark, dump, settings, logger, catalog, s3, schema="raw"):

    config_bronze = settings.bronze

    dump_type = dump["dump_type"]
    dump_date = dump["dump_date"]

    if dump_type not in config_bronze["PRIMARY_KEYS"]:
        return None

    primary_key = config_bronze["PRIMARY_KEYS"][dump_type]

    latest_dump_date_to_process = dump["dump_date"]

    input_file_path = f"s3a://{storage.bucket}/{dump['file']}"

    logger.info(f"Processing {input_file_path}")
    logger.info(f"{config_bronze = }")

    HEADERS = settings.uc.headers
    UC_URL = settings.uc.url
    metrics_schema = "metrics"
    data_table_name = f"{dump_type}_data"  # data_table_name in UC
    metrics_table_name = f"{dump_type}_metrics"  # data_table_name in UC

    output_history_table = (
        f"s3a://{storage.bucket}/{catalog}/bronze/{metrics_table_name}"
    )
    latest_recorded_dump_date = get_latest_dump_date_s3(
        spark, dump_type, output_history_table
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
        df = read_xml_to_df(spark, dump_type, input_file_path, logger=logger)
        num_partitions = spark.sparkContext.defaultParallelism * 2
        logger.info(f"Using {num_partitions} partitions to process {input_file_path}")

        df = df.repartition(num_partitions).cache()

        for c in df.columns:
            df = df.withColumnRenamed(c, c.replace("@", "_"))

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

        df_mergeable = serialize_for_merge(df_hashed, primary_key=primary_key)

        schema_data_table_name = f"{schema}.{data_table_name}"
        full_table_output_path = (
            f"s3a://{settings.project_name}/{catalog}/bronze/{data_table_name}"
        )
        uc_table_list = uc_list_tables(catalog, schema, HEADERS, UC_URL, logger=logger)

        logger.info(f"{uc_table_list = }")

        df_mergeable = df_mergeable.repartition(16, primary_key)

        if data_table_name in uc_table_list:
            merge_into_uc_delta(
                spark,
                df_mergeable,
                full_table_output_path,
                primary_key,
                hash_col="root_hash",
                logger=logger,
            )

            new_table_flag = False

        else:
            new_table_flag = True

            df_mergeable.limit(0).write.format("delta").option(
                "delta.enableChangeDataFeed", "true"
            ).mode("overwrite").option("path", full_table_output_path).option(
                "delta.autoOptimize.optimizeWrite", "true"
            ).option("delta.autoOptimize.autoCompact", "true").save()

            df_mergeable.write.format("delta").mode("append").save(
                full_table_output_path
            )
            create_new_delta_history(spark)
            columns = infer_columns_from_delta(full_table_output_path, spark)

            uc_register_table(
                catalog,
                schema,
                data_table_name,
                full_table_output_path,
                columns,
                HEADERS,
                UC_URL,
                logger,
            )

        export_schemas_s3(
            df_mergeable,
            catalog,
            dump,
            logger=logger,
            metadata_dir=settings.metadata_dir,
        )

        raw_data_s3_path = f"s3://{storage.bucket}/{catalog}/raw/data/"
        raw_data_archive_s3_path = f"s3://{storage.bucket}/{catalog}/raw/data_archive/"

        cleanup_old_raw_files_s3(
            s3,
            latest_dump_date_to_process,
            dump_type,
            origin_s3_path=raw_data_s3_path,
            destination_s3_path=raw_data_archive_s3_path,
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
            input_data_table=full_table_output_path,
            output_history_table=output_history_table,
            dump_type=dump_type,
            dump_date=latest_dump_date_to_process,
            input_layer="raw",
            output_layer="bronze",
            logger=logger,
            LOG_DIR=settings.log_dir,
            csv_file_name = f"{catalog}_bronze_{data_table_name}"
        )

    except Exception:
        logger.exception(
            "Failed processing dump %s-%s",
            latest_dump_date_to_process,
            dump_type,
        )
        raise


def main(spark, settings, bucket: str, logger, s3):

    catalog = settings.project_name + "_" + settings.storage.env
    schema = "raw"

    if settings.storage.type == "minio":
        raw_s3_folder_path = "/".join([catalog, schema, "data"])
        storage_root = f"s3a://{settings.project_name}/{catalog}/"

        catalog_info = uc_create_catalog(
            catalog=catalog,
            HEADERS=settings.uc.headers,
            UC_URL=settings.uc.url,
            logger=logger,
            managed_location=storage_root,
            comment="Discogs test catalog",
        )

        uc_create_schemas(
            catalog,
            schema_list=["raw"],
            HEADERS=settings.uc.headers,
            UC_URL=settings.uc.url,
            logger=logger,
        )

        validate_downloads_s3(
            bucket=settings.project_name,
            s3_folder_path=raw_s3_folder_path,
            logger=logger,
            s3=s3,
            dump_types_to_process=settings.raw["DUMP_TYPE_TO_PROCESS"][settings.env],
        )

        raw_dump_list = get_s3_folder_contents(
            bucket=bucket, s3_folder_path=raw_s3_folder_path, logger=logger, s3=s3
        )

        logger.info(f"{raw_dump_list = }")

        latest_raw_dumps = get_latest_dump_files_info_s3(raw_dump_list, logger)
        logger.info(f"{latest_raw_dumps = }")

        results = []
        for dump in latest_raw_dumps:
            logger.info(f"{dump = }")
            result = process_single_dump(spark, dump, settings, logger, catalog, s3)
            if result:
                results.append(result)


if __name__ == "__main__":
    settings = Settings.load()
    storage = settings.storage
    uc = settings.uc
    catalog = settings.project_name + "_" + settings.storage.env

    try:
        s3 = storage.s3

        spark = create_spark_session(
            app_name="discogs-bronze-ingest",
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
            / f"{settings.project_name}_{settings.env}_bronze_ingest_log.json",
            settings.env,
            settings.project_name,
            storage_type=settings.storage.type,
        )

        logger = logging.getLogger(Path(__file__).stem)

        try:
            main(
                spark,
                settings,
                bucket=settings.project_name,
                logger=logger,
                s3=s3,
            )
        finally:
            spark.stop()
    except Exception:
        logger.exception("Fatal error during Bronze ingest job")
        sys.exit(1)
