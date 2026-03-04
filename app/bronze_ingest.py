from bronze_ingest.archive import cleanup_old_raw_files_s3
from bronze_ingest.file_discovery import (
    get_s3_folder_contents,
    get_latest_dump_date_s3,
    get_latest_dump_files_info_s3,
)
from unitycatalog.uc import uc_create_catalog, uc_list_tables, uc_create_schemas
from download_raw.validate import validate_downloads_s3


from settings import Settings
import logging
from pathlib import Path


from logs.logging_config import setup_logger
from helpers_spark.delta_metrics import (
    export_delta_table_history_s3,
    create_new_delta_history,
)
from helpers_spark.schemas import export_schemas_s3
from helpers_spark.spark_session import create_spark_session
from bronze_ingest.xml import read_xml_to_df
from helpers_spark.hash import apply_hash
from helpers_spark.delta_tables import (
    merge_into_delta,
    create_new_delta_table_s3_overwrite,
    infer_columns_from_delta,
)
from helpers_spark.serialization import serialize_for_merge
from unitycatalog.uc import uc_register_table

import sys

import datetime


def process_single_dump(spark, dump, settings, logger, catalog, s3, schema="raw"):

    config_bronze = settings.bronze

    dump_type = dump["dump_type"]

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

        def sanitize_columns(df):
            for c in df.columns:
                df = df.withColumnRenamed(c, c.replace("@", "_"))
            return df

        df = sanitize_columns(df)

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

        output_dir = f"s3a://{storage.bucket}/{catalog}/bronze"
        output_table_s3_path = output_dir + "/" + data_table_name

        uc_table_list = uc_list_tables(catalog, schema, HEADERS, UC_URL, logger=logger)

        logger.info(f"{uc_table_list = }")

        if data_table_name not in uc_table_list:
            uc_create_schemas(
                catalog,
                schema_list=[schema],
                HEADERS=HEADERS,
                UC_URL=UC_URL,
                logger=logger,
            )

            create_new_delta_table_s3_overwrite(
                df_mergeable,
                output_table_s3_path,
                logger=logger,
            )

            create_new_delta_history(spark)

        else:
            merge_into_delta(
                spark,
                df_mergeable,
                output_table_s3_path,
                primary_key,
                hash_col="root_hash",
                logger=logger,
            )


        columns = infer_columns_from_delta(output_table_s3_path, spark)

        uc_register_table(
            catalog,
            schema,
            data_table_name,
            output_dir,
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

        input_data_table = f"s3a://{storage.bucket}/{catalog}/bronze/{data_table_name}"

        uc_create_schemas(
            catalog, schema_list=[schema], HEADERS=HEADERS, UC_URL=UC_URL, logger=logger
        )

        export_delta_table_history_s3(
            spark=spark,
            input_data_table=input_data_table,
            output_history_table=output_history_table,
            dump_type=dump_type,
            dump_date=latest_dump_date_to_process,
            input_layer="raw",
            output_layer="bronze",
            logger=logger,
            LOG_DIR=settings.log_dir,
        )

        columns = infer_columns_from_delta(output_history_table, spark)

        uc_register_table(
            catalog,
            schema,
            metrics_table_name,
            output_dir,
            columns,
            HEADERS,
            UC_URL,
            logger,
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

        return {
            "dump_type": dump_type,
            "dump_date": latest_dump_date_to_process,
            "status": "processed",
            "duration": duration,
        }

    except Exception:
        logger.exception(
            "Failed processing dump %s-%s",
            latest_dump_date_to_process,
            dump_type,
        )
        raise


def main(spark, settings, bucket: str, logger, s3):
    HEADERS = settings.uc.headers
    UC_URL = settings.uc.url

    catalog = settings.project_name + "_" + settings.storage.env
    schema = "raw"

    uc_create_catalog(catalog, HEADERS=HEADERS, UC_URL=UC_URL, logger=logger)

    uc_create_schemas(
        catalog, schema_list=["raw"], HEADERS=HEADERS, UC_URL=UC_URL, logger=logger
    )

    if settings.storage.type == "minio":
        raw_s3_folder_path = "/".join([catalog, schema, "data"])

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

    try:
        s3 = storage.s3

        spark = create_spark_session(
            app_name="discogs-bronze-ingest",
            endpoint=storage.endpoint,
            access_key=storage.access_key,
            secret_key=storage.secret_key,
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
