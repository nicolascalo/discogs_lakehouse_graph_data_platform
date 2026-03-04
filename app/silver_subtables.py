from delta.tables import DeltaTable
from silver_subtables.archive import cleanup_old_raw_files
from silver_subtables.file_discovery import get_latest_dump_files, get_latest_dump_date

from helpers_files.folders import get_output_dir

from logs.logging_config import setup_logger
from helpers_spark.delta_metrics import (
    create_new_delta_history,
    export_delta_table_history,
)
from helpers_spark.schemas import export_schemas
from helpers_spark.spark_session import create_spark_session
from helpers_spark.hash import apply_hash
from helpers_files.validation import validate_file_hash
from helpers_spark.delta_merge import merge_into_delta
from helpers_spark.create_delta import create_new_silver
from helpers_spark.serialization import serialize_for_merge
from silver_subtables.config import SilverConfig

import datetime


def process_single_dump(spark, dump, config, logger):

    dump_type = dump["dump_type"]
    latest_dump_date_to_process = dump["dump_date"]
    file = dump["file"]
    input_file_path = dump["input_file_path"]
    logger.info(
        "Starting processing of dump %s (%s)",
        dump_type,
        latest_dump_date_to_process,
    )
    primary_key = config.primary_keys[dump_type]
    latest_recorded_dump_date = get_latest_dump_date(spark, dump_type)

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

        if not validate_file_hash(
            str(config.raw_data_dir), latest_dump_date_to_process, file, logger=logger
        ):
            logger.warning(
                "Hash validation failed for %s-%s. Skipping.",
                dump_type,
                latest_dump_date_to_process,
            )
            return {
                "dump_type": dump_type,
                "dump_date": latest_dump_date_to_process,
                "status": "hash_failed",
            }

        else:
            start = datetime.datetime.now()
            df = read_xml_to_df(spark, dump_type, input_file_path, logger=logger)


            df_hashed = apply_hash(
                df,
                primary_key=primary_key,
                latest_dump_date=latest_dump_date_to_process,
                hash_col="root_hash",
            )

            if config.cols_to_hash[dump_type]:
                for col in config.cols_to_hash[dump_type]:
                    df_hashed = apply_hash(
                        df_hashed,
                        primary_key=primary_key,
                        latest_dump_date=latest_dump_date_to_process,
                        hash_col=f"{col}_hash",
                        col_to_hash=col,
                    )

            df_mergeable = serialize_for_merge(
                df_hashed, primary_key=primary_key
            )

            output_dir = get_output_dir(
                dump_type, logger=logger, data_folder=config.silver_data_dir
            )

            if not DeltaTable.isDeltaTable(spark, output_dir):
                create_new_silver(df_mergeable, output_dir, logger=logger)
                create_new_delta_history(spark)

            else:
                merge_into_delta(
                    spark, df_mergeable, output_dir, primary_key,hash_col="root_hash", logger=logger
                )

            export_schemas(
                df_mergeable,
                dump_type=dump_type,
                dump_date=latest_dump_date_to_process,
                logger=logger,
                data_dir=config.silver_data_dir,
            )
            export_delta_table_history(
                spark,
                dump_type,
                latest_dump_date_to_process,
                output_dir=output_dir,
                input_layer="raw",
                output_layer="silver",
                logger=logger,
                LOG_DIR=config.log_dir,
                EXPORT_HISTORY_TO_CSV=config.export_history_to_csv,
            )

            cleanup_old_raw_files(
                latest_dump_date_to_process, dump_type, config.raw_data_dir
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


def main(spark, config: SilverConfig, logger):

    raw_dumps = get_latest_dump_files(raw_dir=config.raw_data_dir, logger=logger)

    results = []
    for dump in raw_dumps:
        result = process_single_dump(spark, dump, config, logger)
        if result:
            results.append(result)


if __name__ == "__main__":
    config = SilverConfig.from_env()
    spark = create_spark_session("discogs-silver-subtables")
    logger = setup_logger(config.log_dir / "discogs_silver_subtables.log")
    try:
        main(spark, config, logger)
    finally:
        spark.stop()
