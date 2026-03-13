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

import silver_subtables.artists as silver_artists
import silver_subtables.helpers as silver_helpers

from inspect import getmembers, isfunction
from settings import Settings
import logging
from pathlib import Path

from logs.logging_config import setup_logger
from helpers_spark.schemas import export_schemas_s3_and_head
from helpers_spark.spark_session import create_spark_session
from helpers_spark.hash import apply_hash
from helpers_spark.spark_uc import (
    merge_into_uc_delta,
    uc_create_catalog,
    uc_create_schemas,
    uc_list_tables,
    uc_register_table,
)
from pyspark.sql import functions as F


from helpers_spark.serialization import serialize_for_merge

import sys

import datetime


def process_bronze(
    spark, input_table, settings, logger, catalog, s3, input_layer, output_layer
):

    config_silver = settings.silver

    source_primary_key = config_silver[input_table].get("SOURCE_PRIMARY_KEY", None)

    if not source_primary_key:
        logger.warning(f"Skipping {input_table} because no primary key defined")
        return None

    HEADERS = settings.uc.headers
    UC_URL = settings.uc.url

    input_layer_data_table_name = (
        f"{input_table}_data"  # output_layer_data_table_name in UC
    )
    metrics_table_name_input = (
        f"{input_table}_metrics"  # output_layer_data_table_name in UC
    )

    input_layer_history_deltatable_s3_path = f"s3a://{settings.project_name}/{catalog}/{input_layer}/{metrics_table_name_input}"

    metrics_table_name_output = (
        f"{input_table}_metrics"  # output_layer_data_table_name in UC
    )
    output_layer_history_deltatable_s3_path = f"s3a://{settings.project_name}/{catalog}/{output_layer}/{metrics_table_name_output}"

    input_layer_data_deltatable_s3_path = f"s3a://{settings.project_name}/{catalog}/{input_layer}/{input_layer_data_table_name}"

    logger.info(
        f"Processing {input_layer_data_deltatable_s3_path} towards silver tables"
    )

    logger.info(f"{input_layer_history_deltatable_s3_path = }")

    latest_recorded_table_date_input = (
        spark.read.format("delta")
        .load(input_layer_history_deltatable_s3_path)
        .orderBy(F.col("dump").desc())
        .limit(1)
        .select("dump")
        .collect()
    )[0]["dump"]

    logger.info(f"{latest_recorded_table_date_input = }")

    if (
        "last_input_version_processed"
        in spark.read.format("delta")
        .load(input_layer_history_deltatable_s3_path)
        .columns
    ):
        last_version = (
            spark.read.format("delta")
            .load(input_layer_history_deltatable_s3_path)
            .agg(F.max("last_input_version_processed"))
            .collect()[0][0]
        )
    else:
        last_version = 0

    logger.info(f"{last_version = }")

    df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", last_version + 1)
        .load(input_layer_data_deltatable_s3_path)
    )

    num_partitions = spark.sparkContext.defaultParallelism * 2
    logger.info(
        f"Using {num_partitions} partitions to process {input_layer_data_deltatable_s3_path}"
    )

    logger.info(f"{config_silver = }")

    uc_table_list = uc_list_tables(
        catalog, output_layer, HEADERS, UC_URL, logger=logger
    )

    logger.info(f"{uc_table_list = }")




    for explode_schema in config_silver[input_table].get("EXPLODE_SCHEMAS", None) :
        
    
        df = df.withColumn(f'{explode_schema["field"]}_struct', F.from_json(explode_schema["field"], explode_schema["schema"]))
        
        df_exploded = silver_helpers.explode_field(df, explode_schema["field"]).cache()
        
        
    df.count()
    
    
    
    
    last_input_version_processed = df.agg(F.max("_commit_version")).collect()[0][0]

    for artist_function in getmembers(silver_artists, isfunction):
        function_name = artist_function[0]
        function_object = artist_function[1]

        logger.info(f"{function_name = }")

        if "create_" not in function_name:
            logger.info("Not a function to create tables: Skipping")
            continue

        dependencies = config_silver[input_table]["SUBTABLES"][function_name].get(
            "TABLE_DEPENDENCIES", []
        )

        logger.info(f"{dependencies = }")

        if "groups" in dependencies:
            hash_col = "groups_hash"

            df_local = df_exploded
        else:
            hash_col = None
            df_local = df

        output_layer_data_table_name = (
            function_name.replace("create_", "").replace("_table", "") + "_data"
        )
        logger.info(f"{output_layer_data_table_name = }")

        output_layer_metrics_table_name = (
            function_name.replace("create_", "").replace("_table", "") + "_metrics"
        )
        logger.info(f"{output_layer_metrics_table_name = }")

        if output_layer_metrics_table_name in uc_table_list:
            latest_recorded_table_date_output = (
                spark.read.format("delta")
                .load(output_layer_history_deltatable_s3_path)
                .orderBy(F.col("dump").desc())
                .limit(1)
                .select("dump")
                .collect()
            )[0]["dump"]
        else:
            latest_recorded_table_date_output = None

        logger.info(f"{latest_recorded_table_date_output = }")

        try:
            if latest_recorded_table_date_output:
                prev = datetime.datetime.strptime(
                    latest_recorded_table_date_output, "%Y%m%d"
                )
                curr = datetime.datetime.strptime(
                    latest_recorded_table_date_input, "%Y%m%d"
                )

                if prev >= curr:
                    logger.info(
                        "Skipping table %s (%s) because previous table %s already processed",
                        output_layer_data_table_name,
                        latest_recorded_table_date_output,
                        latest_recorded_table_date_input,
                    )
                    continue

            start = datetime.datetime.now()

            df_mergeable = function_object(df_local)

            output_layer_data_deltatable_s3_path = f"s3a://{settings.project_name}/{catalog}/{output_layer}/{output_layer_data_table_name}"

            logger.info(f"{output_layer_data_deltatable_s3_path = }")

            target_secondary_key = config_silver[input_table]["SUBTABLES"][
                function_name
            ].get("TARGET_SECONDARY_KEY", None)
            target_primary_key = config_silver[input_table]["SUBTABLES"][
                function_name
            ].get("TARGET_PRIMARY_KEY", None)
            logger.info(f"{target_primary_key = }")
            logger.info(f"{target_secondary_key = }")
            logger.info(f"{df_mergeable.columns = }")

            df_mergeable = df_mergeable.repartition(
                spark.sparkContext.defaultParallelism, target_primary_key
            )

            if output_layer_data_table_name in uc_table_list:
                logger.info(f"merge_into_uc_delta")

                merge_into_uc_delta(
                    spark,
                    df_mergeable,
                    output_layer_data_deltatable_s3_path,
                    target_primary_key,
                    hash_col=hash_col,
                    logger=logger,
                    secondary_key=target_secondary_key,
                )

                new_table_flag = False

            else:
                logger.info(
                    f"Creating new delta table {output_layer_data_deltatable_s3_path}"
                )

                new_table_flag = True

                cdf_cols = ["_change_type", "_commit_version", "_commit_timestamp"]

                df_mergeable = df_mergeable.drop(*cdf_cols)

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

            table_description = f"{output_layer}_{output_layer_data_table_name}"

            export_schemas_s3_and_head(
                df_mergeable,
                catalog,
                table_description,
                logger=logger,
                metadata_dir=settings.metadata_dir,
            )

            duration = (datetime.datetime.now() - start).total_seconds()
            logger.info(
                "table_processed",
                extra={
                    "table_date": latest_recorded_table_date_input,
                    "table": output_layer_data_table_name,
                    "duration": duration,
                },
            )

            export_delta_table_history_s3(
                spark=spark,
                input_data_table=output_layer_data_deltatable_s3_path,
                output_history_table=output_layer_history_deltatable_s3_path,
                input_layer=input_layer,
                output_layer=output_layer,
                logger=logger,
                LOG_DIR=settings.log_dir,
                csv_file_name=f"{catalog}_{output_layer}_{output_layer_data_table_name}",
                settings=settings,
                catalog=catalog,
                dump_type=output_layer_data_table_name,
                dump_date=latest_recorded_table_date_input,
                last_input_version_processed=last_input_version_processed,
            )

            columns = infer_columns_from_delta(
                output_layer_history_deltatable_s3_path, spark
            )

            uc_register_table(
                catalog,
                output_layer,
                output_layer_metrics_table_name,
                output_layer_history_deltatable_s3_path,
                columns,
                HEADERS,
                UC_URL,
                logger,
            )
            
        
            
            
        except Exception:
            logger.exception(
                "Failed processing table %s-%s",
                latest_recorded_table_date_input,
                output_layer_data_table_name,
            )
            raise


    logger.info(
        "silver_artists processed succesfully"
    )
            


def main(spark, settings, logger, s3, input_layer, output_layer):

    catalog = settings.project_name + "_" + settings.storage.env

    if settings.storage.type == "minio":
        uc_create_schemas(
            catalog,
            schema_list=[output_layer],
            HEADERS=settings.uc.headers,
            UC_URL=settings.uc.url,
            logger=logger,
        )

    process_bronze(
        spark,
        "artists",
        settings,
        logger,
        catalog,
        s3,
        input_layer,
        output_layer,
    )


if __name__ == "__main__":
    settings = Settings.load()
    storage = settings.storage
    uc = settings.uc
    catalog = settings.project_name + "_" + settings.storage.env
    input_layer = "bronze"
    output_layer = "silver"

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
