from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os
import re
import gzip
import tempfile
import shutil
import struct
import json
import time
import glob
import datetime
import pandas as pd

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F

from ingest_helpers.spark_helpers import   select_fields_with_alias,  output_single_json, wrap_all_structs, apply_explode,apply_post_split_explode,enforce_required, with_row_hash
from ingest_helpers.folder_helpers import  get_folder_list, get_dump_dates
from ingest_helpers.config_helpers import load_config_from_json

## Logging setup


LOG_DIR = Path(os.getenv("LOG_DIR", "/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "discogs_silver_ingest.log"

# Remove any existing handlers first
root_logger = logging.getLogger()
for h in root_logger.handlers[:]:
    root_logger.removeHandler(h)

# File handler
delta_table_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50 * 1024 * 1024,  # 50 MB
    backupCount=5,
)
delta_table_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
)


# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
)

# Add handlers
root_logger.setLevel(logging.INFO)
root_logger.addHandler(delta_table_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)
logger.info(f"Logging to {LOG_FILE}")
## Env and config variables

DATA_DIR = os.getenv("DATA_DIR")
BRONZE_DATA_DIR = os.getenv("BRONZE_DATA_DIR")
SILVER_DATA_DIR = os.getenv("SILVER_DATA_DIR")
SILVER_GENERAL_CONFIG_PATH = os.getenv("SILVER_GENERAL_CONFIG_PATH")
SILVER_TRANSFORM_BASE_CONFIG_PATH = os.getenv("SILVER_TRANSFORM_BASE_CONFIG_PATH")
SILVER_TRANSFORM_SUB_CONFIG_PATH = os.getenv("SILVER_TRANSFORM_SUB_CONFIG_PATH")



if not BRONZE_DATA_DIR or not SILVER_DATA_DIR or not SILVER_GENERAL_CONFIG_PATH:
    raise ValueError(
        "BRONZE_DATA_DIR and SILVER_DATA_DIR and SILVER_CONFIG_PATH must be set"
    )


CONFIG = load_config_from_json(SILVER_GENERAL_CONFIG_PATH)


logger.info(f"{BRONZE_DATA_DIR = }")
logger.info(f"{SILVER_DATA_DIR = }")

## Spark session

spark = (
    SparkSession.builder.appName("discogs-silver-ingest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


## Listing delta_tables to process




bronze_folders = get_folder_list(BRONZE_DATA_DIR)
silver_folders = get_folder_list(SILVER_DATA_DIR)


dump_dates = get_dump_dates(bronze_folders)
logger.info(f"{dump_dates = }")

n = CONFIG["NB_DUMPS_TO_PROCESS"]
n_latest_dumps_dates = dump_dates[-n - 1 :]

n_latest_dumps_dates.sort()
logger.info(f"Nb of most recent dumps to process = {n}")
logger.info(f"{n_latest_dumps_dates = }")


logger.info(f"{SILVER_TRANSFORM_BASE_CONFIG_PATH = }")
logger.info(f"{SILVER_TRANSFORM_SUB_CONFIG_PATH = }")

table_configs_base = load_config_from_json(SILVER_TRANSFORM_BASE_CONFIG_PATH)

table_configs_sub = load_config_from_json(SILVER_TRANSFORM_SUB_CONFIG_PATH)


cols_to_hash = set([f["source"] for table in table_configs_base for f in table["fields"]])

cols_to_hash.discard('_row_hash') 


print(cols_to_hash)


for dump_date in n_latest_dumps_dates:
    
    previous_input_table = None

    path_merge_log = os.path.join(DATA_DIR, "silver", "silver_merge_log.csv")


    

    for cfg in table_configs_base:

        time_start = datetime.datetime.now()
        
        input_table = cfg["table_input"]
        output_table = cfg["table_output"]
        fields = cfg["fields"]
        layer_input = cfg["layer_input"]
        layer_output = cfg["layer_output"]
        primary_key = cfg["primary_key"]


        output_path = os.path.join(DATA_DIR, layer_output, output_table)
        input_path = os.path.join(DATA_DIR, layer_input, input_table)

        try:
            last_merge_log = pd.read_csv(path_merge_log)
        except:
            last_merge_log = pd.DataFrame(
                columns=["timestamp", "input_path", "output_path", "dump","duration_min"]
            )



        try:
            last_processed_dump = int(last_merge_log[last_merge_log['path'] == output_path]['dump'].max())
        except:
            last_processed_dump = 0


        if int(dump_date) <= last_processed_dump:
            continue



        delta_table = [
            delta_table
            for delta_table in bronze_folders
            if (dump_date in delta_table) and ("/_" not in delta_table)
        ]
        delta_table = os.path.join(
            BRONZE_DATA_DIR, input_table, f"dump={dump_date}"
        )

        df_bronze = spark.read.format("delta").load(delta_table)

        df_bronze = with_row_hash(df_bronze, cols_to_hash)

        


        logger.info(f"{delta_table}  -  select_fields_with_alias")

        df_bronze = wrap_all_structs(df_bronze)
        df_bronze = select_fields_with_alias(df_bronze, cfg["fields"])

        logger.info(f"{delta_table}  -  Exporting Deltatable json to {output_path}")




        if DeltaTable.isDeltaTable(spark, output_path):

            delta_table = DeltaTable.forPath(spark, output_path)


            count_before = delta_table.toDF().count()

            # Count updated rows
            current_df = delta_table.toDF()

            updated = (
                current_df.alias("t")
                .join(df_bronze.alias("s"), on=primary_key, how="inner")
                .filter(F.col("t._row_hash") != F.col("s._row_hash"))
                .count()
            )

            not_modified = current_df.count() - updated

            delta_table.alias("t") \
                .merge(df_bronze.alias("s"), f"t.`{primary_key}` = s.`{primary_key}`") \
                .whenMatchedUpdateAll(condition="t._row_hash <> s._row_hash") \
                .whenNotMatchedInsertAll() \
                .execute()


            count_after = delta_table.toDF().count()
            inserted = max(count_after - count_before, 0)

        else:
            df_bronze.write.format("delta").mode("overwrite").save(output_path)
            delta_table = DeltaTable.forPath(spark, output_path)
            not_modified = 0
            inserted = delta_table.toDF().count()
            updated = 0
            count_before = 0


        time_stop = datetime.datetime.now()
        duration_sec = (time_stop - time_start).total_seconds()
        duration_min = round(duration_sec/60,2)


        silver_merge_log = pd.DataFrame(
            {
                "timestamp": [datetime.datetime.now().isoformat()],
                "input_path" : [input_path],
                "output_path": [output_path],
                "dump": [dump_date],
                "previous_total": count_before,
                "updated": updated,
                "not_modified": not_modified,
                "inserted": inserted,
                "total": delta_table.toDF().count(),
                "duration_min":duration_min
            }
        )

        silver_merge_log = pd.concat([last_merge_log, silver_merge_log], ignore_index=True)

        silver_merge_log.sort_values(by=["input_path", "output_path","dump"]).to_csv(path_merge_log, index=False)

        schema = df_bronze.schema
        

        with open(f"{output_path}_schema.json", "w") as f:
            f.write(schema.json())

        with open(f"{output_path}_schema.txt", "w") as f:
            f.write(schema.simpleString())


        fields = [(f.name, f.dataType.simpleString(), f.nullable) for f in df_bronze.schema.fields]
        schema_df = pd.DataFrame(fields, columns=["column_name", "type", "nullable"])
        schema_df.to_csv(f"{output_path}_schema.csv", index=False)

        output_single_json(df_bronze, output_path)

        output_single_json(df_bronze, f"{output_path}_{dump_date}")

    continue


##########################################
# Subtables
##########################################

    for cfg in table_configs_sub:
        input_table = cfg["table_input"]
        output_table = cfg["table_output"]
        fields = cfg["fields"]
        layer_input = cfg["layer_input"]
        layer_output = cfg["layer_output"]

        logger.info(f"{input_table =}")
        logger.info(f"{output_table =}")
        logger.info(f"{layer_input =}")
        logger.info(f"{layer_output =}")


        output_path = os.path.join(DATA_DIR, layer_output, output_table)


        try:
            last_processed_dump = int(last_merge_log[last_merge_log['path'] == output_path]['dump'].max())

        except:
            last_processed_dump = 0

        if int(dump_date) <= last_processed_dump:
            continue


        if previous_input_table != input_table:
            logger.info(f"======================    Loading new table {input_table}")

            delta_table = os.path.join(DATA_DIR, layer_input, input_table)

            previous_input_table = input_table

        logger.info(f"{previous_input_table =}")
        logger.info(f"{delta_table =}")






        logger.info(
            f"{delta_table}  -  Starting processing"
        )

        logger.info(f"Reading delta table {delta_table}")
        df = spark.read.format("delta").load(delta_table)

        # 1. explode first
        silver_df = df



        if "explode" in cfg:
            logger.info(f"{delta_table}  -  Exploding silver_df")
            silver_df = apply_explode(silver_df, cfg["explode"])

        if "post_split_explode" in cfg:
            logger.info(f"{delta_table}  -  post_split_explode  silver_df")
            silver_df = apply_post_split_explode(silver_df, cfg["post_split_explode"])


        # 2. select + alias
        logger.info(f"{delta_table}  -  select_fields_with_alias  silver_df")
        silver_df = select_fields_with_alias(silver_df, cfg["fields"])

        # 3. enforce required fields
        logger.info(f"{delta_table}  -  enforce_required  silver_df")
        silver_df = enforce_required(silver_df, cfg["fields"])

        logger.info(
            f"{delta_table}  -  Exporting Deltatable to {output_path}"
        )

        # 4. write



        (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("userMetadata", f"dump={dump_date}") 
        .save(
            f"{output_path}"        )

        )
        logger.info(
            f"{delta_table}  -  Exporting Deltatable json to {output_path}"
        )
        

        output_single_json(silver_df, output_path)

        logger.info(f"")

        continue

