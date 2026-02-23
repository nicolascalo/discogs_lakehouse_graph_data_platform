import os
import datetime
import logging
import pandas as pd
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
import pandas as pd

from ingest_helpers.folder_helpers import get_folder_list, get_dump_dates
from ingest_helpers.config_helpers import load_config_from_json
from ingest_helpers.info_output_helpers import (
    get_or_create_last_log,
    get_last_dump_date,
    output_single_json,
    output_schema_json,
    output_schema_txt,
    output_schema_csv,
)

# from ingest_helpers.spark_processing_helpers2 import build_tree, normalize_tree, sort_tree, select_tree,  explode_nested, split_nested

# from ingest_helpers.spark_processing_helpers2_test import apply_enrich

from ingest_helpers.spark_processing_helpers3 import *


# -----------------------------
# Logging setup
# -----------------------------
LOG_DIR = Path(os.getenv("LOG_DIR", "/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "discogs_silver_ingest.log"

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE)],
    )

logger.info(f"Logging to {LOG_FILE}")

# -----------------------------
# Environment / config
# -----------------------------
DATA_DIR = os.getenv("DATA_DIR")
BRONZE_DATA_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_INGEST_CONFIG_PATH = os.getenv("SILVER_INGEST_CONFIG_PATH")

OUTPUT_DEBUG_FILES = True

# -----------------------------
# Spark session
# -----------------------------
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
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


# -----------------------------
# Listing folders & dumps
# -----------------------------
bronze_folders = get_folder_list(BRONZE_DATA_DIR)
dump_dates = get_dump_dates(bronze_folders)

print("")
logger.info(f"{dump_dates = }")
print("")
n = os.getenv("NB_DUMPS_TO_PROCESS", 1)
n_latest_dumps_dates = sorted(dump_dates[-n - 1 :])

print("")

logger.info(f"Processing {n} latest dumps: {n_latest_dumps_dates}")
print("")


# Load configs
table_configs = load_config_from_json(SILVER_INGEST_CONFIG_PATH)


# -----------------------------
# Start ingestion
# -----------------------------
previous_input_path = None

for dump_date in n_latest_dumps_dates:
    for cfg in table_configs:
        logger.info(f"")
        logger.info(f"")
        logger.info(
            f"================================================================================================="
        )
        logger.info(f"Processing dump date {dump_date} with config {cfg}")
        logger.info(
            f"================================================================================================="
        )
        print("")

        time_start = datetime.datetime.now()

        layer_input = cfg["layer_input"]
        layer_output = cfg["layer_output"]
        table_input = cfg["table_input"]
        table_output = cfg["table_output"]
        hash = cfg.get("hash", None)
        homogeneize = cfg.get("homogeneize", None)
        enrich = cfg.get("enrich", None)
        explode = cfg.get("explode", None)

        output_path = os.path.join(DATA_DIR, layer_output, table_output)
        input_path = os.path.join(DATA_DIR, layer_input, table_input)

        # if int(dump_date) <= last_processed_dump:
        #    logger.info(f"{dump_date} <= {last_processed_dump}: skipping")
        #    continue

        delta_table_path = os.path.join(
            DATA_DIR, layer_input, table_input, f"dump={dump_date}"
        )
        print("")
        logger.info(f"Loading {layer_input} table: {delta_table_path}")
        print("")
        df = spark.read.format("delta").load(delta_table_path)

        # Select all columns from DataFrame
        df_selected = df.select(*cfg["keep"])

        # Compute root hash

        df_hashed = df_selected.withColumn(
            "root_hash", F.sha2(F.to_json(F.struct(*df.columns)), 256)
        )

        if OUTPUT_DEBUG_FILES:
            output_single_json(df_hashed, f"{output_path}_{dump_date}_root_hash")


        df_hashed = cast_non_key_columns_to_string(
            df_hashed, primary_key=cfg["primary_key"], hash_col="root_hash"
        )

        if not DeltaTable.isDeltaTable(spark, output_path):
            df_hashed.write.format("delta").mode("overwrite").save(output_path)


        else:
            target = DeltaTable.forPath(spark, output_path)

            (
                target.alias("t")
                .merge(
                    df_hashed.alias("s"),
                    f"t.`{cfg['primary_key']}` = s.`{cfg['primary_key']}`",
                )
                .whenMatchedUpdateAll(condition="t.root_hash <> s.root_hash")
                .whenNotMatchedInsertAll()
                .whenNotMatchedBySourceDelete()
                .execute()
            )

