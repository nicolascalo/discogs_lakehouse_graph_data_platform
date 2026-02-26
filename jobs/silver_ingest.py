import os
import datetime
import logging
import pandas as pd
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F
import pandas as pd

from ingest_helpers.folder_helpers import get_folder_list, get_dump_dates
from helpers_ingest.config_helpers import load_config_from_json
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
n = int(os.getenv("NB_DUMPS_TO_PROCESS", 1))
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
        logger.info(f"")
        
        df = spark.read.format("delta").load(delta_table_path)

        # Select all columns from DataFrame
        logger.info(f'Keeping columns {cfg["keep"]}')
        
        df_selected = df.select(*cfg["keep"])

        # Compute root hash

        logger.info(f'Hashing root column')

        df_hashed = df_selected.withColumn(
            "root_hash", F.sha2(F.to_json(F.struct(*df_selected.columns)), 256)
        )




        if OUTPUT_DEBUG_FILES:
            output_single_json(df_hashed, f"{output_path}_{dump_date}_root_hash")

        logger.info(f'Converting all to string')


        def cast_non_key_columns_to_string(df, primary_key, hash_col):
            cols = []

            for c in df.columns:
                if c in [primary_key, hash_col]:
                    cols.append(F.col(c))  # keep original type
                else:
                    cols.append(F.col(c).cast("string").alias(c))

            return df.select(*cols)

        df_hashed = cast_non_key_columns_to_string(
            df_hashed, primary_key=cfg["primary_key"], hash_col="root_hash"
        )


        logger.info(f'Writing Delta Table')

        if not DeltaTable.isDeltaTable(spark, output_path):
            df_hashed.write.format("delta").mode("overwrite").save(output_path)

            from pyspark.sql import Row

            history_df = spark.createDataFrame(
                [
                    Row(
                        version=0,
                        timestamp=str(
                            datetime.datetime.now()
                        ),  # cast to string explicitly
                        operation="WRITE",
                        operationMetrics={
                            "numTargetRowsInserted": df_hashed.count(),
                            "numTargetRowsUpdated": 0,
                            "numTargetRowsDeleted": 0,
                            "numTargetRowsNotMatchedBySourceDeleted": 0,
                        },
                    )
                ]
            )

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




        csv_path = f"{LOG_DIR}/discogs_{layer_output}_{table_output}_delta_history.csv"

        logger.info(f"Outputing metrics: {csv_path}")

        # 1️⃣ Read Delta history
        history = spark.sql(f"DESCRIBE HISTORY delta.`{output_path}`")

        # 2️⃣ Add metadata columns
        history = (
            history.withColumn("dump", F.lit(dump_date))
            .withColumn("layer_output", F.lit(layer_output))
            .withColumn("table_output", F.lit(table_output))
            .withColumn("layer_input", F.lit(layer_input))
            .withColumn("table_input", F.lit(table_input))
            .withColumn("cols_kept", F.lit(cfg["keep"]))
        )

        # 3️⃣ Flatten operationMetrics dynamically
        metric_keys = (
            history.select(F.explode("operationMetrics").alias("metric", "value"))
            .select("metric")
            .distinct()
            .rdd.flatMap(lambda r: r)
            .collect()
        )

        select_cols = [
            "version",
            "timestamp",
            "operation",
            "dump",
            "layer_output",
            "table_output",
            "layer_input",
            "table_input",
            "cols_kept",
        ] + [F.col("operationMetrics")[k].alias(k) for k in metric_keys]

        history_flat = history.select(*select_cols)

        # 4️⃣ Convert to Pandas
        pdf = history_flat.toPandas()

        # 5️⃣ Append to existing CSV if exists
        csv_file_path = csv_path
        if os.path.exists(csv_file_path):
            pdf_existing = pd.read_csv(csv_file_path)
            pdf = pd.concat([pdf_existing, pdf], ignore_index=True)

        # 6️⃣ Export to single CSV
        pdf.to_csv(csv_file_path, index=False)
        print(f"Delta history exported to {csv_file_path} with metrics: {metric_keys}")
