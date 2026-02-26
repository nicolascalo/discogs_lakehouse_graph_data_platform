import os
import datetime
import logging
import pandas as pd
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

from ingest_helpers.spark_processing_helpers import (
    select_fields_with_alias,
    wrap_all_structs,
    apply_explode,
    apply_post_split_explode,
    enforce_required,
    with_row_hash,
    spark_upsert_based_on_hashes_with_stats,
    spark_create_df_with_stats,
    spark_replace_by_hash,
    extract_required_roots,
)
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
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_FILE)
        ]
    )

logger.info(f"Logging to {LOG_FILE}")

# -----------------------------
# Environment / config
# -----------------------------
DATA_DIR = os.getenv("DATA_DIR")
BRONZE_DATA_DIR = os.getenv("BRONZE_DATA_DIR")
SILVER_DATA_DIR = os.getenv("SILVER_DATA_DIR")
SILVER_GENERAL_CONFIG_PATH = os.getenv("SILVER_GENERAL_CONFIG_PATH")
SILVER_TRANSFORM_BASE_CONFIG_PATH = os.getenv("SILVER_TRANSFORM_BASE_CONFIG_PATH")
SILVER_TRANSFORM_SUB_CONFIG_PATH = os.getenv("SILVER_TRANSFORM_SUB_CONFIG_PATH")

if not BRONZE_DATA_DIR or not SILVER_DATA_DIR or not SILVER_GENERAL_CONFIG_PATH:
    raise ValueError("BRONZE_DATA_DIR, SILVER_DATA_DIR, SILVER_GENERAL_CONFIG_PATH must be set")

CONFIG = load_config_from_json(SILVER_GENERAL_CONFIG_PATH)

logger.info(f"{BRONZE_DATA_DIR = }, {SILVER_DATA_DIR = }")

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

# -----------------------------
# Listing folders & dumps
# -----------------------------
bronze_folders = get_folder_list(BRONZE_DATA_DIR)
silver_folders = get_folder_list(SILVER_DATA_DIR)
dump_dates = get_dump_dates(bronze_folders)
logger.info(f"{dump_dates = }")

n = CONFIG["NB_DUMPS_TO_PROCESS"]
n_latest_dumps_dates = sorted(dump_dates[-n - 1 :])
logger.info(f"Processing {n} latest dumps: {n_latest_dumps_dates}")

# Load configs
table_configs_base = load_config_from_json(SILVER_TRANSFORM_BASE_CONFIG_PATH)
table_configs_sub = load_config_from_json(SILVER_TRANSFORM_SUB_CONFIG_PATH)

# -----------------------------
# Start ingestion
# -----------------------------
path_merge_log = os.path.join(DATA_DIR, "silver", "silver_merge_log.csv")
previous_input_path = None

for dump_date in n_latest_dumps_dates:
    logger.info(f"Processing dump date {dump_date}")

    # -----------------------------
    # Base tables
    # -----------------------------
    for cfg in table_configs_base:

        time_start = datetime.datetime.now()

        input_table = cfg["table_input"]
        output_table = cfg["table_output"]
        primary_key = cfg["primary_key"]
        fields = cfg["fields"]
        layer_input = cfg["layer_input"]
        layer_output = cfg["layer_output"]

        output_path = os.path.join(DATA_DIR, layer_output, output_table)
        input_path = os.path.join(DATA_DIR, layer_input, input_table)

        last_merge_log = get_or_create_last_log(path_merge_log)
        last_processed_dump = get_last_dump_date(last_merge_log, output_path)

        if int(dump_date) <= last_processed_dump:
            logger.info(f"{dump_date} <= {last_processed_dump}: skipping")
            continue

        delta_table_path = os.path.join(BRONZE_DATA_DIR, input_table, f"dump={dump_date}")
        logger.info(f"Loading bronze table: {delta_table_path}")
        df_bronze = spark.read.format("delta").load(delta_table_path)

        # Only hash relevant fields
        cols_to_hash = [f["source"] for f in fields if f["source"] != "_og_row_hash"]
        df_bronze = with_row_hash(df_bronze, cols_to_hash)

        df_bronze = wrap_all_structs(df_bronze)
        df_bronze = select_fields_with_alias(df_bronze, fields)

        # Metadata
        metadata_op_dict = {
            "dump_date": dump_date,
            "input_path": input_path,
            "output_path": output_path
        }

        # Upsert vs create
        if DeltaTable.isDeltaTable(spark, output_path):
            metadata_op_dict["op"] = "merge"
            logger.info(f"Updating Delta table {output_path}")
            stats = spark_upsert_based_on_hashes_with_stats(
                spark, df_bronze, primary_key, output_path, metadata_op_dict
            )
        else:
            metadata_op_dict["op"] = "init"
            logger.info(f"Creating new Delta table {output_path}")
            stats = spark_create_df_with_stats(spark, df_bronze, output_path, metadata_op_dict)

        # -----------------------------
        # Log metrics
        # -----------------------------
        duration_min = round((datetime.datetime.now() - time_start).total_seconds() / 60, 2)  # Optional
        silver_merge_log = pd.DataFrame({
            "timestamp": [datetime.datetime.now().isoformat()],
            "input_path": [input_path],
            "output_path": [output_path],
            "dump": [dump_date],
            "previous_total": [stats["count_before"]],
            "updated": [stats["updated"]],
            "not_modified": [stats["not_modified"]],
            "inserted": [stats["inserted"]],
            "total": [stats["total"]],
            "duration_min": [duration_min],
        })
        silver_merge_log = pd.concat([last_merge_log, silver_merge_log], ignore_index=True)
        silver_merge_log.sort_values(by=["input_path", "output_path", "dump"]).to_csv(path_merge_log, index=False)

        # -----------------------------
        # Output schema + JSON
        # -----------------------------
        output_schema_json(df_bronze, output_path)
        output_schema_txt(df_bronze, output_path)
        output_schema_csv(df_bronze, output_path)
        output_single_json(df_bronze, output_path)
        output_single_json(df_bronze, f"{output_path}_{dump_date}")

    # -----------------------------
    # Subtables
    # -----------------------------
    for cfg in table_configs_sub:
        time_start = datetime.datetime.now()
        input_table = cfg["table_input"]
        output_table = cfg["table_output"]
        fields = cfg["fields"]
        layer_input = cfg["layer_input"]
        layer_output = cfg["layer_output"]



        output_path = os.path.join(DATA_DIR, layer_output, output_table)
        input_path = os.path.join(DATA_DIR, layer_input, input_table)

        print(f"{input_path = }")
        print(f"{output_path = }")


        last_merge_log = get_or_create_last_log(path_merge_log)
        last_processed_dump = get_last_dump_date(last_merge_log, output_path)

        if int(dump_date) <= last_processed_dump:
            continue

        if previous_input_path != input_path:
            logger.info(f"\n\nLoading new subtables from {input_path}")
            previous_input_path = input_path



        df_silver = spark.read.format("delta").load(input_path)

        cfg_explode = cfg.get("explode")
        cfg_post_split_explode = cfg.get("post_split_explode")



        if cfg_post_split_explode:
            fields.append(cfg_post_split_explode)

        if cfg_explode:

            logger.info(f"\n\n{cfg_explode = }")
            to_add = {"source":cfg_explode.split(".")[0], "alias":cfg_explode.split(".")[0]}
            logger.info(f"\n\n{to_add = }")
            fields.append(to_add)



        logger.info(f"\n\n{fields = }")
        logger.info(f"\n\n{cfg_explode = }")
        logger.info(f"\n\n{cfg_post_split_explode = }")

        required_roots = extract_required_roots(
            fields,
            cfg_explode,
            cfg_post_split_explode
        )

        logger.info(f"\n\n{required_roots = }")


        df_silver = df_silver.select(*required_roots)



        cols_to_hash_alias = [f.get("alias",None) for f in fields if f.get("alias",None)  != "_og_row_hash"]
        cols_to_hash_source = [f.get("source",None)  for f in fields if f.get("source",None) != "_og_row_hash"]



        cols_to_hash = cols_to_hash_alias + cols_to_hash_source



        print(f"{cols_to_hash = }")

        # map aliases back to source columns


        logger.info(f"\n\nwith_row_hash\n")

        df_silver = with_row_hash(df_silver, cols_to_hash)
        logger.info(f"\n\nwith_row_hash over\n")
        # 5️⃣ Explode/post_split AFTER hashing




        
        if "explode" in cfg:
            logger.info(f"explode")
            
            df_silver = apply_explode(df_silver, cfg["explode"])



        

        if "post_split_explode" in cfg:
            logger.info(f"post_split_explode")
            
            df_silver = apply_post_split_explode(df_silver, cfg["post_split_explode"])


            
        logger.info(f"{fields = }")
        logger.info(f"{cfg_post_split_explode = }")




        
        logger.info(f"{fields = }")

        

        

        df_silver = select_fields_with_alias(df_silver, cols_to_hash_alias)

        logger.info(f"select_fields_with_alias")



        df_silver = enforce_required(df_silver, fields)
        logger.info(f"enforce_required")
        
        # df_silver.selectExpr("artist_roles").show(5, False)


        metadata_op_dict = {
            "dump_date": dump_date,
            "input_path": input_path,
            "output_path": output_path,
            "op": "init" if not DeltaTable.isDeltaTable(spark, output_path) else "merge"
        }

        if DeltaTable.isDeltaTable(spark, output_path):
            stats = spark_replace_by_hash(
                spark, df_silver, fields[0]["alias"], output_path, metadata_op_dict
            )
        else:
            stats = spark_create_df_with_stats(spark, df_silver, output_path, metadata_op_dict)

        # Output
        output_schema_json(df_silver, output_path)
        output_schema_txt(df_silver, output_path)
        output_schema_csv(df_silver, output_path)
        output_single_json(df_silver, output_path)
        output_single_json(df_silver, f"{output_path}_{dump_date}")

        duration_min = round((datetime.datetime.now() - time_start).total_seconds() / 60, 2)  # Optional
        continue
        # Log metrics
        silver_merge_log = pd.DataFrame({
            "timestamp": [datetime.datetime.now().isoformat()],
            "input_path": [input_path],
            "output_path": [output_path],
            "dump": [dump_date],
            "previous_total": [stats["count_before"]],
            "updated": [stats["updated"]],
            "not_modified": [stats["not_modified"]],
            "inserted": [stats["inserted"]],
            "total": [stats["total"]],
            "duration_min": [duration_min],
        })
        silver_merge_log = pd.concat([last_merge_log, silver_merge_log], ignore_index=True)
        silver_merge_log.sort_values(by=["input_path", "output_path", "dump"]).to_csv(path_merge_log, index=False)
