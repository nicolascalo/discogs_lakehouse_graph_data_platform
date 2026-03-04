import os
import datetime
import logging
import pandas as pd
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


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

from ingest_helpers.spark_processing_helpers2 import build_tree, normalize_tree, sort_tree, select_tree,  explode_nested, split_nested

from ingest_helpers.spark_processing_helpers2_test import apply_enrich


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
SILVER_TRANSFORM_CONFIG_PATH = os.getenv("SILVER_TRANSFORM_CONFIG_PATH")

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
table_configs = load_config_from_json(SILVER_TRANSFORM_CONFIG_PATH)

# -----------------------------
# Helpers
# -----------------------------





def to_array_of_struct(col, target_struct):
    return F.when(F.col(col).isNull(), F.array().cast(ArrayType(target_struct))) \
            .when(F.col(col).dtype == "array", F.col(col)) \
            .otherwise(F.array(F.col(col).cast(target_struct)))





# -----------------------------
# Start ingestion
# -----------------------------
path_merge_log = os.path.join(DATA_DIR, "silver", "silver_merge_log.csv")
previous_input_path = None

for dump_date in n_latest_dumps_dates:
    logger.info(f"Processing dump date {dump_date}")

    for cfg in table_configs:



        time_start = datetime.datetime.now()

        layer_input = cfg["layer_input"]
        layer_output = cfg["layer_output"]
        table_input = cfg["table_input"]
        table_output = cfg["table_output"]
        hash = cfg.get("hash",None)
        homogeneize = cfg.get("homogeneize",None)
        enrich = cfg.get("enrich",None)
        explode = cfg.get("explode",None)



        
        

        output_path = os.path.join(DATA_DIR, layer_output, table_output)
        input_path = os.path.join(DATA_DIR, layer_input, table_input)

        last_merge_log = get_or_create_last_log(path_merge_log)
        last_processed_dump = get_last_dump_date(last_merge_log, output_path)

        if int(dump_date) <= last_processed_dump:
            logger.info(f"{dump_date} <= {last_processed_dump}: skipping")
            continue

        delta_table_path = os.path.join(BRONZE_DATA_DIR, table_input, f"dump={dump_date}")
        logger.info(f"Loading bronze table: {delta_table_path}")
        df_bronze = spark.read.format("delta").load(delta_table_path)




        # Filtering relevant columns

        logger.info(f"#################################################################### build_tree")
        tree = build_tree(cfg["keep"])
        logger.info(f"#################################################################### select")


        df = df_bronze.select(*select_tree(df_bronze, tree))

        # Homogenizing full tree to compute hash
        logger.info(f"#################################################################### normalize_tree")
        df_hash = normalize_tree(df)
        logger.info(f"#################################################################### sort_tree")
        df_hash = sort_tree(df_hash)

        logger.info(f"#################################################################### h_root")
        df = df.withColumn("h_root", F.xxhash64(F.struct(*df_hash.columns)))


        output_schema_json(df, f"{output_path}_{dump_date}_schemaafterh_root")


        logger.info(f"#################################################################### enrich")

        print(f"{cfg = }")
        cfg_enrich = cfg.get("enrich", [])
        df = apply_enrich(df, cfg_enrich)

        output_single_json(df, f"{output_path}_{dump_date}")

        continue
        logger.info(f"#################################################################### split")

        print(f"{cfg = }")
        cfg_split = cfg.get("split", [])
        print(f"{cfg_split = }")
        for cfg in cfg_split:
            print(f"{cfg['path'] = }")
            print(f"{cfg['name'] = }")
            print(f"{cfg['delimiter'] = }")
            
            df = split_nested(df, cfg['path'], cfg['name'], cfg['delimiter'])


        logger.info(f"#################################################################### hash_subtrees")

        output_single_json(df, f"{output_path}_{dump_date}")

    break

    subtables = {}
    parent_cols = cfg.get("parent_cols", df.columns)
    for explode_cfg in cfg.get("explode", []):
        path = explode_cfg["path"]
        table_name = explode_cfg.get("table", path.replace(".", "_"))
        subtables[table_name] = explode_nested(df, path, parent_cols, table_name)

    if subtables:
        for k, v in subtables.items():
            output_single_json(v, f"{output_path}_{dump_date}_{k}")
        



    for col in explode:
        # explode array
        df_exp = df.withColumn("ea", F.explode_outer("extraartists.artist"))

        # compute hash of subtree
        df_exp = df_exp.withColumn(
            "h_subtree",
            F.xxhash64(F.struct("ea.id", "ea.name", "ea.role_arr", "ea.track_arr"))
        )

        # -----------------------------
        # Output schema + JSON
        # -----------------------------
        output_schema_json(df_bronze, output_path)
        output_schema_txt(df_bronze, output_path)
        output_schema_csv(df_bronze, output_path)
        output_single_json(df_bronze, output_path)
        output_single_json(df_bronze, f"{output_path}_{dump_date}")

