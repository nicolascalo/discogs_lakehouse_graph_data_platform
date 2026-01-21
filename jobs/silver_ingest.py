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

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F


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
delta_table_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    "%Y-%m-%d %H:%M:%S",
))

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    "%Y-%m-%d %H:%M:%S",
))

# Add handlers
root_logger.setLevel(logging.INFO)
root_logger.addHandler(delta_table_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)
logger.info(f"Logging to {LOG_FILE}")
## Env and config variables


BRONZE_DATA_DIR = os.getenv("BRONZE_DATA_DIR")
SILVER_DATA_DIR = os.getenv("SILVER_DATA_DIR")
SILVER_GENERAL_CONFIG_PATH = os.getenv("SILVER_GENERAL_CONFIG_PATH")
SILVER_TRANSFORM_CONFIG_PATH = os.getenv("SILVER_TRANSFORM_CONFIG_PATH")





try:
    with open(SILVER_GENERAL_CONFIG_PATH, "r") as json_delta_table:
        CONFIG = json.loads(json_delta_table.read())
except json.JSONDecodeError as e:
    raise ValueError(f"Invalid JSON in {SILVER_GENERAL_CONFIG_PATH}") from e
except OSError as e:
    raise RuntimeError(f"Cannot read {SILVER_GENERAL_CONFIG_PATH}") from e

if not BRONZE_DATA_DIR or not SILVER_DATA_DIR or not SILVER_GENERAL_CONFIG_PATH:
    raise ValueError("BRONZE_DATA_DIR and SILVER_DATA_DIR and SILVER_CONFIG_PATH must be set")

logger.info(f"{BRONZE_DATA_DIR = }")
logger.info(f"{SILVER_DATA_DIR = }")

## Spark session

spark = (
    SparkSession.builder
    .appName("discogs-silver-ingest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

## Spark helpers


def select_nested_fields(df: DataFrame, fields: list)  ->  DataFrame:
    selected = []
    for f in fields:
        if "." in f:
            # nested field
            parts = f.split(".")
            col_expr = F.col(parts[0])
            for p in parts[1:]:
                col_expr = col_expr.getItem(p) if "[" in p else col_expr.getField(p)
            selected.append(col_expr.alias(f.replace(".", "_")))
        else:
            selected.append(F.col(f))
    return df.select(*selected)



def enforce_required(df, fields):
    required_cols = [f["alias"] for f in fields if f.get("required")]
    if not required_cols:
        return df
    cond = None
    for c in required_cols:
        c_cond = F.col(c).isNotNull()
        cond = c_cond if cond is None else cond & c_cond
    return df.filter(cond)



def apply_explode(df, explode_path):
    parts = explode_path.split(".")
    col_expr = F.col(parts[0])
    for p in parts[1:]:
        col_expr = col_expr.getField(p)

    # SAFE explode
    return df.withColumn("element", F.explode_outer(col_expr))



def apply_split_explode(df, cfg, alias="element"):
    from pyspark.sql import functions as F

    src = cfg["source"]
    sep = cfg.get("separator", ",")

    parts = src.split(".")
    col_expr = F.col(parts[0])
    for p in parts[1:]:
        col_expr = col_expr.getField(p)

    return df.withColumn(
        alias,
        F.explode_outer(
            F.split(col_expr, sep)
        )
    )


def select_fields_with_alias(df, fields, explode_alias="element"):
    cols = []

    for f in fields:
        alias = f["alias"]

        # fixed literal (always overwrite)
        if "value" in f:
            cols.append(F.lit(f["value"]).alias(alias))
            continue

        src = f["source"]
        parts = src.split(".")

        if parts[0] == explode_alias:
            col_expr = F.col(explode_alias)
            for p in parts[1:]:
                col_expr = col_expr.getField(p)
        else:
            col_expr = F.col(parts[0])
            for p in parts[1:]:
                col_expr = col_expr.getField(p)

        if "default" in f:
            col_expr = F.coalesce(col_expr, F.lit(f["default"]))

        cols.append(col_expr.alias(alias))

    return df.select(*cols)

def apply_post_split_explode(df, cfg, explode_alias="element"):
    from pyspark.sql import functions as F

    src = cfg["source"]
    sep = cfg.get("separator", ",")
    alias = cfg.get("alias", "element")

    parts = src.split(".")

    if parts[0] == explode_alias:
        col_expr = F.col(explode_alias)
        for p in parts[1:]:
            col_expr = col_expr.getField(p)
    else:
        col_expr = F.col(parts[0])
        for p in parts[1:]:
            col_expr = col_expr.getField(p)

    return df.withColumn(
        alias,
        F.explode_outer(F.split(col_expr, sep))
    )



## Listing delta_tables to process



bronze_folders = [x[0] for x in os.walk(BRONZE_DATA_DIR)]
silver_folders = [x[0] for x in os.walk(SILVER_DATA_DIR)]

def extract_dump_date(fname):
    m = re.search(r"(\d{8})", fname)
    return m.group(1) if m else None

dump_dates = [extract_dump_date(f) for f in bronze_folders]
dump_dates = [d for d in dump_dates if d]
dump_dates  = list(set(dump_dates))


if not dump_dates:
    raise RuntimeError("No Discogs dump delta_tables found in BRONZE_DATA_DIR")


dump_dates  = list(set(dump_dates))
dump_dates.sort()


logger.info(f"{dump_dates = }")

n = CONFIG["NB_DUMPS_TO_PROCESS"]
n_latest_dumps_dates = dump_dates[-n-1:]

n_latest_dumps_dates.sort()
logger.info(f"Nb of most recent dumps to process = {n}")
logger.info(f"{n_latest_dumps_dates = }")



try:
    with open(SILVER_TRANSFORM_CONFIG_PATH) as f:
        table_configs = json.load(f)
except Exception as e:
    
        logger.error(e)



for dump_date in n_latest_dumps_dates:

    previous_input_table = None

    for cfg in table_configs:

        input_table = cfg["table_input"]
        output_table = cfg["table_output"]
        fields = cfg["fields"]
        layer_input = cfg["layer_input"]
        layer_output = cfg["layer_output"]

        logger.info(f"{input_table =}")
        logger.info(f"{output_table =}")
        logger.info(f"{layer_input =}")
        logger.info(f"{layer_output =}")


        if previous_input_table != input_table:
            logger.info(f"===========    Reloading new table")

            if layer_input == 'bronze':

                delta_table = [delta_table for delta_table in bronze_folders if (dump_date in delta_table) and  ("/_" not in delta_table)]
                delta_table =  os.path.join(BRONZE_DATA_DIR, input_table,f"dump={dump_date}") 


            else:
                delta_table =  os.path.join(SILVER_DATA_DIR, input_table) 

            previous_input_table = input_table

        logger.info(f"{previous_input_table =}")
        logger.info(f"{delta_table =}")


        logger.info(f"{delta_table}  -  Starting processing ->  {layer_output}/{output_table}")





        logger.info(f"Reading delta table {delta_table}")
        df = spark.read.format("delta").load(delta_table)






        # 1. explode first
        silver_df = df


        if "explode" in cfg:
            silver_df = apply_explode(silver_df, cfg["explode"])
            logger.info(f"{delta_table}  -  Exploding silver_df")
        
        if "post_split_explode" in cfg:
            silver_df = apply_post_split_explode(silver_df, cfg["post_split_explode"])
            logger.info(f"{delta_table}  -  post_split_explode  silver_df")

        # 2. select + alias
        logger.info(f"{delta_table}  -  select_fields_with_alias  silver_df")
        silver_df = select_fields_with_alias(silver_df, cfg["fields"])

        # 3. enforce required fields
        logger.info(f"{delta_table}  -  enforce_required  silver_df")
        silver_df = enforce_required(silver_df, cfg["fields"])

        logger.info(f"{delta_table}  -  Exporting Deltatable to {SILVER_DATA_DIR}{output_table}")

        # 4. write
        silver_df.write.format("delta") \
            .mode("overwrite") \
            .save(f"{SILVER_DATA_DIR}{output_table}")


        logger.info(f"{delta_table}  -  Exporting Deltatable json to {SILVER_DATA_DIR}{output_table}")
        (
        silver_df.coalesce(1) 
            .write 
            .mode("overwrite") 
            .json(f"{SILVER_DATA_DIR}{output_table}.json_folder")
        )


        json_folder = f"{SILVER_DATA_DIR}{output_table}.json_folder"
        output_file = f"{SILVER_DATA_DIR}{output_table}.json"

        # Spark wrote part-00000-xxxx.json inside the folder
        part_file = glob.glob(os.path.join(json_folder, "part-*.json"))[0]

        # Move/rename
        shutil.move(part_file, output_file)

        # Remove the temporary folder
        shutil.rmtree(json_folder)


        logger.info(f"")


        continue

        # 4. write
        silver_df.write.format("delta") \
            .mode("overwrite") \
            .save(f"/data/silver/{cfg['table_output']}")






        m = re.match(r"discogs_\d+_(.+?)_.*\.xml\.gz", delta_table)
        if not m:
            error_message = f"{delta_table}  -  Unexpected delta_tablename format"
            logger.error(error_message)
            raise ValueError(error_message)

        delta_table_type = m.group(1)


        
        input_delta_table_path = os.path.join(BRONZE_DATA_DIR,delta_table)
        output_dir = os.path.join(SILVER_DATA_DIR,delta_table_type,  f"dump={dump_date}")



        os.makedirs(output_dir,exist_ok=True)

        
        if delta_table_type not in CONFIG['ROW_TAGS']:

            error_message = f"{delta_table}  -  No row tag defined for delta_table type '{delta_table_type}'"
            logger.error(error_message)

            raise KeyError(error_message)

        row_tag = CONFIG['ROW_TAGS'][delta_table_type]
    
        
        logger.info(f"{spark._jsparkSession.catalog().listTables() }")
        
        dump_path = os.path.join(output_dir, f"dump={dump_date}")


        if DeltaTable.isDeltaTable(spark, output_dir):
            logger.info(
                f"{delta_table}  -  Bronze Delta table already exists for {delta_table_type} dump={dump_date}, skipping"
            )
            continue
                

        # Check if enough free space on the disk to decompress the delta_tables



        def gzip_uncompressed_size(path):
            with open(path, "rb") as f:
                f.seek(-4, os.SEEK_END)
                return struct.unpack("<I", f.read(4))[0]

        temp_base = tempfile.gettempdir()
        _, _, free = shutil.disk_usage(temp_base)

        compressed_size = os.path.getsize(input_delta_table_path)

        try:
            estimated_size = gzip_uncompressed_size(input_delta_table_path)
        except Exception:
            estimated_size = compressed_size * 10

        required = int(estimated_size * 1.2)

        

        if free < required:
            raise RuntimeError(
                f"{delta_table}  -  Insufficient disk space in {temp_base}: "
                f"{delta_table}  -  need {required / 1e9:.1f} GB, "
                f"{delta_table}  -  have {free / 1e9:.1f} GB"
            )
        else:
            logger.info(f"{delta_table}  -  Estimated max disk space needed = {required / 1e9:.1f} GB")
            logger.info(f"{delta_table}  -  Free disk space = {free / 1e9:.1f} GB")
            logger.info(f"{delta_table}  -  Enough disk space to proceed")




        with tempfile.TemporaryDirectory() as tempdir:
            with gzip.open(input_delta_table_path, 'rb') as f_in: # Decompression needed to allow parallel processing. !!!!!! NEED A LOT OF FREE DISK SPACE

                unzipped_delta_table_name = delta_table.replace(".gz","")
                unzipped_delta_table_path = os.path.join(tempdir,unzipped_delta_table_name)

                with open(unzipped_delta_table_path, 'wb') as f_out:
                    logger.info(f"{delta_table}  -  Decompressing {input_delta_table_path} to {tempdir}")
                    
                    shutil.copydelta_tableobj(f_in, f_out)

            

            df = (
            spark.read.format("xml") 
                .option("rowTag", row_tag) 
                .option("ignoreSurroundingSpaces", True)
                .option("attributePrefix", "@") \
                .option("valueTag", "_text") \
                .option("treatEmptyValuesAsNulls", True)
                .option("samplingRatio", 1) # Scans the full xml to establish schema
                .load(unzipped_delta_table_path) 
            )

            

            logger.info(f"{delta_table}  -  Exporting schemas")
            xml_schema = df._jdf.schema()


            xml_schema_tree = xml_schema.treeString()

            schema_path = os.path.join(
                SILVER_DATA_DIR,
                delta_table_type,
                f"{delta_table_type}_schema_dump_{dump_date}_tree.txt"
            )

            with open(schema_path, "w") as f:
                f.write(xml_schema_tree)

            xml_schema_json = xml_schema.json()

            schema_path = os.path.join(
                SILVER_DATA_DIR,
                delta_table_type,
                f"{delta_table_type}_schema_dump_{dump_date}.json"
            )

            with open(schema_path, "w") as f:
                f.write(xml_schema_json)


            logger.info(f"{delta_table}  -  Exporting delta delta_tables")


            (
                df
                .withColumn("dump", lit(dump_date))
                .write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(output_dir)
            )

            logger.info(f"{delta_table}  -  Delta delta_tables export over")

'''

    df.limit(100)\
        .write \
        .format("csv") \
        .mode("overwrite") \
        .save(f"{BRONZE_DATA_DIR}/{delta_table_type}.csv")

df = spark.read.format("delta").load("/data/silver/releases")


df.write \
.format("delta") \
.mode("overwrite") \
.option("mergeSchema", "true") \
.save("/data/silver/releases")



from delta.tables import DeltaTable

delta = DeltaTable.forPath(spark, "/data/silver/releases")

(
    delta.alias("t")
    .merge(
        updates.alias("s"),
        "t.release_id = s.release_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)



spark.sql("VACUUM delta.`/data/silver/releases` RETAIN 168 HOURS")
spark.sql("OPTIMIZE delta.`/data/silver/releases`")
'''