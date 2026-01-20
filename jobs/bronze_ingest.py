from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
import os
import re
import gzip
import tempfile
import shutil
import struct
import json

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType
from delta.tables import DeltaTable

## Logging setup


LOG_DIR = Path(os.getenv("LOG_DIR", "/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "discogs_bronze_ingest.log"

# Remove any existing handlers first
root_logger = logging.getLogger()
for h in root_logger.handlers[:]:
    root_logger.removeHandler(h)

# File handler
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50 * 1024 * 1024,  # 50 MB
    backupCount=5,
)
file_handler.setFormatter(logging.Formatter(
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
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)
logger.info(f"Logging to {LOG_FILE}")
## Env and config variables


RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
BRONZE_DATA_DIR = os.getenv("BRONZE_DATA_DIR")
BRONZE_CONFIG_PATH = os.getenv("BRONZE_CONFIG_PATH")





try:
    with open(BRONZE_CONFIG_PATH, "r") as json_file:
        CONFIG = json.loads(json_file.read())
except json.JSONDecodeError as e:
    raise ValueError(f"Invalid JSON in {BRONZE_CONFIG_PATH}") from e
except OSError as e:
    raise RuntimeError(f"Cannot read {BRONZE_CONFIG_PATH}") from e

if not RAW_DATA_DIR or not BRONZE_DATA_DIR or not BRONZE_CONFIG_PATH:
    raise ValueError("RAW_DATA_DIR and BRONZE_DATA_DIR and BRONZE_CONFIG_PATH must be set")

logger.info(f"{RAW_DATA_DIR = }")
logger.info(f"{BRONZE_DATA_DIR = }")

## Spark session

spark = (
    SparkSession.builder
    .appName("discogs-bronze-ingest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


## Listing files to process

raw_data_file_list = os.listdir(RAW_DATA_DIR)
logger.info(f"{raw_data_file_list = }")



def extract_dump_date(fname):
    m = re.search(r"discogs_(\d{8})", fname)
    return m.group(1) if m else None

dump_dates = [extract_dump_date(f) for f in raw_data_file_list]
dump_dates = [d for d in dump_dates if d]

if not dump_dates:
    raise RuntimeError("No Discogs dump files found in RAW_DATA_DIR")


dump_dates  = list(set(dump_dates))
dump_dates.sort()


logger.info(f"{dump_dates = }")

n = CONFIG["NB_DUMPS_TO_PROCESS"]
n_latest_dumps_dates = dump_dates[-n-1:]

n_latest_dumps_dates.sort()
logger.info(f"Nb of most recent dumps to process = {n}")
logger.info(f"{n_latest_dumps_dates = }")


for dump_date in n_latest_dumps_dates:

    raw_data_file_list_dump = [file_name for file_name in raw_data_file_list if (dump_date in file_name) and (file_name.endswith(".xml.gz"))]

    if not raw_data_file_list_dump:
        logger.warning(f"No XML files found for dump {dump_date}")
        continue

    logger.info(f"{dump_date = }")
    logger.info(f"{raw_data_file_list_dump = }")




    for file in raw_data_file_list_dump:


        logger.info(f"{file} -> Starting processing")

        m = re.match(r"discogs_\d+_(.+?)_.*\.xml\.gz", file)
        if not m:
            error_message = f"{file} -> Unexpected filename format"
            logger.error(error_message)
            raise ValueError(error_message)

        file_type = m.group(1)


        
        input_file_path = os.path.join(RAW_DATA_DIR,file)
        output_dir = os.path.join(BRONZE_DATA_DIR,file_type,  f"dump={dump_date}")



        os.makedirs(output_dir,exist_ok=True)

        
        if file_type not in CONFIG['ROW_TAGS']:

            error_message = f"{file} -> No row tag defined for file type '{file_type}'"
            logger.error(error_message)

            raise KeyError(error_message)

        row_tag = CONFIG['ROW_TAGS'][file_type]
    
        
        logger.info(f"{spark._jsparkSession.catalog().listTables() }")
        
        dump_path = os.path.join(output_dir, f"dump={dump_date}")


        if DeltaTable.isDeltaTable(spark, output_dir):
            logger.info(
                f"{file} -> Bronze Delta table already exists for {file_type} dump={dump_date}, skipping"
            )
            continue
                

        # Check if enough free space on the disk to decompress the files



        def gzip_uncompressed_size(path):
            with open(path, "rb") as f:
                f.seek(-4, os.SEEK_END)
                return struct.unpack("<I", f.read(4))[0]

        temp_base = tempfile.gettempdir()
        _, _, free = shutil.disk_usage(temp_base)

        compressed_size = os.path.getsize(input_file_path)

        try:
            estimated_size = gzip_uncompressed_size(input_file_path)
        except Exception:
            estimated_size = compressed_size * 10

        required = int(estimated_size * 1.2)

        

        if free < required:
            raise RuntimeError(
                f"{file} -> Insufficient disk space in {temp_base}: "
                f"{file} -> need {required / 1e9:.1f} GB, "
                f"{file} -> have {free / 1e9:.1f} GB"
            )
        else:
            logger.info(f"{file} -> Estimated max disk space needed = {required / 1e9:.1f} GB")
            logger.info(f"{file} -> Free disk space = {free / 1e9:.1f} GB")
            logger.info(f"{file} -> Enough disk space to proceed")




        with tempfile.TemporaryDirectory() as tempdir:
            with gzip.open(input_file_path, 'rb') as f_in: # Decompression needed to allow parallel processing. !!!!!! NEED A LOT OF FREE DISK SPACE

                unzipped_file_name = file.replace(".gz","")
                unzipped_file_path = os.path.join(tempdir,unzipped_file_name)

                with open(unzipped_file_path, 'wb') as f_out:
                    logger.info(f"{file} -> Decompressing {input_file_path} to {tempdir}")
                    
                    shutil.copyfileobj(f_in, f_out)

            

            df = (
            spark.read.format("xml") 
                .option("rowTag", row_tag) 
                .option("ignoreSurroundingSpaces", True)
                .option("attributePrefix", "@") \
                .option("valueTag", "_text") \
                .option("treatEmptyValuesAsNulls", True)
                .option("samplingRatio", 1) # Scans the full xml to establish schema
                .load(unzipped_file_path) 
            )

            

            logger.info(f"{file} -> Exporting schemas")
            xml_schema = df._jdf.schema()


            xml_schema_tree = xml_schema.treeString()

            schema_path = os.path.join(
                BRONZE_DATA_DIR,
                file_type,
                f"{file_type}_schema_dump_{dump_date}_tree.txt"
            )

            with open(schema_path, "w") as f:
                f.write(xml_schema_tree)

            xml_schema_json = xml_schema.json()

            schema_path = os.path.join(
                BRONZE_DATA_DIR,
                file_type,
                f"{file_type}_schema_dump_{dump_date}.json"
            )

            with open(schema_path, "w") as f:
                f.write(xml_schema_json)


            logger.info(f"{file} -> Exporting delta files")


            (
                df
                .withColumn("dump", lit(dump_date))
                .write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(output_dir)
            )

            logger.info(f"{file} -> Delta files export over")

'''

    df.limit(100)\
        .write \
        .format("csv") \
        .mode("overwrite") \
        .save(f"{RAW_DATA_DIR}/{file_type}.csv")

df = spark.read.format("delta").load("/data/bronze/releases")


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



spark.sql("VACUUM delta.`/data/bronze/releases` RETAIN 168 HOURS")
spark.sql("OPTIMIZE delta.`/data/bronze/releases`")
'''