from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os
import re
import dotenv
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
import datetime
import pandas as pd
import hashlib


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *

from typing import List, Dict, Optional
from pyspark.sql import DataFrame

## config loading

dotenv.load_dotenv()


EXPORT_HISTORY_TO_CSV = os.getenv("EXPORT_HISTORY_TO_CSV", "false").lower() == "true"


## Path setup

LOG_DIR = Path(os.getenv("LOG_DIR", "/logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)


LOG_FILE = LOG_DIR / "discogs_bronze_ingest.log"
LOG_FILE.touch(exist_ok=True)

DATA_DIR = Path(os.getenv("DATA_DIR", "/data_tests"))

RAW_DATA_DIR = DATA_DIR / "raw"


BRONZE_DATA_DIR = DATA_DIR / "bronze"
BRONZE_DATA_DIR.mkdir(parents=True, exist_ok=True)

archive_dir = RAW_DATA_DIR / "archive"
archive_dir.mkdir(parents=True, exist_ok=True)


# Loading bronze config


CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "/config"))
CONFIG_FILE = CONFIG_DIR / "bronze_config.json"

BRONZE_CONFIG = load_config_from_json(CONFIG_FILE)

PRIMARY_KEYS = BRONZE_CONFIG["PRIMARY_KEYS"]


def setup_logger() -> logging.Logger:

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
    file_handler.setFormatter(
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
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logger = logging.getLogger(__name__)
    ## Env and config variables
    return logger


def create_spark_session(app_name: str) -> SparkSession:

    ## Spark session

    # Build the Spark session
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    # This injects the Delta jars from delta-spark Python package
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def get_latest_dump_files(raw_dir: Path) -> List[Dict[str, str]]:
    dumps_to_process = []

    logger.info(f"{RAW_DATA_DIR = }")
    logger.info(f"{BRONZE_DATA_DIR = }")
    logger.info(f"{LOG_DIR = }")
    logger.info(f"{LOG_FILE = }")

    raw_data_file_list = os.listdir(raw_dir)
    logger.info(f"{raw_data_file_list = }")

    dump_types = [extract_dump_type(f) for f in raw_data_file_list]
    dump_types = set([d for d in dump_types if d])

    if not dump_types:
        logger.error(f"No Discogs dump files found in RAW_DATA_DIR")
        raise RuntimeError("No Discogs dump files found in RAW_DATA_DIR")

    for dump_type in dump_types:
        dump_dates = [
            extract_dump_date(f) for f in raw_data_file_list if dump_type in f
        ]
        dump_dates = [d for d in dump_dates if d]

        if not dump_dates:
            logger.error(f"No Discogs dump files found in RAW_DATA_DIR")
            raise RuntimeError("No Discogs dump files found in RAW_DATA_DIR")

        dump_dates = list(set(dump_dates))
        dump_dates.sort(reverse=True)

        latest_dump_date = dump_dates[0]

        raw_data_file_list_dump = [
            file_name
            for file_name in raw_data_file_list
            if (latest_dump_date in file_name)
            and (file_name.endswith(".xml.gz") and (dump_type in file_name))
        ]

        if not raw_data_file_list_dump:
            logger.warning(f"No XML files found for dump {latest_dump_date}")

        for file in raw_data_file_list_dump:
            input_file_path = os.path.join(RAW_DATA_DIR, file)

            dumps_to_process.append(
                {
                    "dump_type": dump_type,
                    "latest_dump_date": latest_dump_date,
                    "file": file,
                    "input_file_path": input_file_path,
                }
            )

    return dumps_to_process


def read_xml_to_df(
    spark: SparkSession, dump_type: str, input_file_path: str
) -> DataFrame:

    row_tag = re.sub("s$", "", dump_type)
    logger.info(f"Importing xml {input_file_path} to Delta Table")
    df = (
        spark.read.format("xml")
        .option("rowTag", row_tag)
        .option("ignoreSurroundingSpaces", True)
        .option("attributePrefix", "@")
        .option("valueTag", "_text")
        .option("samplingRatio", 1)
        .option("treatEmptyValuesAsNulls", True)
        .option("mode", "PERMISSIVE")  # keep malformed rows in a special column
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(input_file_path)
    )

    if df.rdd.getNumPartitions() == 1:
        df = df.repartition(spark.sparkContext.defaultParallelism)

    return df


def apply_hash(df: DataFrame, primary_key: str, latest_dump_date: str) -> DataFrame:
    cols_to_hash = sorted(
        [c for c in df.columns if c not in ["complete_root_hash", "last_dump_update"]]
    )

    df = df.withColumn(
        "complete_root_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                    for c in cols_to_hash
                ],
            ),
            256,
        ),
    ).withColumn("last_dump_update", F.lit(latest_dump_date))

    df = cast_non_key_columns_to_string(
        df, primary_key=primary_key, hash_col="complete_root_hash"
    )

    return df


def get_output_dir(dump_type: str) -> str:
    output_dir = os.path.join(BRONZE_DATA_DIR, dump_type)
    logger.info(f"{output_dir = }")

    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    return output_dir


def create_new_bronze(df: DataFrame, output_dir) -> None:

    logger.info(f"Writing new Delta Table")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        output_dir
    )
    return None


def create_new_delta_history(spark: SparkSession) -> DataFrame:

    from pyspark.sql import Row

    history = spark.createDataFrame(
        [
            Row(
                version=0,
                timestamp=str(datetime.datetime.now()),  # cast to string explicitly
                operation="WRITE",
                operationMetrics={
                    "numTargetRowsUpdated": 0,
                    "numTargetRowsDeleted": 0,
                    "numTargetRowsNotMatchedBySourceDeleted": 0,
                },
            )
        ]
    )

    return history


def get_latest_dump_date(spark: SparkSession, dump_type: str):

    metrics_path = "/metrics/bronze"

    if not DeltaTable.isDeltaTable(spark, metrics_path):
        return None

    history = spark.read.format("delta").load(metrics_path)

    previous = (
        history.filter(F.col("table_input") == dump_type)
        .orderBy(F.col("dump").desc())
        .limit(1)
    )

    row = previous.collect()
    if row:
        return row[0]["dump"]
    else:
        return None


def merge_into_bronze(
    spark: SparkSession, df: DataFrame, output_dir: str, primary_key: str
) -> None:
    logger.info(f"Merging Delta Table into Bronze")

    target = DeltaTable.forPath(spark, output_dir)

    (
        target.alias("t")
        .merge(
            df.alias("s"),
            f"t.`{primary_key}` = s.`{primary_key}`",
        )
        .whenMatchedUpdateAll(condition="t.complete_root_hash <> s.complete_root_hash")
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )

    spark.sql(f"OPTIMIZE delta.`{output_dir}`")
    return None


def export_schemas(df, dump_type: str, latest_dump_date: str) -> None:

    logger.info(f"{latest_dump_date} -> Exporting schemas")

    xml_schema_tree = df.schema.treeString()
    xml_schema_json = df.schema.json()

    schema_path = os.path.join(
        BRONZE_DATA_DIR, f"{dump_type}_schema_dump_{latest_dump_date}_tree.txt"
    )

    with open(schema_path, "w") as f:
        f.write(xml_schema_tree)

    schema_path = os.path.join(
        BRONZE_DATA_DIR, f"{dump_type}_schema_dump_{latest_dump_date}.json"
    )

    with open(schema_path, "w") as f:
        f.write(xml_schema_json)

    return None


def cleanup_old_raw_files(latest_dump_date: str, dump_type: str) -> None:
    raw_data_file_list = os.listdir(RAW_DATA_DIR)
    raw_data_file_list_to_archive = [
        file_name
        for file_name in raw_data_file_list
        if (latest_dump_date not in file_name)
        and (file_name.endswith(".xml.gz") and (dump_type in file_name))
    ]

    for file in raw_data_file_list_to_archive:
        file_path = Path(RAW_DATA_DIR) / file
        if os.path.exists(file_path):
            archive_dir = RAW_DATA_DIR / "archive"
            file_path.rename(archive_dir / file_path.name)
    return None


def sha256_file(path: str, chunk_size=8192) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def cast_non_key_columns_to_string(df, primary_key: str, hash_col: str) -> DataFrame:
    cols = []

    for c in df.columns:
        if c in [primary_key, hash_col]:
            cols.append(F.col(c))
        else:
            cols.append(F.col(c).cast("string").alias(c))

    return df.select(*cols)


def validate_file_hash(dir: str, date: str, file):
    logger.info("Validating downloads for date = %s", date)
    error_flag = False
    files = os.listdir(dir)
    checksum_files = [f for f in files if "CHECKSUM" in f and date in f]

    if not checksum_files:
        raise RuntimeError(f"No checksum file found for date {date}")
    checksum_file = checksum_files[0]

    df = pd.read_csv(
        os.path.join(dir, checksum_file),
        sep=r"\s+",
        engine="python",
        names=["checksum", "file_name"],
        usecols=[0, 1],
    )

    df = df[df["file_name"] == file]
    if df.empty:
        raise RuntimeError(f"No checksum entry for file {file}")

    for _, row in df.iterrows():
        file_path = os.path.join(dir, row["file_name"])
        if not os.path.exists(file_path):
            logger.error("Missing file: %s", file_path)
            error_flag = True
            continue

        hash_file = sha256_file(file_path)
        if hash_file != row["checksum"]:
            logger.info("sha256 hash failed for %s", row["file_name"])
            error_flag = True
        else:
            logger.info("sha256 hash OK for %s", row["file_name"])

    if error_flag:
        raise RuntimeError("One or more files failed validation")
    else:
        return True


def export_delta_table_history(
    spark: SparkSession,
    dump_type: str,
    latest_dump_date: str,
    output_dir: str,
) -> None:
    """
    Export the latest MERGE or WRITE operation from a Delta table history
    along with all available operationMetrics.
    """
    csv_path = f"{LOG_DIR}/discogs_bronze_{dump_type}_delta_history.csv"
    logger.info(f"Outputting metrics: {csv_path}")

    # Read full Delta history
    history = spark.sql(f"DESCRIBE HISTORY delta.`{output_dir}`")

    # Keep only MERGE or WRITE operations
    history = history.filter(F.col("operation").isin("MERGE", "WRITE"))

    if history.count() == 0:
        logger.warning(f"No MERGE or WRITE operations found for {output_dir}")
        return None

    # Take the latest operation by version (guaranteed monotonic)
    latest_history = history.orderBy(F.col("version").desc()).limit(1)

    # Extract operationMetrics keys safely
    rows = latest_history.select("operationMetrics").collect()
    operation_metrics = rows[0]["operationMetrics"] or {}
    metric_keys = list(operation_metrics.keys())

    # Add extra metadata columns
    latest_history = (
        latest_history.withColumn("dump", F.lit(latest_dump_date))
        .withColumn("layer_output", F.lit("bronze"))
        .withColumn("table_output", F.lit(dump_type))
        .withColumn("layer_input", F.lit("raw"))
        .withColumn("table_input", F.lit(dump_type))
    )

    # Select all relevant columns including metrics
    select_cols = [
        "version",
        "timestamp",
        "operation",
        "dump",
        "layer_output",
        "table_output",
        "layer_input",
        "table_input",
    ] + [F.col("operationMetrics")[k].alias(k) for k in metric_keys]

    history_selected = latest_history.select(*select_cols)

    # Write to metrics Delta table (append)
    history_selected.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save("/metrics/bronze")

    # Collect metrics for logging
    latest_row_dict = history_selected.collect()[0].asDict()

    logger.info(f"Latest MERGE/WRITE metrics for {dump_type}:")

    metric_keys_rows = [metric for metric in metric_keys if "Rows" in metric]
    for key in metric_keys_rows:
        logger.info(f"  {key}: {latest_row_dict.get(key)}")

    # Optional CSV export
    if EXPORT_HISTORY_TO_CSV:
        pdf = history_selected.toPandas()
        if os.path.exists(csv_path):
            pdf_existing = pd.read_csv(csv_path)
            pdf = pd.concat([pdf_existing, pdf], ignore_index=True)
        pdf.to_csv(csv_path, index=False)
        logger.info(f"Delta history exported to {csv_path} with metrics: {metric_keys}")

    return None


logger = setup_logger()


def main():
    spark = create_spark_session("discogs-bronze-ingest")
    raw_dumps = get_latest_dump_files(RAW_DATA_DIR)

    failures = []

    for dump in raw_dumps:
        logger.info(
            "=================================================================================="
        )

        dump_type = dump["dump_type"]
        latest_dump_date_to_process = dump["latest_dump_date"]
        file = dump["file"]
        input_file_path = dump["input_file_path"]
        logger.info(
            "Starting processing of dump %s (%s)",
            dump_type,
            latest_dump_date_to_process,
        )
        primary_key = PRIMARY_KEYS[dump_type]
        latest_recorded_dump_date = get_latest_dump_date(spark, dump_type)

        try:
            if latest_recorded_dump_date:
                prev = datetime.datetime.strptime(latest_recorded_dump_date, "%Y%m%d")
            else:
                prev = datetime.datetime.strptime("19000101", "%Y%m%d")

            curr = datetime.datetime.strptime(latest_dump_date_to_process, "%Y%m%d")

            if latest_recorded_dump_date is not None and prev >= curr:
                logger.info(
                    "Skipping dump %s (%s) because previous dump %s already processed",
                    dump_type,
                    latest_dump_date_to_process,
                    latest_recorded_dump_date,
                )
                continue

            if validate_file_hash(str(RAW_DATA_DIR), latest_dump_date_to_process, file):
                start = datetime.datetime.now()
                df = read_xml_to_df(spark, dump_type, input_file_path)

                df = apply_hash(df, primary_key, latest_dump_date_to_process)

                output_dir = get_output_dir(dump_type)

                if not DeltaTable.isDeltaTable(spark, output_dir):
                    create_new_bronze(df, output_dir)
                    create_new_delta_history(spark)

                else:
                    merge_into_bronze(spark, df, output_dir, primary_key)

                export_schemas(df, dump_type, latest_dump_date_to_process)
                export_delta_table_history(
                    spark, dump_type, latest_dump_date_to_process, output_dir
                )

                cleanup_old_raw_files(latest_dump_date_to_process, dump_type)
                logger.info(
                    f"Finished processing of dump {latest_dump_date_to_process} {dump_type}"
                )
                duration = (datetime.datetime.now() - start).total_seconds()
                logger.info(
                    f"Dump {latest_dump_date_to_process} {dump_type} processed in {duration} seconds"
                )

            else:
                continue

        except Exception as e:
            logger.exception(
                f"Failed processing dump {latest_dump_date_to_process} - {dump_type}"
            )
            failures.append(
                {"dump_date": latest_dump_date_to_process, "dump_type": dump_type}
            )
            continue

    if failures:
        logger.error("The following dumps failed: %s", failures)
        raise RuntimeError("One or more dumps failed")


if __name__ == "__main__":
    main()
