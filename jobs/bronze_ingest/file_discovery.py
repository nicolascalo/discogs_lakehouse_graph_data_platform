from pyspark.sql import SparkSession
import os
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *

from typing import List, Dict


def get_latest_dump_date(
    spark: SparkSession, dump_type: str, metrics_path: str = "/metrics/bronze"
):

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


def get_output_dir(dump_type: str, logger, data_folder) -> str:
    output_dir = os.path.join(data_folder, dump_type)
    logger.info(f"{output_dir = }")

    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    return output_dir


def get_latest_dump_files(raw_dir: Path, logger) -> List[Dict[str, str]]:
    dumps_to_process = []

    raw_data_file_list = os.listdir(raw_dir)
    logger.info(f"{raw_data_file_list = }")

    dump_types = [extract_dump_type(f) for f in raw_data_file_list]
    dump_types = set([d for d in dump_types if d])

    if not dump_types:
        logger.error(f"No Discogs dump files found in {raw_dir}")
        raise RuntimeError(f"No Discogs dump files found in {raw_dir}")

    for dump_type in dump_types:
        dump_dates = [
            extract_dump_date(f) for f in raw_data_file_list if dump_type in f
        ]
        dump_dates = [d for d in dump_dates if d]

        if not dump_dates:
            logger.error(f"No Discogs dump files found in {raw_dir}")
            raise RuntimeError(f"No Discogs dump files found in {raw_dir}")

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
            input_file_path = os.path.join(raw_dir, file)

            dumps_to_process.append(
                {
                    "dump_type": dump_type,
                    "latest_dump_date": latest_dump_date,
                    "file": file,
                    "input_file_path": input_file_path,
                }
            )

    return dumps_to_process
