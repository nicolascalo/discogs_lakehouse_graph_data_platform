from pyspark.sql import SparkSession
import os
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from unitycatalog.uc import uc_read_table

from helpers_ingest.file_info_helpers import extract_dump_date, extract_dump_type

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

def get_latest_dump_date_s3(
    spark: SparkSession, dump_type: str, metrics_path: str 
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


def get_latest_dump_date_from_metrics_uc(
    dump_type, catalog, schema, name, HEADERS, UC_URL, spark, logger
):
    history = uc_read_table(catalog, schema, name, HEADERS, UC_URL, spark, logger)

    if not history:
        return None

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



def get_local_folder_contents(raw_dir: Path, logger) -> List[str]:

    raw_data_file_list = os.listdir(raw_dir)
    logger.info(f"Looking for raw data files in {raw_dir}")
    logger.info(f"{raw_data_file_list = }")
    return raw_data_file_list


def get_s3_folder_contents(
    bucket: str, s3_folder_path: str,  logger, s3
) -> List[str]:

    s3_folder_path = s3_folder_path +"/"
    s3_folder_path.replace("//","/")
    
    paginator = s3.get_paginator("list_objects_v2")

    logger.info(f"Looking for files in {bucket}/{s3_folder_path}")

    file_list = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=s3_folder_path):
        for obj in page.get("Contents", []):
            file_list.append(obj["Key"])

    logger.info(f"{file_list = }")
    return file_list


def get_latest_dump_files_info(file_list: list[str], logger):
    dumps_to_process = []
    logger.info(f"{file_list = }")

    dump_types = [extract_dump_type(f) for f in file_list]
    dump_types = set([d for d in dump_types if d])

    if not dump_types:
        logger.error(f"No Discogs dump files found in the defined folder")
        raise RuntimeError(f"No Discogs dump files found in the defined folder")

    for dump_type in dump_types:
        dump_dates = [extract_dump_date(f) for f in file_list if dump_type in f]
        dump_dates = [d for d in dump_dates if d]

        if not dump_dates:
            logger.error(f"No Discogs dump files found in the defined folder")
            raise RuntimeError(f"No Discogs dump files found in the defined folder")

        dump_dates = list(set(dump_dates))
        dump_dates.sort(reverse=True)

        latest_dump_date = dump_dates[0]

        file_list_dump = [
            file_name
            for file_name in file_list
            if (latest_dump_date in file_name)
            and (file_name.endswith(".xml.gz") and (dump_type in file_name))
        ]

        if not file_list_dump:
            logger.warning(f"No XML files found for dump {latest_dump_date}")

        for file in file_list_dump:
            dumps_to_process.append(
                {
                    "dump_type": dump_type,
                    "latest_dump_date": latest_dump_date,
                    "file": file,
                    "input_file_path": None,
                }
            )

    return dumps_to_process


def get_latest_dump_files_info_s3(file_list: list[str], logger):
    dumps_to_process = []

    dump_types = [extract_dump_type(f) for f in file_list]
    dump_types = set([d for d in dump_types if d])

    if not dump_types:
        logger.error(f"No Discogs dump files found in the defined folder")
        raise RuntimeError(f"No Discogs dump files found in the defined folder")

    for dump_type in dump_types:
        dump_dates = [
            extract_dump_date(f)
            for f in file_list
            if (dump_type in f) and (f.endswith(".xml.gz"))
        ]
        dump_dates = [d for d in dump_dates if d]

        if not dump_dates:
            logger.error(f"No Discogs dump files found in the defined folder")
            raise RuntimeError(f"No Discogs dump files found in the defined folder")

        dump_dates = list(set(dump_dates))
        dump_dates.sort(reverse=True)

        latest_dump_date = dump_dates[0]

        file_list_dump = [
            file_name
            for file_name in file_list
            if (latest_dump_date in file_name)
            and (file_name.endswith(".xml.gz") and (dump_type in file_name))
        ]

        if not file_list_dump:
            logger.warning(f"No XML files found for dump {latest_dump_date}")

        for file in file_list_dump:
            dumps_to_process.append(
                {
                    "dump_type": dump_type,
                    "dump_date": latest_dump_date,
                    "file": file
                }
            )
            
        

    return dumps_to_process
