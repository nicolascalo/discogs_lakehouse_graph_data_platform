from pyspark.sql import SparkSession
import re
import logging
from pyspark.sql import functions as F


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *

from pyspark.sql import DataFrame


def cast_non_key_columns_to_string(df, primary_key: str, hash_col: str) -> DataFrame:
    cols = []

    for c in df.columns:
        if c in [primary_key, hash_col]:
            cols.append(F.col(c))
        else:
            cols.append(F.col(c).cast("string").alias(c))

    return df.select(*cols)


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


def read_xml_to_df(
    spark: SparkSession, dump_type: str, input_file_path: str, logger: logging.Logger
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
