from pyspark.sql import SparkSession
import re
import logging


from pyspark.sql import DataFrame


def read_xml_to_df(
    spark: SparkSession, dump_type: str, input_file_path: str, logger: logging.Logger
) -> DataFrame:

    row_tag = re.sub("s$", "", dump_type)
    logger.info(f"Importing xml {input_file_path} to Delta Table")
    df = (
        spark.read.format("xml")
        .option("rowTag", row_tag)
        .option("ignoreSurroundingSpaces", True)
        .option("attributecatalog", "@")
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
