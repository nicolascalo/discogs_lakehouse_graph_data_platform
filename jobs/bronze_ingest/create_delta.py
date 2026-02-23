import logging


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *

from pyspark.sql import DataFrame


def create_new_bronze(df: DataFrame, output_dir,logger:logging.Logger) -> None:

    logger.info("Writing new Delta Table")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        output_dir
    )
    return None
