from pyspark.sql import SparkSession
import logging
from delta.tables import DeltaTable


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *

from pyspark.sql import DataFrame


def merge_into_bronze(
    spark: SparkSession,
    df: DataFrame,
    output_dir: str,
    primary_key: str,
    logger: logging.Logger,
) -> None:
    logger.info("Merging Delta Table into Bronze")

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
