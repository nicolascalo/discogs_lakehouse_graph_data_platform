from pyspark.sql import SparkSession
import logging
from delta.tables import DeltaTable


from pyspark.sql import DataFrame


def merge_into_delta(
    spark: SparkSession,
    df: DataFrame,
    output_dir: str,
    primary_key: str,
    hash_col: str | None,
    logger: logging.Logger,
) -> None:
    logger.info(
        f"Merging Delta Table into {output_dir} based on {hash_col} and {primary_key}"
    )

    target = DeltaTable.forPath(spark, output_dir)

    if hash_col:
        (
            target.alias("t")
            .merge(
                df.alias("s"),
                f"t.`{primary_key}` = s.`{primary_key}`",
            )
            .whenMatchedUpdateAll(condition=f"t.{hash_col} <> s.{hash_col}")
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
    else:
        (
            target.alias("t")
            .merge(
                df.alias("s"),
                f"t.`{primary_key}` = s.`{primary_key}`",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )

    spark.sql(f"OPTIMIZE delta.`{output_dir}`")
    return None
