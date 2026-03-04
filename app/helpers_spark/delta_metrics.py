from pyspark.sql import SparkSession
import os
import logging
from pathlib import Path
from pyspark.sql import functions as F
import datetime
import pandas as pd


from pyspark.sql import DataFrame


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



def export_delta_table_history(
    spark: SparkSession,
    dump_type: str,
    latest_dump_date: str,
    input_layer: str,
    output_layer:str,
    output_dir: str,
    table_name:str,
    logger: logging.Logger,
    LOG_DIR: Path,
) -> None:
    """
    Export the latest MERGE or WRITE operation from a Delta table history
    along with all available operationMetrics.
    """
    csv_path = f"{LOG_DIR}/discogs_{output_layer}_{dump_type}_delta_history.csv"
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
        .withColumn("layer_output", F.lit(output_layer))
        .withColumn("table_output", F.lit(dump_type))
        .withColumn("layer_input", F.lit(input_layer))
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
    ).option("path", f"/metrics/{output_layer}").saveAsTable(table_name)
    # Collect metrics for logging
    latest_row_dict = history_selected.collect()[0].asDict()

    logger.info(f"Latest MERGE/WRITE metrics for {dump_type}:")

    metric_keys_rows = [metric for metric in metric_keys if "Rows" in metric]
    for key in metric_keys_rows:
        logger.info(f"  {key}: {latest_row_dict.get(key)}")

    # Optional CSV export
    pdf = history_selected.toPandas()
    if os.path.exists(csv_path):
        pdf_existing = pd.read_csv(csv_path)
        pdf = pd.concat([pdf_existing, pdf], ignore_index=True)
    pdf.to_csv(csv_path, index=False)
    logger.info(f"Delta history exported to {csv_path} with metrics: {metric_keys}")

    return None




def export_delta_table_history_s3(
    spark: SparkSession,
    input_data_table:str,
    output_history_table:str,
    
    dump_type: str,
    dump_date: str,
    input_layer: str,
    output_layer:str,
    logger: logging.Logger,
    LOG_DIR: Path
) -> None:
    """
    Export the latest MERGE or WRITE operation from a Delta table history
    along with all available operationMetrics.
    """
    csv_path = f"{LOG_DIR}/discogs_{output_layer}_{dump_type}_delta_history.csv"
    logger.info(f"Outputting metrics: {csv_path}")

    # Read full Delta history
    history = spark.sql(f"DESCRIBE HISTORY delta.`{input_data_table}`")

    # Keep only MERGE or WRITE operations
    history = history.filter(F.col("operation").isin("MERGE", "WRITE"))

    if history.count() == 0:
        logger.warning(f"No MERGE or WRITE operations found for {input_data_table}")
        return None

    # Take the latest operation by version (guaranteed monotonic)
    latest_history = history.orderBy(F.col("version").desc()).limit(1)

    # Extract operationMetrics keys safely
    rows = latest_history.select("operationMetrics").collect()
    operation_metrics = rows[0]["operationMetrics"] or {}
    metric_keys = list(operation_metrics.keys())

    # Add extra metadata columns
    latest_history = (
        latest_history.withColumn("dump", F.lit(dump_date))
        .withColumn("layer_output", F.lit(output_layer))
        .withColumn("table_output", F.lit(dump_type))
        .withColumn("layer_input", F.lit(input_layer))
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
    ).save(output_history_table)
    
    
    
    # Collect metrics for logging
    latest_row_dict = history_selected.collect()[0].asDict()

    logger.info(f"Latest MERGE/WRITE metrics for {dump_type}:")

    metric_keys_rows = [metric for metric in metric_keys if "Rows" in metric]
    for key in metric_keys_rows:
        logger.info(f"  {key}: {latest_row_dict.get(key)}")

    # Optional CSV export
    pdf = history_selected.toPandas()
    if os.path.exists(csv_path):
        pdf_existing = pd.read_csv(csv_path)
        pdf = pd.concat([pdf_existing, pdf], ignore_index=True)
    pdf.to_csv(csv_path, index=False)
    logger.info(f"Delta history exported to {csv_path} with metrics: {metric_keys}")

    return None
