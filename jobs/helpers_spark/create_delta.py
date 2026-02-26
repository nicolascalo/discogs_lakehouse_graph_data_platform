import logging


from pyspark.sql import DataFrame


def create_new_bronze(df: DataFrame, output_dir,table_name:str,  logger: logging.Logger) -> None:

    logger.info("Writing new Delta Table")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", output_dir).saveAsTable(table_name)
    return None


