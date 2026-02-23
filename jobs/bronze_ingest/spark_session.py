from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *


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
