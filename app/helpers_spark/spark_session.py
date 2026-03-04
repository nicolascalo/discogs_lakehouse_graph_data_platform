from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session_local(app_name: str) -> SparkSession:

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




def create_spark_session(
    app_name: str, endpoint: str, access_key: str, secret_key: str
) -> SparkSession:

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.catalog.unity",
            "org.apache.spark.sql.connector.catalog.rest.RestCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark
