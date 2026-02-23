"""
Minimal test to debug the enrichment issue
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("minimal-debug") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 80)
print("MINIMAL DEBUG TEST")
print("=" * 80)

# Simplest possible test case
sample_data = [
    {
        "@id": 1001,
        "artists": {
            "artist": [
                {"id": 101, "name": "Artist One"}
            ]
        }
    }
]

df = spark.createDataFrame(sample_data)

print("\n" + "=" * 80)
print("ORIGINAL SCHEMA")
print("=" * 80)
df.printSchema()

print("\n" + "=" * 80)
print("ORIGINAL DATA")
print("=" * 80)
df.show(truncate=False)

print("\n" + "=" * 80)
print("TEST 1: Manual struct rebuild (should work)")
print("=" * 80)

# Manually rebuild artists.artist with new field
df_manual = df.withColumn(
    "artists",
    F.struct(
        F.transform(
            F.col("artists.artist"),
            lambda x: F.struct(
                x["id"].alias("id"),
                x["name"].alias("name"),
                F.lit("Release artist").alias("role")
            )
        ).alias("artist")
    )
)

print("MANUAL ENRICHMENT SCHEMA:")
df_manual.printSchema()

print("\nMANUAL ENRICHMENT DATA:")
df_manual.select(
    F.col("@id"),
    F.explode("artists.artist").alias("artist")
).select(
    "@id",
    "artist.id",
    "artist.name",
    "artist.role"
).show(truncate=False)

print("\n" + "=" * 80)
print("TEST 2: Using apply_enrich")
print("=" * 80)

from spark_processing_helpers3 import apply_enrich

enrich_configs = [
    {
        "path": "artists.artist",
        "name": "role",
        "expr": "'Release artist'"
    }
]

df_enriched = apply_enrich(df, enrich_configs)

print("\nENRICHED SCHEMA:")
df_enriched.printSchema()

print("\nENRICHED DATA:")
try:
    df_enriched.select(
        F.col("@id"),
        F.explode("artists.artist").alias("artist")
    ).select(
        "@id",
        "artist.id",
        "artist.name",
        "artist.role"
    ).show(truncate=False)
except Exception as e:
    print(f"ERROR: {e}")
    print("\nTrying to show full row:")
    df_enriched.show(truncate=False)

print("\n" + "=" * 80)
print("TEST 3: Check what artists column looks like")
print("=" * 80)
df_enriched.select("artists").show(truncate=False)
df_enriched.select(F.col("artists").cast("string")).show(truncate=False)

spark.stop()