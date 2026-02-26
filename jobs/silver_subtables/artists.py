from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType


def define_group_schema():
    schema = StructType(
        [
            StructField(
                "name",
                ArrayType(
                    StructType(
                        [
                            StructField("@id", LongType()),
                            StructField("_text", StringType()),
                        ]
                    )
                ),
            )
        ]
    )
    return schema


def explode_field(df, field, schema):

    subfield = field[:-1]

    return df.withColumn(f"{field}_struct", F.from_json(field, schema)).withColumn(
        subfield, F.explode_outer(f"{field}_struct.name")
    )


def create_artists_groups_table(df):

    groups_schema = define_group_schema()

    df = explode_field(df, "groups", groups_schema)

    df = df.select(
        F.col("id").cast("long").alias("artist_id"),
        F.col("group.@id").cast("long").alias("group_id"),
        F.col("last_dump_update").cast("string"),
        F.col("groups_hash").cast("string"),
    ).filter(F.col("group_id").isNotNull())
    return df


def create_artists_groups_table_complete(df):

    groups_schema = define_group_schema()

    df = explode_field(df, "groups", groups_schema)

    df = df.select(
        F.col("id").cast("long").alias("artist_id"),
        F.col("name").alias("artist_name"),
        F.col("group.@id").cast("long").alias("group_id"),
        F.col("group._text").cast("string").alias("group_name"),
        F.col("last_dump_update").cast("string"),
        F.col("groups_hash").cast("string"),
    ).filter(F.col("group_id").isNotNull())

    return df


def create_groups_table(df):

    groups_schema = define_group_schema()

    df = explode_field(df, "groups", groups_schema)

    df = (
        df.select(
            F.col("group.@id").cast("long").alias("group_id"),
            F.col("group._text").cast("string").alias("group_name"),
            F.col("last_dump_update").cast("string"),
            F.col("groups_hash").cast("string"),
        )
        .filter(F.col("group_id").isNotNull())
        .distinct()
    )

    return df


def create_artists_table(df):

    df = df.select(
        F.col("id").cast("long").alias("artist_id"),
        F.col("name").cast("string").alias("artist_name"),
    )

    return df
