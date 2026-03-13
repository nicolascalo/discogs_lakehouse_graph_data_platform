from pyspark.sql import functions as F


def create_artistsGroups_table(df):

    df = df.select(
        F.col("id").cast("long").alias("artist_id"),
        F.col("group._id").cast("long").alias("group_id"),
        "_change_type",
        F.col("last_dump_update").cast("string"),
        F.col("groups_hash").cast("string"),
    ).filter(F.col("group_id").isNotNull())
    return df


def create_artistsGroupsComplete_table(df):

    df = df.select(
        F.col("id").cast("long").alias("artist_id"),
        F.col("name").alias("artist_name"),
        F.col("group._id").cast("long").alias("group_id"),
        F.col("group._text").cast("string").alias("group_name"),
        F.col("last_dump_update").cast("string"),
        F.col("groups_hash").cast("string"),
    ).filter(F.col("group_id").isNotNull())

    return df


def create_groups_table(df):

    df = (
        df.select(
            F.col("group._id").cast("long").alias("group_id"),
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
    ).distinct()

    return df
