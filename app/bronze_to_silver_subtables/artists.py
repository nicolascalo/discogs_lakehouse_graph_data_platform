from pyspark.sql import functions as F


def create_artistsGroups_table(df):

    config = {
        "TABLE_TARGET": "artists_artistsGroups",
        "TABLE_DEPENDENCIES": None,
        "TARGET_PRIMARY_KEY": "artist_id",
        "TARGET_SECONDARY_KEYS": ["group_id"],
    }

    df = (
        df.withColumn("group", F.explode_outer("groups.name"))
        .select(
            "root_hash",
            F.col("id").cast("long").alias("artist_id"),
            F.col("group._id").cast("long").alias("group_id"),
        )
        .filter(F.col("group_id").isNotNull())
        .dropDuplicates(
            [config["TARGET_PRIMARY_KEY"], *config["TARGET_SECONDARY_KEYS"]]
        )
    )

    return {
        "df": df,
    } | config


def create_artistsGroupsComplete_table(df):

    config = {
        "TABLE_TARGET": "artists_artistsGroupsComplete",
        "TABLE_DEPENDENCIES": None,
        "TARGET_PRIMARY_KEY": "artist_id",
        "TARGET_SECONDARY_KEYS": ["group_id"],
    }
    df = (
        df.withColumn("group", F.explode_outer("groups.name"))
        .select(
            "root_hash",
            F.col("id").cast("long").alias("artist_id"),
            F.col("name").alias("artist_name"),
            F.col("group._id").cast("long").alias("group_id"),
            F.col("group._text").cast("string").alias("group_name"),
        )
        .filter(F.col("group_id").isNotNull())
        .dropDuplicates(
            [config["TARGET_PRIMARY_KEY"], *config["TARGET_SECONDARY_KEYS"]]
        )
    )

    return {
        "df": df,
    } | config


def create_groups_table(df):

    config = {
        "TABLE_TARGET": "artists_groups",
        "TABLE_DEPENDENCIES": None,
        "TARGET_PRIMARY_KEY": "group_id",
        "TARGET_SECONDARY_KEYS": [],
    }
    df = (
        df.withColumn("group", F.explode_outer("groups.name"))
        .select(
            "root_hash",
            F.col("group._id").cast("long").alias("group_id"),
            F.col("group._text").cast("string").alias("group_name"),
        )
        .filter(F.col("group_id").isNotNull())
        .dropDuplicates([config["TARGET_PRIMARY_KEY"]])
    )

    return {
        "df": df,
    } | config


def create_artistsRoot_table(df):

    config = {
        "TABLE_TARGET": "artists_artists",
        "TABLE_DEPENDENCIES": None,
        "TARGET_PRIMARY_KEY": "artist_id",
        "TARGET_SECONDARY_KEYS": [],
    }
    df = df.select(
        "root_hash",
        F.col("id").cast("long").alias("artist_id"),
        F.col("name").cast("string").alias("artist_name"),
    ).dropDuplicates([config["TARGET_PRIMARY_KEY"]])
    return {
        "df": df,
    } | config
