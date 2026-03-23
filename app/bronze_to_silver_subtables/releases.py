import pyspark.sql.functions as F
from pyspark.sql.functions import split, explode


def create_releasesTracksArtists_table(df):

    config = {
        "TABLE_TARGET": "releases_releasesTracks",
        "TABLE_DEPENDENCIES": "tracklist",
        "TARGET_PRIMARY_KEY": "release_id",
        "TARGET_SECONDARY_KEYS": ["track_position", "artist_id", "role"],
    }

    df = (
        df.select(
            "root_hash",
            F.col("_id").alias("release_id"),
            F.explode("tracklist.track").alias("tracks"),
        )
        .select(
            "root_hash",
            "release_id",
            F.col("tracks.duration").alias("track_duration"),
            F.col("tracks.title").alias("track_title"),
            F.col("tracks.position").alias("track_position"),
            F.explode("tracks.artists.artist").alias("artist"),
        )
        .select(
            "root_hash",
            "release_id",
            "track_duration",
            "track_title",
            "track_position",
            F.col("artist.id").alias("artist_id"),
            F.col("artist.name").alias("artist_name"),
        )
        .withColumn("role", F.lit("Track Artist"))
        .filter(F.col("artist_id").isNotNull())
        .dropDuplicates([config['TARGET_PRIMARY_KEY'], *config['TARGET_SECONDARY_KEYS'], "role"])
    )

    return {
        "df": df,
    } | config


def create_releasesTracksSubtracksArtists_table(df):
    config = {
        "TABLE_TARGET": "releases_releasesTracks",
        "TABLE_DEPENDENCIES": "tracklist",
        "TARGET_PRIMARY_KEY": "release_id",
        "TARGET_SECONDARY_KEYS": ["track_position", "artist_id"],
    }

    df = (
        df.select(
            "root_hash",
            F.col("_id").alias("release_id"),
            F.col("title").alias("release_title"),
            F.explode("tracklist.track").alias("tracks"),
        )
        .select(
            "root_hash",
            "release_id",
            "release_title",
            F.col("tracks.position").alias("track_position"),
            F.col("tracks.title").alias("track_title"),
            F.col("tracks.duration").alias("track_duration"),
            F.explode("tracks.sub_tracks.track").alias("subtrack"),
        )
        .select(
            "root_hash",
            "release_id",
            "release_title",
            "track_duration",
            "track_title",
            F.col("subtrack.duration").alias("subtrack_duration"),
            F.col("subtrack.position").alias("track_position"),
            F.col("subtrack.title").alias("subtrack_title"),
            F.explode("subtrack.artists.artist").alias("subtrack_artist"),
        )
        .select(
            "root_hash",
            "release_id",
            "release_title",
            "track_duration",
            "track_title",
            "track_position",
            "subtrack_duration",
            "subtrack_title",
            F.col("subtrack_artist.id").alias("artist_id"),
            F.col("subtrack_artist.name").alias("artist_name"),
        )
        .withColumn("role", F.lit("Subtrack Artist"))
        .filter(F.col("artist_id").isNotNull())
        .dropDuplicates([config['TARGET_PRIMARY_KEY'], *config['TARGET_SECONDARY_KEYS'], "role"])
    )

    return {
        "df": df,
    } | config


def create_releasesTracksSubtracksExtraartists_table(df):
    config = {
        "TABLE_TARGET": "releases_releasesTracks",
        "TABLE_DEPENDENCIES": "tracklist",
        "TARGET_PRIMARY_KEY": "release_id",
        "TARGET_SECONDARY_KEYS": ["track_position", "artist_id"],
    }



    df = (
        df.select(
            "root_hash",
            F.col("_id").alias("release_id"),
            F.col("title").alias("release_title"),
            F.explode("tracklist.track").alias("tracks"),
        )
        .select(
            "root_hash",
            "release_id",
            "release_title",
            F.col("tracks.position").alias("track_position"),
            F.col("tracks.title").alias("track_title"),
            F.col("tracks.duration").alias("track_duration"),
            F.explode("tracks.sub_tracks.track").alias("subtrack"),
        )
        .select(
            "root_hash",
            "release_id",
            "release_title",
            "track_duration",
            "track_title",
            F.col("subtrack.duration").alias("subtrack_duration"),
            F.col("subtrack.position").alias("track_position"),
            F.col("subtrack.title").alias("subtrack_title"),
            F.explode("subtrack.extraartists.artist").alias("subtrack_extraartists"),
        )
        .select(
            "root_hash",
            "release_id",
            "release_title",
            "track_duration",
            "track_title",
            "track_position",
            "subtrack_duration",
            "subtrack_title",
            F.col("subtrack_extraartists.id").alias("artist_id"),
            F.col("subtrack_extraartists.name").alias("artist_name"),
            F.explode(F.split("subtrack_extraartists.role", ", ").alias("role")),
        )
        .withColumnRenamed("col", "role")
        .filter(F.col("artist_id").isNotNull())
        .withColumn(
            "role_comments",
            F.when(
                F.col("role").contains("["),
                F.regexp_replace(
                    F.regexp_replace(F.col("role"), "^.*\\[", ""), "\\]", ""
                ),
            ).otherwise(F.lit(None)),
        )
        .withColumn("role", (F.regexp_replace(F.col("role"), " \\[.*", "")))
        .dropDuplicates([config['TARGET_PRIMARY_KEY'], *config['TARGET_SECONDARY_KEYS'], "role","role_comments"])
    )

    return {
        "df": df,
    } | config


def create_releasesArtists_table(df):
    config = {
        "TABLE_TARGET": "releases_releasesTracks",
        "TABLE_DEPENDENCIES": None,
        "TARGET_PRIMARY_KEY": "release_id",
        "TARGET_SECONDARY_KEYS": ["artist_id"],
    }
    df = (
        df.select(
            "root_hash",
            "_id",
            explode("artists.artist").alias("artist"),
        )
        .select("root_hash", "_id", "artist.id", "artist.name")
        .withColumn("track_position", F.lit("All"))
        .withColumn("role", F.lit("Release Artist"))
        .withColumnsRenamed({"id": "artist_id", "name": "artist_name"})
        .withColumnRenamed("_id", "release_id")
        .filter(F.col("artist_id").isNotNull())
        .dropDuplicates([config['TARGET_PRIMARY_KEY'], *config['TARGET_SECONDARY_KEYS'], "role"])
    )

    return {
        "df": df,
    } | config


def create_releasesGenres_table(df):
    config = {
        "TABLE_TARGET": "releases_releasesGenres",
        "TABLE_DEPENDENCIES": None,
        "TARGET_PRIMARY_KEY": "release_id",
        "TARGET_SECONDARY_KEYS": [],
    }
    df = df.select(
        "root_hash",
        F.col("_id").alias("release_id"),
        F.explode("genres.genre").alias("genre"),
    ).dropDuplicates([config['TARGET_PRIMARY_KEY'], *config['TARGET_SECONDARY_KEYS'], "genre"])

    return {
        "df": df,
    } | config


def create_releasesStyles_table(df):
    config = {
        "TABLE_TARGET": "releases_releasesStyles",
        "TABLE_DEPENDENCIES": None,
        "TARGET_PRIMARY_KEY": "release_id",
        "TARGET_SECONDARY_KEYS": [],
    }

    df = df.select(
        "root_hash",
        F.col("_id").alias("release_id"),
        F.explode("styles.style").alias("style"),
    ).dropDuplicates([config['TARGET_PRIMARY_KEY'], *config['TARGET_SECONDARY_KEYS'], "style"])

    return {
        "df": df,
    } | config


def create_releasesRoot_table(df):
    config = {
        "TABLE_TARGET": "releases_releases",
        "TABLE_DEPENDENCIES": None,
        "TARGET_PRIMARY_KEY": "release_id",
        "TARGET_SECONDARY_KEYS": [],
    }

    df = (
        df.select(
            "root_hash",
            "_id",
            "released",
            "title",
            F.col("master_id._is_main_release").alias("_is_main_release"),
            F.col("master_id._text").alias("master_release_id"),
        )
        .withColumnRenamed("_id", "release_id")
        .dropDuplicates([config['TARGET_PRIMARY_KEY'], *config['TARGET_SECONDARY_KEYS'], "master_release_id"])
    )

    return {
        "df": df,
    } | config


def create_releasesExtraartists_table(df):
    config = {
        "TABLE_TARGET": "releases_releasesTracks",
        "TABLE_DEPENDENCIES": "extraartists",
        "TARGET_PRIMARY_KEY": "release_id",
        "TARGET_SECONDARY_KEYS": ["track_position", "artist_id", "role"],
    }

    df = (
        df.select(
            "root_hash",
            F.col("_id").alias("release_id"),
            F.explode("extraartists.artist").alias("artist"),
        )
        .select(
            "root_hash",
            "release_id",
            F.col("artist.id").alias("artist_id"),
            F.col("artist.name").alias("artist_name"),
            F.col("artist.role"),
            F.col("artist.tracks").alias("track_position"),
        )
        .withColumn(
            "track_position",
            F.when(F.col("track_position").isNull(), F.lit("All")).otherwise(
                F.col("track_position")
            ),
        )
        .withColumn("role", F.explode(F.split("role", ", ")))
        .withColumn("track_position", F.explode(F.split("track_position", ", ")))
        .filter(F.col("artist_id").isNotNull())
        .withColumn(
            "role_comments",
            F.when(
                F.col("role").contains("["),
                F.regexp_replace(
                    F.regexp_replace(F.col("role"), "^.*\\[", ""), "\\]", ""
                ),
            ).otherwise(F.lit(None)),
        )
        .withColumn("role", (F.regexp_replace(F.col("role"), " \\[.*", "")))
        .dropDuplicates([config['TARGET_PRIMARY_KEY'], *config['TARGET_SECONDARY_KEYS'], "role","role_comments"])
    )

    return {
        "df": df,
    } | config


def create_releasesTracksExtraartists_table(df):

    config = {
        "TABLE_TARGET": "releases_releasesTracks",
        "TABLE_DEPENDENCIES": "tracklist",
        "TARGET_PRIMARY_KEY": "release_id",
        "TARGET_SECONDARY_KEYS": ["track_position", "artist_id", "role"],
    }

    df = (
        df.select(
            "root_hash",
            "_id",
            explode("tracklist.track").alias("tracks"),
        )
        .select(
            "root_hash",
            "_id",
            explode("tracks.extraartists.artist").alias("artists"),
            "tracks.position",
            "tracks.duration",
            "tracks.title",
        )
        .select(
            "root_hash",
            "_id",
            "position",
            "duration",
            "title",
            "artists.id",
            "artists.name",
            "artists.role",
        )
        .withColumnsRenamed(
            {
                "id": "artist_id",
                "title": "track_title",
                "position": "track_position",
                "duration": "track_duration",
                "name": "artist_name",
            }
        )
        .withColumnRenamed("_id", "release_id")
        .select(
            "root_hash",
            "release_id",
            "artist_id",
            "artist_name",
            "track_position",
            "track_duration",
            "track_title",
            explode(split("role", ", ")).alias("role"),
        )
        .filter(F.col("artist_id").isNotNull())
        .withColumn(
            "role_comments",
            F.when(
                F.col("role").contains("["),
                F.regexp_replace(
                    F.regexp_replace(F.col("role"), "^.*\\[", ""), "\\]", ""
                ),
            ).otherwise(F.lit(None)),
        )
        .withColumn("role", (F.regexp_replace(F.col("role"), " \\[.*", "")))
        .dropDuplicates([config['TARGET_PRIMARY_KEY'], *config['TARGET_SECONDARY_KEYS'], "role","role_comments"])
    )

    return {
        "df": df,
    } | config
