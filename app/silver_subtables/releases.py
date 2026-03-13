import pyspark.sql.functions as F
from pyspark.sql.functions import split, explode


def create_releaseArtists_table(df):

    df = (
        df.select("@id", explode("artists.artist").alias("artist"))
        .select("@id", "artist.id", "artist.name")
        .withColumn("track_position", F.lit("All"))
        .withColumn("role", F.lit("Release Artist"))
        .withColumnsRenamed({"id": "artist_id", "name": "artist_name"})
        .withColumnRenamed("@id", "release_id")
    )
    return df


def create_releaseExtraartists_table(df):

    df = (
        df.select(
            F.col("@id").alias("release_id"),
            F.explode("extraartists.artist").alias("artist"),
        )
        .select(
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
        .withColumn("track_title", F.lit("All"))
    )

    return df


def create_releaseTracksartists_table(df):

    df = (
        df.select("@id", explode("tracklist.track").alias("tracks"))
        .select(
            "@id",
            "tracks.position",
            "tracks.duration",
            "tracks.position",
            "tracks.title",
        )
        .select("@id", "position", "duration", "title", "artists.id", "artists.name")
        .withColumn("role", F.lit("Track artist"))
        .withColumnsRenamed(
            {
                "id": "artist_id",
                "title": "track_title",
                "position": "track_position",
                "duration": "track_duration",
                "name": "artist_name",
            }
        )
        .withColumnRenamed("@id", "release_id")
    )
    return df


def create_releaseTracksextraartists_table(df):
    df = (
        df.select("@id", explode("tracklist.track").alias("tracks"))
        .select(
            "@id",
            explode("tracks.extraartists.artist").alias("artists"),
            "tracks.position",
            "tracks.duration",
            "tracks.title",
        )
        .select(
            "@id",
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
        .withColumnRenamed("@id", "release_id")
        .select(
            "release_id",
            "artist_id",
            "artist_name",
            "track_position",
            "track_duration",
            "track_title",
            explode(split("role", ", ")).alias("role"),
        )
    )
    return df
