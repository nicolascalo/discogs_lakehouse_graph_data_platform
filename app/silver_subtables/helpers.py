from pyspark.sql import functions as F

def explode_field(df, field):

    subfield = field[:-1]

    # split CDF
    df_delete = df.filter(F.col("_change_type") == "delete")

    df_upsert = df.filter(F.col("_change_type").isin("insert", "update_postimage"))

    # explode only for upserts
    df_upsert = df_upsert.withColumn(subfield, F.explode_outer(f"{field}_struct.name"))

    # for deletes we keep one row per parent
    df_delete = df_delete.withColumn(subfield, F.lit(None))

    return df_upsert.unionByName(df_delete)

