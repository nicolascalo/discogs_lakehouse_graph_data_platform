from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType, MapType


def apply_hash(
    df: DataFrame,
    primary_key: str,
    latest_dump_date: str,
    hash_col: str,
    col_to_hash: str | None = None,
) -> DataFrame:
    
    

    if not col_to_hash:
        cols_to_hash = sorted(
            [
                c
                for c in df.columns
                if ((c not in ["last_dump_update", primary_key]) or ("hash" not in c))
            ]
        )
    else:
        cols_to_hash = [col_to_hash]



    print(f"{primary_key = }")
    print(f"{cols_to_hash = }")

    def hash_col_expr(c):
        dtype = df.schema[c].dataType

        if isinstance(dtype, (StructType, ArrayType, MapType)):
            col_expr = F.to_json(F.col(c))
        else:
            col_expr = F.col(c).cast("string")

        return F.coalesce(col_expr, F.lit("NULL"))

    df_hashed = df.withColumn(
        hash_col,
        F.sha2(F.concat_ws("||", *[hash_col_expr(c) for c in cols_to_hash]), 256),
    ).withColumn("last_dump_update", F.lit(latest_dump_date))

    return df_hashed
