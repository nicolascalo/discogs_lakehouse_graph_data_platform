from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, ArrayType, MapType, DataType
from pyspark.sql.functions import sha2, concat_ws, col
from delta.tables import DeltaTable
import re

## Spark helpers


def select_nested_fields(df: DataFrame, fields: list) -> DataFrame:
    selected = []
    for f in fields:
        if "." in f:
            # nested field
            parts = f.split(".")
            col_expr = F.col(parts[0])
            for p in parts[1:]:
                col_expr = col_expr.getItem(p) if "[" in p else col_expr.getField(p)
            selected.append(col_expr.alias(f.replace(".", "_")))
        else:
            selected.append(F.col(f))
    return df.select(*selected)


def enforce_required(df, fields):
    """
    Filter dataframe to keep only rows where all required fields are:
    - not null
    - arrays are not empty
    - structs are not empty (all fields null)
    
    Args:
        df: Spark DataFrame
        fields: list of dicts, each containing 'alias' and optional 'required'
    """
    required_cols = [f["alias"] for f in fields if f.get("required")]
    if not required_cols:
        return df

    def col_not_empty(col_expr, dtype):
        """Return a condition that the column is not empty / null."""
        if isinstance(dtype, StructType):
            # A struct is considered empty if all its fields are null
            field_conds = [col_not_empty(col_expr.getField(f.name), f.dataType) for f in dtype.fields]
            return F.reduce(lambda a, b: a | b, field_conds)
        elif isinstance(dtype, ArrayType):
            # Array is not empty if not null and size > 0
            return (col_expr.isNotNull()) & (F.size(col_expr) > 0)
        else:
            # Primitive: just not null
            return col_expr.isNotNull()

    # Build condition for all required columns
    cond = None
    for c in required_cols:
        dtype = df.schema[c].dataType
        c_cond = col_not_empty(F.col(c), dtype)
        cond = c_cond if cond is None else cond & c_cond

    return df.filter(cond)


def extract_required_roots(fields, explode_path=None, post_split_cfg=None):
    roots = set()

    post_alias = None
    if post_split_cfg:
        post_alias = post_split_cfg.get("alias", "element")

    for f in fields:
        if "source" not in f:
            continue

        src = f["source"]

        # ❌ derived later
        if src == "element":
            continue
        if src.startswith("element."):
            continue
        if post_alias and src == post_alias:
            continue

        roots.add(src.split(".")[0])

    # always keep hash
    roots.add("_og_row_hash")

    # keep explode root
    if explode_path:
        roots.add(explode_path.split(".")[0])

    return list(roots)




def apply_explode(df, explode_path, explode_alias="element"):
    parts = explode_path.split(".")

    col_expr = F.col(parts[0])
    for p in parts[1:]:
        col_expr = col_expr.getField(p)

    df = df.withColumn(explode_alias, F.explode_outer(col_expr))

    # If still array → explode again
    while isinstance(df.schema[explode_alias].dataType, F.ArrayType):
        df = df.withColumn(explode_alias, F.explode_outer(explode_alias))

    return df


def apply_explode_old(df, explode_path):
    parts = explode_path.split(".")
    col_expr = F.col(parts[0])
    for p in parts[1:]:
        col_expr = col_expr.getField(p)

    # SAFE explode
    return df.withColumn("element", F.explode_outer(col_expr))


def apply_split_explode(df, cfg, alias="element"):
    from pyspark.sql import functions as F

    src = cfg["source"]
    sep = cfg.get("separator", ",")

    parts = src.split(".")
    col_expr = F.col(parts[0])
    for p in parts[1:]:
        col_expr = col_expr.getField(p)

    return df.withColumn(alias, F.explode_outer(F.split(col_expr, sep)))


def select_fields_with_alias(df, fields, explode_alias="element"):
    cols = []

    for f in fields:
        alias = f["alias"]

        # fixed literal (always overwrite)
        if "value" in f:
            cols.append(F.lit(f["value"]).alias(alias))
            continue

        src = f["source"]
        parts = src.split(".")

        if parts[0] == explode_alias:
            col_expr = F.col(explode_alias)
            for p in parts[1:]:
                col_expr = col_expr.getField(p)
        else:
            col_expr = F.col(parts[0])
            for p in parts[1:]:
                col_expr = col_expr.getField(p)

        if "default" in f:
            col_expr = F.coalesce(col_expr, F.lit(f["default"]))

        cols.append(col_expr.alias(alias))

    return df.select(*cols)


def apply_post_split_explode(df, cfg, explode_alias="element"):
    src = cfg["source"]
    sep = cfg.get("separator", ",")

    # escape regex metacharacters
    sep_regex = re.escape(sep)

    alias = cfg.get("alias", "element")

    parts = src.split(".")
    if parts[0] == explode_alias:
        col_expr = F.col(explode_alias)
        for p in parts[1:]:
            col_expr = col_expr.getField(p)
    else:
        col_expr = F.col(parts[0])
        for p in parts[1:]:
            col_expr = col_expr.getField(p)

    return df.withColumn(
        alias,
        F.explode_outer(F.split(col_expr, sep_regex))
    )


def normalize_arrays(df):
    """Wrap any struct as array to enforce array consistency."""
    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType

        # Wrap struct into array
        if isinstance(dtype, StructType):
            df = df.withColumn(name, F.array(F.col(name)))

        # Flatten array-of-arrays
        elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, ArrayType):
            df = df.withColumn(name, F.flatten(F.col(name)))

        # Recurse into structs?
        # Optional: you can handle nested structs/arrays recursively here if needed
    return df


def flatten_nested_arrays(df):
    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType

        if isinstance(dtype, ArrayType):
            # Check for array of array
            if isinstance(dtype.elementType, ArrayType):
                df = df.withColumn(name, F.flatten(F.col(name)))
            # If array of struct, recurse inside structs if needed
            if isinstance(dtype.elementType, StructType):
                for f in dtype.elementType.fields:
                    sub_name = f"{name}.{f.name}"
                    # recursion for nested arrays
                    df = flatten_nested_arrays(
                        df.select(F.col(sub_name).alias(f"{name}_{f.name}"))
                    )
    return df


def wrap_top_level_structs(df):
    """Wrap all top-level structs into arrays. Arrays stay arrays."""
    for field in df.schema.fields:
        name = field.name
        dtype = field.dataType

        if isinstance(dtype, StructType):
            # Wrap struct as array
            df = df.withColumn(name, F.array(F.col(name)))
        elif isinstance(dtype, ArrayType):
            # Keep array as is (don't wrap again)
            continue
        # primitives (String, Int, etc.) are untouched
    return df


def wrap_structs(df: DataFrame) -> DataFrame:
    """
    Wrap all StructType columns into arrays, but leave ArrayType columns untouched.
    Works recursively on nested structs, but avoids double arrays.
    """

    def wrap(col_expr, dtype):
        if isinstance(dtype, StructType):
            # wrap struct in array
            return F.array(col_expr)
        elif isinstance(dtype, ArrayType):
            # If array of structs, leave as-is
            return col_expr
        else:
            # primitives, leave as-is
            return col_expr

    for field in df.schema.fields:
        df = df.withColumn(field.name, wrap(F.col(field.name), field.dataType))
    return df


def wrap_structs_recursively(df: DataFrame) -> DataFrame:
    """
    Recursively wrap all StructType columns into ArrayType<Struct>,
    but leave existing ArrayType and primitive types untouched.
    Works for top-level and nested fields.
    """

    def wrap_field(col_expr, dtype):
        if isinstance(dtype, StructType):
            # wrap struct in array
            return F.array(col_expr)
        elif isinstance(dtype, ArrayType):
            # recurse into ArrayType element if it's StructType
            if isinstance(dtype.elementType, StructType):
                # map over array of structs recursively
                return F.transform(
                    col_expr, lambda x: wrap_struct_fields(x, dtype.elementType)
                )
            else:
                return col_expr  # leave primitives or array of primitives as is
        elif isinstance(dtype, MapType):
            # leave maps as is
            return col_expr
        else:
            return col_expr  # primitive types

    def wrap_struct_fields(col_expr, struct_type):
        """
        Apply wrap_field to each field of the struct.
        Returns a struct with possibly wrapped nested fields.
        """
        fields = []
        for f in struct_type.fields:
            fields.append(
                wrap_field(col_expr.getField(f.name), f.dataType).alias(f.name)
            )
        return F.struct(*fields)

    for field in df.schema.fields:
        df = df.withColumn(field.name, wrap_field(F.col(field.name), field.dataType))

    return df


def wrap_all_structs(df: DataFrame) -> DataFrame:
    """
    Recursively wrap any StructType into ArrayType<Struct>, including nested structs
    inside arrays, but leave existing Array<Struct> untouched.
    """

    def wrap_field(col_expr, dtype: DataType):
        if isinstance(dtype, StructType):
            # Plain Struct: wrap in array
            return F.array(wrap_struct_fields(col_expr, dtype))
        elif isinstance(dtype, ArrayType):
            if isinstance(dtype.elementType, StructType):
                # Array of structs: recursively wrap nested structs inside
                return F.transform(
                    col_expr, lambda x: wrap_struct_fields(x, dtype.elementType)
                )
            else:
                # Array of primitives: leave as is
                return col_expr
        elif isinstance(dtype, MapType):
            return col_expr  # leave maps as is
        else:
            return col_expr  # leave primitives as is

    def wrap_struct_fields(col_expr, struct_type: StructType):
        """
        Recursively wrap fields inside a StructType.
        Returns a Struct with possibly wrapped nested structs.
        """
        fields = []
        for f in struct_type.fields:
            nested_col = wrap_field(col_expr.getField(f.name), f.dataType).alias(f.name)
            fields.append(nested_col)
        return F.struct(*fields)

    for field in df.schema.fields:
        df = df.withColumn(field.name, wrap_field(F.col(field.name), field.dataType))

    return df


def with_row_hash(df, cols):
    print(f"{cols = }")
    print(f"{df.columns = }")
    cols_found = [c for c in cols if c in df.columns]
    print(f"{cols_found = }")
    if not cols_found:
        raise ValueError(f"No hash columns found in df: {cols}")

    cols_found = sorted(cols_found)
    print(cols_found)

    return df.withColumn(
        "_og_row_hash",
        sha2(concat_ws("||", *[col(c).cast("string") for c in cols_found]), 256),
    )

def spark_upsert_based_on_hashes_with_stats(
    spark,
    spark_df: DataFrame,
    primary_key: str,
    output_path: str,
    metadata: dict[str, str] | None = None
):
    """
    Upsert a Delta table based on row hashes, returning stats:
    - updated: rows whose _og_row_hash changed
    - not_modified: rows unchanged
    - inserted: new rows
    Optimized for large tables: avoids full scans by only selecting primary_key + _og_row_hash.
    """

    delta_table = DeltaTable.forPath(spark, output_path)

    # ---- commit metadata ----
    if metadata:
        spark.conf.set(
            "spark.databricks.delta.commitInfo.userMetadata",
            ";".join([f"{k}={v}" for k, v in metadata.items()])
        )

    # ---- select only PK + hash ----
    existing_hash_df = delta_table.toDF().select(primary_key, "_og_row_hash")

    # count total rows before
    count_before = existing_hash_df.count()  # cheap if table is partitioned and small metadata

    # ---- compute updated / not_modified ----
    # join only on PK + hash
    joined = existing_hash_df.alias("t").join(
        spark_df.select(primary_key, "_og_row_hash").alias("s"),
        on=primary_key,
        how="left"
    )

    updated = joined.filter(F.col("t._og_row_hash") != F.col("s._og_row_hash")).count()
    not_modified = count_before - updated

    # ---- inserted: new rows ----
    # left anti join: rows in spark_df not in existing
    inserted = spark_df.join(existing_hash_df, on=primary_key, how="left_anti").count()

    # ---- merge into Delta ----
    delta_table.alias("t").merge(
        spark_df.alias("s"),
        f"t.`{primary_key}` = s.`{primary_key}`"
    ).whenMatchedUpdateAll(condition="t._og_row_hash <> s._og_row_hash") \
     .whenNotMatchedInsertAll().execute()

    # total after merge (optional, approximate)
    total = count_before + inserted  # avoids full table scan

    return {
        "count_before": count_before,
        "updated": updated,
        "not_modified": not_modified,
        "inserted": inserted,
        "total": total,
        "delta_table": delta_table
    }


def spark_create_df_with_stats(
    spark, spark_df, output_path: str, metadata: dict[str, str] | None = None
):
    """
    Write a new Delta table (overwrite mode) and return ingestion stats:
    count_before, updated, not_modified, inserted, total, delta_table.
    """

    # Commit metadata if provided
    if metadata:
        meta_str = ";".join([f"{k}={v}" for k, v in metadata.items()])
        spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", meta_str)

    # Write Delta table
    spark_df.write.format("delta").mode("overwrite").save(output_path)

    # Load DeltaTable object
    delta_table = DeltaTable.forPath(spark, output_path)
    total = delta_table.toDF().count()

    # For new table:
    count_before = 0
    updated = 0
    not_modified = 0
    inserted = total  # all rows are “inserted”

    return {
        "count_before": count_before,
        "updated": updated,
        "not_modified": not_modified,
        "inserted": inserted,
        "total": total,
        "delta_table": delta_table,
    }



def spark_replace_by_hash(
    spark,
    df_new: DataFrame,
    primary_key: str,
    output_path: str,
    metadata: dict[str,str] | None = None
):
    from delta.tables import DeltaTable

    delta_table = DeltaTable.forPath(spark, output_path)
    current_df = delta_table.toDF()
    count_before = current_df.count()

    # Map primary key → hash from new data
    new_hash_df = df_new.select(primary_key, "_og_row_hash").distinct()

    # Delete old rows whose primary key exists and hash differs
    delta_table.alias("t").merge(
        new_hash_df.alias("s"),
        f"t.`{primary_key}` = s.`{primary_key}` AND t._og_row_hash <> s._og_row_hash"
    ).whenMatchedDelete().execute()

    # Append all new rows (safe, duplicate PKs already deleted)
    df_new.write.format("delta").mode("append").save(output_path)

    count_after = delta_table.toDF().count()
    inserted = count_after - count_before
    updated = count_before - delta_table.toDF().count() + inserted  # optional calculation
    not_modified = count_before - updated

    # Commit metadata if provided
    if metadata:
        spark.conf.set(
            "spark.databricks.delta.commitInfo.userMetadata",
            ";".join([f"{k}={v}" for k, v in metadata.items()])
        )

    return {
        "count_before": count_before,
        "updated": updated,
        "not_modified": not_modified,
        "inserted": inserted,
        "total": count_after,
        "delta_table": delta_table
    }
