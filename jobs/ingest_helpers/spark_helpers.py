from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, ArrayType, MapType, DataType
import shutil
import os
import glob
from pyspark.sql.functions import sha2, concat_ws, col

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
    required_cols = [f["alias"] for f in fields if f.get("required")]
    if not required_cols:
        return df
    cond = None
    for c in required_cols:
        c_cond = F.col(c).isNotNull()
        cond = c_cond if cond is None else cond & c_cond
    return df.filter(cond)

def apply_explode(df, explode_path):
    parts = explode_path.split(".")
    col_expr = F.col(parts[0])

    for p in parts[1:]:
        col_expr = col_expr.getField(p)

    # First explode
    df = df.withColumn("element", F.explode_outer(col_expr))

    # If the exploded element is still an array → explode again
    if dict(df.schema["element"].dataType.jsonValue())["type"] == "array":
        df = df.withColumn("element", F.explode_outer("element"))

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
    from pyspark.sql import functions as F

    src = cfg["source"]
    sep = cfg.get("separator", ",")
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

    return df.withColumn(alias, F.explode_outer(F.split(col_expr, sep)))



def output_single_json(df, output_path):

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .json(f"{output_path}.json_folder")
    )

    json_folder = f"{output_path}.json_folder"
    output_file = f"{output_path}.json"

    # Spark wrote part-00000-xxxx.json inside the folder
    part_file = glob.glob(os.path.join(json_folder, "part-*.json"))[0]

    # Move/rename
    shutil.move(part_file, output_file)

    # Remove the temporary folder
    shutil.rmtree(json_folder)

    return None


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
                    df = flatten_nested_arrays(df.select(F.col(sub_name).alias(f"{name}_{f.name}")))
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
                return F.transform(col_expr, lambda x: wrap_struct_fields(x, dtype.elementType))
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
            fields.append(wrap_field(col_expr.getField(f.name), f.dataType).alias(f.name))
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
                return F.transform(col_expr, lambda x: wrap_struct_fields(x, dtype.elementType))
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
    return df.withColumn(
        "_row_hash",
        sha2(concat_ws("||", *[col(c).cast("string") for c in cols if c in df.columns]), 256)
    )
