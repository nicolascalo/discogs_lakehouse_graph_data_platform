from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType

from pyspark.sql import functions as F
from pyspark.sql import types as T


def clean_nested_columns(df):
    """
    Recursively remove '@' from all column names in a DataFrame schema.
    """

    def clean_struct(schema, prefix=None):
        cols = []

        for field in schema.fields:

            old_name = field.name
            new_name = old_name.replace("@", "_")

            col_path = f"{prefix}.{old_name}" if prefix else old_name

            dtype = field.dataType

            if isinstance(dtype, T.StructType):

                nested = clean_struct(dtype, col_path)

                cols.append(
                    F.struct(*nested).alias(new_name)
                )

            elif isinstance(dtype, T.ArrayType) and isinstance(dtype.elementType, T.StructType):

                nested = clean_struct(dtype.elementType, None)

                cols.append(
                    F.transform(
                        F.col(col_path),
                        lambda x: F.struct(
                            *[
                                x[f.name].alias(f.name.replace("@", "_"))
                                for f in dtype.elementType.fields
                            ]
                        )
                    ).alias(new_name)
                )

            else:
                cols.append(F.col(col_path).alias(new_name))

        return cols

    return df.select(*clean_struct(df.schema))

def serialize_for_merge(df, primary_key: str) -> DataFrame:
    """
    Convert complex columns to JSON strings for Delta merge.
    Also store their original types in metadata for later deserialization.
    """
    cols = []
    for c in df.columns:
        
        hash_cols = [c for c in df.columns if "hash" in c]
        if c in [primary_key, hash_cols]:
            cols.append(F.col(c))
        else:
            dtype = df.schema[c].dataType
            if isinstance(dtype, (T.StructType, T.ArrayType, T.MapType)):
                # Convert complex types to JSON
                cols.append(F.to_json(F.col(c)).alias(c))
            else:
                cols.append(F.col(c).cast("string").alias(c))
    return df.select(*cols)

# ============================================================
# 3️⃣ DESERIALIZATION BACK TO ORIGINAL SCHEMA
# ============================================================

def deserialize_json_columns(
    df: DataFrame,
    original_schema: StructType,
) -> DataFrame:
    """
    Rebuild complex columns from JSON strings
    using a previously captured original schema.
    """

    cols = []

    for field in original_schema.fields:
        name = field.name
        dtype = field.dataType

        if name not in df.columns:
            continue

        if isinstance(dtype, (StructType, ArrayType, MapType)):
            cols.append(F.from_json(F.col(name), dtype).alias(name))
        else:
            cols.append(F.col(name).cast(dtype).alias(name))

    return df.select(*cols)


# ============================================================
# 4️⃣ RESOLVE DATATYPE FROM DOT PATH
# ============================================================

def get_field_dtype(schema: StructType, path: str):
    """
    Traverse schema using dot notation and return the DataType
    for nested fields.
    """

    parts = path.split(".")
    current_type = schema

    for part in parts:
        if isinstance(current_type, StructType):
            current_type = current_type[part].dataType

        elif isinstance(current_type, ArrayType):
            current_type = current_type.elementType
            if isinstance(current_type, StructType):
                current_type = current_type[part].dataType
            else:
                raise TypeError(
                    f"Cannot traverse into non-struct array element at {part}"
                )
        else:
            raise TypeError(
                f"Unsupported type encountered while traversing schema: {current_type}"
            )

    return current_type


# ============================================================
# 5️⃣ EXPLODE SINGLE ARRAY-OF-STRUCT COLUMN
# ============================================================

def explode_array_of_struct(df: DataFrame, column_path: str) -> DataFrame:
    """
    Explode a specific array-of-struct column using dot path.
    """

    dtype = get_field_dtype(df.schema, column_path)

    if not isinstance(dtype, ArrayType) or not isinstance(
        dtype.elementType, StructType
    ):
        raise ValueError(f"{column_path} is not ArrayType(StructType)")

    exploded_col = column_path.replace(".", "_") + "_exploded"

    df = df.withColumn(exploded_col, F.explode_outer(F.col(column_path)))

    struct_fields = [
        F.col(f"{exploded_col}.{f.name}").alias(f.name)
        for f in dtype.elementType.fields
    ]

    return (
        df.select("*", *struct_fields)
        .drop(column_path)
        .drop(exploded_col)
    )


# ============================================================
# 6️⃣ RECURSIVELY EXPLODE ALL ARRAY-OF-STRUCT FIELDS
# ============================================================

def recursive_explode_all_arrays(df: DataFrame) -> DataFrame:
    """
    Automatically find and explode all array-of-struct columns recursively.
    """

    def find_array_struct_paths(schema, prefix="") -> List[str]:
        paths = []

        for field in schema.fields:
            field_name = f"{prefix}.{field.name}" if prefix else field.name
            dtype = field.dataType

            if isinstance(dtype, ArrayType) and isinstance(
                dtype.elementType, StructType
            ):
                paths.append(field_name)
                paths.extend(
                    find_array_struct_paths(dtype.elementType, field_name)
                )

            elif isinstance(dtype, StructType):
                paths.extend(find_array_struct_paths(dtype, field_name))

        return paths

    array_paths = find_array_struct_paths(df.schema)

    for path in array_paths:
        df = explode_array_of_struct(df, path)

    return df