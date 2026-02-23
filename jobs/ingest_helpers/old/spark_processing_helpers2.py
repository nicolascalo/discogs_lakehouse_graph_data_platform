from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *
import re



def select_tree(df, tree, prefix=""):
    cols = []

    for name, children in tree.items():
        path = f"{prefix}.{name}" if prefix else name
        dtype = get_dtype(df.schema, path)

        # Field missing → null placeholder
        if isinstance(dtype, NullType):
            cols.append(F.lit(None).alias(name))
            continue

        # Leaf node → select directly
        if not children:
            cols.append(F.col(path).alias(name))
            continue

        # Struct with children
        if isinstance(dtype, StructType):
            subcols = select_tree(df, children, path)
            cols.append(F.struct(*subcols).alias(name))

        # Array<Struct> with children
        elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
            child_names = list(children.keys())
            cols.append(
                F.transform(
                    F.col(path),
                    lambda x: F.struct(*[
                        # safe access inside element
                        x[c] if c in [f.name for f in dtype.elementType.fields]
                        else F.lit(None)
                        for c in child_names
                    ])
                ).alias(name)
            )

        # Other type → just select
        else:
            cols.append(F.col(path).alias(name))

    return cols



def homogenize(df, paths):
    for path in paths:
        dtype = get_dtype(df.schema, path)
        df = df.withColumn(path, normalize(F.col(path), dtype))
    return df




def infer_dtype_by_name(field_name: str):
    """
    Infer canonical type based on field name.
    Fields ending with '@id' or 'id' -> LongType, else StringType.
    """
    if field_name.lower().endswith("@id") or field_name.lower().endswith("id"):
        return LongType()
    return StringType()


def normalize_tree(df, max_depth=20):
    """
    Iteratively normalize a DataFrame tree, safe for arrays, structs, missing fields.
    Automatically casts '@id' and 'id' fields to LongType, others to StringType.
    """
    """
    Iteratively normalize a DataFrame tree, safe for arrays, structs, missing fields.
    Automatically casts '@id' and 'id' fields to LongType, others to StringType.
    """
    queue = [("", df.schema, 0)]
    visited = set()

    while queue:
        prefix, dtype, depth = queue.pop(0)
        if depth > max_depth:
            continue

        if isinstance(dtype, ArrayType):
            elem_type = dtype.elementType
            if isinstance(elem_type, StructType):
                for field in elem_type.fields:
                    path = f"{prefix}.{field.name}" if prefix else field.name
                    key = (path, str(field.dataType))
                    if key not in visited:
                        visited.add(key)
                        queue.append((path, field.dataType, depth + 1))
            continue

        if isinstance(dtype, StructType):
            for field in dtype.fields:
                path = f"{prefix}.{field.name}" if prefix else field.name
                key = (path, str(field.dataType))
                if key in visited:
                    continue
                visited.add(key)

                try:
                    field_dtype_actual = get_dtype(df.schema, path)
                except:
                    field_dtype_actual = StringType()

                # null → empty array
                if isinstance(field_dtype_actual, NullType):
                    df = df.withColumn(path, F.array())
                    continue

                canonical_dtype = infer_dtype_by_name(field.name)
                col_safe = F.col(path).cast(canonical_dtype.simpleString())
                df = df.withColumn(
                    path,
                    F.when(col_safe.isNull(), F.array()).otherwise(F.array(F.struct(col_safe.alias("value"))))
                )

                # enqueue deeper fields
                if isinstance(field_dtype_actual, StructType):
                    queue.append((path, field_dtype_actual, depth + 1))
                elif isinstance(field_dtype_actual, ArrayType):
                    queue.append((path, field_dtype_actual.elementType, depth + 1))
            continue

        # Scalar → wrap into array
        canonical_dtype = infer_dtype_by_name(prefix.split(".")[-1])
        col_safe = F.col(prefix).cast(canonical_dtype.simpleString())
        df = df.withColumn(prefix, F.when(col_safe.isNull(), F.array()).otherwise(F.array(F.struct(col_safe.alias("value")))))

    return df

def explode_nested(df, path, parent_cols, table_name, root_hash_col="h_root"):
    """
    Explodes nested arrays sequentially and generates:
    - synthetic unique IDs
    - subtree hash
    """
    parts = path.split(".")
    current_df = df
    current_path = []

    for i, part in enumerate(parts):
        current_path.append(part)
        col_path = ".".join(current_path)

        # Check type
        dtype = current_df.schema[col_path.split(".")[0]].dataType
        for p in col_path.split(".")[1:]:
            if isinstance(dtype, StructType):
                dtype = next(f.dataType for f in dtype.fields if f.name == p)
            elif isinstance(dtype, ArrayType):
                dtype = dtype.elementType

        if not isinstance(dtype, ArrayType):
            continue  # nothing to explode here

        exploded_col = "_".join(current_path) + "_exploded"
        current_df = current_df.withColumn(exploded_col, F.explode_outer(F.col(col_path)))

        # synthetic UID
        current_df = current_df.withColumn(
            f"{table_name}_uid",
            F.xxhash64(F.struct(*(parent_cols + [exploded_col])))
        )

        current_path[-1] = exploded_col

    final_col = current_path[-1]
    exploded_df = current_df.select(*parent_cols, final_col).withColumnRenamed(final_col, "data")
    exploded_df = exploded_df.withColumn(f"h_{table_name}", F.xxhash64(F.col("data")))
    return exploded_df



def get_dtype(schema, path: str):
    """
    Returns the DataType of a dotted path inside a Spark schema.
    Works through StructType and ArrayType(elementType).
    """
    parts = path.split(".")
    current = schema

    for p in parts:
        if isinstance(current, StructType):
            field = next((f for f in current.fields if f.name == p), None)
            if field is None:
                return NullType()
            current = field.dataType

        elif isinstance(current, ArrayType):
            current = current.elementType
            if isinstance(current, StructType):
                field = next((f for f in current.fields if f.name == p), None)
                if field is None:
                    return NullType()
                current = field.dataType
            else:
                return current

        else:
            return NullType()

    return current




def build_tree(paths):
    tree = {}
    for path in paths:
        node = tree
        for part in path.split("."):
            node = node.setdefault(part, {})
    return tree


def sort_tree(df, max_depth=20):
    """
    Recursively sorts all array<struct> in the dataframe tree
    in a safe iterative way to avoid RecursionError.
    """
    queue = [("", df.schema, 0)]
    visited = set()

    while queue:
        prefix, schema, depth = queue.pop(0)

        if depth > max_depth:
            continue

        for field in schema.fields:
            path = f"{prefix}.{field.name}" if prefix else field.name
            key = (path, str(field.dataType))

            if key in visited:
                continue
            visited.add(key)

            dtype = field.dataType

            # array<struct> → sort array
            if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
                df = df.withColumn(path, F.sort_array(F.col(path)))
                queue.append((path, dtype.elementType, depth + 1))

            # struct → enqueue children
            elif isinstance(dtype, StructType):
                queue.append((path, dtype, depth + 1))

    return df




def split_nested(df, path, target_name, delimiter):
    parts = path.split(".")

    def recurse_from_root(prefix, parts_left):
        head, *tail = parts_left
        full = f"{prefix}.{head}" if prefix else head
        dtype = get_dtype(df.schema, full)

        # 🔹 LEAF
        if not tail:
            return F.split(F.col(full).cast(StringType()), delimiter)

        # 🔹 STRUCT
        if isinstance(dtype, StructType):
            return F.struct(*[
                recurse_from_root(full, tail).alias(f.name) if f.name == tail[0]
                else F.col(f"{full}.{f.name}")
                for f in dtype.fields
            ])

        # 🔹 ARRAY (may be array<array<struct>>)
        if isinstance(dtype, ArrayType):
            return F.transform(
                F.col(full),
                lambda x: recurse_from_struct(x, dtype.elementType, tail)
            )

        # 🔹 fallback
        return F.split(F.col(full).cast(StringType()), delimiter)

    def recurse_from_struct(col, dtype, parts_left):

        # 🔹 unwrap array layers
        if isinstance(dtype, ArrayType):
            return F.transform(
                col,
                lambda x: recurse_from_struct(x, dtype.elementType, parts_left)
            )

        # 🔹 scalar fallback
        if not isinstance(dtype, StructType):
            return F.split(col.cast(StringType()), delimiter)

        head, *tail = parts_left

        # 🔹 leaf inside struct
        if not tail:
            return F.struct(*[
                F.split(col[f.name].cast(StringType()), delimiter).alias(target_name)
                if f.name == head else col[f.name]
                for f in dtype.fields
            ])

        # 🔹 recursive struct rebuild
        return F.struct(*[
            recurse_from_struct(col[f.name], f.dataType, tail).alias(f.name)
            if f.name == head else col[f.name]
            for f in dtype.fields
        ])

    top = parts[0]
    df = df.withColumn(top, recurse_from_root("", parts).alias(top))
    return df





