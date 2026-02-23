# ingest_helpers/spark_processing_helpers_keep_full.py
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# -----------------------------
# Build tree from top-level keep list, auto-expanding structs and arrays
# -----------------------------

def build_keep_tree(df, keep_list):
    """
    Build a tree of columns to keep.
    - keep_list: top-level column names
    - auto-expands structs and arrays of structs recursively
    Returns dict representing tree.
    """
    schema = df.schema
    tree = {}
    logger.info(f"Building keep tree from top-level columns: {keep_list}")

    for col_name in keep_list:
        field = schema[col_name] if col_name in schema.fieldNames() else None
        if field is None:
            logger.warning(f"Column {col_name} not in schema, skipping")
            continue
        tree[col_name] = _expand_field(field)
    logger.info(f"Built keep tree: {tree}")
    return tree


def _expand_field(field):
    """
    Recursively expand struct/array types into dict tree.
    Scalars return empty dict.
    """
    if isinstance(field.dataType, StructType):
        logger.info(f"Expanding struct field: {field.name}")
        subtree = {}
        for subfield in field.dataType.fields:
            subtree[subfield.name] = _expand_field(subfield)
        return subtree

    elif isinstance(field.dataType, ArrayType):
        # If array of struct, expand struct
        element_type = field.dataType.elementType
        if isinstance(element_type, StructType):
            logger.info(f"Expanding array of struct field: {field.name}")
            subtree = {}
            for subfield in element_type.fields:
                subtree[subfield.name] = _expand_field(subfield)
            return subtree
        else:
            # array of scalar
            return {}
    else:
        # scalar
        return {}


# -----------------------------
# Select tree recursively
# -----------------------------
def select_tree(df, tree, parent_path=""):
    """
    Recursively build a list of Column objects for df.select() based on the tree.
    """
    from pyspark.sql import functions as F
    cols = []
    for key, subtree in tree.items():
        current_path = f"{parent_path}.{key}" if parent_path else key
        if subtree:
            # nested struct or array
            cols.append(F.col(current_path))
        else:
            # scalar
            cols.append(F.col(current_path))
    return cols


# -----------------------------
# Export dataframe as JSON for review
# -----------------------------
def output_single_json(df, path):
    logger.info(f"Exporting JSON to: {path}")
    df.write.mode("overwrite").json(path)
    logger.info("Export completed.")


# -----------------------------
# Utility: convert scalar to array of struct
# -----------------------------
def to_array_of_struct(col, struct_type):
    """
    Converts a scalar or array column to array of structs
    """
    logger.info(f"Converting column {col} to array of struct: {struct_type}")
    return F.when(F.col(col).isNull(), F.array().cast(ArrayType(struct_type))) \
            .when(F.col(col).dtype == "array", F.col(col)) \
            .otherwise(F.array(F.col(col).cast(struct_type)))


# -----------------------------
# Step 1: Root hash
# -----------------------------



def compute_root_hash(df, hash_col="root_hash"):
    """Compute a hash for the full root row."""
    logger.info(f"[INFO] Computing root hash -> column: {hash_col}")
    df_hashed = df.withColumn(hash_col, F.sha2(F.to_json(F.struct(*df.columns)), 256))
    logger.info(f"[INFO] Root hash completed")
    return df_hashed

