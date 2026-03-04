import pyspark
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.master("local[*]").appName("EnrichTest").getOrCreate()

# -----------------------------
# CONFIG-DRIVEN RULES
# -----------------------------
rules = [
    {'path': 'test', 'name': 'test', 'expr': "'fdgdfh'"},
    {'path': 'artists.artist', 'name': 'role', 'expr': "'Release artist'"},
    {'path': 'tracklist.track', 'name': 'track_id', 'expr': "concat(`@id`, '-', __anc0.position)"},
    {'path': 'tracklist.track.sub_tracks.track', 'name': 'subtrack_id', 'expr': "concat(`@id`, '-', __anc1.track_id, '-', __anc0.position)"},
    {'path': 'extraartists.artist', 'name': 'tracks', 'expr': "CASE WHEN size(parent.tracklist.track) > 0 THEN 'All' ELSE tracks END"},
]

# -----------------------------
# HELPERS
# -----------------------------



def ensure_root_column(df, root):
    """Ensure the root column exists as a struct (with a dummy field if needed)."""
    if root not in df.columns:
        print(f"[DEBUG] Creating missing root column '{root}' with a dummy field")
        # PySpark requires at least one field in a struct
        df = df.withColumn(root, F.struct(F.lit(None).alias("__dummy__")))
    else:
        print(f"[DEBUG] Root column '{root}' already exists")
    return df

def parse_path(path_str):
    """Split path like 'tracklist.track.sub_tracks.track' into list"""
    path_list = path_str.split(".") if path_str else []
    print(f"[DEBUG] Parsed path '{path_str}' -> {path_list}")
    return path_list

def apply_rule_to_column(col, path, expr, ancestors):
    """
    Recursively descend into structs/arrays and apply the rule
    `ancestors` is a list of ancestor columns for __anc0, __anc1...
    """
    if not path:
        # Base case: apply expression
        anc_map = {f"__anc{i}": ancestors[i] for i in range(len(ancestors))}
        expr_rewritten = expr
        for k, v in anc_map.items():
            expr_rewritten = expr_rewritten.replace(f"ancestor[{k[-1]}]", k)
        print(f"[DEBUG] Applying expression: {expr} -> {expr_rewritten}")
        return F.expr(expr_rewritten)

    head, *tail = path
    print(f"[DEBUG] Descending into '{head}', remaining path: {tail}")

    # If column is array, use transform
    if isinstance(col.expr.dataType, T.ArrayType):
        print(f"[DEBUG] Column is array, using transform on '{head}'")
        return F.transform(
            col,
            lambda x: apply_rule_to_column(x, path, expr, ancestors + [x])
        )

    # Otherwise descend into struct field
    if isinstance(col.expr.dataType, T.StructType):
        if head not in [f.name for f in col.expr.dataType.fields]:
            print(f"[DEBUG] Struct missing field '{head}', adding null")
            col = F.struct(*col.expr.dataType.fields, F.lit(None).alias(head))
        child = col[head]
        updated_child = apply_rule_to_column(child, tail, expr, ancestors)
        return F.expr(f"update_fields({col._jc}, WithField({head}, {updated_child._jc}))")

    print(f"[WARNING] Unknown column type at path '{path}'")
    return col

def apply_enrich(df, rules):
    for i, rule in enumerate(rules):
        path_str = rule['path']
        name = rule['name']
        expr = rule['expr']
        print("\n" + "-"*40)
        print(f"[APPLY RULE {i}] path={path_str}, expr={expr}")

        path = parse_path(path_str)
        root = path[0] if path else name
        df = ensure_root_column(df, root)

        sub_path = path[1:] if len(path) > 1 else []
        col = df[root]

        try:
            new_col = apply_rule_to_column(col, sub_path, expr, ancestors=[])
            df = df.withColumn(root, new_col)
            print(f"[RULE {i}] Updated column '{root}'")
        except Exception as e:
            print(f"[ERROR] Failed to apply rule {i} on '{root}': {e}")
            raise
    return df

