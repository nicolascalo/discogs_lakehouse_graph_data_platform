# ============================
# CONFIG-DRIVEN ENRICH ENGINE
# ============================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys

# ----------------------------
# Silence Spark non-error logs
# ----------------------------
spark = (
    SparkSession.builder
    .appName("enrich-debug")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ----------------------------
# DEBUG PRINT
# ----------------------------
def debug(msg):
    print("[DEBUG]", msg)
    sys.stdout.flush()

# ----------------------------
# SAMPLE DATA
# ----------------------------
data = [
{
    "@id": 3360,
    "@status": "Accepted",
    "artists": {
        "artist": [
            {
                "id": 791,
                "name": "Mr. Oizo"
            }
        ]
    },
    "country": "US",
    "data_quality": "Needs Vote",
    "extraartists": {
        "artist": [
            {
                "anv": "RichardReachMvogo",
                "id": 4223270,
                "name": "Richard Mvogo",
                "role": "Artwork"
            },
            {
                "name": "Rock",
                "role": "Artwork By [Mr. Oizo Tag]"
            },
            {
                "id": 791,
                "name": "Mr. Oizo",
                "role": "Illustration [Worms Drawings]"
            },
            {
                "id": 449452,
                "name": "Guillaume Dickvebraz",
                "role": "Mixed By"
            },
            {
                "id": 217597,
                "name": "Quentin Dupieux",
                "role": "Mixed By"
            },
            {
                "anv": "DJFabienFeadzPianta",
                "id": 795067,
                "name": "Fabien Pianta",
                "role": "Scratches"
            },
            {
                "anv": "QuentinOizoDupieux",
                "id": 217597,
                "name": "Quentin Dupieux",
                "role": "Written-By, Producer"
            }
        ]
    },
    "genres": {
        "genre": [
            "Electronic"
        ]
    },
    "released": "2000-11-21",
    "styles": {
        "style": [
            "Techno",
            "Electro",
            "Noise",
            "Experimental"
        ]
    },
    "title": "Analog Worms Attack",
    "tracklist": {
        "track": [
            {
                "duration": "1:46",
                "position": "1",
                "title": "Bad Start"
            },
            {
                "duration": "3:56",
                "position": "2",
                "title": "Monophonic Shit"
            },
            {
                "duration": "4:17",
                "position": "3",
                "title": "No Day Massacre"
            },
            {
                "duration": "1:31",
                "position": "4",
                "title": "Smoking Tape"
            },
            {
                "duration": "4:28",
                "position": "5",
                "title": "Last Night A DJ Killed My Dog"
            },
            {
                "duration": "3:03",
                "position": "6",
                "title": "The Salad"
            },
            {
                "duration": "2:46",
                "position": "7",
                "title": "Bobby Can’t Dance"
            },
            {
                "duration": "4:53",
                "position": "8",
                "title": "Analog Worms Attack"
            },
            {
                "duration": "1:14",
                "position": "9",
                "title": "One Minute Shakin"
            },
            {
                "duration": "4:50",
                "position": "10",
                "title": "Inside The Kidney Machine"
            },
            {
                "duration": "4:23",
                "position": "11",
                "title": "Miaaaw"
            },
            {
                "duration": "2:23",
                "position": "12",
                "title": "Flat 55"
            },
            {
                "duration": "1:11",
                "position": "13",
                "title": "Feadz On"
            },
            {
                "sub_tracks": {
                    "track": [
                        {
                            "duration": "3:39",
                            "position": "14.1",
                            "title": "Analog Wormz Sequel"
                        },
                        {
                            "position": "14.Video",
                            "title": "Flat Beat"
                        },
                        {
                            "duration": "5:00",
                            "position": "14.3",
                            "title": "(silence)"
                        }
                    ]
                },
                "title": "Analog Wormz Sequel"
            },
            {
                "title": "Bonus Track"
            },
            {
                "duration": "5:17",
                "position": "15",
                "title": "Flat Beat"
            }
        ]
    }
}

]

df = spark.createDataFrame(data)

debug("Initial schema:")
df.printSchema()

# ----------------------------
# RULE CONFIG
# ----------------------------
rules = [
    {"path": "test", "name": "test", "expr": "'fdgdfh'"},
    {"path": "artists.artist", "name": "role", "expr": "'Release artist'"},
    {"path": "artists.artist", "name": "tracks", "expr": "'All'"},
    {
        "path": "tracklist.track",
        "name": "track_id",
        "expr": "concat(`@id`, '-', x.position)"
    }
]

# ----------------------------
# UTILS
# ----------------------------
def ensure_root(df, root):
    if root in df.columns:
        debug("Root '" + root + "' already exists")
        return df

    debug("Creating missing root '" + root + "' with dummy field")
    return df.withColumn(root, F.struct(F.lit(None).alias("_dummy")))

def get_field_names(struct_type):
    return [f.name for f in struct_type.fields]

# ----------------------------
# BUILD TRANSFORM FOR ARRAY<STRUCT>
# ----------------------------
def build_array_transform(array_col, struct_type, new_field, expr_sql):
    debug("Building transform for array '" + array_col + "', adding field '" + new_field + "'")

    field_names = get_field_names(struct_type)

    named_parts = []
    for fname in field_names:
        named_parts.append("'" + fname + "'")
        named_parts.append("x." + fname)

    named_parts.append("'" + new_field + "'")
    named_parts.append(expr_sql)

    named_struct_sql = "named_struct(" + ", ".join(named_parts) + ")"

    transform_sql = (
        "transform(" +
        array_col +
        ", x -> " +
        named_struct_sql +
        ")"
    )

    debug("Generated SQL: " + transform_sql)
    return F.expr(transform_sql)

# ----------------------------
# APPLY ONE RULE
# ----------------------------
def apply_rule(df, rule):
    path = rule["path"]
    new_field = rule["name"]
    expr_sql = rule["expr"]

    debug("")
    debug("--- APPLY RULE ---")
    debug("Path = " + path)
    debug("Expr = " + expr_sql)

    parts = path.split(".")
    root = parts[0]

    df = ensure_root(df, root)

    # ROOT SCALAR
    if len(parts) == 1:
        debug("Applying scalar root expression on '" + root + "'")
        return df.withColumn(root, F.expr(expr_sql))

    # STRUCT -> ARRAY CASE
    parent = parts[0]
    child = parts[1]

    parent_type = df.schema[parent].dataType

    if not isinstance(parent_type, StructType):
        debug("ERROR: Root '" + parent + "' is not a struct")
        return df

    if child not in get_field_names(parent_type):
        debug("ERROR: Field '" + child + "' not in struct '" + parent + "'")
        return df

    child_field = parent_type[child]
    if not isinstance(child_field.dataType, ArrayType):
        debug("ERROR: Field '" + parent + "." + child + "' is not an array")
        return df

    element_type = child_field.dataType.elementType
    if not isinstance(element_type, StructType):
        debug("ERROR: Array elements are not struct")
        return df

    debug("Detected ARRAY<STRUCT> at '" + parent + "." + child + "'")

    transformed_array = build_array_transform(
        parent + "." + child,
        element_type,
        new_field,
        expr_sql
    )

    debug("Rebuilding parent struct '" + parent + "'")

    parent_fields = get_field_names(parent_type)
    struct_parts = []

    for fname in parent_fields:
        struct_parts.append("'" + fname + "'")
        if fname == child:
            struct_parts.append(transformed_array._jc.toString())
        else:
            struct_parts.append(parent + "." + fname)

    struct_sql = "named_struct(" + ", ".join(struct_parts) + ")"
    debug("Final struct SQL: " + struct_sql)

    return df.withColumn(parent, F.expr(struct_sql))

# ----------------------------
# APPLY ALL RULES
# ----------------------------
def apply_enrich(df, rules):
    for idx, rule in enumerate(rules):
        debug("")
        debug("[APPLY RULE " + str(idx) + "] " + str(rule))
        df = apply_rule(df, rule)
        debug("Schema after rule " + str(idx))
        df.printSchema()
    return df

# ----------------------------
# RUN
# ----------------------------
df = apply_enrich(df, rules)

debug("")
debug("FINAL DATAFRAME")
df.show(truncate=False)

debug("")
debug("FINAL JSON")
df.select(F.to_json(F.struct(*df.columns)).alias("json")).show(truncate=False)
