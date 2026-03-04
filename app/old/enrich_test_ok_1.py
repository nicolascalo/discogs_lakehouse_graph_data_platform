from pyspark.sql import SparkSession, functions as F, types as T

# -----------------------------
# CONFIGURATION
# -----------------------------
rules = [
    {"path": "test", "name": "test", "expr": "'fdgdfh'"},
    {"path": "artists.artist", "name": "role", "expr": "'Release artist'"},
                    {
                "path": "artists.artist",
                "name": "tracks",
                "expr": " 'All'"
            },
                {
                "path": "extraartists.artist",
                "name": "tracks",
                "expr": "coalesce(x.tracks, 'All')"
            },

]

# -----------------------------
# SPARK SETUP
# -----------------------------
spark = SparkSession.builder \
    .appName("EnrichmentTest") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# -----------------------------
# SAMPLE DATA
# -----------------------------
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

# -----------------------------
# DEBUG HELPERS
# -----------------------------
def debug(msg):
    print(f"[DEBUG] {msg}")

def ensure_root_column(df, root):
    if root not in df.columns:
        debug(f"Creating missing root column '{root}' with dummy field")
        df = df.withColumn(root, F.struct(F.lit(None).alias("dummy_field")))
    return df

# -----------------------------
# APPLY RULE
# -----------------------------
def apply_rule(df, rule):
    path = rule["path"]
    name = rule["name"]
    expr = rule["expr"]
    debug(f"\n--- APPLY RULE ---\n[APPLY RULE] path={path}, expr={expr}")

    parts = path.split(".")
    root = parts[0]
    subpath = parts[1:]

    df = ensure_root_column(df, root)

    # Traverse struct fields until the leaf
    col_ref = root
    dtype = dict(df.dtypes).get(root)

    for i, p in enumerate(subpath):
        # Get struct field type
        struct_type = df.select(F.col(col_ref)).schema[0].dataType
        if isinstance(struct_type, T.StructType):
            if p not in [f.name for f in struct_type.fields]:
                debug(f"[WARNING] Field '{p}' not found in struct '{col_ref}'")
                return df
            # Drill down one level
            field_type = [f.dataType for f in struct_type.fields if f.name == p][0]
            col_ref = f"{col_ref}.{p}"
        else:
            debug(f"[WARNING] Unexpected type for '{col_ref}', skipping")
            return df

    # Now col_ref points to the leaf field
    field_type = df.select(F.col(col_ref)).schema[0].dataType

    if isinstance(field_type, T.ArrayType) and isinstance(field_type.elementType, T.StructType):
        debug(f"[DEBUG] Array of structs detected at leaf '{col_ref}', applying transform")
        struct_fields = [f"'{f.name}', x.{f.name}" for f in field_type.elementType.fields]
        struct_fields.append(f"'{name}', {expr}")
        transform_expr = f"transform({col_ref}, x -> named_struct({', '.join(struct_fields)}))"
        debug(f"[DEBUG] Transform expression: {transform_expr}")
        df = df.withColumn(col_ref, F.expr(transform_expr))
    else:
        debug(f"[DEBUG] Leaf field is scalar, applying expression directly: {col_ref}")
        df = df.withColumn(col_ref, F.expr(expr))

    debug(f"[DEBUG] Rule applied on '{col_ref}'")
    return df

# -----------------------------
# APPLY ALL RULES
# -----------------------------
def apply_enrich(df, rules):
    for i, rule in enumerate(rules):
        debug(f"[APPLY RULE {i}] {rule}")
        df = apply_rule(df, rule)
    return df

# -----------------------------
# RUN ENRICHMENT
# -----------------------------
df = apply_enrich(df, rules)

# -----------------------------
# SHOW RESULTS
# -----------------------------
df.show(truncate=False)
df_json = df.select(F.to_json(F.struct([F.col(c) for c in df.columns])).alias("json"))
df_json.show(truncate=False)
