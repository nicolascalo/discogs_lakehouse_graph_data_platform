import json
from pyspark.sql import DataFrame
import logging
from pyspark.sql import SparkSession
from delta.tables import DeltaTable




def merge_into_delta(
    spark: SparkSession,
    df: DataFrame,
    output_dir: str,
    primary_key: str,
    hash_col: str | None,
    logger: logging.Logger,
) -> None:
    logger.info(
        f"Merging Delta Table into {output_dir} based on {hash_col} and {primary_key}"
    )

    target = DeltaTable.forPath(spark, output_dir)

    if hash_col:
        (
            target.alias("t")
            .merge(
                df.alias("s"),
                f"t.`{primary_key}` = s.`{primary_key}`",
            )
            .whenMatchedUpdateAll(condition=f"t.{hash_col} <> s.{hash_col}")
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
    else:
        (
            target.alias("t")
            .merge(
                df.alias("s"),
                f"t.`{primary_key}` = s.`{primary_key}`",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )

    spark.sql(f"OPTIMIZE delta.`{output_dir}`")
    
    return None



def is_delta_table(bucket, prefix, s3):
    """Check if a MinIO prefix contains a Delta table (_delta_log directory)."""
    # Check for any objects under _delta_log/ (json or checkpoint files)
    result = s3.list_objects_v2(
        Bucket=bucket, catalog=f"{prefix}/_delta_log/", MaxKeys=1
    )
    
    print(f"  Checking {prefix}/_delta_log/ → KeyCount={result.get('KeyCount', 0)}, Contents={[o['Key'] for o in result.get('Contents', [])]}")
    if result.get("KeyCount", 0) > 0:
        return True
    
    # Also check for the directory marker itself
    result2 = s3.list_objects_v2(
        Bucket=bucket, catalog=f"{prefix}/_delta_log", MaxKeys=1
    )
    return result2.get("KeyCount", 0) > 0



def infer_columns_from_delta(s3a_path, spark):
    """Read Delta table schema from Spark and convert to UC column format."""
    try:
        
        df = spark.read.format("delta").load(s3a_path)
        columns = []
        type_map = {
            "integer": ("INT", "int"),
            "long": ("LONG", "bigint"),
            "double": ("DOUBLE", "double"),
            "float": ("FLOAT", "float"),
            "string": ("STRING", "string"),
            "boolean": ("BOOLEAN", "boolean"),
            "date": ("DATE", "date"),
            "timestamp": ("TIMESTAMP", "timestamp"),
        }
        for i, field in enumerate(df.schema.fields):
            spark_type = field.dataType.typeName().lower()
            uc_type, text_type = type_map.get(spark_type, ("STRING", "string"))
            columns.append(
                {
                    "name": field.name,
                    "type_name": uc_type,
                    "type_text": text_type,
                    "type_json": json.dumps(
                        {
                            "name": field.name,
                            "type": text_type,
                            "nullable": field.nullable,
                            "metadata": {},
                        }
                    ),
                    "position": i,
                    "nullable": field.nullable,
                }
            )
        return columns
    except Exception as e:
        print(f"Could not infer schema from {s3a_path}: {e}")



def scan_minio_for_delta_tables(bucket, s3):
    """
    Scan MinIO bucket for Delta tables.
    Expects structure: bucket/catalog/schema/table/
    Returns list of (catalog, schema, table, s3a_path)
    """
    found = []
    paginator = s3.get_paginator("list_objects_v2")
    print(f"{bucket = }")

    # List top-level prefixes (catalogs)
    for catalog_page in paginator.paginate(Bucket=bucket, Delimiter="/"):
        for catalog_prefix in catalog_page.get("Commoncataloges", []):
            
            #print(f"{catalog_prefix = }")
            catalog = catalog_prefix["catalog"].rstrip("/")

            # List second-level prefixes (schemas)
            for schema_page in paginator.paginate(
                Bucket=bucket, Prefix=f"{catalog}/", Delimiter="/"
            ):
                #print(f"{schema_page = }")
                
                for schema_prefix in schema_page.get("Commoncataloges", []):
                    #print(f"{schema_prefix = }")
                    
                    schema = schema_prefix["catalog"].rstrip("/").split("/")[-1]

                    # List third-level prefixes (tables)
                    for table_page in paginator.paginate(
                        Bucket=bucket, Prefix=f"{catalog}/{schema}/", Delimiter="/"
                    ):
                        
                        for table_prefix in table_page.get("Commoncataloges", []):
                            #print(f"{table_prefix = }")
                            table = table_prefix["catalog"].rstrip("/").split("/")[-1]
                            prefix = f"{catalog}/{schema}/{table}"
                            #print(f"{prefix = }")
                            
                            
                            s3a_path = f"s3a://{bucket}/{prefix}"
                            #print(f"{s3a_path = }")
                            
                            if is_delta_table(bucket, prefix, s3):
                                s3a_path = f"s3a://{bucket}/{prefix}"
                                
                                
                                found.append((catalog, schema, table, s3a_path))

    return found



def create_new_delta_table(df: DataFrame, output_dir,table_name:str,  logger: logging.Logger) -> None:

    logger.info("Writing new Delta Table")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", output_dir).saveAsTable(table_name)
    return None


def create_new_delta_table_s3_overwrite(df: DataFrame, output_table_s3_path,logger: logging.Logger) -> None:

    logger.info(f"Writing new Delta Table {output_table_s3_path}")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_table_s3_path)
    return None


