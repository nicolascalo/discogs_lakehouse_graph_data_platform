import os
import logging
from pathlib import Path
import json

def export_schemas(
    df, dump_type: str, metadata_dir: Path, dump_date: str, logger: logging.Logger
) -> None:

    logger.info(f"{dump_date} -> Exporting schemas")

    xml_schema_tree = df.schema.treeString()
    xml_schema_json = df.schema.json()

    schema_path = os.path.join(
        metadata_dir, f"{dump_type}_schema_dump_{dump_date}_tree.txt"
    )

    with open(schema_path, "w") as f:
        f.write(xml_schema_tree)

    schema_path = os.path.join(metadata_dir, f"{dump_type}_schema_dump_{dump_date}.json")

    with open(schema_path, "w") as f:
        f.write(xml_schema_json)

    return None


def export_schemas_s3_and_head(
    df, catalog, dump_description , metadata_dir: str, logger: logging.Logger, n:int|None = 1000
) -> None:




    logger.info(f"{dump_description} -> Exporting schemas")

    xml_schema_tree = df.schema.treeString()
    xml_schema_json = json.loads(df.schema.json())

    schema_path = os.path.join(
        metadata_dir, f"{catalog}_{dump_description}_schema_tree.txt"
    )

    with open(schema_path, "w") as f:
        f.write(xml_schema_tree)

    schema_path = os.path.join(metadata_dir, f"{catalog}_{dump_description}_schema.json")

    with open(schema_path, "w") as f:
         json.dump(xml_schema_json, f, indent=4, ensure_ascii=False)
         
         
    head_path = os.path.join(metadata_dir, f"{catalog}_{dump_description}_head{n}.csv")
    
    if n:
    
        df.limit(n).coalesce(1).write.mode("overwrite").option("escapeQuotes", False).option("header", True).option("extension","tsv").option("sep","\t").csv(head_path)
        
    else:
        df.coalesce(1).write.mode("overwrite").option("escapeQuotes", False).option("header", True).option("extension","tsv").option("sep","\t").csv(head_path)
    logger.info(f"Head({n}) of exported to {head_path}")


    return None
