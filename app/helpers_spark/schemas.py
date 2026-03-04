import os
import logging
from pathlib import Path


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


def export_schemas_s3(
    df, catalog, dump , metadata_dir: str, logger: logging.Logger
) -> None:




    logger.info(f"{dump['file']} -> Exporting schemas")

    xml_schema_tree = df.schema.treeString()
    xml_schema_json = df.schema.json()

    schema_path = os.path.join(
        metadata_dir, f"{catalog}_{dump['dump_type']}_schema_dump_{dump['dump_date']}_tree.txt"
    )

    with open(schema_path, "w") as f:
        f.write(xml_schema_tree)

    schema_path = os.path.join(metadata_dir, f"{catalog}_{dump['dump_type']}_schema_dump_{dump['dump_date']}.json")

    with open(schema_path, "w") as f:
        f.write(xml_schema_json)

    return None
