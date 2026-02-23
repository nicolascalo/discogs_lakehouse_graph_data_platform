import os
import logging
from pathlib import Path


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *


def export_schemas(
    df, dump_type: str, data_dir: Path, dump_date: str, logger: logging.Logger
) -> None:

    logger.info(f"{dump_date} -> Exporting schemas")

    xml_schema_tree = df.schema.treeString()
    xml_schema_json = df.schema.json()

    schema_path = os.path.join(
        data_dir, f"{dump_type}_schema_dump_{dump_date}_tree.txt"
    )

    with open(schema_path, "w") as f:
        f.write(xml_schema_tree)

    schema_path = os.path.join(data_dir, f"{dump_type}_schema_dump_{dump_date}.json")

    with open(schema_path, "w") as f:
        f.write(xml_schema_json)

    return None
