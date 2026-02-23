import os
from pathlib import Path


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *


def cleanup_old_raw_files(
    latest_dump_date: str, dump_type: str, RAW_DATA_DIR: Path
) -> None:
    raw_data_file_list = os.listdir(RAW_DATA_DIR)
    raw_data_file_list_to_archive = [
        file_name
        for file_name in raw_data_file_list
        if (latest_dump_date not in file_name)
        and (file_name.endswith(".xml.gz") and (dump_type in file_name))
    ]

    for file in raw_data_file_list_to_archive:
        file_path = Path(RAW_DATA_DIR) / file
        if os.path.exists(file_path):
            archive_dir = RAW_DATA_DIR / "archive"
            file_path.rename(archive_dir / file_path.name)
    return None
