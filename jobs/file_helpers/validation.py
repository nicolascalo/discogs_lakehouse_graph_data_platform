import os
import logging
import pandas as pd
import hashlib


from ingest_helpers.file_info_helpers import *
from ingest_helpers.spark_df_helpers import *
from ingest_helpers.config_helpers import *


def sha256_file(path: str, chunk_size=8192) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_file_hash(dir: str, date: str, file, logger: logging.Logger):
    logger.info("Validating downloads for date = %s", date)
    error_flag = False
    files = os.listdir(dir)
    checksum_files = [f for f in files if "CHECKSUM" in f and date in f]

    if not checksum_files:
        raise RuntimeError(f"No checksum file found for date {date}")
    checksum_file = checksum_files[0]

    df = pd.read_csv(
        os.path.join(dir, checksum_file),
        sep=r"\s+",
        engine="python",
        names=["checksum", "file_name"],
        usecols=[0, 1],
    )

    df = df[df["file_name"] == file]
    if df.empty:
        raise RuntimeError(f"No checksum entry for file {file}")

    for _, row in df.iterrows():
        file_path = os.path.join(dir, row["file_name"])
        if not os.path.exists(file_path):
            logger.error("Missing file: %s", file_path)
            error_flag = True
            continue

        hash_file = sha256_file(file_path)
        if hash_file != row["checksum"]:
            logger.info("sha256 hash failed for %s", row["file_name"])
            error_flag = True
        else:
            logger.info("sha256 hash OK for %s", row["file_name"])

    if error_flag:
        raise RuntimeError("One or more files failed validation")
    else:
        return True
