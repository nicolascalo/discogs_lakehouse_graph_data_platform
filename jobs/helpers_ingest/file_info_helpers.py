import re
import hashlib
import pandas as pd
import os
import logging


def extract_dump_date(fname):
    m = re.search(r"discogs_(\d{8})", fname)
    return m.group(1) if m else None

def extract_dump_type(fname):
    m = re.search(r"discogs_\d{8}_(.*?)[\._].*xml", fname)
    return m.group(1) if m else None

def sha256_file(path: str, chunk_size=8192) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()



def validate_file_hash(dir: str, date: str, file, logger:logging.Logger):
    logger.info("Validating downloads for date = %s", date)
    error_flag = False
    
    logger.info(f"{dir = }")
    files = os.listdir(dir)
    
    checksum_files = [f for f in files if "CHECKSUM" in f and date in f]
    if not checksum_files:
        raise RuntimeError(f"No checksum file found for date {date}")
    checksum_file = checksum_files[0]
    logger.info(f"{checksum_file = }")
    logger.info(f"{file = }")
    df = pd.read_csv(
        os.path.join(dir, checksum_file),
        sep=r"\s+",
        engine="python",
        names=["checksum", "file_name"],
        usecols=[0, 1]
    )





    df = df[df['file_name'] == file]


    for _, row in df.iterrows():
        file_path = os.path.join(dir, row["file_name"])
        if not os.path.exists(file_path):
            logger.error("Missing file: %s", file_path)
            error_flag = True
            continue

        hash_file = sha256_file(file_path)
        if hash_file != row["checksum"]:
            logger.warning("sha256 hash not OK for %s, deleting file", row["file_name"])
            os.remove(file_path)
            error_flag = True
        else:
            logger.info("sha256 hash OK for %s", row["file_name"])
            return True

    if error_flag:
        raise RuntimeError("One or more files failed validation")
