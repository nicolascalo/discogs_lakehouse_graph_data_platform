import os
import pandas as pd
import hashlib
from io import StringIO

def sha256_file(path: str, chunk_size=8192) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_file_s3(bucket: str, object_key: str, s3, chunk_size: int = 8192) -> str:
    """
    Compute SHA256 of a file stored in S3/MinIO without downloading it to disk.
    """
    h = hashlib.sha256()
    
    response = s3.get_object(Bucket=bucket, Key=object_key)
    body = response["Body"]

    while True:
        chunk = body.read(chunk_size)
        if not chunk:
            break
        h.update(chunk)
    body.close()
    return h.hexdigest()

def validate_downloads(dir: str, date: str, logger):
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

    if error_flag:
        raise RuntimeError("One or more files failed validation")



def validate_downloads_s3(bucket: str, prefix: str, date: str, logger, s3,suffixes:list[str]|None=None):
    """
    Validate downloads stored in MinIO under bucket/prefix for a given date.
    Assumes a CHECKSUM file exists in the prefix.
    """

    # list objects to find the checksum file
    paginator = s3.get_paginator("list_objects_v2")
    checksum_file_key = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if "CHECKSUM" in obj["Key"] and date in obj["Key"]:
                checksum_file_key = obj["Key"]
                break
        if checksum_file_key:
            break

    if not checksum_file_key:
        raise RuntimeError(f"No checksum file found for date {date} in s3://{bucket}/{prefix}")

    # download checksum file into memory
    response = s3.get_object(Bucket=bucket, Key=checksum_file_key)
    checksum_content = response["Body"].read().decode("utf-8").splitlines()

    df = pd.read_csv(
        StringIO("\n".join(checksum_content)),
        sep=r"\s+",
        engine="python",
        names=["checksum", "file_name"],
        usecols=[0, 1],
    )

    
    
    error_flag = False
    for _, row in df.iterrows():
        
        suffix_flag = False
        if suffixes:
            for suffix in suffixes:
                if str(row['file_name']).endswith(suffix):
                    suffix_flag = True
            
        if suffix_flag:
        
            object_key = f"{prefix}{row['file_name']}"
            try:
                hash_file = sha256_file_s3(bucket, object_key, s3=s3)
                
                
            except s3.exceptions.ClientError:
                logger.error("Missing file: s3://%s/%s", bucket, object_key)
                error_flag = True
                continue

            if hash_file != row["checksum"]:
                logger.warning("sha256 hash not OK for %s, deleting file", object_key)
                s3.delete_object(Bucket=bucket, Key=object_key)
                error_flag = True
            else:
                logger.info("sha256 hash OK for %s", object_key)

    if error_flag:
        raise RuntimeError("One or more files failed validation")