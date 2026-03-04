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
            logger.warning("Missing file: %s", file_path)
            #error_flag = True
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


def validate_downloads_s3(
    bucket: str, s3_folder_path: str,  logger, s3, dump_types_to_process: list[str] | None = None
):
    """
    Validate downloads stored in MinIO under bucket/catalog.
    Assumes a CHECKSUM file exists in folder.
    """
    s3_folder_path = s3_folder_path +"/"
    s3_folder_path.replace("//","/")

    # list objects to find the checksum file
    paginator = s3.get_paginator("list_objects_v2")
    checksum_file_keys = []
    prefix = s3_folder_path
    logger.info(f'{prefix = }')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        
        logger.info(f'{page.get("Contents", []) = }')
        for obj in page.get("Contents", []):
            if ("CHECKSUM" in obj["Key"]) :
                checksum_file_keys.append(obj["Key"])


    if not checksum_file_keys:
        raise RuntimeError(
            f"No checksum files found in s3a://{bucket}/{s3_folder_path}"
        )

    df_checksums = pd.DataFrame()
    
    
    for checksum_file_key in checksum_file_keys:

        logger.info(f"Checksum file {checksum_file_key} found for in s3a://{bucket}/{s3_folder_path}")
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
        
        
        df_checksums = pd.concat([df_checksums, df], ignore_index=True)
        
        

    error_flag = False
    for _, row in df_checksums.iterrows():
        dump_type_flag = False
        if dump_types_to_process:
            logger.info(f'{dump_types_to_process = }')

            for dump_type in dump_types_to_process:
                
                if dump_type in str(row["file_name"]):
                    dump_type_flag = True
                    
                    


        if dump_type_flag:
            object_key = f"{s3_folder_path}{row['file_name']}"
            logger.info(f'{object_key = }')
            try:
                hash_file = sha256_file_s3(bucket, object_key, s3=s3)
                logger.info(f'Hash for file {row["file_name"]} in s3a://{bucket}/{object_key}/ : {hash_file}. Reference hash: {row["checksum"]}')
                
            except s3.exceptions.ClientError:
                logger.warning("Missing file: s3a://%s/%s", bucket, object_key)
                #error_flag = True
                continue


            if hash_file != row["checksum"]:
                
                
                
                logger.warning("sha256 hash not OK for %s, deleting file", object_key)
                s3.delete_object(Bucket=bucket, Key=object_key)
                error_flag = True
            else:
                logger.info("sha256 hash OK for %s", object_key)

    if error_flag:
        raise RuntimeError("One or more files failed validation")
