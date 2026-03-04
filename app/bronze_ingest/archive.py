import os
from pathlib import Path


def cleanup_old_raw_files(
    latest_dump_date: str,
    dump_type: str,
    RAW_DATA_DIR: Path,
    archive_dir_name: str = "archive",
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
            archive_dir = RAW_DATA_DIR / archive_dir_name
            file_path.rename(archive_dir / file_path.name)
    return None


from urllib.parse import urlparse


def cleanup_old_raw_files_s3(
    s3,
    latest_dump_date: str,
    dump_type: str,
    origin_s3_path: str,
    destination_s3_path: str,
    logger,
) -> None:
    """
    Move old raw dump files from one S3/MinIO prefix to another.

    Parameters
    ----------
    storage : StorageSettings
        Your StorageSettings instance (contains boto3 client)
    latest_dump_date : str
        Date string to keep (files NOT containing this date will be archived)
    dump_type : str
        Filter files containing this dump type
    origin_s3_path : str
        e.g. s3://bucket/dev/raw/
    destination_s3_path : str
        e.g. s3://bucket/dev/raw/archive/
    """

    # Parse S3 URLs
    origin = urlparse(origin_s3_path)
    destination = urlparse(destination_s3_path)

    bucket = origin.netloc
    origin_prefix = origin.path.lstrip("/")
    destination_prefix = destination.path.lstrip("/")

    logger.info(f"Listing objects in s3://{bucket}/{origin_prefix}")

    paginator = s3.get_paginator("list_objects_v2")

    to_delete = []


    for page in paginator.paginate(Bucket=bucket, Prefix=origin_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            logger.info(f"{key = }")
            
            
            filename = key.split("/")[-1]

            # Filtering logic
            if (
                latest_dump_date not in filename
                and filename.endswith(".xml.gz")
                and dump_type in filename
            ):
                new_key = key.replace(origin_prefix, destination_prefix, 1)
                logger.info(f"{new_key = }")
                exists_flag = False
                try:
                    s3.head_object(Bucket=bucket, Key=new_key)
                    logger.warning(f"{new_key} already exists, skipping")
                    exists_flag = True
                except:
                    pass

                if not exists_flag:

                    logger.info(f"Moving {key} → {new_key}")

                    # Copy
                    s3.copy({"Bucket": bucket, "Key": key}, bucket, new_key)

                # Delete original

                to_delete.append({"Key": key})

                if len(to_delete) == 1000:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
                    to_delete = []
                    
    s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})

    logger.info("Cleanup completed.")
