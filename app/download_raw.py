import os
import re

from minio.minio_client import create_s3_bucket

from logs.logging_config import setup_logger
from download_raw.validate import validate_downloads_s3
from download_raw.download import (
    get_latest_dump_date,
    retrieve_downloaded_dumps_s3,
    download_file_to_minio_stream,
)
from pathlib import Path
import logging
from settings import Settings

import sys


def download_dump_s3(
    latest_discogs_dump_url_root,
    suffix,
    chunk_size,
    timeout,
    max_retries,
    logger,
    s3,
    destination_path,
    bucket,
):

    url = f"{latest_discogs_dump_url_root}{suffix}"
    filename = os.path.basename(url)
    object_key = f"{destination_path}/{filename}"

    if settings.storage.type == "minio":
        download_file_to_minio_stream(
            url,
            bucket,
            object_key,
            logger,
            s3,
            chunk_size,
            timeout,
            max_retries,
        )


if __name__ == "__main__":
    settings = Settings.load()

    setup_logger(
        settings.log_dir
        / f"{settings.project_name}_{settings.env}_raw_download_log.json",
        settings.env,
        settings.project_name,
        settings.storage.type,
    )

    logger = logging.getLogger(Path(__file__).stem)

    try:
        storage = settings.storage
        create_s3_bucket(storage.s3, settings.project_name, logger=logger)

        raw_path = (
            f"s3a://{storage.bucket}/{settings.project_name}_{settings.env}/raw/data"
        )
        destination_path = f"{settings.project_name}_{settings.env}/raw/data"
        s3 = settings.storage.s3

        latest_discogs_dump_url_root = get_latest_dump_date(
            settings.raw["DISCOGS_DUMP_URL_ROOT"],
            settings.raw["YEAR_LIMIT_PAST"],
            settings.raw["DISC_PREFIX"],
            TIMEOUT=settings.raw["TIMEOUT"],
            logger=logger,
        )

        date_latest_dump = re.sub(".*_", "", latest_discogs_dump_url_root)

        logger.info(f"{date_latest_dump = }")

        if storage.type == "minio":
            create_s3_bucket(s3, bucket_name=settings.project_name, logger=logger)

            already_downloaded = retrieve_downloaded_dumps_s3(
                bucket=settings.project_name,
                prefix=f"{settings.env}/raw/data",
                s3=s3,
                catalog_root=settings.project_name,
            )

            logger.info(f"{already_downloaded = }")
            dump_types_to_download = settings.raw["DISC_SUFFIX_LIST"][settings.env]
            logger.info(f"{dump_types_to_download = }")

            for dump_type in dump_types_to_download:
                download_dump_s3(
                    latest_discogs_dump_url_root,
                    dump_type,
                    settings.raw["CHUNK_SIZE"],
                    settings.raw["TIMEOUT"],
                    settings.raw["RETRIES"],
                    logger,
                    s3,
                    destination_path,
                    storage.bucket,
                )

            catalog = settings.project_name + "_" + settings.storage.env
            schema = "raw"
            raw_s3_folder_path = "/".join([catalog, schema, "data"])

            validate_downloads_s3(
                bucket=settings.project_name,
                s3_folder_path=raw_s3_folder_path,
                logger=logger,
                s3=s3,
                dump_types_to_process=settings.raw["DUMP_TYPE_TO_PROCESS"][
                    settings.env
                ],
            )

        elif storage.type == "local":
            already_downloaded = ""

        else:
            already_downloaded = None

        # validate_downloads(str(settings.raw.raw_data_dir), date_latest_dump, logger,s3)

        logger.info("All downloads completed and validated successfully!")

    except Exception:
        logger.exception("Fatal error during raw download job")
        sys.exit(1)
