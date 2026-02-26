import os
import re
from download_raw.config import RawConfig

from unitycatalog.config import UCConfig
from helpers_minio.minio_client import get_minio_client

from download_raw.logging_config import setup_logger
from download_raw.validate import validate_downloads_s3
from download_raw.download import (
    get_latest_dump_date,
    retrieve_downloaded_dumps_s3,
    download_file_to_minio_stream,
)


def download_dump(latest_discogs_dump_url_root, suffix, config, logger, s3):

    url = f"{latest_discogs_dump_url_root}{suffix}"
    filename = os.path.basename(url)

    logger.info(f"{url = }")
    logger.info(f"{filename = }")

    download_file_to_minio_stream(
        url=url,
        bucket="discogs",
        object_key=f"{config.data_dir}/raw/{filename}",
        logger=logger,
        s3=s3,
        chunk_size=config.CHUNK_SIZE,
        timeout=config.TIMEOUT,
        max_retries=config.RETRIES,
    )


if __name__ == "__main__":
    config = RawConfig.from_env()
    config_uc = UCConfig.from_env()

    s3 = get_minio_client(
        config_uc.minio_endpoint,
        config_uc.minio_access_key,
        config_uc.minio_secret_key,
        config_uc.region_name,
    )

    logger = setup_logger(config.log_dir / "discogs_raw_download.log")

    logger.info(f"{config.data_dir}/raw/")

    already_downloaded = retrieve_downloaded_dumps_s3(
        bucket="discogs", prefix=f"{config.data_dir}/raw/", s3=s3
    )
    
    
    
    logger.info(f"{already_downloaded = }")

    latest_discogs_dump_url_root = get_latest_dump_date(
        config.DISCOGS_DUMP_URL_ROOT,
        config.YEAR_LIMIT_PAST,
        config.DISC_PREFIX,
        TIMEOUT=config.TIMEOUT,
        logger=logger,
    )

    date_latest_dump = re.sub(".*_", "", latest_discogs_dump_url_root)

    logger.info(f"{date_latest_dump = }")

    for suffix in config.DISC_SUFFIX_LIST:
        download_dump(latest_discogs_dump_url_root, suffix, config, logger, s3)

    #validate_downloads(str(config.raw_data_dir), date_latest_dump, logger,s3)

    validate_downloads_s3(
        bucket="discogs",
        prefix=f"{config.data_dir}/raw/",
        date=date_latest_dump,
        logger=logger,
        s3=s3,
        suffixes = config.DISC_SUFFIX_LIST
    )
    
    logger.info("All downloads completed and validated successfully!")
