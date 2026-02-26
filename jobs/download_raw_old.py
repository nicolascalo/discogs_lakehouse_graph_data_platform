import os
import re
from download_raw.config import RawConfig


from download_raw.logging_config import setup_logger
from download_raw.validate import validate_downloads
from download_raw.download import (
    download_large_file_parallel,
    get_latest_dump_date,
    retrieve_downloaded_dumps,
    download_file_httpx,
    check_server_parallel_download,
)


def download_dump(latest_discogs_dump_url_root, suffix, config):
    full_url = f"{latest_discogs_dump_url_root}{suffix}"

    url = full_url
    output_dir = config.raw_data_dir
    num_threads = config.PARALLEL_RANGES
    num_retries = config.RETRIES
    LOG_PROGRESS_EVERY_MB = config.LOG_PROGRESS_EVERY_MB
    CHUNK_SIZE = config.CHUNK_SIZE
    TIMEOUT = config.TIMEOUT

    if check_server_parallel_download(url, logger):
        download_large_file_parallel(
            url,
            output_dir,
            num_threads,
            num_retries,
            LOG_PROGRESS_EVERY_MB,
            CHUNK_SIZE,
            TIMEOUT,
            logger,
        )
    else:
        download_file_httpx(url, output_dir, logger, CHUNK_SIZE, num_retries, TIMEOUT)


if __name__ == "__main__":
    config = RawConfig.from_env()
    logger = setup_logger(config.log_dir / "discogs_raw_download.log")
    os.makedirs(config.raw_data_dir, exist_ok=True)

    already_downloaded = retrieve_downloaded_dumps(str(config.raw_data_dir))
    latest_discogs_dump_url_root = get_latest_dump_date(
        config.DISCOGS_DUMP_URL_ROOT,
        config.YEAR_LIMIT_PAST,
        config.DISC_PREFIX,
        TIMEOUT=config.TIMEOUT,
        logger=logger,
    )
    date_latest_dump = re.sub(".*_", "", latest_discogs_dump_url_root)

    for suffix in config.DISC_SUFFIX_LIST:
        download_dump(latest_discogs_dump_url_root, suffix, config)

    validate_downloads(str(config.raw_data_dir), date_latest_dump, logger)
    logger.info("All downloads completed and validated successfully!")
