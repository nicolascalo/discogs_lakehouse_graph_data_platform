import os
import math
import requests
import datetime
from concurrent.futures import ThreadPoolExecutor
import re
import httpx
import time


def supports_range(url):
    r = requests.get(url, headers={"Range": "bytes=0-0"}, stream=True)
    return r.status_code == 206


def retrieve_downloaded_dumps(dir: str) -> set[str]:
    return {f for f in os.listdir(dir) if f.startswith("discogs_")}


def check_server_parallel_download(url: str, logger):
    test = requests.get(url, headers={"Range": "bytes=0-0"}, stream=True)
    if test.status_code != 206:
        logger.info("Server does not support range requests")
        return False
    else:
        logger.info("Server supports range requests")
        return True


def download_file_httpx(
    url: str,
    output_dir: str,
    logger,
    chunk_size: int = 1024 * 1024 * 4,  # 4MB chunks (faster than 1MB)
    timeout: int = 60,
    max_retries: int = 3,
):
    os.makedirs(output_dir, exist_ok=True)
    logger.info("Downloading from url: %s", url)
    filename = os.path.basename(url) or "downloaded_file"
    final_path = os.path.join(output_dir, filename)
    temp_path = final_path + ".part"

    if os.path.exists(final_path):
        logger.info("File already downloaded: %s", filename)
        return final_path

    limits = httpx.Limits(
        max_keepalive_connections=10,
        max_connections=20,
    )

    timeout_config = httpx.Timeout(
        connect=timeout,
        read=timeout,
        write=timeout,
        pool=timeout,
    )

    with httpx.Client(
        http2=True,  # 🔥 enable HTTP/2
        limits=limits,
        timeout=timeout_config,
        follow_redirects=True,
    ) as client:
        for attempt in range(1, max_retries + 1):
            try:
                with client.stream("GET", url) as response:
                    response.raise_for_status()

                    total_size = int(response.headers.get("Content-Length", 0))
                    downloaded = 0
                    start_time = time.time()

                    with open(temp_path, "wb") as f:
                        for chunk in response.iter_bytes(chunk_size):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)

                                if total_size:
                                    elapsed = time.time() - start_time
                                    speed = downloaded / elapsed / 1e6
                                    print(
                                        f"\r{downloaded / 1e6:.2f}/{total_size / 1e6:.2f} MB "
                                        f"({speed:.2f} MB/s)",
                                        end="",
                                        flush=True,
                                    )

                break  # success

            except (httpx.ReadTimeout, httpx.RemoteProtocolError) as e:
                print(f"\nRetry {attempt}/{max_retries} due to: {e}")
                if attempt == max_retries:
                    raise

    os.rename(temp_path, final_path)
    logger.info("Downloading from url %s complete", url)
    return final_path


def download_file(
    url: str, output_dir: str, chunk_size: int = 1024 * 1024, timeout: int = 30
):
    os.makedirs(output_dir, exist_ok=True)

    # Get filename (fallback if URL doesn't contain it)
    local_filename = os.path.basename(url) or "downloaded_file"
    local_path = os.path.join(output_dir, local_filename)

    if os.path.exists(local_path):
        print(f"File already exists: {local_filename}")
        return local_path

    with requests.get(url, stream=True, timeout=timeout, allow_redirects=True) as r:
        r.raise_for_status()

        total_size = int(r.headers.get("Content-Length", 0))
        downloaded = 0

        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

                    if total_size:
                        print(
                            f"\rDownloading: {downloaded / 1e6:.2f} / {total_size / 1e6:.2f} MB",
                            end="",
                            flush=True,
                        )

    print("\nDownload complete.")
    return local_path


def download_large_file_parallel(
    url: str,
    output_dir: str,
    num_threads: int,
    num_retries: int,
    LOG_PROGRESS_EVERY_MB: int,
    CHUNK_SIZE: int,
    TIMEOUT: int,
    logger,
):

    filename = os.path.basename(url)
    local_path = os.path.join(output_dir, filename)
    temp_dir = os.path.join(output_dir, filename + "_parts")
    os.makedirs(temp_dir, exist_ok=True)

    if os.path.exists(local_path):
        logger.info("File already downloaded: %s", filename)
        return

    # Get total file size
    logger.info(f"{url = }")
    # r = requests.head(url, timeout=TIMEOUT, allow_redirects=True)

    with requests.get(url, stream=True, allow_redirects=True) as r:
        total_size = r.headers.get("Content-Length")

    if total_size:
        total_size = int(total_size)
    else:
        raise RuntimeError("Cannot determine file size for parallel download")

    # total_size = int(r.headers["Content-Length"])
    logger.info(f"{total_size = }")

    part_size = math.ceil(total_size / num_threads)
    ranges = [
        (i * part_size, min((i + 1) * part_size - 1, total_size - 1))
        for i in range(num_threads)
    ]

    logger.info(
        "Downloading %s with %d parallel ranges, total %.2f MB",
        filename,
        num_threads,
        total_size / 1e6,
    )

    # Internal function for downloading a range
    def download_range(idx, start, end):
        part_path = os.path.join(temp_dir, f"part_{idx}")
        downloaded = os.path.getsize(part_path) if os.path.exists(part_path) else 0
        headers = {"Range": f"bytes={start + downloaded}-{end}"}
        mode = "ab" if downloaded > 0 else "wb"

        retries = 0
        mb_counter = 0
        while retries <= num_retries:
            try:
                with requests.get(
                    url, headers=headers, stream=True, timeout=TIMEOUT
                ) as r:
                    r.raise_for_status()
                    with open(part_path, mode) as f:
                        for chunk in r.iter_content(CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                mb_counter += len(chunk)
                                if mb_counter >= LOG_PROGRESS_EVERY_MB * 1024 * 1024:
                                    total_downloaded = sum(
                                        os.path.getsize(
                                            os.path.join(temp_dir, f"part_{i}")
                                        )
                                        for i in range(num_threads)
                                    )
                                    logger.info(
                                        "Downloading %s: %.2f / %.2f MB",
                                        filename,
                                        total_downloaded / 1e6,
                                        total_size / 1e6,
                                    )
                                    mb_counter = 0
                break  # success
            except requests.exceptions.ReadTimeout:
                retries += 1
                logger.warning(
                    "TIMEOUT in part %d, retry %d/%d", idx, retries, num_retries
                )
            except Exception as e:
                logger.error("Error in part %d: %s", idx, e)
                raise
        else:
            raise RuntimeError(
                f"Failed to download part {idx} after {num_retries} retries"
            )

    # Download all ranges in parallel
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(download_range, idx, start, end)
            for idx, (start, end) in enumerate(ranges)
        ]
        for f in futures:
            f.result()  # raise any exceptions

    # Combine parts
    with open(local_path, "wb") as out_f:
        for i in range(num_threads):
            part_path = os.path.join(temp_dir, f"part_{i}")
            with open(part_path, "rb") as pf:
                out_f.write(pf.read())
            os.remove(part_path)
    os.rmdir(temp_dir)
    logger.info("Finished downloading %s", filename)


def get_latest_dump_date(
    url: str, YEAR_LIMIT_PAST: int, DISC_PREFIX: str, TIMEOUT, logger
) -> str:
    logger.info("Searching for latest Discogs dump...")
    test_date = datetime.datetime.today().date()
    while test_date.year > YEAR_LIMIT_PAST:
        day = str(test_date.day).zfill(2)
        month = str(test_date.month).zfill(2)

        url_date = f"{url}/{test_date.year}/{DISC_PREFIX}{test_date.year}{month}{day}"
        # logger.info(f"Testing {url_date}")

        checksum_url = url_date + "_CHECKSUM.txt"

        try:
            r = requests.get(
                checksum_url, headers={"Range": "bytes=0-2048"}, timeout=TIMEOUT
            )
            text = r.text
            if re.search(r"^[a-fA-F0-9]{64}\s+discogs_\d{8}_", text, re.MULTILINE):
                logger.info("Found latest dump: %s", checksum_url)
                return url_date
        except Exception as e:
            logger.warning("Could not check %s: %s", checksum_url, e)

        test_date -= datetime.timedelta(days=1)

    raise RuntimeError(f"No valid Discogs dump found after {YEAR_LIMIT_PAST}")
