import os
import math
import requests
import logging
import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import re
import hashlib
import json
import dotenv

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ----------------------------
# Config
# ----------------------------

dotenv.load_dotenv()

DATA_RETRIEVAL_CONFIG_PATH = os.getenv("DATA_RETRIEVAL_CONFIG_PATH")

try:
    with open(DATA_RETRIEVAL_CONFIG_PATH, "r") as json_file:
        CONFIG = json.loads(json_file.read())
except Exception as e:
    raise(e)
 




RAW_DIR = os.getenv("RAW_DIR")
DISCOGS_DUMP_URL_ROOT = os.getenv("DISCOGS_DUMP_URL_ROOT")


os.makedirs(RAW_DIR, exist_ok=True)


# ----------------------------
# Utilities
# ----------------------------
def sha256_file(path: str, chunk_size=8192) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def retrieve_downloaded_dumps(dir: str) -> set[str]:
    return {f for f in os.listdir(dir) if f.startswith("discogs_")}

# ----------------------------
# Latest dump detection
# ----------------------------
def get_latest_dump_date(url: str) -> str:
    logger.info("Searching for latest Discogs dump...")
    test_date = datetime.datetime.today().date()
    while test_date.year > CONFIG["YEAR_LIMIT_PAST"]:
        day = str(test_date.day).zfill(2)
        month = str(test_date.month).zfill(2)
        url_date = f"{url}/{test_date.year}/{CONFIG["DISC_PREFIX"]}{test_date.year}{month}{day}"
        checksum_url = url_date + "_CHECKSUM.txt"

        try:
            r = requests.get(checksum_url, headers={"Range": "bytes=0-2048"}, timeout=30)
            text = r.text
            if re.search(r"^[a-fA-F0-9]{64}\s+discogs_\d{8}_", text, re.MULTILINE):
                logger.info("Found latest dump: %s", checksum_url)
                return url_date
        except Exception as e:
            logger.warning("Could not check %s: %s", checksum_url, e)

        test_date -= datetime.timedelta(days=1)

    raise RuntimeError(f"No valid Discogs dump found after {CONFIG["YEAR_LIMIT_PAST"]}")

# ----------------------------
# SHA256 validation
# ----------------------------
def validate_downloads(dir: str, date: str):
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
        usecols=[0, 1]
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

# ----------------------------
# Parallel range download with resume and retries
# ----------------------------
def download_large_file_parallel(url: str, output_dir: str, num_threads: int = CONFIG["PARALLEL_RANGES"]):
    filename = os.path.basename(url)
    local_path = os.path.join(output_dir, filename)
    temp_dir = os.path.join(output_dir, filename + "_parts")
    os.makedirs(temp_dir, exist_ok=True)

    if os.path.exists(local_path):
        logger.info("File already downloaded: %s", filename)
        return

    # Get total file size
    r = requests.head(url, allow_redirects=True, timeout=30)
    if "Content-Length" not in r.headers:
        raise RuntimeError("Cannot determine file size for parallel download")
    total_size = int(r.headers["Content-Length"])
    part_size = math.ceil(total_size / num_threads)
    ranges = [(i*part_size, min((i+1)*part_size-1, total_size-1)) for i in range(num_threads)]

    logger.info("Downloading %s with %d parallel ranges, total %.2f MB", filename, num_threads, total_size/1e6)

    # Internal function for downloading a range
    def download_range(idx, start, end):
        part_path = os.path.join(temp_dir, f"part_{idx}")
        downloaded = os.path.getsize(part_path) if os.path.exists(part_path) else 0
        headers = {"Range": f"bytes={start + downloaded}-{end}"}
        mode = "ab" if downloaded > 0 else "wb"

        retries = 0
        mb_counter = 0
        while retries <= CONFIG["RETRIES"]:
            try:
                with requests.get(url, headers=headers, stream=True, timeout=CONFIG["TIMEOUT"]) as r:
                    r.raise_for_status()
                    with open(part_path, mode) as f:
                        for chunk in r.iter_content(chunk_size=CONFIG["CHUNK_SIZE"]):
                            if chunk:
                                f.write(chunk)
                                mb_counter += len(chunk)
                                if mb_counter >= CONFIG["LOG_PROGRESS_EVERY_MB"]*1024*1024:
                                    total_downloaded = sum(os.path.getsize(os.path.join(temp_dir, f"part_{i}")) for i in range(num_threads))
                                    logger.info("Downloading %s: %.2f / %.2f MB", filename, total_downloaded/1e6, total_size/1e6)
                                    mb_counter = 0
                break  # success
            except requests.exceptions.ReadTimeout:
                retries += 1
                logger.warning("Timeout in part %d, retry %d/%d", idx, retries, CONFIG["RETRIES"])
            except Exception as e:
                logger.error("Error in part %d: %s", idx, e)
                raise
        else:
            raise RuntimeError(f"Failed to download part {idx} after {CONFIG["RETRIES"]} retries")

    # Download all ranges in parallel
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(download_range, idx, start, end) for idx, (start, end) in enumerate(ranges)]
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

# ----------------------------
# Main execution
# ----------------------------requests
if __name__ == "__main__":
    already_downloaded = retrieve_downloaded_dumps(RAW_DIR)
    latest_discogs_dump_url_root = get_latest_dump_date(DISCOGS_DUMP_URL_ROOT)
    date_latest_dump = re.sub(".*_", "", latest_discogs_dump_url_root)

    for suffix in CONFIG["DISC_SUFFIX_LIST"]:
        full_url = f"{latest_discogs_dump_url_root}{suffix}"
        download_large_file_parallel(full_url, RAW_DIR)

    validate_downloads(RAW_DIR, date_latest_dump)
    logger.info("All downloads completed and validated successfully!")
