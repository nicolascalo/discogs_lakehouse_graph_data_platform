import requests
import os
import datetime
import pandas as pd
import hashlib
import re
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry



session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)


CHOCOLATE_DIR = './data/chocolate/'
YEAR_LIMIT_PAST = 2007

url_root = 'https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data'


os.makedirs(CHOCOLATE_DIR, exist_ok=True)

discogs_suffix_list = ["_releases.xml.gz","_masters.xml.gz","_labels.xml.gz","_artists.xml.gz","_CHECKSUM.txt"]
discogs_prefix = "discogs_"



def sha256_file(path, chunk_size=8192):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

    
def get_file_headers(url: str) -> dict:
    r = session.head(url, allow_redirects=True, timeout=60)
    return r.headers




def download_large_file(path:str, url:str):

    local_filename = os.path.join(path, os.path.basename(url))

    try:
        with session.get(url, stream=True,  timeout=(10, 60)) as response:
            response.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info("File downloaded successfully!")
    except requests.exceptions.RequestException as e:
        logger.exception("Error downloading the file:", e)
        
        


def retrieve_downloaded_dumps(dir: str) -> set[str]:
    return {
        f for f in os.listdir(dir)
        if f.startswith("discogs_")
    }

def get_latest_dump_date(url_root):
    
    logger.info("Starting Discogs' dump page exploration")
    
    test_date = datetime.datetime.today().date()
    found_checksum_file = False
    
    while test_date.year > YEAR_LIMIT_PAST :
        
        test_date_day = test_date.day.__str__().zfill(2)
        test_date_month = test_date.month.__str__().zfill(2)
        
        
        url_root_date = f"{url_root}/{test_date.year}/{discogs_prefix}{test_date.year}{test_date_month}{test_date_day}"
        
        logger.debug(f"Testing {url_root_date}")
        checksum_url = url_root_date + "_CHECKSUM.txt"
        
        
        r = session.get(checksum_url, headers={"Range": "bytes=0-2048"}, timeout=60)
        text = r.text

        if not re.search(r"^[a-fA-F0-9]{64}\s+discogs_\d{8}_", text, re.MULTILINE):
            test_date -= datetime.timedelta(days=1)
            continue        
        
        else:
            logger.info("Found file: %s", checksum_url)
            found_checksum_file = True
            break
    
    if found_checksum_file != True:
        raise RuntimeError("No valid Discogs dump found after %s" % YEAR_LIMIT_PAST)
    
    return url_root_date
        
    
        
def validate_downloads(dir:str,date:str):
    
    file_list = os.listdir(dir)
    
    error_flag = False
    
    logger.info("Validating date=%s", date)

    checksum_file = [file for file in file_list if ("CHECKSUM" in file) and (date in file)]
    
    try:
        checksum_file = checksum_file[0]
    except Exception as e:
        logger.exception("No checksum file found for date %s", date)
        error_flag = True

        raise


    logger.info("Validating checksums from %s", checksum_file)

        
    df = pd.read_csv(
        os.path.join(dir, checksum_file),
        sep=r"\s+",
        engine="python",
        names=["checksum", "file_name"],
        usecols=[0, 1]
    )    
        
    
    for i, row in df.iterrows():
        
        file_path = os.path.join(dir, row["file_name"])
        
        if not os.path.exists(file_path):
            error_flag = True
            logger.error("Missing file: %s", file_path)
            continue
        
        
        hash_file = sha256_file(file_path)
        
            
        if hash_file != row['checksum'] :
            error_flag = True
            os.remove(file_path)
            logger.warning("sha256 hash not OK for %s", row["file_name"])
        else:
            logger.info("sha256 hash OK for %s", row["file_name"])
            
    if error_flag == True:
        raise Exception("At least one file was not downloaded correctly. Faulty files were deleted")
             
        
        
            
        
    



if __name__ == "__main__":
    
    already_downloaded = retrieve_downloaded_dumps(CHOCOLATE_DIR)
    latest_dump_date_root_url = get_latest_dump_date(url_root)
    date_latest_dump = re.sub(".*_","",latest_dump_date_root_url)


    for suffix in discogs_suffix_list :
    
        full_url = f"{latest_dump_date_root_url}{suffix}"
        
        logger.info("Trying to download %s", full_url)
        
        headers = get_file_headers(full_url)
        
        
        filename = os.path.basename(full_url)
        if filename in already_downloaded:
            logger.info("File already downloaded: %s", filename)
            continue


        
                    
        try:
            logger.info("Starting to download %s", full_url)
            size = headers.get("Content-Length")
            
            logger.info("File size = %s", size)
            download_large_file(CHOCOLATE_DIR, full_url)
            logger.info("Finished downloading %s", full_url)
                    
        except Exception as e:
            logger.exception(e)
            raise
        

    
    validate_downloads(CHOCOLATE_DIR, date_latest_dump)