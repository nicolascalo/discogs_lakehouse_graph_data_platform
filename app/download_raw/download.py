import os
import requests
import datetime
import re
import httpx
import time



from boto3.s3.transfer import TransferConfig


MIN_S3_PART_SIZE = 5 * 1024 * 1024  # 5 MB minimum for multipart



def download_file_to_minio_stream(
    url: str,
    bucket: str,
    object_key: str,
    logger,
    s3,
    chunk_size: int = 4 * 1024 * 1024,
    timeout: int = 60,
    max_retries: int = 3
):
    """
    Fully streaming download from HTTP → MinIO (S3) with manual multipart upload.
    Works for unknown Content-Length and very large files.
    """

    # skip if exists
    try:
        s3.head_object(Bucket=bucket, Key=object_key)
        logger.info("Already exists in MinIO: s3://%s/%s", bucket, object_key)
        return
    except s3.exceptions.ClientError:
        pass

    for attempt in range(1, max_retries + 1):
        
        try:
            logger.info(f"{attempt = }")
            logger.info(f"{bucket = }")
            logger.info(f"{object_key = }")
            # start multipart upload
            
            logger.info(f"Test0")
            mpu = s3.create_multipart_upload(Bucket=bucket, Key=object_key)
            logger.info(f"Test1")

            upload_id = mpu["UploadId"]
            parts = []
            part_number = 1
            buffer = bytearray()
            downloaded = 0
            
            logger.info(f"Starting httpx.stream")

            with httpx.stream("GET", url, timeout=timeout) as response:
                
                logger.info(f"{url = }")
                
                
                response.raise_for_status()
                
                
                total_size = int(response.headers.get("Content-Length", 0))
                
                logger.info(f"{total_size = }")

                start_time = time.time()

                for chunk in response.iter_bytes(chunk_size):
                    buffer.extend(chunk)
                    downloaded += len(chunk)

                    # show progress
                    if total_size:
                        percent = downloaded / total_size * 100
                        speed = downloaded / (time.time() - start_time) / 1e6
                        print(f"\r{downloaded / 1e6:.2f}/{total_size / 1e6:.2f} MB "
                              f"({percent:.1f}%, {speed:.2f} MB/s)", end="", flush=True)
                    else:
                        speed = downloaded / (time.time() - start_time) / 1e6
                        print(f"\r{downloaded / 1e6:.2f} MB downloaded ({speed:.2f} MB/s)",
                              end="", flush=True)

                    # upload full parts
                    while len(buffer) >= MIN_S3_PART_SIZE:
                        part = s3.upload_part(
                            Bucket=bucket,
                            Key=object_key,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=buffer[:MIN_S3_PART_SIZE]
                        )
                        parts.append({"ETag": part["ETag"], "PartNumber": part_number})
                        buffer = buffer[MIN_S3_PART_SIZE:]
                        part_number += 1

                # upload remaining buffer
                if buffer:
                    part = s3.upload_part(
                        Bucket=bucket,
                        Key=object_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=bytes(buffer)
                    )
                    parts.append({"ETag": part["ETag"], "PartNumber": part_number})

            # complete multipart upload
            s3.complete_multipart_upload(
                Bucket=bucket,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts}
            )

            print("\nUpload complete")
            logger.info("Streaming upload complete: s3://%s/%s", bucket, object_key)
            return

        except Exception as e:
            print()
            logger.warning("Retry %d/%d failed: %s", attempt, max_retries, e)
            try:
                s3.abort_multipart_upload(Bucket=bucket, Key=object_key, UploadId=upload_id)
            except Exception:
                pass
            if attempt == max_retries:
                raise




            
def download_file_httpx_s3(
    url: str,
    bucket: str,
    object_key: str,
    logger,
    s3,
    chunk_size: int = 4 * 1024 * 1024,  # 4MB
    timeout: int = 60,
    max_retries: int = 3,
):
    """
    Download a file via httpx and upload it to S3/MinIO.
    Works with unknown Content-Length and large files.
    """

    # Check if object already exists in MinIO
    try:
        s3.head_object(Bucket=bucket, Key=object_key)
        logger.info("File already exists in MinIO: s3://%s/%s", bucket, object_key)
        return
    except s3.exceptions.ClientError:
        pass  # proceed to download

    # Use a temp file like in your local function
    temp_path = object_key.replace("/", "_") + ".part"

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    timeout_config = httpx.Timeout(connect=timeout, read=timeout, write=timeout, pool=timeout)

    for attempt in range(1, max_retries + 1):
        try:
            with httpx.Client(http2=True, limits=limits, timeout=timeout_config, follow_redirects=True) as client:
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

            # download complete, now upload to S3/MinIO
            logger.info("\nUploading to s3://%s/%s", bucket, object_key)
            s3.upload_file(temp_path, bucket, object_key)
            logger.info("Upload complete: s3://%s/%s", bucket, object_key)

            # cleanup temp file
            os.remove(temp_path)
            break  # success

        except (httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.HTTPError) as e:
            print(f"\nRetry {attempt}/{max_retries} due to: {e}")
            if attempt == max_retries:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise


class HTTPXStreamAdapter:
    """
    Wrap httpx byte iterator into file-like object for boto3.
    """

    def __init__(self, iterator):
        self.iterator = iterator
        self.buffer = b""

    def read(self, size=-1):
        try:
            return next(self.iterator)
        except StopIteration:
            return b""


def download_file_to_minio(
    url: str,
    bucket: str,
    object_key: str,
    logger,
    chunk_size: int = 8 * 1024 * 1024,
    timeout: int = 60,
    max_retries: int = 3,
):
    
    


    # Skip if exists
    try:
        s3.head_object(Bucket=bucket, Key=object_key)
        logger.info("Already exists in MinIO: %s", object_key)
        return
    except s3.exceptions.ClientError:
        pass

    logger.info("Downloading %s → s3://%s/%s", url, bucket, object_key)

    transfer_config = TransferConfig(
        multipart_threshold=chunk_size,
        multipart_chunksize=chunk_size,
        max_concurrency=4,
        use_threads=True,
    )

    for attempt in range(1, max_retries + 1):
        try:
            with httpx.stream("GET", url, timeout=timeout) as response:
                response.raise_for_status()

                stream = HTTPXStreamAdapter(
                    response.iter_bytes(chunk_size)
                )

                s3.upload_fileobj(
                    stream,
                    bucket,
                    object_key,
                    Config=transfer_config,
                )

            logger.info("Upload complete: %s", object_key)
            return

        except Exception as e:
            logger.warning("Retry %d/%d failed: %s", attempt, max_retries, e)
            if attempt == max_retries:
                raise
            
            
def retrieve_downloaded_dumps_s3(bucket: str, prefix: str, s3, catalog_root:str) -> set[str]|None:
    
    bucket_list = s3.list_buckets()
    
    if bucket in bucket_list:
    
        paginator = s3.get_paginator("list_objects_v2")

        dumps = set()
        print(f"{paginator.paginate(Bucket=bucket, Prefix=prefix)}")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            print(f"{page}")
            
            for obj in page.get("Contents", []):
                print(f"{obj}")
                name = obj["Key"].split("/")[-1]
                print(f"{name}")
                
                if name.startswith(catalog_root):
                    dumps.add(name)

        return dumps

    else:
        return None



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
