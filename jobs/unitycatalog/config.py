from dataclasses import dataclass
from pathlib import Path
import os
import dotenv
from helpers_ingest.config_helpers import load_config_from_json


@dataclass
class UCConfig:
    log_dir: Path
    data_dir: Path
    archive_dir: Path
    config_file: Path
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    uc_url: str
    headers: dict
    region_name:str
    signature_version:str

    @classmethod
    def from_env(cls):
        dotenv.load_dotenv()

        log_dir = Path(os.getenv("LOG_DIR", "/logs"))
        data_dir = Path(os.getenv("DATA_DIR"))
        config_dir = Path(os.getenv("CONFIG_DIR", "/config"))
        config_file = config_dir / "uc_config.json"

        uc_config = load_config_from_json(config_file)

        return cls(
            log_dir=log_dir,
            data_dir=data_dir,
            archive_dir=(data_dir / "raw" / "archive"),
            config_file=config_file,
            minio_endpoint=uc_config["MINIO_ENDPOINT"],
            minio_access_key=uc_config["MINIO_ACCESS_KEY"],
            minio_secret_key=uc_config["MINIO_SECRET_KEY"],
            minio_bucket=uc_config["MINIO_BUCKET"],
            region_name=uc_config["REGION_NAME"],
            signature_version=uc_config["SIGNATURE_VERSION"],
            uc_url=uc_config["UC_URL"],
            headers=uc_config["HEADERS"],
        )




