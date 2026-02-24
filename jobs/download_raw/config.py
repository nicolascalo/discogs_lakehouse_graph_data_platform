from dataclasses import dataclass
from pathlib import Path
import os
import dotenv
from ingest_helpers.config_helpers import load_config_from_json


@dataclass
class RawConfig:
    log_dir: Path
    data_dir: Path
    raw_data_dir: Path
    DISCOGS_DUMP_URL_ROOT: str
    DISC_PREFIX: str
    DISC_SUFFIX_LIST: list[str]
    YEAR_LIMIT_PAST: int
    PARALLEL_RANGES: int
    LOG_PROGRESS_EVERY_MB: int
    CHUNK_SIZE: int
    RETRIES: int
    TIMEOUT:int

    @classmethod
    def from_env(cls):
        dotenv.load_dotenv()
        log_dir = Path(os.getenv("LOG_DIR", "/logs"))
        data_dir = Path(os.getenv("DATA_DIR", "/data_tests"))
        config_dir = Path(os.getenv("CONFIG_DIR", "/config"))
        config_file = config_dir / "raw_config.json"
        
        raw_config = load_config_from_json(config_file)

        return cls(
            log_dir=log_dir,
            data_dir=data_dir,
            raw_data_dir=data_dir / "raw",
            DISCOGS_DUMP_URL_ROOT=raw_config["DISCOGS_DUMP_URL_ROOT"],
            DISC_PREFIX=raw_config["DISC_PREFIX"],
            DISC_SUFFIX_LIST=raw_config["DISC_SUFFIX_LIST"],
            YEAR_LIMIT_PAST=raw_config["YEAR_LIMIT_PAST"],
            PARALLEL_RANGES=raw_config["PARALLEL_RANGES"],
            LOG_PROGRESS_EVERY_MB=raw_config["LOG_PROGRESS_EVERY_MB"],
            CHUNK_SIZE=raw_config["CHUNK_SIZE"],
            RETRIES=raw_config["RETRIES"],
            TIMEOUT=raw_config["TIMEOUT"]


        )