from dataclasses import dataclass
from pathlib import Path
import os
import dotenv
from helpers_ingest.config_helpers import load_config_from_json


@dataclass
class SilverConfig:
    log_dir: Path
    data_dir: Path
    raw_data_dir: Path
    bronze_data_dir: Path
    silver_data_dir: Path
    archive_dir: Path
    config_file: Path

    @classmethod
    def from_env(cls):
        dotenv.load_dotenv()

        log_dir = Path(os.getenv("LOG_DIR", "/logs"))
        data_dir = Path(os.getenv("DATA_DIR"))
        config_dir = Path(os.getenv("CONFIG_DIR", "/config"))
        config_file = config_dir / "silver_config.json"

        silver_config = load_config_from_json(config_file)

        return cls(
            log_dir=log_dir,
            data_dir=data_dir,
            raw_data_dir=data_dir / "raw",
            silver_data_dir=data_dir / "silver",
            bronze_data_dir=data_dir / "bronze",
            archive_dir=(data_dir / "raw" / "archive"),
            config_file=config_file,
        )
