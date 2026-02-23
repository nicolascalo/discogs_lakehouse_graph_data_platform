from dataclasses import dataclass
from pathlib import Path
import os
import dotenv
from ingest_helpers.config_helpers import load_config_from_json


@dataclass
class BronzeConfig:
    log_dir: Path
    data_dir: Path
    raw_data_dir: Path
    bronze_data_dir: Path
    archive_dir: Path
    config_file: Path
    primary_keys: dict
    export_history_to_csv: bool

    @classmethod
    def from_env(cls):
        dotenv.load_dotenv()

        log_dir = Path(os.getenv("LOG_DIR", "/logs"))
        data_dir = Path(os.getenv("DATA_DIR", "/data_tests"))
        config_dir = Path(os.getenv("CONFIG_DIR", "/config"))
        config_file = config_dir / "bronze_config.json"

        bronze_config = load_config_from_json(config_file)

        return cls(
            log_dir=log_dir,
            data_dir=data_dir,
            raw_data_dir=data_dir / "raw",
            bronze_data_dir=data_dir / "bronze",
            archive_dir=(data_dir / "raw" / "archive"),
            config_file=config_file,
            primary_keys=bronze_config["PRIMARY_KEYS"],
            export_history_to_csv=os.getenv("EXPORT_HISTORY_TO_CSV", "false").lower() == "true",
        )