from dataclasses import dataclass
from pathlib import Path
import os
import dotenv
import json
import boto3
from typing import Any
from minio.minio_client import create_s3_bucket

dotenv.load_dotenv()


def load_json(path: Path):
    with open(path) as f:
        return json.load(f)


@dataclass(frozen=True)
class StorageSettings:
    type: str  # minio or local
    env: str
    endpoint: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    bucket: str | None = None
    region: str | None = None
    local_path: Path | None = None
    s3 : Any= None

    @classmethod
    def from_env(cls):
        env = os.getenv("ENV", "test")
        storage_type = os.getenv("STORAGE_TYPE", "minio").lower()

        if storage_type == "minio":
            type = "minio"
            env = env
            endpoint =                 os.getenv(
                    f"{storage_type.upper()}_{env.upper()}_OBJECT_STORAGE_ENDPOINT"
                )
            
            access_key =                 os.getenv(
                    f"{storage_type.upper()}_{env.upper()}_OBJECT_STORAGE_ACCESS_KEY"
                )
            
            secret_key =                 os.getenv(
                    f"{storage_type.upper()}_{env.upper()}_OBJECT_STORAGE_SECRET_KEY"
                )
            
            region =                 os.getenv(
                    f"{storage_type.upper()}_{env.upper()}_OBJECT_STORAGE_REGION"
                )
            
            bucket = os.getenv("PROJECT_NAME")
            
            
            s3 = boto3.client(
                "s3",
                endpoint_url = endpoint,
                aws_access_key_id= access_key,
                aws_secret_access_key = secret_key,
                region_name = region
            )
            
            create_s3_bucket(s3, bucket, logger=None)

            return cls(
                type=type, env=env, endpoint=endpoint, access_key=access_key, secret_key=secret_key, bucket=bucket,region=region, s3=s3
            )
        else:  # local
            return cls(
                type="local",
                env=env,
                local_path=Path(os.getenv("LOCAL_DATA_PATH", "./data")),
            )



@dataclass(frozen=True)
class UCSettings:
    url: str
    token: str
    signature_version: str

    @classmethod
    def from_env(cls):

        return cls(
            url=os.getenv("UC_URL"),
            token=os.getenv("UC_TOKEN"),
            signature_version=os.getenv("SIGNATURE_VERSION"),
        )

    @property
    def headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }


@dataclass(frozen=True)
class Settings:
    env: str
    project_name: str
    log_dir: Path
    metadata_dir: Path
    storage: StorageSettings
    uc: UCSettings
    bronze: dict
    silver: dict
    gold: dict
    
    raw: dict

    @classmethod
    def load(cls):
        env = os.getenv("ENV", "dev")
        project_name = os.getenv("PROJECT_NAME", "discogs")
        log_dir = Path(os.getenv("LOG_DIR", "/logs"))
        config_dir = Path(os.getenv("CONFIG_DIR", "/config"))
        metadata_dir = Path(os.getenv("METADATA_DIR", "/metadata"))

        return cls(
            env=env,
            project_name=project_name,
            log_dir=log_dir,
            metadata_dir=metadata_dir,
            storage=StorageSettings.from_env(),
            uc=UCSettings.from_env(),
            bronze=load_json(Path(f"{config_dir}/bronze_config.json")),
            silver=load_json(Path(f"{config_dir}/silver_config.json")),
            raw=load_json(Path(f"{config_dir}/raw_config.json")),
            gold=load_json(Path(f"{config_dir}/gold_config.json")),
        )
