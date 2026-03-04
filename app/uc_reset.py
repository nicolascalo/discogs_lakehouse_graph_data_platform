#!/usr/bin/env python3
import requests
import logging
import dotenv
from settings import Settings

dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dev_reset")

# -------------------------------
# CONFIG
# -------------------------------


settings = Settings.load()

UC_URL = settings.uc.url
UC_TOKEN = settings.uc.token
SCHEMA_LIST = ["raw","bronze","silver","gold","metrics","data"]
CATALOG = settings.project_name +"_"+settings.env

HEADERS = {
    "Authorization": f"Bearer {UC_TOKEN}",
    "Content-Type": "application/json",
}


# -------------------------------
# FUNCTIONS
# -------------------------------


def delete_uc_tables():
    for SCHEMA in SCHEMA_LIST:
        try:
            logger.info(f"Deleting tables in UC schema {CATALOG}.{SCHEMA} ...")
            response = requests.get(
                f"{UC_URL}/tables?catalog_name={CATALOG}&schema_name={SCHEMA}",
                headers=HEADERS,
            )
            tables = response.json().get("tables", [])
            for table in tables:
                name = table["name"]
                logger.info(f"Deleting table {name} ...")
                requests.delete(f"{UC_URL}/tables/{CATALOG}.{SCHEMA}.{name}", headers=HEADERS)
        except:
            pass

def delete_uc_schema():
    for SCHEMA in SCHEMA_LIST:
        try:
    
            logger.info(f"Deleting schema {CATALOG}.{SCHEMA} ...")
            requests.delete(
                f"{UC_URL}/schemas?catalog_name={CATALOG}&schema_name={SCHEMA}",
                headers=HEADERS,
            )
        except:
            pass


def delete_catalog(catalog_name: str):
    """Delete a UC catalog and all its schemas and tables."""
    # 1️⃣ List all schemas in the catalog
    schemas_resp = requests.get(f"{UC_URL}/schemas?catalog_name={catalog_name}", headers=HEADERS)
    if schemas_resp.status_code != 200:
        logger.error(f"Failed to list schemas for catalog {catalog_name}: {schemas_resp.text}")
        return

    schemas = schemas_resp.json().get("schemas", [])
    for s in schemas:
        schema_name = s["name"]
        logger.info(f"Deleting tables in schema {catalog_name}.{schema_name} ...")
        # List tables
        tables_resp = requests.get(
            f"{UC_URL}/tables?catalog_name={catalog_name}&schema_name={schema_name}",
            headers=HEADERS,
        )
        tables = tables_resp.json().get("tables", [])
        for t in tables:
            table_name = t["name"]
            logger.info(f"Deleting table {catalog_name}.{schema_name}.{table_name} ...")
            requests.delete(f"{UC_URL}/tables/{catalog_name}.{schema_name}.{table_name}", headers=HEADERS)

        # Delete schema
        logger.info(f"Deleting schema {catalog_name}.{schema_name} ...")
        requests.delete(f"{UC_URL}/schemas?catalog_name={catalog_name}&schema_name={schema_name}", headers=HEADERS)

    # 2️⃣ Delete catalog itself
    logger.info(f"Deleting catalog {catalog_name} ...")
    resp = requests.delete(f"{UC_URL}/catalogs/{catalog_name}?force=true", headers=HEADERS)
    if resp.status_code == 200:
        logger.info(f"Catalog {catalog_name} deleted successfully.")
    else:
        logger.error(f"Failed to delete catalog {catalog_name}: {resp.text}")



# -------------------------------
# MAIN
# -------------------------------

def main():
    delete_catalog(CATALOG)
    logger.info("Unity catalog environment reset complete.")


if __name__ == "__main__":
    main()