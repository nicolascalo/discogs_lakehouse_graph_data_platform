import requests


def uc_register_table(
    catalog, schema, name, s3a_location, columns, HEADERS, UC_URL, logger
):
    """Register a table in UC. Uses s3:// internally (UC requirement)."""
    s3_location = s3a_location.replace("s3a://", "s3://")
    # Delete if exists
    requests.delete(f"{UC_URL}/tables/{catalog}.{schema}.{name}", headers=HEADERS)
    response = requests.post(
        f"{UC_URL}/tables",
        headers=HEADERS,
        json={
            "name": name,
            "catalog_name": catalog,
            "schema_name": schema,
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": s3_location,
            "columns": columns,
        },
    )

    if response.status_code == 200:
        logger.info(
            f"Delta Table {UC_URL}/tables/{catalog}.{schema}.{name}: registered in UC: {response.json()}"
        )
        return None
    else:
        logger.error(
            f"Delta Table {UC_URL}/tables/{catalog}.{schema}.{name}: Failed to register in UC: {response.json()}"
        )
        return None


def uc_read_table(catalog, schema, name, HEADERS, UC_URL, spark, logger):
    """Read a UC-registered table via Spark, bypassing credential vending."""
    logger.info(f"Trying to read Delta Table {UC_URL}/tables/{catalog}.{schema}.{name}")
    response = requests.get(
        f"{UC_URL}/tables/{catalog}.{schema}.{name}", headers=HEADERS
    )
    if response.status_code == 200:
        logger.info(
            f"Delta Table {UC_URL}/tables/{catalog}.{schema}.{name} found: {response.json()}"
        )
        location = response.json()["storage_location"].replace("s3://", "s3a://")
        return spark.read.format("delta").load(location)
    else:
        logger.warning(
            f"Delta Table {UC_URL}/tables/{catalog}.{schema}.{name} not found: {response.json()}"
        )
        return None


def uc_list_tables(catalog, schema, HEADERS, UC_URL, logger):
    """List all tables in a UC schema."""
    response = requests.get(
        f"{UC_URL}/tables?catalog_name={catalog}&schema_name={schema}", headers=HEADERS
    )
    return [t["name"] for t in response.json().get("tables", [])]


def uc_create_catalog(catalog, HEADERS, UC_URL, logger, comment=""):
    r = requests.post(f"{UC_URL}/catalogs", headers=HEADERS, json={"name": catalog})
    if r.status_code == 200:
        logger.info(
            f"Created catalog: {catalog}"
        )
    elif "already exists" in r.text:
        pass  # already exists, fine
    else:
        logger.error(
            f"Catalog {catalog}: {r.json().get('message')}"
        )
    return r.json()


def uc_create_schemas(catalog, schema_list:list[str], HEADERS, UC_URL, logger, comment=""):
    response_list = []
    for schema in schema_list:
        r = requests.post(
            f"{UC_URL}/schemas",
            headers=HEADERS,
            json={"name": schema, "catalog_name": catalog},
        )
        if r.status_code == 200:
            logger.info(f"Created schema: {catalog}.{schema}")
        elif "already exists" in r.text:
            pass
        else:
            logger.error(f"Schema {catalog}.{schema}: {r.json().get('message')}")
            
        response_list.append(r.json())
    return response_list


def uc_delete_table(catalog, schema, name, HEADERS, UC_URL, logger):
    response = requests.delete(
        f"{UC_URL}/tables/{catalog}.{schema}.{name}", headers=HEADERS
    )
    logger.info(f"Deleted {catalog}.{schema}.{name} from UC")
    return response.json()


def uc_list_catalogs(HEADERS, UC_URL, logger):
    r = requests.get(f"{UC_URL}/catalogs", headers=HEADERS)
    return {c["name"] for c in r.json().get("catalogs", [])}


def uc_list_schemas(catalog, HEADERS, UC_URL, logger):
    r = requests.get(f"{UC_URL}/schemas?catalog_name={catalog}", headers=HEADERS)
    return {s["name"] for s in r.json().get("schemas", [])}
