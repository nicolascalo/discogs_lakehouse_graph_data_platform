import requests

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
import logging


def merge_into_uc_delta(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,  # catalog.schema.table
    primary_key: str,
    hash_col: str | None,
    logger: logging.Logger,
    secondary_keys: list[str] | None = None,
) -> None:

    if secondary_keys:
        logger.info(f"Merging into {table_name} based on {hash_col}, {primary_key} and {secondary_keys}")
        
        if len(secondary_keys) == 1:
            
            secondary_key = secondary_keys[0]
            merge_logic= f"(t.`{primary_key}` = s.`{primary_key}` ) AND (t.`{secondary_key}` = s.`{secondary_key}` ) "
        
        else:
            merge_logic_secondary = []
            
            for key in secondary_keys:
                merge_logic_secondary.append(f"(t.{key} <> s.{key})")
            
            merge_logic_secondary = " AND ".join(merge_logic_secondary)

        
        
            merge_logic= f"(t.`{primary_key}` = s.`{primary_key}` ) AND {merge_logic_secondary}"
        
    else:
        logger.info(f"Merging into {table_name} based on {hash_col} and {primary_key}")
        merge_logic= f"t.`{primary_key}` = s.`{primary_key}`"
        
    logger.info(f"{merge_logic = }")
    target = DeltaTable.forPath(spark, table_name)

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


    if hash_col:
        
        logger.info(f"{hash_col = }")
        
        
        (
            target.alias("t")
            .merge(
                df.alias("s"),
                merge_logic,
            )
            .whenMatchedUpdateAll(condition=f"t.{hash_col} <> s.{hash_col}")
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )
    else:
        (
            target.alias("t")
            .merge(
                df.alias("s"),
                merge_logic,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )


def uc_create_table_if_not_exists(
    spark,
    catalog,
    schema,
    table_name,
    columns,
    location,
):
    cols = ", ".join([f"{c['name']} {c['type']}" for c in columns])

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name}
    ({cols})
    USING DELTA
    LOCATION '{location}'
    """)


def uc_create_catalog(
    catalog, HEADERS, UC_URL, logger, comment="", managed_location=""
):

    payload = {"name": catalog}
    if managed_location:
        payload["managedLocation"] = managed_location
    if comment:
        payload["comment"] = comment

    r = requests.post(f"{UC_URL}/catalogs", headers=HEADERS, json=payload)

    if r.status_code in (200, 201):
        logger.info(f"Created catalog: {catalog}")
    elif r.status_code == 409:  # Conflict → already exists
        logger.info(f"Catalog {catalog} already exists, skipping creation")
    else:
        logger.error(f"Failed to create catalog {catalog}: {r.text}")
        r.raise_for_status()

    return r.json()


def uc_create_schemas(
    catalog, schema_list: list[str], HEADERS, UC_URL, logger, comment=""
):
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


def uc_list_tables(catalog, schema, HEADERS, UC_URL, logger):
    """List all tables in a UC schema."""
    response = requests.get(
        f"{UC_URL}/tables?catalog_name={catalog}&schema_name={schema}", headers=HEADERS
    )
    return [t["name"] for t in response.json().get("tables", [])]


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
