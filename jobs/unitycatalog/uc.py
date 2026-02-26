import requests


def uc_register_table(catalog, schema, name, s3a_location, columns, HEADERS, UC_URL):
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
    assert response.status_code == 200, f"Registration failed: {response.json()}"
    print(f"✅ Registered {catalog}.{schema}.{name} in UC")
    return response.json()


def uc_read_table(catalog, schema, name, HEADERS, UC_URL, spark):
    """Read a UC-registered table via Spark, bypassing credential vending."""
    response = requests.get(
        f"{UC_URL}/tables/{catalog}.{schema}.{name}", headers=HEADERS
    )
    assert response.status_code == 200, f"Table not found: {response.json()}"
    location = response.json()["storage_location"].replace("s3://", "s3a://")
    return spark.read.format("delta").load(location)


def uc_list_tables(catalog, schema, HEADERS, UC_URL):
    """List all tables in a UC schema."""
    response = requests.get(
        f"{UC_URL}/tables?catalog_name={catalog}&schema_name={schema}", headers=HEADERS
    )
    return [t["name"] for t in response.json().get("tables", [])]


def uc_create_catalog(catalog, HEADERS, UC_URL, comment=""):
    r = requests.post(f"{UC_URL}/catalogs", headers=HEADERS, json={"name": catalog})
    if r.status_code == 200:
        print(f"  ✅ Created catalog: {catalog}")
    elif "already exists" in r.text:
        pass  # already exists, fine
    else:
        print(f"  ⚠️  Catalog {catalog}: {r.json().get('message')}")
    return r.json()


def uc_create_schema(catalog, schema, HEADERS, UC_URL, comment=""):
    r = requests.post(
        f"{UC_URL}/schemas",
        headers=HEADERS,
        json={"name": schema, "catalog_name": catalog},
    )
    if r.status_code == 200:
        print(f"  ✅ Created schema: {catalog}.{schema}")
    elif "already exists" in r.text:
        pass
    else:
        print(f"  ⚠️  Schema {catalog}.{schema}: {r.json().get('message')}")
    return r.json()


def uc_delete_table(catalog, schema, name, HEADERS, UC_URL):
    response = requests.delete(
        f"{UC_URL}/tables/{catalog}.{schema}.{name}", headers=HEADERS
    )
    print(f"✅ Deleted {catalog}.{schema}.{name} from UC")
    return response.json()


def uc_list_catalogs(HEADERS, UC_URL):
    r = requests.get(f"{UC_URL}/catalogs", headers=HEADERS)
    return {c["name"] for c in r.json().get("catalogs", [])}


def uc_list_schemas(catalog,  HEADERS, UC_URL):
    r = requests.get(f"{UC_URL}/schemas?catalog_name={catalog}", headers=HEADERS)
    return {s["name"] for s in r.json().get("schemas", [])}
