# ==========================================================
# 016_DATA_LOAD_GENERATOR.py
# Metadata Driven Data Load Generator
# Manhattan -> Databricks
# ==========================================================

from datetime import datetime
import json

print("=" * 80)
print("016 DATA LOAD GENERATOR")
print("=" * 80)

# ==========================================================
# CONFIGURATION
# ==========================================================

TARGET_CATALOG = "migration_dev"

REPOSITORY_CATALOG = "migration"
REPOSITORY_SCHEMA = "repository"

BASE_REPOSITORY = (
    f"{REPOSITORY_CATALOG}.{REPOSITORY_SCHEMA}"
)

# ==========================================================
# FIND LATEST DEPLOYMENT PACKAGE
# ==========================================================

latest_package = (
    spark.table(
        f"{BASE_REPOSITORY}.repo_deployment_assets"
    )
    .orderBy(
        "generated_time",
        ascending=False
    )
    .first()
)

if latest_package is None:
    raise Exception(
        "No deployment package found."
    )

ARTIFACT_NAME = (
    latest_package["artifact_name"]
)

BASE_PATH = (
    "dbfs:/Volumes/"
    "migration/repository/"
    "deployment_artifacts/"
    f"{ARTIFACT_NAME}"
)

print(
    f"\nArtifact: {ARTIFACT_NAME}"
)

print(
    f"Base Path: {BASE_PATH}"
)

# ==========================================================
# LOAD METADATA
# ==========================================================

TABLE_METADATA = spark.table(
    f"{BASE_REPOSITORY}.repo_tables"
)

COLUMN_METADATA = spark.table(
    f"{BASE_REPOSITORY}.repo_table_columns"
)

table_rows = TABLE_METADATA.collect()

column_rows = COLUMN_METADATA.collect()

print(
    f"Tables discovered: "
    f"{len(table_rows)}"
)

print(
    f"Columns discovered: "
    f"{len(column_rows)}"
)

# ==========================================================
# BUILD COLUMN MAP
# ==========================================================

column_mapping = {}

for row in column_rows:

    table_name = (
        str(row["table_name"])
        .strip()
        .lower()
    )

    column_mapping.setdefault(
        table_name,
        []
    ).append(
        {
            "column_name":
                str(row["column_name"]),

            "data_type":
                str(row["data_type"])
        }
    )

# ==========================================================
# BUILD LOAD CONFIG
# ==========================================================

load_config = []

source_target_mapping = []

for row in table_rows:

    source_table = (
        str(row["table_name"])
        .strip()
    )

    target_table = (
        source_table.lower()
    )

    table_columns = (
        column_mapping.get(
            target_table,
            []
        )
    )

    load_config.append(
        {
            "source_table":
                source_table,

            "target_table":
                target_table,

            "target_fqn":
                (
                    f"{TARGET_CATALOG}"
                    f".bronze."
                    f"{target_table}"
                ),

            "source_type":
                "csv",

            "source_path":
                f"/landing/{target_table}.csv",

            "load_type":
                "FULL",

            "primary_key":
                None,

            "cdc_column":
                None
        }
    )

    source_target_mapping.append(
        {
            "table_name":
                source_table,

            "columns":
                table_columns
        }
    )

# ==========================================================
# SAVE LOAD CONFIG
# ==========================================================

dbutils.fs.mkdirs(
    f"{BASE_PATH}/configs"
)

dbutils.fs.put(
    f"{BASE_PATH}/configs/data_load_config.json",
    json.dumps(
        load_config,
        indent=4
    ),
    True
)

print(
    "Generated : data_load_config.json"
)

# ==========================================================
# SAVE SOURCE TARGET MAPPING
# ==========================================================

dbutils.fs.put(
    f"{BASE_PATH}/configs/source_target_mapping.json",
    json.dumps(
        source_target_mapping,
        indent=4
    ),
    True
)

print(
    "Generated : source_target_mapping.json"
)

# ==========================================================
# GENERATE LOAD MANIFEST
# ==========================================================

LOAD_MANIFEST = {
    "artifact_name":
        ARTIFACT_NAME,

    "generated_time":
        str(datetime.now()),

    "table_count":
        len(load_config),

    "tables":
        [
            cfg["target_table"]
            for cfg in load_config
        ]
}

dbutils.fs.mkdirs(
    f"{BASE_PATH}/metadata"
)

dbutils.fs.put(
    f"{BASE_PATH}/metadata/load_manifest.json",
    json.dumps(
        LOAD_MANIFEST,
        indent=4
    ),
    True
)

# ==========================================================
# CREATE LOAD AUDIT TABLE
# ==========================================================

spark.sql(f"""

CREATE TABLE IF NOT EXISTS
{BASE_REPOSITORY}.repo_load_audit
(
    load_id STRING,
    artifact_name STRING,
    table_name STRING,
    source_path STRING,
    target_table STRING,
    load_type STRING,
    rows_loaded BIGINT,
    status STRING,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message STRING
)

USING DELTA

""")

print(
    "Load audit repository ready."
)

# ==========================================================
# GENERATE DATA LOADER
# ==========================================================

LOADER_CODE = '''

import json

CONFIG_PATH = (
    "dbfs:/Volumes/"
    "migration/repository/"
    "deployment_artifacts/"
    "%ARTIFACT%"
    "/configs/data_load_config.json"
)

config_text = dbutils.fs.head(
    CONFIG_PATH,
    1024 * 1024
)

configs = json.loads(
    config_text
)

for cfg in configs:

    source_type = cfg["source_type"]

    source_path = cfg["source_path"]

    target_table = cfg["target_fqn"]

    load_type = cfg["load_type"]

    print(
        f"Loading {source_path}"
        f" -> {target_table}"
    )

    if source_type == "csv":

        df = (
            spark.read
            .option("header", True)
            .csv(source_path)
        )

    elif source_type == "json":

        df = spark.read.json(
            source_path
        )

    elif source_type == "parquet":

        df = spark.read.parquet(
            source_path
        )

    else:

        raise Exception(
            f"Unsupported source type "
            f"{source_type}"
        )

    write_mode = (
        "overwrite"
        if load_type == "FULL"
        else "append"
    )

    (
        df.write
        .format("delta")
        .mode(write_mode)
        .saveAsTable(
            target_table
        )
    )

    print(
        f"Rows Loaded : "
        f"{df.count()}"
    )

print(
    "DATA LOAD COMPLETED"
)

'''

LOADER_CODE = LOADER_CODE.replace(
    "%ARTIFACT%",
    ARTIFACT_NAME
)

dbutils.fs.mkdirs(
    f"{BASE_PATH}/src/loaders"
)

dbutils.fs.put(
    f"{BASE_PATH}/src/loaders/data_loader.py",
    LOADER_CODE,
    True
)

print(
    "Generated : data_loader.py"
)

# ==========================================================
# GENERATE VALIDATION SQL
# ==========================================================

validation_sql = []

for cfg in load_config:

    validation_sql.append(
        f"""
SELECT
'{cfg['target_table']}' AS table_name,
COUNT(*) AS row_count
FROM {cfg['target_fqn']};
"""
    )

dbutils.fs.mkdirs(
    f"{BASE_PATH}/sql/validation"
)

dbutils.fs.put(
    f"{BASE_PATH}/sql/validation/rowcount_validation.sql",
    "\n".join(validation_sql),
    True
)

print(
    "Generated : rowcount_validation.sql"
)

# ==========================================================
# SUMMARY
# ==========================================================

print("=" * 80)
print("016 DATA LOAD PACKAGE GENERATED")
print("=" * 80)

print(
    f"Artifact : {ARTIFACT_NAME}"
)

print(
    f"Tables   : {len(load_config)}"
)

print(
    f"Path     : {BASE_PATH}"
)