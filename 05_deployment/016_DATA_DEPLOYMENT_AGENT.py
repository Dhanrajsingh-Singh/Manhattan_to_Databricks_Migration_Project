# ============================================================
# 016_DATA_DEPLOYMENT_AGENT.py
# Manhattan -> Databricks
# File Based POC Deployment Agent
# ============================================================

import json
import uuid
import traceback
from datetime import datetime
from pyspark.sql.types import *

# ============================================================
# WIDGETS
# ============================================================

dbutils.widgets.removeAll()

dbutils.widgets.text(
    "artifact_name",
    "",
    "Artifact Name (Blank = Latest)"
)

dbutils.widgets.dropdown(
    "environment",
    "dev",
    ["dev", "qa", "uat", "prod"],
    "Environment"
)

dbutils.widgets.dropdown(
    "dry_run",
    "false",
    ["true", "false"],
    "Dry Run"
)

dbutils.widgets.text(
    "source_path",
    "/Volumes/migration/default/migration_poc/manhattan_mock_artifacts/manhattan_mock_artifacts/source_data",
    "Source Data Path"
)

ARTIFACT_NAME = dbutils.widgets.get(
    "artifact_name"
).strip()

ENVIRONMENT = dbutils.widgets.get(
    "environment"
)

DRY_RUN = (
    dbutils.widgets.get(
        "dry_run"
    ).lower() == "true"
)

SOURCE_PATH = dbutils.widgets.get(
    "source_path"
).strip()

# ============================================================
# CONFIG
# ============================================================

PACKAGE_ROOT = (
    "dbfs:/Volumes/migration/repository/"
    "deployment_artifacts"
)

AUDIT_TABLE = (
    "migration.repository."
    "repo_data_migration_audit"
)

# ============================================================
# HELPERS
# ============================================================

def section(title):

    print("\n")
    print("=" * 80)
    print(title)
    print("=" * 80)

def now():

    return datetime.now()

# ============================================================
# AUDIT TABLE
# ============================================================

AUDIT_SCHEMA = StructType([

    StructField("migration_id", StringType(), True),
    StructField("artifact_name", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("rows_loaded", LongType(), True),
    StructField("status", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("completed_at", TimestampType(), True),
    StructField("error_message", StringType(), True)

])

# ============================================================
# DISCOVER PACKAGE
# ============================================================

section(
    "PACKAGE DISCOVERY"
)

packages = []

for item in dbutils.fs.ls(
    PACKAGE_ROOT
):

    if item.isDir():

        packages.append(
            {
                "name":
                    item.name.rstrip("/"),

                "path":
                    item.path,

                "modified":
                    item.modificationTime
            }
        )

if len(packages) == 0:

    raise Exception(
        "No deployment packages found."
    )

if ARTIFACT_NAME:

    package = None

    for p in packages:

        if p["name"] == ARTIFACT_NAME:

            package = p
            break

    if package is None:

        raise Exception(
            f"Package not found: "
            f"{ARTIFACT_NAME}"
        )

else:

    package = sorted(
        packages,
        key=lambda x:
            x["modified"],
        reverse=True
    )[0]

ARTIFACT_NAME = package["name"]

PACKAGE_PATH = package["path"]

print(
    f"Artifact : {ARTIFACT_NAME}"
)

print(
    f"Path     : {PACKAGE_PATH}"
)

# ============================================================
# LOAD METADATA
# ============================================================

section(
    "LOAD MIGRATION METADATA"
)

metadata_path = (
    f"{PACKAGE_PATH}/metadata/"
    f"migration_metadata.json"
)

metadata_json = dbutils.fs.head(
    metadata_path,
    1024 * 1024 * 5
)

metadata = json.loads(
    metadata_json
)

SOURCE_SYSTEM = metadata[
    "source_system"
]

tables = metadata[
    "tables"
]
# ============================================================
# METADATA VALIDATION
# ============================================================
required_fields = [

    "source_table",

    "target_table",

    "primary_key",

    "load_type",

    "cdc_column"
]

for table in tables:
    for field in required_fields:
        if field not in table:
            raise Exception(
                f"Missing metadata field: "

                f"{field}"
            )
print(
    f"Tables Found : "
    f"{len(tables)}"
)

# ============================================================
# SOURCE FILE DISCOVERY
# ============================================================

section(
    "SOURCE FILE DISCOVERY"
)

source_files = dbutils.fs.ls(
    SOURCE_PATH
)

available_files = {

    file.name.lower()

    for file in source_files
}

print(
    f"Source Files Found : "
    f"{len(available_files)}"
)

for file_name in sorted(
    available_files
):

    print(
        f" - {file_name}"
    )

# ============================================================
# MIGRATION START
# ============================================================

migration_id = (

    "MIG_"

    + datetime.now().strftime(
        "%Y%m%d%H%M%S"
    )

    + "_"

    + uuid.uuid4().hex[:6]
)

section(
    f"MIGRATION START : {migration_id}"
)

loaded_tables = []

skipped_tables = []

processed_files = set()

total_rows_loaded = 0

# ============================================================
# PROCESS TABLES
# ============================================================

for table in tables:

    source_table = (
        table["source_table"]
    )

    target_table = (
        table["target_table"]
    )

    load_type = (
        table["load_type"]
    )

    expected_file = (
        f"{source_table.lower()}.csv"
    )

    started_at = now()

    print(
        f"\nProcessing "
        f"{source_table}"
    )

    if expected_file not in available_files:

        print(
            f"Missing File : "
            f"{expected_file}"
        )

        skipped_tables.append(
            source_table
        )

        audit_data = [
            (
                migration_id,
                ARTIFACT_NAME,
                SOURCE_SYSTEM,
                source_table,
                target_table,
                load_type,
                0,
                "SKIPPED",
                started_at,
                now(),
                "Source file not found"
            )
        ]

        spark.createDataFrame(
            audit_data,
            schema=AUDIT_SCHEMA
        ).write.mode(
            "append"
        ).saveAsTable(
            AUDIT_TABLE
        )

        continue

    try:

        source_file = (
            f"{SOURCE_PATH}/"
            f"{expected_file}"
        )
        print(
            f"Source File : "
            f"{source_file}"
        )

        processed_files.add(
            expected_file
        )

        df = (

            spark.read

            .option(
                "header",
                True
            )

            .option(
                "inferSchema",
                True
            )

            .csv(
                source_file
            )
        )

        rows_loaded = (
            df.count()
        )
        total_rows_loaded += rows_loaded

        print(
            f"Rows : "
            f"{rows_loaded}"
        )

        if not DRY_RUN:

            (
                df.write

                .format(
                    "delta"
                )

                .mode(
                    "overwrite"
                )

                .option(
                    "overwriteSchema",
                    "true"
                )

                .saveAsTable(
                    target_table
                )
            )

        status = (

            "DRY_RUN"

            if DRY_RUN

            else "COMPLETED"
        )

        audit_data = [
            (
                migration_id,
                ARTIFACT_NAME,
                SOURCE_SYSTEM,
                source_table,
                target_table,
                load_type,
                rows_loaded,
                status,
                started_at,
                now(),
                ""
            )
        ]

        spark.createDataFrame(
            audit_data,
            schema=AUDIT_SCHEMA
        ).write.mode(
            "append"
        ).saveAsTable(
            AUDIT_TABLE
        )
        loaded_tables.append(
            source_table
        )

        print(
            f"Loaded -> "
            f"{target_table}"
        )

    except Exception as e:

        print(
            f"FAILED : {str(e)}"
        )

        audit_data = [
            (
                migration_id,
                ARTIFACT_NAME,
                SOURCE_SYSTEM,
                source_table,
                target_table,
                load_type,
                0,
                "FAILED",
                started_at,
                now(),
                str(e)
            )
        ]

        spark.createDataFrame(
            audit_data,
            schema=AUDIT_SCHEMA
        ).write.mode(
            "append"
        ).saveAsTable(
            AUDIT_TABLE
        )

        print(
            traceback.format_exc()
        )

# ============================================================
# ORPHAN FILES
# ============================================================

orphan_files = sorted(

    available_files
    -
    processed_files

)

# ============================================================
# SUMMARY
# ============================================================

section(
    "DATA DEPLOYMENT SUMMARY"
)

print(
    f"Migration ID    : "
    f"{migration_id}"
)

print(
    f"Artifact        : "
    f"{ARTIFACT_NAME}"
)

print(
    f"Environment     : "
    f"{ENVIRONMENT}"
)

print(
    f"Source System   : "
    f"{SOURCE_SYSTEM}"
)

print(
    f"Metadata Tables : "
    f"{len(tables)}"
)

print(
    f"Source Files    : "
    f"{len(available_files)}"
)

print(
    f"Loaded Tables   : "
    f"{len(loaded_tables)}"
)
print(
    f"Total Rows      : "
    f"{total_rows_loaded}"
)

print(
    f"Skipped Tables  : "
    f"{len(skipped_tables)}"
)

print(
    f"Orphan Files    : "
    f"{len(orphan_files)}"
)

print("\nLOADED")

for item in loaded_tables:

    print(
        f"  {item}"
    )

print("\nSKIPPED")

for item in skipped_tables:

    print(
        f"  {item}"
    )

print("\nORPHAN FILES")

for item in orphan_files:

    print(
        f"  {item}"
    )

print("\n")

print("=" * 80)
print("016 DATA DEPLOYMENT COMPLETED")
print("=" * 80)