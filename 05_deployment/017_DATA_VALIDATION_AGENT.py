# ============================================================
# 017_DATA_VALIDATION_AGENT.py
# Manhattan -> Databricks Migration Certification Agent
# ============================================================

import json
import uuid

from datetime import datetime
from pyspark.sql import Row

# ============================================================
# WIDGETS
# ============================================================

dbutils.widgets.removeAll()

dbutils.widgets.text(
    "artifact_name",
    "",
    "Artifact Name (Blank = Latest)"
)

dbutils.widgets.text(
    "source_path",
    "/Volumes/migration/default/migration_poc/manhattan_mock_artifacts/manhattan_mock_artifacts/source_data",
    "Source Data Path"
)

ARTIFACT_NAME = (
    dbutils.widgets.get(
        "artifact_name"
    ).strip()
)

SOURCE_PATH = (
    dbutils.widgets.get(
        "source_path"
    ).strip()
)

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

VALIDATION_TABLE = (
    "migration.repository."
    "repo_validation_results"
)

# ============================================================
# HELPERS
# ============================================================

def section(title):

    print("\n")
    print("=" * 80)
    print(title)
    print("=" * 80)

# ============================================================
# PACKAGE DISCOVERY
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
    f"Package  : {PACKAGE_PATH}"
)

# ============================================================
# LOAD METADATA
# ============================================================

section(
    "LOAD METADATA"
)

metadata_path = (
    f"{PACKAGE_PATH}/metadata/"
    f"migration_metadata.json"
)

metadata = json.loads(
    dbutils.fs.head(
        metadata_path,
        1024 * 1024 * 5
    )
)

tables = metadata["tables"]

print(
    f"Tables Found : "
    f"{len(tables)}"
)

# ============================================================
# SOURCE FILE INVENTORY
# ============================================================

available_files = {

    file.name.lower()

    for file in dbutils.fs.ls(
        SOURCE_PATH
    )
}

# ============================================================
# VALIDATION START
# ============================================================

validation_id = (

    "VAL_"

    + datetime.now().strftime(
        "%Y%m%d%H%M%S"
    )

    + "_"

    + uuid.uuid4().hex[:6]
)

section(
    f"VALIDATION START : "
    f"{validation_id}"
)

results = []

scores = []

validated_tables = []

missing_tables = []

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

    primary_keys = (
        table.get(
            "primary_key",
            []
        )
    )

    cdc_column = (
        table.get(
            "cdc_column"
        )
    )

    source_file = (
        f"{SOURCE_PATH}/"
        f"{source_table.lower()}.csv"
    )

    expected_file = (
        f"{source_table.lower()}.csv"
    )

    print(
        f"\nValidating : "
        f"{source_table}"
    )

    # ========================================================
    # FILE CHECK
    # ========================================================

    if expected_file not in available_files:

        print(
            f"Missing Source File : "
            f"{expected_file}"
        )

        missing_tables.append(
            source_table
        )

        results.append(

            Row(

                validation_id=
                    validation_id,

                artifact_name=
                    ARTIFACT_NAME,

                source_table=
                    source_table,

                target_table=
                    target_table,

                source_rows=
                    0,

                target_rows=
                    0,

                audit_check=
                    "FAIL",

                file_check=
                    "FAIL",

                row_count_check=
                    "SKIPPED",

                schema_check=
                    "SKIPPED",

                duplicate_check=
                    "SKIPPED",

                null_check=
                    "SKIPPED",

                cdc_check=
                    "SKIPPED",

                validation_score=
                    0.0,

                validation_status=
                    "SKIPPED",

                validation_time=
                    datetime.now()
            )
        )

        continue

    # ========================================================
    # LOAD SOURCE
    # ========================================================

    source_df = (

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

    # ========================================================
    # LOAD TARGET
    # ========================================================

    try:

        target_df = spark.table(
            target_table
        )

    except Exception:

        print(
            f"Missing Target Table : "
            f"{target_table}"
        )

        results.append(

            Row(

                validation_id=
                    validation_id,

                artifact_name=
                    ARTIFACT_NAME,

                source_table=
                    source_table,

                target_table=
                    target_table,

                source_rows=
                    source_df.count(),

                target_rows=
                    0,

                audit_check=
                    "FAIL",

                file_check=
                    "PASS",

                row_count_check=
                    "FAIL",

                schema_check=
                    "FAIL",

                duplicate_check=
                    "FAIL",

                null_check=
                    "FAIL",

                cdc_check=
                    "FAIL",

                validation_score=
                    0.0,

                validation_status=
                    "FAIL",

                validation_time=
                    datetime.now()
            )
        )

        continue

    # ========================================================
    # AUDIT CHECK
    # ========================================================

    audit_check = "FAIL"

    try:

        audit_count = (

            spark.table(
                AUDIT_TABLE
            )

            .filter(
                f"artifact_name='{ARTIFACT_NAME}'"
            )

            .filter(
                f"source_table='{source_table}'"
            )

            .filter(
                "status IN ('COMPLETED','DRY_RUN')"
            )

            .count()
        )

        if audit_count > 0:

            audit_check = "PASS"

    except:

        audit_check = "FAIL"

    # ========================================================
    # ROW COUNT CHECK
    # ========================================================

    source_rows = (
        source_df.count()
    )

    target_rows = (
        target_df.count()
    )

    row_count_check = (

        "PASS"

        if source_rows ==
           target_rows

        else "FAIL"
    )

    # ========================================================
    # SCHEMA CHECK
    # ========================================================

    source_schema = {

        field.name.lower():
        field.dataType.simpleString()

        for field in source_df.schema
    }

    target_schema = {

        field.name.lower():
        field.dataType.simpleString()

        for field in target_df.schema
    }

    schema_check = (

        "PASS"

        if source_schema ==
           target_schema

        else "FAIL"
    )

    # ========================================================
    # CASE INSENSITIVE COLUMN MAPS
    # ========================================================

    source_columns_map = {

        c.upper(): c

        for c in source_df.columns
    }

    target_columns_map = {

        c.upper(): c

        for c in target_df.columns
    }

    actual_pk_columns = [

        target_columns_map[
            pk.upper()
        ]

        for pk in primary_keys

        if pk.upper()
        in target_columns_map
    ]

    # ========================================================
    # DUPLICATE CHECK
    # ========================================================

    duplicate_check = "PASS"

    if len(actual_pk_columns) > 0:

        duplicate_count = (

            target_df

            .groupBy(
                *actual_pk_columns
            )

            .count()

            .filter(
                "count > 1"
            )

            .count()
        )

        if duplicate_count > 0:

            duplicate_check = "FAIL"

    # ========================================================
    # NULL CHECK
    # ========================================================

    null_check = "PASS"

    for column_name in actual_pk_columns:

        null_count = (

            target_df

            .filter(
                f"{column_name} IS NULL"
            )

            .count()
        )

        if null_count > 0:

            null_check = "FAIL"

    # ========================================================
    # CDC CHECK
    # ========================================================

    cdc_check = "NA"

    if (
        cdc_column
        and
        cdc_column.upper() in source_columns_map
        and
        cdc_column.upper() in target_columns_map
    ):

        source_cdc = (

            source_columns_map[
                cdc_column.upper()
            ]
        )

        target_cdc = (

            target_columns_map[
                cdc_column.upper()
            ]
        )

        source_max = (

            source_df

            .agg(
                {source_cdc: "max"}
            )

            .collect()[0][0]
        )

        target_max = (

            target_df

            .agg(
                {target_cdc: "max"}
            )

            .collect()[0][0]
        )

        cdc_check = (

            "PASS"

            if source_max ==
               target_max

            else "FAIL"
        )

    # ========================================================
    # SCORE
    # ========================================================

    validation_checks = [

        audit_check,
        row_count_check,
        schema_check,
        duplicate_check,
        null_check
    ]

    if cdc_check != "NA":

        validation_checks.append(
            cdc_check
        )

    score = (

        validation_checks.count(
            "PASS"
        )

        * 100.0

        /

        len(validation_checks)
    )

    validation_status = (

        "PASS"

        if score >= 90

        else "FAIL"
    )

    scores.append(
        score
    )

    validated_tables.append(
        source_table
    )

    results.append(

        Row(

            validation_id=
                validation_id,

            artifact_name=
                ARTIFACT_NAME,

            source_table=
                source_table,

            target_table=
                target_table,

            source_rows=
                source_rows,

            target_rows=
                target_rows,

            audit_check=
                audit_check,

            file_check=
                "PASS",

            row_count_check=
                row_count_check,

            schema_check=
                schema_check,

            duplicate_check=
                duplicate_check,

            null_check=
                null_check,

            cdc_check=
                cdc_check,

            validation_score=
                float(score),

            validation_status=
                validation_status,

            validation_time=
                datetime.now()
        )
    )

    print(
        f"Source Rows : {source_rows}"
    )

    print(
        f"Target Rows : {target_rows}"
    )

    print(
        f"Score       : "
        f"{round(score,2)}"
    )

# ============================================================
# SAVE RESULTS
# ============================================================

section(
    "SAVE RESULTS"
)

if len(results) > 0:

    (
        spark
        .createDataFrame(
            results
        )
        .write
        .mode(
            "append"
        )
        .saveAsTable(
            VALIDATION_TABLE
        )
    )

    print(
        f"Saved Results : "
        f"{len(results)}"
    )

# ============================================================
# READINESS SCORE
# ============================================================

quality_score = (

    sum(scores)

    / len(scores)

    if len(scores) > 0

    else 0
)

expected_table_count = (
    len(tables)
)

validated_table_count = (
    len(validated_tables)
)

completeness_score = (

    validated_table_count

    / expected_table_count

) * 100

readiness_score = (

    (quality_score * 0.8)

    +

    (completeness_score * 0.2)
)

# ============================================================
# SUMMARY
# ============================================================

section(
    "VALIDATION SUMMARY"
)

print(
    f"Validation ID      : "
    f"{validation_id}"
)

print(
    f"Expected Tables    : "
    f"{expected_table_count}"
)

print(
    f"Validated Tables   : "
    f"{validated_table_count}"
)

print(
    f"Quality Score      : "
    f"{round(quality_score,2)}"
)

print(
    f"Completeness Score : "
    f"{round(completeness_score,2)}"
)

print(
    f"Readiness Score    : "
    f"{round(readiness_score,2)}"
)

if readiness_score >= 90:

    deployment_status = (
        "READY"
    )

elif readiness_score >= 75:

    deployment_status = (
        "CONDITIONAL"
    )

else:

    deployment_status = (
        "NOT_READY"
    )

print(
    f"Deployment Status  : "
    f"{deployment_status}"
)

print("\nVALIDATED TABLES")

for t in validated_tables:

    print(
        f"  {t}"
    )

print("\nMISSING TABLES")

for t in missing_tables:

    print(
        f"  {t}"
    )

print("\n")

print("=" * 80)
print("017 DATA VALIDATION COMPLETED")
print("=" * 80)