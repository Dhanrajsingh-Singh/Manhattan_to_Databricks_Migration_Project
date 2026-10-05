# ==========================================================
# 014_DEPLOYMENT_PACKAGE_GENERATOR.py
# Metadata-Driven Enterprise Deployment Package Generator
# Manhattan -> Databricks
# ==========================================================

from datetime import datetime
import json
import re

from pyspark.sql import Row


print("=" * 80)
print("METADATA-DRIVEN DEPLOYMENT PACKAGE GENERATOR")
print("=" * 80)


# ==========================================================
# 1. CONFIGURATION
# ==========================================================

SOURCE_SYSTEM = "Manhattan"
TARGET_CATALOG = "migration_dev"

REPOSITORY_CATALOG = "migration"
REPOSITORY_SCHEMA = "repository"

BASE_REPOSITORY = (
    f"{REPOSITORY_CATALOG}.{REPOSITORY_SCHEMA}"
)


# ==========================================================
# 2. LOAD WORKFLOW ANALYSIS
# ==========================================================

print("\n[1] Loading workflow analysis...")


workflow_row = (
    spark.table(
        f"{BASE_REPOSITORY}.repo_workflow_analysis"
    )
    .orderBy(
        "created_time",
        ascending=False
    )
    .first()
)


if workflow_row is None:

    raise Exception(
        "Workflow analysis not found."
    )


WORKFLOW_ANALYSIS = workflow_row["analysis"]


# ==========================================================
# 3. LOAD VALIDATION RESULT
# ==========================================================

print("[2] Loading validation result...")


validation_row = (
    spark.table(
        f"{BASE_REPOSITORY}.repo_validation_results_v2"
    )
    .orderBy(
        "created_time",
        ascending=False
    )
    .first()
)


if validation_row is None:

    raise Exception(
        "Validation result not found."
    )


VALIDATION_REPORT = (
    validation_row["validation_report"]
)
OVERALL_READINESS = (
    validation_row["overall_readiness_pct"]
)

# ==========================================================
# 4. LOAD GENERATED PYSPARK
# ==========================================================

print("[3] Loading generated PySpark...")


code_row = (
    spark.table(
        f"{BASE_REPOSITORY}.repo_generated_pyspark_v2"
    )
    .orderBy(
        "created_time",
        ascending=False
    )
    .first()
)


if code_row is None:

    raise Exception(
        "Generated PySpark code not found."
    )


GENERATED_CODE = (
    code_row["pyspark_code"]
)


QUESTION = (
    code_row["question"]
)


# ==========================================================
# 5. GENERATE ARTIFACT NAME
# ==========================================================

ARTIFACT_NAME = re.sub(
    r"[^A-Za-z0-9_]",
    "",
    QUESTION.replace(" ", "_")
).upper()


if not ARTIFACT_NAME:

    ARTIFACT_NAME = "MANHATTAN_MIGRATION"


ARTIFACT_NAME = ARTIFACT_NAME[:80]


BUNDLE_NAME = (
    ARTIFACT_NAME
    .lower()
    .replace("_", "-")
)


GENERATED_TIME = datetime.now()


print("\nArtifact Name :", ARTIFACT_NAME)
print("Bundle Name   :", BUNDLE_NAME)


# ==========================================================
# 6. PACKAGE PATH
# ==========================================================

BASE_PATH = (
    "dbfs:/Volumes/"
    "migration/repository/"
    "deployment_artifacts/"
    f"{ARTIFACT_NAME}"
)


print("Package Path  :", BASE_PATH)


# ==========================================================
# 7. CREATE PACKAGE DIRECTORIES
# ==========================================================

folders = [

    "src",
    "src/bronze",
    "src/silver",
    "src/gold",

    "tests",

    "configs",

    "resources",

    "sql",

    "sql/bronze",
    "sql/silver",
    "sql/gold",

    "schemas",
    "schemas/bronze",
    "schemas/silver",
    "schemas/gold",

    "metadata",

    "workflows",

    "deployment"

]


dbutils.fs.mkdirs(BASE_PATH)


for folder in folders:

    dbutils.fs.mkdirs(
        f"{BASE_PATH}/{folder}"
    )


# ==========================================================
# 8. HELPER FUNCTION
# ==========================================================

def table_exists(table_name):

    try:

        return spark.catalog.tableExists(
            table_name
        )

    except Exception:

        return False


def clean_name(value):

    if value is None:

        return "UNKNOWN"

    value = str(value)

    value = re.sub(
        r"[^A-Za-z0-9_]",
        "_",
        value
    )

    return value.lower()


# ==========================================================
# 9. LOAD METADATA INVENTORY
# ==========================================================

print("\n[4] Loading metadata inventory...")

METADATA_TABLE = (
    f"{BASE_REPOSITORY}.repo_tables"
)

metadata_rows = []

if table_exists(METADATA_TABLE):

    metadata_df = spark.table(
        METADATA_TABLE
    )

    print(
        f"Metadata columns: {metadata_df.columns}"
    )

    metadata_rows = (
        metadata_df.collect()
    )

else:

    print(
        "WARNING: Metadata inventory table not found."
    )

print(
    f"Metadata records: {len(metadata_rows)}"
)

# ==========================================================
# LOAD COLUMN METADATA
# ==========================================================

COLUMN_METADATA_TABLE = (
    f"{BASE_REPOSITORY}.repo_table_columns"
)

column_metadata = {}

if table_exists(COLUMN_METADATA_TABLE):

    column_df = spark.table(
        COLUMN_METADATA_TABLE
    )

    for row in column_df.collect():

        table_name = (
            str(row["table_name"])
            .strip()
            .lower()
        )

        data_type = (
            str(row["data_type"])
            .strip()
        )

        column_metadata.setdefault(
            table_name,
            []
        ).append(
            {
                "column_name":
                    str(row["column_name"]).strip(),

                "data_type":
                    data_type,

                "nullable":
                    str(row["nullable"]).strip()
            }
        )

else:

    print(
        "WARNING: repo_table_columns not found."
    )

# ==========================================================
# 10. NORMALIZE METADATA
# ==========================================================

metadata_objects = []

for row in metadata_rows:

    data = row.asDict()

    metadata_objects.append({

        "system":
            SOURCE_SYSTEM,

        "table_name":
            data.get("table_name"),

        "domain":
            data.get("domain"),

        "load_frequency":
            data.get("load_frequency"),

        "cdc_column":
            data.get("cdc_column"),

        "retention_days":
            data.get("retention_days"),

        "sensitivity":
            data.get("sensitivity")

    })


# ==========================================================
# 11. LOAD BUSINESS RULES
# ==========================================================

print("\n[5] Loading business rules...")

business_rules = []

print(
    "Business-rule repository table not available."
)

print(
    f"Business rules: {len(business_rules)}"
)


# ==========================================================
# 12. LOAD RULE DEPENDENCIES
# ==========================================================

print("\n[6] Loading rule dependencies...")

DEPENDENCY_TABLE = (
    f"{BASE_REPOSITORY}.repo_dependencies"
)

dependencies = []

if table_exists(DEPENDENCY_TABLE):

    dependency_df = spark.table(
        DEPENDENCY_TABLE
    )

    print(
        f"Dependency columns: {dependency_df.columns}"
    )

    dependencies = [
        row.asDict()
        for row in dependency_df.collect()
    ]

else:

    print(
        "WARNING: Dependency table not found."
    )

print(
    f"Dependencies: {len(dependencies)}"
)


# ==========================================================
# 13. LOAD WORKFLOWS
# ==========================================================

print("\n[7] Loading workflows...")

WORKFLOW_TABLE = (
    f"{BASE_REPOSITORY}.repo_workflows"
)

workflows = []

if table_exists(WORKFLOW_TABLE):

    workflows_df = spark.table(
        WORKFLOW_TABLE
    )

    print(
        f"Workflow columns: {workflows_df.columns}"
    )

    workflows = [
        row.asDict()
        for row in workflows_df.collect()
    ]

else:

    print(
        "WARNING: Workflow table not found."
    )

print(
    f"Workflows: {len(workflows)}"
)


# ==========================================================
# 14. LOAD WORKFLOW STEPS
# ==========================================================

print("\n[8] Loading workflow steps...")

workflow_steps = []

print(
    "Workflow-step repository table not available."
)

print(
    f"Workflow steps: {len(workflow_steps)}"
)


# ==========================================================
# 15. DETERMINE TARGET TABLES
# ==========================================================

print("\n[9] Building target metadata...")

if len(metadata_objects) == 0:

    print(
        "WARNING: No metadata discovered. "
        "Generating fallback metadata."
    )

    metadata_objects.append({

        "system":
            SOURCE_SYSTEM,

        "table_name":
            ARTIFACT_NAME,

        "domain":
            "UNKNOWN",

        "load_frequency":
            "UNKNOWN",

        "cdc_column":
            None,

        "retention_days":
            None,

        "sensitivity":
            "UNKNOWN"

    })

target_tables = []


for obj in metadata_objects:

    table_name = clean_name(
        obj["table_name"]
    )

    target_tables.append({

        "source_system":
            obj["system"],

        "source_table":
            obj["table_name"],

        "target_table":
            table_name,

        "domain":
            obj.get("domain"),

        "load_frequency":
            obj.get("load_frequency"),

        "cdc_column":
            obj.get("cdc_column"),

        "retention_days":
            obj.get("retention_days"),

        "sensitivity":
            obj.get("sensitivity")

    })


print(
    f"Target tables generated: "
    f"{len(target_tables)}"
)


# ==========================================================
# 16. GENERATE SCHEMA DDL
# ==========================================================

print("\n[10] Generating Bronze/Silver/Gold schemas...")

CATALOG_SQL = f"""
CREATE CATALOG IF NOT EXISTS
{TARGET_CATALOG};
"""

dbutils.fs.put(
    f"{BASE_PATH}/schemas/create_catalog.sql",
    CATALOG_SQL,
    True
)

BRONZE_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS
{TARGET_CATALOG}.bronze;
"""


SILVER_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS
{TARGET_CATALOG}.silver;
"""


GOLD_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS
{TARGET_CATALOG}.gold;
"""


dbutils.fs.put(
    f"{BASE_PATH}/schemas/bronze/bronze_schema.sql",
    BRONZE_SCHEMA,
    True
)


dbutils.fs.put(
    f"{BASE_PATH}/schemas/silver/silver_schema.sql",
    SILVER_SCHEMA,
    True
)


dbutils.fs.put(
    f"{BASE_PATH}/schemas/gold/gold_schema.sql",
    GOLD_SCHEMA,
    True
)


# ==========================================================
# 17. GENERATE TABLE DDL
# ==========================================================

print("\n[11] Generating table DDL...")


bronze_sql = [
    f"-- Generated Bronze DDL",
    f"-- Generated: {GENERATED_TIME}",
    ""
]


silver_sql = [
    f"-- Generated Silver DDL",
    f"-- Generated: {GENERATED_TIME}",
    ""
]


gold_sql = [
    f"-- Generated Gold DDL",
    f"-- Generated: {GENERATED_TIME}",
    ""
]


for table in target_tables:

    table_name = table["target_table"]

    table_columns = column_metadata.get(
        table_name,
        []
    )

    if table_columns:

        column_definition = ",\n".join(
            [
                f"    {col['column_name']} {col['data_type']}"
                for col in table_columns
            ]
        )

    else:

        print(
            f"WARNING: No columns found for {table_name}"
        )

        column_definition = (
            "    ID STRING"
        )

    bronze_sql.append(
        f"""
CREATE TABLE IF NOT EXISTS
{TARGET_CATALOG}.bronze.{table_name}
(
{column_definition}
)
USING DELTA;
"""
    )

    silver_sql.append(
        f"""
CREATE TABLE IF NOT EXISTS
{TARGET_CATALOG}.silver.{table_name}
(
{column_definition}
)
USING DELTA;
"""
    )

    gold_sql.append(
        f"""
CREATE TABLE IF NOT EXISTS
{TARGET_CATALOG}.gold.{table_name}
(
{column_definition}
)
USING DELTA;
"""
    )


dbutils.fs.put(
    f"{BASE_PATH}/sql/bronze/bronze_tables.sql",
    "\n".join(bronze_sql),
    True
)


dbutils.fs.put(
    f"{BASE_PATH}/sql/silver/silver_tables.sql",
    "\n".join(silver_sql),
    True
)


dbutils.fs.put(
    f"{BASE_PATH}/sql/gold/gold_tables.sql",
    "\n".join(gold_sql),
    True
)


# ==========================================================
# 18. SAVE METADATA SNAPSHOT
# ==========================================================

METADATA_SNAPSHOT = {

    "source_system":
        SOURCE_SYSTEM,

    "generated_time":
        str(GENERATED_TIME),

    "tables":
        target_tables,

    "business_rules":
        business_rules,

    "dependencies":
        dependencies,

    "workflows":
        workflows,

    "workflow_steps":
        workflow_steps

}


dbutils.fs.put(

    f"{BASE_PATH}/metadata/"
    f"migration_metadata.json",

    json.dumps(
        METADATA_SNAPSHOT,
        indent=4,
        default=str
    ),

    True
)


# ==========================================================
# 19. GENERATE BRONZE PYSPARK
# ==========================================================

BRONZE_CODE = f"""
# Auto-generated Bronze layer
# Source system: {SOURCE_SYSTEM}

from pyspark.sql.functions import current_timestamp

def load_bronze(df, target_table):

    result = (
        df
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
    )

    (
        result
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )


# Generated by migration framework
"""


dbutils.fs.put(
    f"{BASE_PATH}/src/bronze/bronze_loader.py",
    BRONZE_CODE,
    True
)


# ==========================================================
# 20. GENERATE SILVER PYSPARK
# ==========================================================

SILVER_CODE = """
# Auto-generated Silver layer

from pyspark.sql import functions as F


def transform_silver(df):

    result = (
        df
        .dropDuplicates()
    )

    return result
"""


dbutils.fs.put(
    f"{BASE_PATH}/src/silver/silver_transform.py",
    SILVER_CODE,
    True
)


# ==========================================================
# 21. GENERATE GOLD PYSPARK
# ==========================================================

GOLD_CODE = """
# Auto-generated Gold layer

from pyspark.sql import functions as F


def build_gold(df):

    result = (
        df
    )

    return result
"""


dbutils.fs.put(
    f"{BASE_PATH}/src/gold/gold_transform.py",
    GOLD_CODE,
    True
)


# ==========================================================
# 22. SAVE GENERATED MIGRATION CODE
# ==========================================================

dbutils.fs.put(
    f"{BASE_PATH}/src/migration_generated.py",
    GENERATED_CODE,
    True
)


# ==========================================================
# 23. GENERATE JOB RESOURCE
# ==========================================================

JOB_YAML = f"""
resources:

  jobs:

    {ARTIFACT_NAME}_JOB:

      name: JOB_{ARTIFACT_NAME}

      tasks:

        - task_key: bronze_load

          spark_python_task:

            python_file: ../src/bronze/bronze_loader.py

        - task_key: silver_transform

          depends_on:

            - task_key: bronze_load

          spark_python_task:

            python_file: ../src/silver/silver_transform.py

        - task_key: gold_publish

          depends_on:

            - task_key: silver_transform

          spark_python_task:

            python_file: ../src/gold/gold_transform.py
"""

dbutils.fs.put(
    f"{BASE_PATH}/resources/jobs.yml",
    JOB_YAML,
    True
)


# ==========================================================
# 24. GENERATE PIPELINE RESOURCE
# ==========================================================

PIPELINE_YAML = f"""
resources:

  pipelines:

    {ARTIFACT_NAME}_PIPELINE:

      name: {ARTIFACT_NAME}_PIPELINE

      catalog: {TARGET_CATALOG}

      target: bronze

      serverless: true

      libraries:

        - file:
            path: ../src/bronze/bronze_loader.py

        - file:
            path: ../src/silver/silver_transform.py

        - file:
            path: ../src/gold/gold_transform.py
"""


dbutils.fs.put(
    f"{BASE_PATH}/resources/pipelines.yml",
    PIPELINE_YAML,
    True
)

# Generate Workflow JSON
WORKFLOW_JSON = {

    "workflow_name": ARTIFACT_NAME,

    "generated_time": str(GENERATED_TIME),

    "steps": [

        {
            "step": "bronze_load",
            "depends_on": []
        },

        {
            "step": "silver_transform",
            "depends_on": [
                "bronze_load"
            ]
        },

        {
            "step": "gold_publish",
            "depends_on": [
                "silver_transform"
            ]
        }

    ]
}

dbutils.fs.put(

    f"{BASE_PATH}/workflows/workflow.json",

    json.dumps(
        WORKFLOW_JSON,
        indent=4
    ),

    True
)

# ==========================================================
# 25. PERMISSIONS
# ==========================================================

PERMISSIONS_YAML = f"""
resources:

  jobs:

    {ARTIFACT_NAME}_JOB:

      permissions:

        - level: CAN_VIEW
          group_name: data-engineers

        - level: CAN_MANAGE_RUN
          group_name: data-engineers
"""


dbutils.fs.put(
    f"{BASE_PATH}/resources/permissions.yml",
    PERMISSIONS_YAML,
    True
)


# ==========================================================
# 26. DATABRICKS.YML
# ==========================================================

DATABRICKS_YML = f"""
bundle:

  name: {BUNDLE_NAME}


include:

  - resources/*.yml


sync:

  include:

    - src/**
    - configs/**
    - sql/**
    - schemas/**


targets:

  dev:

    mode: development

    default: true


  qa:

    mode: development


  uat:

    mode: production


  prod:

    mode: production
"""


dbutils.fs.put(
    f"{BASE_PATH}/databricks.yml",
    DATABRICKS_YML,
    True
)


# ==========================================================
# 27. APPLICATION CONFIG
# ==========================================================

APPLICATION_CONFIG = {

    "source_system":
        SOURCE_SYSTEM,

    "target_platform":
        "Databricks",

    "catalog":
        TARGET_CATALOG,

    "bronze_schema":
        "bronze",

    "silver_schema":
        "silver",

    "gold_schema":
        "gold",

    "deployment_framework":
        "Databricks Declarative Automation Bundles",

    "metadata_driven":
        True,

    "tables_discovered":
        len(target_tables),

    "business_rules":
        len(business_rules),

    "dependencies":
        len(dependencies),

    "workflows":
        len(workflows),

    "workflow_steps":
        len(workflow_steps)

}


dbutils.fs.put(

    f"{BASE_PATH}/configs/"
    f"application_config.json",

    json.dumps(
        APPLICATION_CONFIG,
        indent=4
    ),

    True
)


# ==========================================================
# 28. MANIFEST
# ==========================================================

MANIFEST = {

    "artifact_name":
        ARTIFACT_NAME,

    "bundle_name":
        BUNDLE_NAME,

    "version":
        "2.0.0",

    "generated_time":
        str(GENERATED_TIME),

    "source_system":
        SOURCE_SYSTEM,

    "overall_readiness":
        OVERALL_READINESS,

    "deployment_status":
        (
            "APPROVED"
            if OVERALL_READINESS >= 85
            else "CONDITIONAL"
            if OVERALL_READINESS >= 70
            else "REJECTED"
        ),

    "target_platform":
        "Databricks",

    "metadata_driven":
        True,

    "metadata_tables": [

        "repo_tables",

        "repo_dependencies",

        "repo_workflows",

        "repo_workflow_analysis",

        "repo_validation_results_v2",

        "repo_generated_pyspark_v2"
    ],

    "statistics": {

        "tables":
            len(target_tables),

        "business_rules":
            len(business_rules),

        "dependencies":
            len(dependencies),

        "workflows":
            len(workflows),

        "workflow_steps":
            len(workflow_steps)

    },

    "deployment_targets": [

        "dev",
        "qa",
        "uat",
        "prod"

    ]

}


dbutils.fs.put(

    f"{BASE_PATH}/metadata/"
    f"package_manifest.json",

    json.dumps(
        MANIFEST,
        indent=4
    ),

    True
)
DEPLOYMENT_MANIFEST = {

    "deployment_order": [

        "schemas/create_catalog.sql",

        "schemas/bronze/bronze_schema.sql",

        "schemas/silver/silver_schema.sql",

        "schemas/gold/gold_schema.sql",

        "sql/bronze/bronze_tables.sql",

        "sql/silver/silver_tables.sql",

        "sql/gold/gold_tables.sql",

        "resources/jobs.yml",

        "resources/pipelines.yml",

        "resources/permissions.yml"
    ]
}

dbutils.fs.put(

    f"{BASE_PATH}/metadata/deployment_manifest.json",

    json.dumps(
        DEPLOYMENT_MANIFEST,
        indent=4
    ),

    True
)



# ==========================================================
# 29. DEPLOYMENT GUIDE
# ==========================================================

DEPLOYMENT_GUIDE = f"""
# Manhattan Migration Deployment Guide

## Bundle

{BUNDLE_NAME}

## Step 1 - Move package to Git

The generated package should be committed
to the migration repository.

## Step 2 - Validate

Run:

databricks bundle validate -t dev

## Step 3 - Deploy DEV

Run:

databricks bundle deploy -t dev

## Step 4 - Run Job

Run:

databricks bundle run -t dev {ARTIFACT_NAME}_JOB

## Step 5 - Validate Data

Validate:

- Bronze records
- Silver records
- Gold records
- Data quality
- Record counts
- Duplicate records
- Transformation rules

## Step 6 - Promote

DEV
 |
 v
QA
 |
 v
UAT
 |
 v
PROD
"""


dbutils.fs.put(
    f"{BASE_PATH}/deployment/deployment_guide.md",
    DEPLOYMENT_GUIDE,
    True
)


# ==========================================================
# 30. VALIDATION DOCUMENT
# ==========================================================

VALIDATION_DOC = f"""
# Migration Validation

## Validation Report

{VALIDATION_REPORT}

## Generated Metadata

Tables:
{len(target_tables)}

Business Rules:
{len(business_rules)}

Dependencies:
{len(dependencies)}

Workflows:
{len(workflows)}

Workflow Steps:
{len(workflow_steps)}
"""


dbutils.fs.put(
    f"{BASE_PATH}/deployment/validation_report.md",
    VALIDATION_DOC,
    True
)


# ==========================================================
# 31. ROLLBACK
# ==========================================================

ROLLBACK = """
# Rollback Guide

## Code Rollback

Restore the previous Git version
and redeploy the previous bundle.

## Data Rollback

Use Delta Lake:

RESTORE TABLE

or

VERSION AS OF

according to the approved
migration recovery procedure.

## Job Rollback

Deploy the previous bundle version.
"""


dbutils.fs.put(
    f"{BASE_PATH}/deployment/rollback_guide.md",
    ROLLBACK,
    True
)


# ==========================================================
# 32. REGISTER PACKAGE
# ==========================================================

package_df = spark.createDataFrame(

    [

        Row(

            artifact_name=
                ARTIFACT_NAME,

            artifact_path=
                BASE_PATH,

            bundle_name=
                BUNDLE_NAME,

            source_system=
                SOURCE_SYSTEM,

            target_platform=
                "Databricks",

            metadata_driven=
                True,

            table_count=
                len(target_tables),

            rule_count=
                len(business_rules),

            workflow_count=
                len(workflows),

            generated_time=
                GENERATED_TIME,
                
            overall_readiness=OVERALL_READINESS

        )

    ]

)


package_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true")\
    .option(
        "mergeSchema",
        "true"
    ) \
    .saveAsTable(
        f"{BASE_REPOSITORY}.repo_deployment_assets"
    )


# ==========================================================
# 33. FINAL VERIFICATION
# ==========================================================

print("\n" + "=" * 80)
print("DEPLOYMENT PACKAGE GENERATED SUCCESSFULLY")
print("=" * 80)

print(
    f"\nArtifact          : {ARTIFACT_NAME}"
)

print(
    f"Bundle            : {BUNDLE_NAME}"
)

print(
    f"Tables discovered : {len(target_tables)}"
)

print(
    f"Business rules    : {len(business_rules)}"
)

print(
    f"Dependencies      : {len(dependencies)}"
)

print(
    f"Workflows         : {len(workflows)}"
)

print(
    f"Workflow steps    : {len(workflow_steps)}"
)

print(
    f"\nPackage path      : {BASE_PATH}"
)


print("\nGenerated files:")


for item in dbutils.fs.ls(BASE_PATH):

    print(
        " ",
        item.path
    )


print("\n" + "=" * 80)
print("GENERATOR COMPLETED")
print("=" * 80)