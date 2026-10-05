# ==========================================================
# 002_METADATA_EXTRACTOR.py
#
# Reads migration_normalized.json
# Creates Metadata Repository Tables
#
# Output:
#   migration.repository.repo_tables
#   migration.repository.repo_dependencies
#   migration.repository.repo_workflows
#   migration.repository.repo_metadata_summary
# ==========================================================

import json
from pyspark.sql import Row

# ----------------------------------------------------------
# INPUT JSON
# ----------------------------------------------------------

INPUT_JSON = "/Volumes/migration/default/migration_poc/manhattan_mock_artifacts/migration_normalized.json"

content = dbutils.fs.head(
    INPUT_JSON,
    5 * 1024 * 1024
)
ir = json.loads(content)

print("JSON IR Loaded Successfully")

# ----------------------------------------------------------
# CREATE DATABASE
# ----------------------------------------------------------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS migration.repository
""")

# ==========================================================
# TABLE METADATA
# ==========================================================

table_records = []

table_metadata = (
    ir.get("metadata", {})
      .get("table_metadata", {})
      .get("tables", [])
)

# ==========================================================
# TABLE COLUMN METADATA
# ==========================================================

column_records = []

for table in table_metadata:

    table_name = table.get("table_name")

    for col in table.get("columns", []):

        column_records.append(
            Row(
                table_name=table_name,
                column_name=col.get("column_name"),
                data_type=col.get("data_type"),
                nullable=col.get("nullable")
            )
        )

if column_records:

    columns_df = spark.createDataFrame(
        column_records
    )

    columns_df.write \
        .mode("overwrite") \
        .saveAsTable(
            "migration.repository.repo_table_columns"
        )

    print(
        f"repo_table_columns created with "
        f"{columns_df.count()} records"
    )

    display(columns_df)

else:

    print(
        "WARNING: No column metadata found."
    )

for table in table_metadata:

    table_records.append(
        Row(
            table_name=table.get("table_name"),
            domain=table.get("domain"),
            load_frequency=table.get("load_frequency"),
            cdc_column=table.get("cdc_column"),
            retention_days=table.get("retention_days"),
            sensitivity=table.get("sensitivity")
        )
    )

if table_records:

    tables_df = spark.createDataFrame(table_records)

    tables_df.write \
        .mode("overwrite") \
        .saveAsTable(
            "migration.repository.repo_tables"
        )

    print(
        f"repo_tables created with {tables_df.count()} records"
    )

    display(tables_df)

# ==========================================================
# DEPENDENCIES
# ==========================================================

dependency_records = []

for dep in ir.get("dependencies", []):

    dependency_records.append(
        Row(
            source_table=dep.get("source"),
            target_table=dep.get("target"),
            transformation=dep.get("transformation")
        )
    )

if dependency_records:

    dependency_df = spark.createDataFrame(
        dependency_records
    )

    dependency_df.write \
        .mode("overwrite") \
        .saveAsTable(
            "migration.repository.repo_dependencies"
        )

    print(
        f"repo_dependencies created with {dependency_df.count()} records"
    )

    display(dependency_df)

# ==========================================================
# WORKFLOW METADATA
# ==========================================================

workflow_records = []

for workflow in ir.get("workflows", []):

    workflow_id = workflow.get("workflow_id")
    workflow_name = workflow.get("name")
    schedule = workflow.get("schedule")

    for task in workflow.get("tasks", []):

        workflow_records.append(
            Row(
                workflow_id=workflow_id,
                workflow_name=workflow_name,
                schedule=schedule,
                task_id=task.get("task_id"),
                task_name=task.get("name"),
                depends_on=",".join(
                    task.get("depends_on", [])
                )
            )
        )

if workflow_records:

    workflow_df = spark.createDataFrame(
        workflow_records
    )

    workflow_df.write \
        .mode("overwrite") \
        .saveAsTable(
            "migration.repository.repo_workflows"
        )

    print(
        f"repo_workflows created with {workflow_df.count()} records"
    )

    display(workflow_df)

# ==========================================================
# METADATA SUMMARY
# ==========================================================

summary_records = [
    Row(
        source_system=ir.get("source_system"),
        table_count=len(table_metadata),
        dependency_count=len(ir.get("dependencies", [])),
        workflow_count=len(ir.get("workflows", [])),
        business_rule_count=len(ir.get("business_rules", [])),
        etl_process_count=len(ir.get("etl_processes", [])),
        report_count=len(ir.get("reports", []))
    )
]

summary_df = spark.createDataFrame(
    summary_records
)

summary_df.write \
    .mode("overwrite") \
    .saveAsTable(
        "migration.repository.repo_metadata_summary"
    )

print("repo_metadata_summary created")

display(summary_df)

# ==========================================================
# COMPLETION
# ==========================================================

print("=" * 60)
print("METADATA EXTRACTION COMPLETED")
print("=" * 60)

print("""
Created Tables:

1. migration.repository.repo_tables
2. migration.repository.repo_dependencies
3. migration.repository.repo_workflows
4. migration.repository.repo_metadata_summary
""")