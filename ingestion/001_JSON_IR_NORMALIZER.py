import json
import csv
import io

INPUT_FILES = "/Volumes/migration/default/migration_poc/manhattan_mock_artifacts/canonical_json/"

OUTPUT_FILE = "/Volumes/migration/default/migration_poc/manhattan_mock_artifacts/migration_normalized.json"

# Read parser output — canonical_json/ contains multiple JSON files
artifacts = []
for item in dbutils.fs.ls(INPUT_FILES):
    if item.name.endswith(".json"):
        content = dbutils.fs.head(item.path, 5 * 1024 * 1024)
        artifact = json.loads(content)
        artifact["file_name"] = artifact.get("source_file", "")
        artifacts.append(artifact)

raw_ir = {"source_system": "Manhattan", "artifacts": artifacts}

normalized_ir = {
    "source_system": raw_ir.get("source_system", "Manhattan"),
    "metadata": {
        "tables": [],
        "columns": [],
        "foreign_keys": []
    },
    "business_rules": [],
    "etl_processes": [],
    "workflows": [],
    "dependencies": [],
    "reports": []
}

for artifact in raw_ir.get("artifacts", []):

    file_name = artifact.get("file_name", "").lower()
    content = artifact.get("content", {})

    # --------------------------------------------------
    # DATABASE METADATA
    # --------------------------------------------------

    if file_name == "tables.csv":
        normalized_ir["metadata"]["tables"].append({
            "file_name": file_name,
            "columns": artifact.get("columns", []),
            "record_count": artifact.get("record_count")
        })

    elif file_name == "columns.csv":
        normalized_ir["metadata"]["columns"].append({
            "file_name": file_name,
            "columns": artifact.get("columns", []),
            "record_count": artifact.get("record_count")
        })

    elif file_name == "foreign_keys.csv":
        normalized_ir["metadata"]["foreign_keys"].append({
            "file_name": file_name,
            "columns": artifact.get("columns", []),
            "record_count": artifact.get("record_count")
        })

    elif file_name == "table_metadata.json":
        normalized_ir["metadata"]["table_metadata"] = content

    # --------------------------------------------------
    # BUSINESS RULES
    # --------------------------------------------------

    elif "rule" in file_name:

        if "rules" in content:
            normalized_ir["business_rules"].extend(
                content["rules"]
            )

    # --------------------------------------------------
    # ETL
    # --------------------------------------------------

    elif (
        "etl" in file_name
        or "load" in file_name
        or "streaming" in file_name
    ):

        normalized_ir["etl_processes"].append(content)

    # --------------------------------------------------
    # WORKFLOWS
    # --------------------------------------------------

    elif "workflow" in file_name:

        normalized_ir["workflows"].append(content)

    # --------------------------------------------------
    # DEPENDENCIES
    # --------------------------------------------------

    elif "dependenc" in file_name:

        if "dependencies" in content:
            normalized_ir["dependencies"].extend(
                content["dependencies"]
            )

    # --------------------------------------------------
    # REPORTS
    # --------------------------------------------------

    elif "report" in file_name:

        normalized_ir["reports"].append(content)

# ==================================================
# MERGE COLUMN METADATA INTO TABLE METADATA
# ==================================================

column_lookup = {}

for artifact in raw_ir.get("artifacts", []):

    if artifact.get("file_name", "").lower() == "columns.csv":

        sql_text = (
            artifact
            .get("content", {})
            .get("sql_logic", "")
        )

        reader = csv.DictReader(
            io.StringIO(sql_text)
        )

        for row in reader:

            table_name = row.get(
                "table_name",
                ""
            ).strip().upper()

            column_info = {
                "column_name":
                    row.get(
                        "column_name",
                        ""
                    ).strip(),

                "data_type":
                    row.get(
                        "data_type",
                        ""
                    ).strip(),

                "nullable":
                    row.get(
                        "nullable",
                        ""
                    ).strip()
            }

            column_lookup.setdefault(
                table_name,
                []
            ).append(
                column_info
            )

# attach columns to table metadata

tables = (
    normalized_ir["metadata"]
    ["table_metadata"]
    .get("tables", [])
)

for table in tables:

    table_name = (
        table.get("table_name", "")
            .strip()
            .upper()
    )
    table["columns"] = (
        column_lookup.get(
            table_name,
            []
        )
    )
    print(
        f"{table_name} -> "
        f"{len(table['columns'])} columns"
    )

# Write normalized json

with open(OUTPUT_FILE, "w") as f:
    json.dump(
        normalized_ir,
        f,
        indent=4
    )

print(f"Normalized JSON IR created: {OUTPUT_FILE}")