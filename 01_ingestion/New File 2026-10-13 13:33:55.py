# ============================================================
# 002_JSON_IR_NORMALIZER_DEBUG.py
# ============================================================

import json
import csv
import io

INPUT_FILES = "/Volumes/migration/default/migration_poc/manhattan_mock_artifacts/canonical_json/"
OUTPUT_FILE = "/Volumes/migration/default/migration_poc/manhattan_mock_artifacts/migration_normalized.json"

print("=" * 80)
print("STARTING JSON IR NORMALIZER")
print("=" * 80)

# ============================================================
# LOAD ALL PARSER OUTPUT FILES
# ============================================================

artifacts = []

print("\nFILES FOUND IN CANONICAL_JSON:")
print("-" * 80)

for item in dbutils.fs.ls(INPUT_FILES):

    print(f"FOUND FILE : {item.name}")

    if item.name.endswith(".json"):

        content = dbutils.fs.head(
            item.path,
            5 * 1024 * 1024
        )

        artifact = json.loads(content)

        artifact["file_name"] = artifact.get(
            "source_file",
            item.name
        )

        artifacts.append(artifact)

        print(
            f"LOADED -> {artifact.get('file_name')}"
        )

print()
print(f"TOTAL ARTIFACTS LOADED : {len(artifacts)}")


# ============================================================
# SHOW ALL SOURCE FILES
# ============================================================

print("\nSOURCE FILES DISCOVERED:")
print("-" * 80)

for artifact in artifacts:

    print(
        f"FILE_NAME    : {artifact.get('file_name')}"
    )

    print(
        f"SOURCE_FILE  : {artifact.get('source_file')}"
    )

    print("-" * 40)


raw_ir = {
    "source_system": "Manhattan",
    "artifacts": artifacts
}

normalized_ir = {
    "source_system": raw_ir.get(
        "source_system",
        "Manhattan"
    ),

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

# ============================================================
# CATEGORIZE FILES
# ============================================================

print("\nCATEGORIZING ARTIFACTS")
print("=" * 80)

for artifact in raw_ir.get("artifacts", []):

    file_name = artifact.get(
        "file_name",
        ""
    ).lower()

    content = artifact.get(
        "content",
        {}
    )

    print(f"\nPROCESSING : {file_name}")

    # --------------------------------------------------------
    # TABLES
    # --------------------------------------------------------

    if file_name == "tables.csv":

        print("-> TABLE METADATA FILE")

        normalized_ir["metadata"]["tables"].append({
            "file_name": file_name,
            "columns": artifact.get("columns", []),
            "record_count": artifact.get("record_count")
        })

    # --------------------------------------------------------
    # COLUMNS
    # --------------------------------------------------------

    elif file_name == "columns.csv":

        print("-> COLUMN METADATA FILE")

        normalized_ir["metadata"]["columns"].append({
            "file_name": file_name,
            "columns": artifact.get("columns", []),
            "record_count": artifact.get("record_count")
        })

    # --------------------------------------------------------
    # FOREIGN KEYS
    # --------------------------------------------------------

    elif file_name == "foreign_keys.csv":

        print("-> FOREIGN KEY FILE")

        normalized_ir["metadata"]["foreign_keys"].append({
            "file_name": file_name,
            "columns": artifact.get("columns", []),
            "record_count": artifact.get("record_count")
        })

    # --------------------------------------------------------
    # TABLE_METADATA.JSON
    # --------------------------------------------------------

    elif file_name == "table_metadata.json":

        print("\nTABLE_METADATA.JSON FOUND")
        print("-" * 80)

        print(
            json.dumps(
                content,
                indent=4
            )
        )

        normalized_ir["metadata"][
            "table_metadata"
        ] = content

    # --------------------------------------------------------
    # BUSINESS RULES
    # --------------------------------------------------------

    elif "rule" in file_name:

        print("-> BUSINESS RULE")

        if "rules" in content:

            normalized_ir[
                "business_rules"
            ].extend(
                content["rules"]
            )

    # --------------------------------------------------------
    # ETL
    # --------------------------------------------------------

    elif (
        "etl" in file_name
        or "load" in file_name
        or "streaming" in file_name
    ):

        print("-> ETL FILE")

        normalized_ir[
            "etl_processes"
        ].append(
            content
        )

    # --------------------------------------------------------
    # WORKFLOW
    # --------------------------------------------------------

    elif "workflow" in file_name:

        print("-> WORKFLOW FILE")

        normalized_ir[
            "workflows"
        ].append(
            content
        )

    # --------------------------------------------------------
    # DEPENDENCIES
    # --------------------------------------------------------

    elif "dependenc" in file_name:

        print("-> DEPENDENCY FILE")

        if "dependencies" in content:

            normalized_ir[
                "dependencies"
            ].extend(
                content[
                    "dependencies"
                ]
            )

    # --------------------------------------------------------
    # REPORT
    # --------------------------------------------------------

    elif "report" in file_name:

        print("-> REPORT FILE")

        normalized_ir[
            "reports"
        ].append(
            content
        )

# ============================================================
# COLUMN LOOKUP
# ============================================================

print("\nBUILDING COLUMN LOOKUP")
print("=" * 80)

column_lookup = {}

for artifact in raw_ir.get(
    "artifacts",
    []
):

    if (
        artifact
        .get(
            "file_name",
            ""
        )
        .lower()
        == "columns.csv"
    ):

        sql_text = (
            artifact
            .get(
                "content",
                {}
            )
            .get(
                "sql_logic",
                ""
            )
        )

        print("\nRAW COLUMNS.CSV CONTENT")
        print("-" * 80)
        print(sql_text)

        reader = csv.DictReader(
            io.StringIO(
                sql_text
            )
        )

        for row in reader:

            table_name = (
                row.get(
                    "table_name",
                    ""
                )
                .strip()
                .upper()
            )

            column_name = (
                row.get(
                    "column_name",
                    ""
                )
                .strip()
            )

            print(
                f"TABLE={table_name} "
                f"COLUMN={column_name}"
            )

            column_info = {

                "column_name":
                    column_name,

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

# ============================================================
# SHOW COLUMN LOOKUP
# ============================================================

print("\nCOLUMN LOOKUP CREATED")
print("=" * 80)

for table_name, cols in column_lookup.items():

    print(
        f"{table_name} -> "
        f"{len(cols)} columns"
    )

# ============================================================
# TABLE METADATA DEBUG
# ============================================================

table_metadata = (
    normalized_ir["metadata"]
    .get(
        "table_metadata",
        {}
    )
)

print("\nTABLE_METADATA CONTENT")
print("=" * 80)

print(
    json.dumps(
        table_metadata,
        indent=4
    )
)

tables = []

for table_name in column_lookup.keys():

    tables.append({
        "table_name": table_name
    })


print("\nTABLES FOUND:")
print("-" * 80)

for table in tables:

    print(
        table.get(
            "table_name",
            "UNKNOWN"
        )
    )

# ============================================================
# ATTACH COLUMNS
# ============================================================

print("\nFINAL TABLE TO COLUMN MAPPING")
print("=" * 80)

for table in tables:

    table_name = (
        table.get(
            "table_name",
            ""
        )
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

# ============================================================
# WRITE OUTPUT
# ============================================================

with open(
    OUTPUT_FILE,
    "w"
) as f:

    json.dump(
        normalized_ir,
        f,
        indent=4
    )

print()
print("=" * 80)
print(
    f"NORMALIZED JSON WRITTEN TO:\n{OUTPUT_FILE}"
)
print("=" * 80)