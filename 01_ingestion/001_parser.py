# Databricks Notebook
# 001_parser.py

import json
import xml.etree.ElementTree as ET

SOURCE_PATH = "/Volumes/migration/default/migration_poc/manhattan_mock_artifacts/manhattan_mock_artifacts/"
TARGET_PATH = "/Volumes/migration/default/migration_poc/manhattan_mock_artifacts/canonical_json/"

# =====================================================
# Ensure Output Folder Exists
# =====================================================

try:
    dbutils.fs.mkdirs(TARGET_PATH)
except:
    pass

# =====================================================
# XML Parser
# =====================================================

def parse_xml(content, source_file):

    root = ET.fromstring(content)

    artifact_name = (
        root.attrib.get("name")
        or source_file.split(".")[0]
    )

    return {
        "artifact_name": artifact_name,
        "artifact_type": root.tag,
        "source_file": source_file,
        "content": content
    }

# =====================================================
# JSON Parser
# =====================================================

def parse_json(content, source_file):

    data = json.loads(content)

    artifact_name = source_file.split(".")[0]

    if (
        isinstance(data, dict)
        and "dependencies" in data
    ):
        artifact_name = (
            data.get("workflow")
            and f"{data['workflow']}_DEPENDENCY"
        ) or source_file.split(".")[0]
    else:
        artifact_name = (
            data.get("rule_name")
            or data.get("object_name")
            or data.get("mapping_name")
            or data.get("configuration_name")
            or source_file.split(".")[0]
        )


    return {
        "artifact_name": artifact_name,
        "artifact_type": "json_artifact",
        "source_file": source_file,
        "content": data
    }

# =====================================================
# SQL Parser
# =====================================================

def parse_sql(content, source_file):

    artifact_name = source_file.split(".")[0]

    return {
        "artifact_name": artifact_name,
        "artifact_type": "sql_logic",
        "source_file": source_file,
        "content": {
            "sql_logic": content
        }
    }

# =====================================================
# Read Manhattan Artifacts
# =====================================================

# =====================================================
# Recursive File Discovery
# =====================================================

def get_all_files(path):

    all_files = []

    for item in dbutils.fs.ls(path):

        if item.path.endswith("/"):
            all_files.extend(
                get_all_files(item.path)
            )
        else:
            all_files.append(item)

    return all_files


files = get_all_files(SOURCE_PATH)

print(f"Total Files Found = {len(files)}")


generated_files = []

for file in files:

    # Safety check
    if file.path.endswith("/"):
        print(f"Skipping Directory -> {file.name}")
        continue

    print(f"\nProcessing -> {file.name}")

    try:

        content = dbutils.fs.head(
            file.path,
            5 * 1024 * 1024
        )

        content_trim = content.strip()

        # -------------------------------------------------
        # XML
        # -------------------------------------------------

        if content_trim.startswith("<"):

            artifact = parse_xml(
                content,
                file.name
            )

        # -------------------------------------------------
        # JSON
        # -------------------------------------------------

        elif (
            content_trim.startswith("{")
            or content_trim.startswith("[")
        ):

            artifact = parse_json(
                content,
                file.name
            )

        # -------------------------------------------------
        # SQL / Text
        # -------------------------------------------------

        else:

            artifact = parse_sql(
                content,
                file.name
            )

        # -------------------------------------------------
        # Output File
        # -------------------------------------------------

        output_file = (
            TARGET_PATH
            + artifact["artifact_name"]
            + ".json"
        )

        dbutils.fs.put(
            output_file,
            json.dumps(
                artifact,
                indent=4
            ),
            True
        )

        generated_files.append(
            artifact["artifact_name"]
        )

        print(
            f"SUCCESS -> {artifact['artifact_name']}.json"
        )

    except Exception as e:

        print(f"FAILED -> {file.name}")
        print(str(e))

# =====================================================
# Summary
# =====================================================

print("\n===================================")
print("Generated JSON Files")
print("===================================")

for f in generated_files:
    print(f)

print(f"\nTotal Generated = {len(generated_files)}")