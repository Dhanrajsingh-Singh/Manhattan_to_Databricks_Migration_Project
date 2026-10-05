# ==========================================================
# 005A_KNOWLEDGE_EXTRACTOR.py
# Knowledge Extraction Agent
# ==========================================================

from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime
import json

print("=" * 60)
print("KNOWLEDGE EXTRACTION AGENT STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

CANONICAL_JSON_PATH = (
    "/Volumes/migration/default/"
    "migration_poc/manhattan_mock_artifacts/"
    "canonical_json/"
)

# ----------------------------------------------------------
# NORMALIZE CONTENT
# ----------------------------------------------------------

def normalize_content(content):

    if isinstance(content, (dict, list)):

        return json.dumps(
            content,
            indent=2
        )

    return str(content)

# ----------------------------------------------------------
# READ JSON FILES
# ----------------------------------------------------------

json_files = [

    file.path

    for file in dbutils.fs.ls(
        CANONICAL_JSON_PATH
    )

    if file.path.endswith(".json")
]

print(
    f"JSON Files Found : {len(json_files)}"
)

# ----------------------------------------------------------
# INITIALIZE
# ----------------------------------------------------------

knowledge_records = []

knowledge_counter = 1

# ----------------------------------------------------------
# PROCESS FILES
# ----------------------------------------------------------

for json_file in json_files:

    print(f"Processing : {json_file}")

    try:

        content = dbutils.fs.head(
            json_file,
            1000000
        )

        data = json.loads(content)

        artifact_name = data.get(
            "artifact_name",
            "UNKNOWN"
        )

        artifact_type = data.get(
            "artifact_type",
            "UNKNOWN"
        )

        source_file = data.get(
            "source_file",
            json_file
        )

        artifact_content = data.get(
            "content",
            ""
        )

        # --------------------------------------------------
        # MAIN KNOWLEDGE RECORD
        # --------------------------------------------------

        knowledge_records.append(

            Row(
                knowledge_id=f"KR{knowledge_counter:05}",

                artifact_name=str(
                    artifact_name
                ),

                artifact_type=str(
                    artifact_type
                ),

                knowledge_type=str(
                    artifact_type
                ).upper(),

                source_file=str(
                    source_file
                ),

                content=normalize_content(
                    artifact_content
                ),

                created_time=str(
                    datetime.now()
                )
            )
        )

        knowledge_counter += 1

        # --------------------------------------------------
        # SEMANTIC CONTEXT
        # --------------------------------------------------

        semantic_context = f"""
Artifact Name : {artifact_name}

Artifact Type : {artifact_type}

Source File : {source_file}

This artifact represents a
{artifact_type} component
within the Manhattan platform.
"""

        knowledge_records.append(

            Row(
                knowledge_id=f"KR{knowledge_counter:05}",

                artifact_name=str(
                    artifact_name
                ),

                artifact_type=str(
                    artifact_type
                ),

                knowledge_type="SEMANTIC_CONTEXT",

                source_file=str(
                    source_file
                ),

                content=normalize_content(
                    semantic_context
                ),

                created_time=str(
                    datetime.now()
                )
            )
        )

        knowledge_counter += 1

        # --------------------------------------------------
        # ETL CONTEXT
        # --------------------------------------------------

        etl_context = f"""
Artifact Name : {artifact_name}

Artifact Type : {artifact_type}

Source File : {source_file}

This artifact may contain:

Workflow Steps
ETL Logic
Transformations
Business Rules
Data Movement Logic

Used for migration knowledge discovery.
"""

        knowledge_records.append(

            Row(
                knowledge_id=f"KR{knowledge_counter:05}",

                artifact_name=str(
                    artifact_name
                ),

                artifact_type=str(
                    artifact_type
                ),

                knowledge_type="ETL_CONTEXT",

                source_file=str(
                    source_file
                ),

                content=normalize_content(
                    etl_context
                ),

                created_time=str(
                    datetime.now()
                )
            )
        )

        knowledge_counter += 1

    except Exception as e:

        print(
            f"FAILED : {json_file}"
        )

        print(str(e))

# ----------------------------------------------------------
# VALIDATION
# ----------------------------------------------------------

print(
    f"Knowledge Records Extracted : "
    f"{len(knowledge_records)}"
)

if len(knowledge_records) == 0:

    raise Exception(
        "No knowledge records extracted."
    )

# ----------------------------------------------------------
# SCHEMA
# ----------------------------------------------------------

knowledge_schema = StructType([

    StructField(
        "knowledge_id",
        StringType(),
        True
    ),

    StructField(
        "artifact_name",
        StringType(),
        True
    ),

    StructField(
        "artifact_type",
        StringType(),
        True
    ),

    StructField(
        "knowledge_type",
        StringType(),
        True
    ),

    StructField(
        "source_file",
        StringType(),
        True
    ),

    StructField(
        "content",
        StringType(),
        True
    ),

    StructField(
        "created_time",
        StringType(),
        True
    )

])

# ----------------------------------------------------------
# CREATE DATAFRAME
# ----------------------------------------------------------

knowledge_df = spark.createDataFrame(
    knowledge_records,
    schema=knowledge_schema
)

display(knowledge_df)

# ----------------------------------------------------------
# DROP OLD TABLE
# ----------------------------------------------------------

spark.sql("""

DROP TABLE IF EXISTS
migration.repository.repo_knowledge

""")

# ----------------------------------------------------------
# SAVE REPOSITORY
# ----------------------------------------------------------

knowledge_df.write \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "migration.repository.repo_knowledge"
    )

print(
    "repo_knowledge created successfully"
)

# ----------------------------------------------------------
# SUMMARY
# ----------------------------------------------------------

print("\nKNOWLEDGE SUMMARY")

spark.sql("""

SELECT

    knowledge_type,
    COUNT(*) AS total_records

FROM migration.repository.repo_knowledge

GROUP BY knowledge_type

ORDER BY knowledge_type

""").show()

# ----------------------------------------------------------
# PREVIEW
# ----------------------------------------------------------

display(

    spark.sql("""

    SELECT *

    FROM migration.repository.repo_knowledge

    ORDER BY knowledge_id

    """)

)

print("=" * 60)
print("KNOWLEDGE EXTRACTION COMPLETED")
print("=" * 60)