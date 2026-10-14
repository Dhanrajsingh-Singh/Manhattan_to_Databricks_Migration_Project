# ==========================================================
# 008_PYSPARK_GENERATOR.py
# PySpark Generator Agent
# ==========================================================

from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole
)

from pyspark.sql import Row

print("=" * 60)
print("PYSPARK GENERATOR STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

LLM_ENDPOINT = "databricks-gpt-oss-120b"

# ----------------------------------------------------------
# LOAD SOURCE ANALYSIS
# ----------------------------------------------------------

source_df = spark.table(
    "migration.repository.repo_source_analysis"
)

source_row = (
    source_df
    .orderBy("created_time", ascending=False)
    .first()
)

SOURCE_ANALYSIS = source_row["analysis"]

# ----------------------------------------------------------
# LOAD LOGIC ANALYSIS
# ----------------------------------------------------------

logic_df = spark.table(
    "migration.repository.repo_logic_analysis"
)

logic_row = (
    logic_df
    .orderBy("created_time", ascending=False)
    .first()
)

LOGIC_ANALYSIS = logic_row["analysis"]

# ----------------------------------------------------------
# LOAD WORKFLOW ANALYSIS
# ----------------------------------------------------------

workflow_df = spark.table(
    "migration.repository.repo_workflow_analysis"
)

workflow_row = (
    workflow_df
    .orderBy("created_time", ascending=False)
    .first()
)

WORKFLOW_ANALYSIS = workflow_row["analysis"]

QUESTION = workflow_row["question"]

# ----------------------------------------------------------
# BUILD PROMPT
# ----------------------------------------------------------

PROMPT = f"""
You are a Senior Databricks Migration Engineer.

Generate ONLY executable Databricks PySpark code.

DO NOT provide explanations.
DO NOT provide reasoning.
DO NOT provide markdown.

Source Analysis:
{SOURCE_ANALYSIS}

Logic Analysis:
{LOGIC_ANALYSIS}

Workflow Analysis:
{WORKFLOW_ANALYSIS}

Requirements:

1. Databricks PySpark only.
2. Use Spark DataFrame API.
3. Implement validations.
4. Implement transformations.
5. Implement joins.
6. Implement logging.
7. Add production-quality comments.
8. Use Delta tables.
9. Include error handling.
10. Include reusable functions.

Return ONLY Python code.
"""

# ----------------------------------------------------------
# CALL LLM
# ----------------------------------------------------------

print("\nCALLING PYSPARK GENERATOR...")

w = WorkspaceClient()

response = w.serving_endpoints.query(

    name=LLM_ENDPOINT,

    messages=[

        ChatMessage(
            role=ChatMessageRole.SYSTEM,
            content=
            "You are an expert Databricks PySpark developer."
        ),

        ChatMessage(
            role=ChatMessageRole.USER,
            content=PROMPT
        )

    ],

    max_tokens=3000

)

print("LLM RESPONSE RECEIVED")

# ----------------------------------------------------------
# EXTRACT GENERATED CODE
# ----------------------------------------------------------

generated_code = ""

try:

    content = (
        response.choices[0]
        .message.content
    )

    if isinstance(content, list):

        for item in content:

            try:

                # SDK object format

                if hasattr(item, "type"):

                    if item.type == "text":

                        generated_code += getattr(
                            item,
                            "text",
                            ""
                        )

                # Dict format

                elif isinstance(item, dict):

                    if item.get("type") == "text":

                        generated_code += item.get(
                            "text",
                            ""
                        )

            except Exception:

                pass

    else:

        generated_code = str(content)

except Exception as e:

    print(
        f"Parsing Error : {str(e)}"
    )

    generated_code = str(response)

# ----------------------------------------------------------
# CLEAN OUTPUT
# ----------------------------------------------------------

generated_code = (
    generated_code
    .replace("```python", "")
    .replace("```", "")
    .strip()
)

# ----------------------------------------------------------
# DISPLAY PREVIEW
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("GENERATED PYSPARK PREVIEW")
print("=" * 60)

print(
    generated_code[:5000]
)

# ----------------------------------------------------------
# SAVE GENERATED CODE
# ----------------------------------------------------------

pyspark_df = spark.createDataFrame(

    [

        Row(

            artifact_name=workflow_row["artifact_name"],

            question=QUESTION,

            pyspark_code=generated_code,

            created_time=datetime.now()

        )

    ]

)

pyspark_df.write \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "migration.repository.repo_generated_pyspark"
    )

# ----------------------------------------------------------
# VALIDATION
# ----------------------------------------------------------

print("\nPySpark Code Saved Successfully")

spark.sql("""

SELECT
artifact_name,
length(pyspark_code) AS code_length,
created_time
FROM migration.repository.repo_generated_pyspark

""").show(
    truncate=False
)

# ----------------------------------------------------------
# COMPLETE
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("PYSPARK GENERATOR COMPLETED")
print("=" * 60)