# ==========================================================
# 011_PYSPARK_FIX_AGENT.py
# PySpark Fix Agent
# ==========================================================

from datetime import datetime

from databricks.sdk import WorkspaceClient

from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole
)

from pyspark.sql import Row

print("=" * 60)
print("PYSPARK FIX AGENT STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------

LLM_ENDPOINT = "databricks-gpt-oss-120b"

# ----------------------------------------------------------
# LOAD GENERATED CODE
# ----------------------------------------------------------

code_row = (

    spark.table(
        "migration.repository.repo_generated_pyspark"
    )

    .orderBy(
        "created_time",
        ascending=False
    )

    .first()

)

GENERATED_CODE = code_row["pyspark_code"]

QUESTION = code_row["question"]

# ----------------------------------------------------------
# LOAD CODE REVIEW
# ----------------------------------------------------------

review_row = (

    spark.table(
        "migration.repository.repo_code_review"
    )

    .orderBy(
        "created_time",
        ascending=False
    )

    .first()

)

CODE_REVIEW = review_row["review_comments"]

# ----------------------------------------------------------
# LOAD ANALYSIS
# ----------------------------------------------------------

source_row = (

    spark.table(
        "migration.repository.repo_source_analysis"
    )

    .orderBy(
        "created_time",
        ascending=False
    )

    .first()

)

logic_row = (

    spark.table(
        "migration.repository.repo_logic_analysis"
    )

    .orderBy(
        "created_time",
        ascending=False
    )

    .first()

)

workflow_row = (

    spark.table(
        "migration.repository.repo_workflow_analysis"
    )

    .orderBy(
        "created_time",
        ascending=False
    )

    .first()

)

SOURCE_ANALYSIS = source_row["analysis"]
LOGIC_ANALYSIS = logic_row["analysis"]
WORKFLOW_ANALYSIS = workflow_row["analysis"]

# ----------------------------------------------------------
# BUILD PROMPT
# ----------------------------------------------------------

PROMPT = f"""
You are a Principal Databricks Architect.

Improve the generated PySpark code.

SOURCE ANALYSIS:
{SOURCE_ANALYSIS}

LOGIC ANALYSIS:
{LOGIC_ANALYSIS}

WORKFLOW ANALYSIS:
{WORKFLOW_ANALYSIS}

CODE REVIEW FINDINGS:
{CODE_REVIEW}

CURRENT CODE:
{GENERATED_CODE}

Apply all relevant improvements:

1. Structured logging
2. Delta MERGE instead of overwrite
3. SCD Type 2 handling
4. Auto Loader where appropriate
5. Broadcast joins
6. Partitioning strategy
7. Caching improvements
8. Retry-safe idempotent processing
9. Better error handling
10. Data quality validations
11. Performance optimization
12. Lakehouse best practices

Return ONLY executable Databricks PySpark code.

Do not return explanations.
Do not return markdown.
"""

# ----------------------------------------------------------
# CALL LLM
# ----------------------------------------------------------

w = WorkspaceClient()

response = w.serving_endpoints.query(

    name=LLM_ENDPOINT,

    messages=[

        ChatMessage(
            role=ChatMessageRole.SYSTEM,
            content=
            "You are a principal Databricks migration architect."
        ),

        ChatMessage(
            role=ChatMessageRole.USER,
            content=PROMPT
        )

    ],

    max_tokens=4000

)

# ----------------------------------------------------------
# PARSE CODE
# ----------------------------------------------------------

generated_code = ""

try:

    content = response.choices[0].message.content

    if isinstance(content, list):

        for item in content:

            if hasattr(item, "type"):

                if item.type == "text":

                    generated_code += getattr(
                        item,
                        "text",
                        ""
                    )

            elif isinstance(item, dict):

                if item.get("type") == "text":

                    generated_code += item.get(
                    "text",
                    ""
                )

    else:

        generated_code = str(content)

except Exception:

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
# DISPLAY
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("IMPROVED PYSPARK PREVIEW")
print("=" * 60)

print(
    generated_code[:5000]
)

# ----------------------------------------------------------
# SAVE V2 CODE
# ----------------------------------------------------------

v2_df = spark.createDataFrame(

    [

        Row(

            artifact_name=code_row["artifact_name"],

            question=QUESTION,

            pyspark_code=generated_code,

            created_time=datetime.now()

        )

    ]

)

v2_df.write \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "migration.repository.repo_generated_pyspark_v2"
    )

print("\nIMPROVED CODE SAVED")

spark.sql("""

SELECT
artifact_name,
created_time
FROM migration.repository.repo_generated_pyspark_v2

""").show(
    truncate=False
)

print("=" * 60)
print("PYSPARK FIX AGENT COMPLETED")
print("=" * 60)