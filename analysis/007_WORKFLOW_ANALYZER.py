# ==========================================================
# 007_WORKFLOW_ANALYZER.py
# Workflow Analyzer Agent
# ==========================================================

from datetime import datetime

from databricks.sdk import WorkspaceClient

from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole
)

from pyspark.sql import Row

print("=" * 60)
print("WORKFLOW ANALYZER STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

LLM_ENDPOINT = "databricks-gpt-oss-120b"

# ----------------------------------------------------------
# LOAD RAG ANSWER
# ----------------------------------------------------------

rag_df = spark.table(
    "migration.repository.repo_rag_answers"
)

rag_row = (
    rag_df
    .orderBy("created_time", ascending=False)
    .first()
)

QUESTION = rag_row["question"]

RAG_ANALYSIS = rag_row["answer"]

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
# LOAD WORKFLOW METADATA
# ----------------------------------------------------------

workflow_df = spark.table(
    "migration.repository.repo_workflows"
)

dependency_df = spark.table(
    "migration.repository.repo_dependencies"
)

workflow_context = "\n".join(

    [
        str(row)
        for row in workflow_df.limit(100).collect()
    ]

)

dependency_context = "\n".join(

    [
        str(row)
        for row in dependency_df.limit(100).collect()
    ]

)

# ----------------------------------------------------------
# BUILD PROMPT
# ----------------------------------------------------------

PROMPT = f"""
You are a Senior Migration Workflow Architect.

QUESTION:
{QUESTION}

RAG ANALYSIS:
{RAG_ANALYSIS}

SOURCE ANALYSIS:
{SOURCE_ANALYSIS}

LOGIC ANALYSIS:
{LOGIC_ANALYSIS}

WORKFLOW METADATA:
{workflow_context}

DEPENDENCIES:
{dependency_context}

Generate a detailed workflow analysis.

Include:

1. End-to-End Workflow Overview

2. Execution Sequence

3. Task Flow Diagram

4. Upstream Dependencies

5. Downstream Dependencies

6. Critical Processing Steps

7. Business Logic Checkpoints

8. Failure Points

9. Restart and Recovery Strategy

10. Recommended Databricks Workflow Design

11. Recommended Lakeflow Architecture

12. Migration Risks

13. Optimization Opportunities

Format professionally.
"""

# ----------------------------------------------------------
# CALL LLM
# ----------------------------------------------------------

print("\nCALLING WORKFLOW ANALYZER...")

w = WorkspaceClient()

response = w.serving_endpoints.query(

    name=LLM_ENDPOINT,

    messages=[

        ChatMessage(
            role=ChatMessageRole.SYSTEM,
            content=
            "You are an expert ETL workflow architect and Databricks migration specialist."
        ),

        ChatMessage(
            role=ChatMessageRole.USER,
            content=PROMPT
        )

    ],

    max_tokens=2000

)

print("WORKFLOW ANALYSIS RECEIVED")

# ----------------------------------------------------------
# PARSE RESPONSE
# ----------------------------------------------------------

analysis = ""

try:

    content = (
        response.choices[0]
        .message.content
    )

    if isinstance(content, list):

        for item in content:

            try:

                if item.get("type") == "text":

                    analysis += item.get(
                        "text",
                        ""
                    )

            except:

                analysis += str(item)

    else:

        analysis = str(content)

except Exception:

    analysis = str(response)

# ----------------------------------------------------------
# DISPLAY
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("WORKFLOW ANALYSIS")
print("=" * 60)

print(analysis)

# ----------------------------------------------------------
# SAVE RESULT
# ----------------------------------------------------------

workflow_analysis_df = spark.createDataFrame(

    [

        Row(

            artifact_name=source_row["artifact_name"],

            question=QUESTION,

            analysis=analysis,

            created_time=datetime.now()

        )

    ]

)

workflow_analysis_df.write \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "migration.repository.repo_workflow_analysis"
    )

# ----------------------------------------------------------
# VALIDATION
# ----------------------------------------------------------

print("\nWorkflow Analysis Saved Successfully")

spark.sql("""

SELECT
artifact_name,
created_time
FROM migration.repository.repo_workflow_analysis

""").show(
    truncate=False
)

# ----------------------------------------------------------
# COMPLETE
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("WORKFLOW ANALYZER COMPLETED")
print("=" * 60)