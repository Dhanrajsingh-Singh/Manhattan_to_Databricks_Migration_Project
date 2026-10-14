# ==========================================================
# 005_SOURCE_ANALYZER.py
# Source Analyzer Agent
# ==========================================================

from datetime import datetime

from databricks.sdk import WorkspaceClient

from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole
)

from pyspark.sql import Row

print("=" * 60)
print("SOURCE ANALYZER STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIG
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

RAG_ANSWER = rag_row["answer"]

# ----------------------------------------------------------
# LOAD METADATA
# ----------------------------------------------------------

deps_df = spark.table(
    "migration.repository.repo_dependencies"
)

tables_df = spark.table(
    "migration.repository.repo_tables"
)

workflow_df = spark.table(
    "migration.repository.repo_workflows"
)

deps_context = "\n".join(
    [str(x) for x in deps_df.limit(50).collect()]
)

tables_context = "\n".join(
    [str(x) for x in tables_df.limit(50).collect()]
)

workflow_context = "\n".join(
    [str(x) for x in workflow_df.limit(50).collect()]
)

# ----------------------------------------------------------
# PROMPT
# ----------------------------------------------------------

PROMPT = f"""
You are a Manhattan Migration Source Analyzer.

QUESTION:
{QUESTION}

RAG ANSWER:
{RAG_ANSWER}

DEPENDENCIES:
{deps_context}

TABLES:
{tables_context}

WORKFLOWS:
{workflow_context}

Provide:

1. Source Tables
2. Target Tables
3. Input Files
4. Output Files
5. Upstream Dependencies
6. Downstream Dependencies
7. Data Flow
8. Migration Risks

Generate a structured analysis.
"""

# ----------------------------------------------------------
# LLM CALL
# ----------------------------------------------------------

w = WorkspaceClient()

response = w.serving_endpoints.query(

    name=LLM_ENDPOINT,

    messages=[

        ChatMessage(
            role=ChatMessageRole.SYSTEM,
            content=
            "You are an expert migration source analyzer."
        ),

        ChatMessage(
            role=ChatMessageRole.USER,
            content=PROMPT
        )

    ],

    max_tokens=1500

)

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

print("\nSOURCE ANALYSIS")
print("=" * 60)

print(analysis)

# ----------------------------------------------------------
# SAVE
# ----------------------------------------------------------

artifact_name = rag_row["question"]

analysis_df = spark.createDataFrame(

    [

        Row(

            artifact_name=artifact_name,

            question=QUESTION,

            analysis=analysis,

            created_time=datetime.now()

        )

    ]

)

analysis_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "migration.repository.repo_source_analysis"
    )

print("\nAnalysis Saved Successfully")

# ----------------------------------------------------------
# COMPLETE
# ----------------------------------------------------------

print("\n" + "=" * 60)

print(
    "SOURCE ANALYZER COMPLETED"
)

print("=" * 60)