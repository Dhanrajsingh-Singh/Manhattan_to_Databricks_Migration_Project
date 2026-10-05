6# ==========================================================
# 006_LOGIC_ANALYZER.py
# Logic Analyzer Agent
# ==========================================================

from datetime import datetime

from databricks.sdk import WorkspaceClient

from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole
)

from pyspark.sql import Row

print("=" * 60)
print("LOGIC ANALYZER STARTED")
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
RAG_ANSWER = rag_row["answer"]

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
# LOAD KNOWLEDGE
# ----------------------------------------------------------

knowledge_df = spark.table(
    "migration.repository.repo_knowledge"
)

knowledge_context = "\n".join(

    [

        str(row)

        for row in

        knowledge_df.limit(50).collect()

    ]

)

# ----------------------------------------------------------
# PROMPT
# ----------------------------------------------------------

PROMPT = f"""
You are a Manhattan Migration Logic Analyzer.

QUESTION:
{QUESTION}

RAG ANSWER:
{RAG_ANSWER}

SOURCE ANALYSIS:
{SOURCE_ANALYSIS}

KNOWLEDGE:
{knowledge_context}

Identify and explain:

1. Business Rules
2. Validation Rules
3. Transformations
4. Lookups
5. Joins
6. Filters
7. Derived Fields
8. Aggregations
9. Error Handling Logic
10. Migration Recommendations

Generate a detailed technical analysis.

Use sections and bullet points.
"""

# ----------------------------------------------------------
# CALL LLM
# ----------------------------------------------------------

print("\nCALLING LOGIC ANALYZER LLM...")

w = WorkspaceClient()

response = w.serving_endpoints.query(

    name=LLM_ENDPOINT,

    messages=[

        ChatMessage(
            role=ChatMessageRole.SYSTEM,
            content=
            "You are an expert ETL and migration logic analyst."
        ),

        ChatMessage(
            role=ChatMessageRole.USER,
            content=PROMPT
        )

    ],

    max_tokens=1500

)

print("LLM RESPONSE RECEIVED")

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

                if item.get("type")== "text":

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
print("LOGIC ANALYSIS")
print("=" * 60)

print(analysis)

# ----------------------------------------------------------
# SAVE
# ----------------------------------------------------------

logic_df = spark.createDataFrame(

    [

        Row(

            artifact_name = source_row["artifact_name"],

            question=QUESTION,

            analysis=analysis,

            created_time=datetime.now()

        )

    ]

)

logic_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "migration.repository.repo_logic_analysis"
    )

print("\nLogic Analysis Saved Successfully")

print(
    "Table : migration.repository.repo_logic_analysis"
)

# ----------------------------------------------------------
# VALIDATION
# ----------------------------------------------------------

print("\nLATEST ANALYSIS")

spark.sql("""

SELECT
artifact_name,
created_time
FROM migration.repository.repo_logic_analysis

""").show(truncate=False)

# ----------------------------------------------------------
# COMPLETE
# ----------------------------------------------------------

print("\n" + "=" * 60)

print(
    "LOGIC ANALYZER COMPLETED"
)

print("=" * 60)