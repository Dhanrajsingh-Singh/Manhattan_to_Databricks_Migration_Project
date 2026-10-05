# ==========================================================
# 004F_RAG_ENGINE.py
# Retrieval Augmented Generation Engine
# ==========================================================

from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole
)

from pyspark.sql import Row

print("=" * 60)
print("RAG ENGINE STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

LLM_ENDPOINT = "databricks-gpt-oss-120b"

# ----------------------------------------------------------
# LOAD CONTEXT
# ----------------------------------------------------------

context_df = spark.table(
    "migration.repository.repo_search_context"
)

row = context_df.first()

QUESTION = row["question"]
CONTEXT = row["context"]

print("\nQUESTION:")
print(QUESTION)

# ----------------------------------------------------------
# PROMPT
# ----------------------------------------------------------

PROMPT = f"""
You are an enterprise migration architect.

Use ONLY the supplied context.

QUESTION:
{QUESTION}

CONTEXT:
{CONTEXT}

Provide:

1. Executive Summary
2. Workflow Explanation
3. Business Logic
4. Dependencies
5. Migration Considerations

Answer in professional format.
"""

# ----------------------------------------------------------
# CALL LLM
# ----------------------------------------------------------

print("\nCALLING LLM...")

w = WorkspaceClient()

response = w.serving_endpoints.query(

    name=LLM_ENDPOINT,

    messages=[

        ChatMessage(
            role=ChatMessageRole.SYSTEM,
            content=
            "You are a senior Databricks migration architect."
        ),

        ChatMessage(
            role=ChatMessageRole.USER,
            content=PROMPT
        )

    ],

    max_tokens=1000

)

print("LLM RESPONSE RECEIVED")

# ----------------------------------------------------------
# PARSE ANSWER
# ----------------------------------------------------------

answer = ""

try:

    content = (
        response.choices[0]
        .message.content
    )

    if isinstance(content, list):

        for item in content:

            try:

                if item.get("type") == "text":

                    answer += item.get(
                        "text",
                        ""
                    )

            except:

                answer += str(item)

    else:

        answer = str(content)

except Exception:

    answer = str(response)

# ----------------------------------------------------------
# DISPLAY ANSWER
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("RAG ANSWER")
print("=" * 60)

print(answer)

# ----------------------------------------------------------
# SAVE ANSWER
# ----------------------------------------------------------

answer_df = spark.createDataFrame(

    [

        Row(

            question=QUESTION,

            answer=answer,

            created_time=datetime.now()

        )

    ]

)

answer_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "migration.repository.repo_rag_answers"
    )

print(
    "\nAnswer Saved Successfully"
)

print(
    "Table : migration.repository.repo_rag_answers"
)

# ----------------------------------------------------------
# VALIDATION
# ----------------------------------------------------------

print("\nLATEST ANSWER")

spark.sql("""

SELECT *
FROM migration.repository.repo_rag_answers
ORDER BY created_time DESC
LIMIT 1

""").show(truncate=False)

# ----------------------------------------------------------
# COMPLETE
# ----------------------------------------------------------

print("\n" + "=" * 60)

print(
    "RAG ENGINE COMPLETED"
)

print("=" * 60)