# ==========================================================
# 009_CODE_REVIEW_AGENT.py
# Code Review Agent
# ==========================================================

from datetime import datetime
import re
from databricks.sdk import WorkspaceClient

from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole
)

from pyspark.sql import Row

print("=" * 60)
print("CODE REVIEW AGENT STARTED")
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
# LOAD ANALYSIS CONTEXT
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
# PROMPT
# ----------------------------------------------------------

PROMPT = f"""
You are a Senior Databricks Architect.

Review the generated PySpark code.

SOURCE ANALYSIS:
{SOURCE_ANALYSIS}

LOGIC ANALYSIS:
{LOGIC_ANALYSIS}

WORKFLOW ANALYSIS:
{WORKFLOW_ANALYSIS}

GENERATED PYSPARK:
{GENERATED_CODE}

Review the code for:

1. Code Quality
2. Readability
3. Modularity
4. Error Handling
5. Logging
6. Data Validation
7. Delta Lake Best Practices
8. Performance Optimization
9. Broadcast Join Opportunities
10. Partitioning Strategy
11. Caching Opportunities
12. SCD Type 2 Support
13. Idempotency
14. Auto Loader Opportunity
15. Lakeflow Design Alignment

Provide:

- Strengths
- Findings
- Risks
- Improvement Recommendations
- Overall Review Score (0-100)

IMPORTANT:

The FIRST LINE of your response must be:

OVERALL_REVIEW_SCORE: <number>

Example:

OVERALL_REVIEW_SCORE: 85

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
            "You are a principal Databricks architect performing a code review."
        ),

        ChatMessage(
            role=ChatMessageRole.USER,
            content=PROMPT
        )

    ],

    max_tokens=2500

)

# ----------------------------------------------------------
# PARSE RESPONSE
# ----------------------------------------------------------

review_comments = ""

try:

    content = (
        response.choices[0]
        .message.content
    )

    if isinstance(content, list):

        for item in content:
            try:
                if item.get("type") == "text":
                    review_comments += item.get(
                        "text",
                        ""
                    )

            except:
                review_comments += str(item)
    else:

        review_comments = str(content)

except Exception:

    review_comments = str(response)

# ----------------------------------------------------------
# DISPLAY
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("CODE REVIEW REPORT")
print("=" * 60)

print(review_comments)

# ----------------------------------------------------------
# EXTRACT REVIEW SCORE
# ----------------------------------------------------------

review_score = 0

try:

    score_match = re.search(
        r"OVERALL_REVIEW_SCORE\s*:\s*(\d+)",
        review_comments,
        re.IGNORECASE
    )

    if not score_match:
        score_match = re.search(
            r"Overall\s+Score.*?(\d+)\s*/\s*100",
            review_comments,
            re.IGNORECASE | re.DOTALL
        )
    if score_match:
        review_score = int(
            score_match.group(1)
        )

except Exception:

    review_score = 0

print(
    f"\nREVIEW SCORE : {review_score}"
)

# ----------------------------------------------------------
# SAVE
# ----------------------------------------------------------

review_df = spark.createDataFrame(

    [

        Row(
            artifact_name=code_row["artifact_name"],
            question=QUESTION,
            review_score=review_score,
            review_comments=review_comments,
            created_time=datetime.now()
        )
    ]

)

review_df.write \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "migration.repository.repo_code_review"
    )

print("\nCODE REVIEW SAVED")

spark.sql("""

SELECT
artifact_name,
review_score,
created_time
FROM migration.repository.repo_code_review

""").show(truncate=False)

print("=" * 60)
print("CODE REVIEW AGENT COMPLETED")
print("=" * 60)