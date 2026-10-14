# ==========================================================
# 012_TEST_CASE_GENERATOR.py
# Test Case Generator Agent
# ==========================================================

from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole
)

from pyspark.sql import Row

print("=" * 60)
print("TEST CASE GENERATOR STARTED")
print("=" * 60)

LLM_ENDPOINT = "databricks-gpt-oss-120b"

# ----------------------------------------------------------
# LOAD ANALYSIS
# ----------------------------------------------------------

source_row = (
    spark.table(
        "migration.repository.repo_source_analysis"
    )
    .orderBy("created_time", ascending=False)
    .first()
)

logic_row = (
    spark.table(
        "migration.repository.repo_logic_analysis"
    )
    .orderBy("created_time", ascending=False)
    .first()
)

workflow_row = (
    spark.table(
        "migration.repository.repo_workflow_analysis"
    )
    .orderBy("created_time", ascending=False)
    .first()
)

code_row = (
    spark.table(
        "migration.repository.repo_generated_pyspark_v2"
    )
    .orderBy("created_time", ascending=False)
    .first()
)

QUESTION = code_row["question"]

SOURCE_ANALYSIS = source_row["analysis"]
LOGIC_ANALYSIS = logic_row["analysis"]
WORKFLOW_ANALYSIS = workflow_row["analysis"]
GENERATED_CODE = code_row["pyspark_code"]

# ----------------------------------------------------------
# PROMPT
# ----------------------------------------------------------

PROMPT = f"""
You are a Senior QA Architect.

Generate complete migration test cases.

SOURCE ANALYSIS:
{SOURCE_ANALYSIS}

LOGIC ANALYSIS:
{LOGIC_ANALYSIS}

WORKFLOW ANALYSIS:
{WORKFLOW_ANALYSIS}

GENERATED PYSPARK:
{GENERATED_CODE}

Generate:

1. Functional Test Cases
2. Business Rule Test Cases
3. Data Validation Test Cases
4. Negative Test Cases
5. Reconciliation Test Cases
6. Duplicate Data Test Cases
7. SCD Type 2 Test Cases
8. Performance Test Cases
9. Recovery Test Cases
10. End-to-End Workflow Test Cases

For each test case provide:

- Test Case ID
- Test Scenario
- Test Steps
- Expected Result
- Priority

Return professional QA documentation.
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
            "You are a principal QA and migration testing architect."
        ),

        ChatMessage(
            role=ChatMessageRole.USER,
            content=PROMPT
        )

    ],

    max_tokens=3000

)

# ----------------------------------------------------------
# PARSE RESPONSE
# ----------------------------------------------------------

# ----------------------------------------------------------
# PARSE RESPONSE
# ----------------------------------------------------------

test_cases = ""

try:

    content = (
        response.choices[0]
        .message.content
    )

    if isinstance(content, list):

        for item in content:

            try:

                if item.get("type") == "text":

                    test_cases += item.get(
                        "text",
                        ""
                    )

            except:

                test_cases += str(item)

    else:

        test_cases = str(content)

except Exception:

    test_cases = str(response)

# ----------------------------------------------------------
# DISPLAY
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("GENERATED TEST CASES")
print("=" * 60)

print(test_cases)

# ----------------------------------------------------------
# SAVE
# ----------------------------------------------------------

test_df = spark.createDataFrame(

    [

        Row(

            artifact_name=code_row["artifact_name"],

            question=QUESTION,

            test_cases=test_cases,

            created_time=datetime.now()

        )

    ]

)

test_df.write \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "migration.repository.repo_test_cases"
    )

print("\nTEST CASES SAVED")

spark.sql("""

SELECT
artifact_name,
created_time
FROM migration.repository.repo_test_cases

""").show(truncate=False)

print("=" * 60)
print("TEST CASE GENERATOR COMPLETED")
print("=" * 60)
