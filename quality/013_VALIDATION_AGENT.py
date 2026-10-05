# ==========================================================
# 013_VALIDATION_AGENT.py
# Final Migration Validation Agent
# ==========================================================

from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ChatMessage,
    ChatMessageRole
)

from pyspark.sql import Row

print("=" * 60)
print("FINAL VALIDATION AGENT STARTED")
print("=" * 60)

LLM_ENDPOINT = "databricks-gpt-oss-120b"

# ----------------------------------------------------------
# LOAD INPUTS
# ----------------------------------------------------------

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

review_row = (
    spark.table(
        "migration.repository.repo_code_review"
    )
    .orderBy("created_time", ascending=False)
    .first()
)

test_row = (
    spark.table(
        "migration.repository.repo_test_cases"
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

LOGIC_ANALYSIS = logic_row["analysis"]

WORKFLOW_ANALYSIS = workflow_row["analysis"]

CODE_REVIEW = review_row["review_comments"]

REVIEW_SCORE = review_row["review_score"]

TEST_CASES = test_row["test_cases"]

GENERATED_CODE = code_row["pyspark_code"]

# ----------------------------------------------------------
# TECHNICAL COVERAGE
# ----------------------------------------------------------

technical_checks = [

    "spark.read",
    ".write",
    ".save",
    "broadcast(",
    "merge",
    "logger",
    "cache(",
    "partitionBy",
    "cloudFiles"

]

technical_found = sum(
    1
    for check in technical_checks
    if check.lower() in GENERATED_CODE.lower()
)

technical_coverage = round(

    (
        technical_found /
        len(technical_checks)
    ) * 100,

    2

)

# ----------------------------------------------------------
# BUSINESS RULE COVERAGE
# ----------------------------------------------------------

business_checks = [

    "tax",
    "current_flag",
    "valid_to",
    "duplicate",
    "allocation",
    "customer"

]

business_found = sum(

    1
    for check in business_checks
    if check.lower() in GENERATED_CODE.lower()

)

business_rule_coverage = round(

    (
        business_found /
        len(business_checks)
    ) * 100,

    2

)

# ----------------------------------------------------------
# WORKFLOW COVERAGE
# ----------------------------------------------------------

workflow_checks = [

    "extract",
    "transform",
    "load",
    "archive",
    "validation"

]

workflow_found = sum(

    1
    for check in workflow_checks
    if check.lower() in GENERATED_CODE.lower()

)

workflow_coverage = round(

    (
        workflow_found /
        len(workflow_checks)
    ) * 100,

    2

)

# ----------------------------------------------------------
# TEST COVERAGE
# ----------------------------------------------------------

test_checks = [

    "functional",
    "business",
    "validation",
    "negative",
    "reconciliation",
    "duplicate",
    "scd"

]

test_found = sum(

    1
    for check in test_checks
    if check.lower() in TEST_CASES.lower()

)

test_coverage = round(

    (
        test_found /
        len(test_checks)
    ) * 100,

    2

)

# ----------------------------------------------------------
# OVERALL READINESS
# ----------------------------------------------------------

overall_readiness = round(

(
    technical_coverage +
    business_rule_coverage +
    workflow_coverage +
    test_coverage +
    REVIEW_SCORE

) / 5,

2
)

# ----------------------------------------------------------
# DISPLAY METRICS
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("MIGRATION READINESS METRICS")
print("=" * 60)

print(
    f"Technical Coverage      : {technical_coverage}%"
)

print(
    f"Business Rule Coverage  : {business_rule_coverage}%"
)

print(
    f"Workflow Coverage       : {workflow_coverage}%"
)

print(
    f"Test Coverage           : {test_coverage}%"
)

print(
    f"Overall Readiness       : {overall_readiness}%"
)

# ----------------------------------------------------------
# LLM VALIDATION REPORT
# ----------------------------------------------------------

PROMPT = f"""
You are a Principal Migration Validation Architect.

Assess migration readiness.

LOGIC ANALYSIS:
{LOGIC_ANALYSIS}

WORKFLOW ANALYSIS:
{WORKFLOW_ANALYSIS}

CODE REVIEW:
{CODE_REVIEW}

TEST CASES:
{TEST_CASES}

GENERATED PYSPARK:
{GENERATED_CODE}

Current scores:

Technical Coverage:
{technical_coverage}

Business Rule Coverage:
{business_rule_coverage}

Workflow Coverage:
{workflow_coverage}

Test Coverage:
{test_coverage}

Provide:

1. Readiness Assessment
2. Remaining Risks
3. Missing Features
4. UAT Readiness
5. SIT Readiness
6. Production Readiness
7. Final Recommendation
"""

w = WorkspaceClient()

response = w.serving_endpoints.query(

    name=LLM_ENDPOINT,

    messages=[

        ChatMessage(
            role=ChatMessageRole.SYSTEM,
            content=
            "You are a senior migration validation specialist."
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

validation_report = ""

try:

    content = (
        response.choices[0]
        .message.content
    )

    if isinstance(content, list):

        for item in content:

            try:

                if item.get("type") == "text":

                    validation_report += item.get(
                        "text",
                        ""
                    )

            except:

                validation_report += str(item)

    else:

        validation_report = str(content)

except Exception:

    validation_report = str(response)

# ----------------------------------------------------------
# APPEND METRICS
# ----------------------------------------------------------

validation_report = f"""

==================================================

Technical Coverage     : {technical_coverage}%

Business Coverage      : {business_rule_coverage}%

Workflow Coverage      : {workflow_coverage}%

Review Score          : {REVIEW_SCORE}%

Test Coverage          : {test_coverage}%

Overall Readiness      : {overall_readiness}%

==================================================

{validation_report}

"""

# ----------------------------------------------------------
# DISPLAY REPORT
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("FINAL VALIDATION REPORT")
print("=" * 60)

print(validation_report)

# ----------------------------------------------------------
# SAVE REPORT
# ----------------------------------------------------------

validation_df = spark.createDataFrame(

    [

        Row(

            artifact_name=code_row["artifact_name"],

            question=QUESTION,

            review_score=REVIEW_SCORE,

            business_rule_coverage_pct=business_rule_coverage,

            workflow_coverage_pct=workflow_coverage,

            technical_coverage_pct=technical_coverage,

            test_coverage_pct=test_coverage,

            overall_readiness_pct=overall_readiness,

            validation_report=validation_report,

            created_time=datetime.now()

        )

    ]

)

validation_df.write \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "migration.repository.repo_validation_results_v2"
    )

print("\nVALIDATION RESULTS SAVED")

spark.sql("""

SELECT
artifact_name,
review_score,
technical_coverage_pct,
business_rule_coverage_pct,
workflow_coverage_pct,
test_coverage_pct,
overall_readiness_pct
FROM migration.repository.repo_validation_results_v2

""").show(truncate=False)

print("=" * 60)
print("FINAL VALIDATION AGENT COMPLETED")
print("=" * 60)
