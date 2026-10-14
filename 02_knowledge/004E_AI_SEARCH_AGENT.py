# ==========================================================
# 005E_AI_SEARCH_AGENT.py
# Hybrid AI Search Agent
# Databricks Serverless Compatible
# ==========================================================

from databricks.sdk import WorkspaceClient

print("=" * 60)
print("AI SEARCH AGENT STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

INDEX_NAME = (
    "migration.repository.migration_knowledge_index"
)

EMBEDDING_ENDPOINT = (
    "databricks-gte-large-en"
)

TOP_K = 5

QUESTION = (
    "How is Daily Order Workflow executed?"
)

print(f"\nQUESTION : {QUESTION}")

# ----------------------------------------------------------
# DATABRICKS CLIENT
# ----------------------------------------------------------

w = WorkspaceClient()

# ----------------------------------------------------------
# STEP 1 : STRUCTURED SEARCH
# ----------------------------------------------------------

print("\nSTEP 1 : STRUCTURED SEARCH")

structured_context = []

metadata_tables = [

    "repo_workflows",
    "repo_dependencies",
    "repo_tables",
    "repo_metadata_summary"

]

for table_name in metadata_tables:

    try:

        df = spark.sql(f"""
            SELECT *
            FROM migration.repository.{table_name}
        """)

        structured_context.append(
            f"\n=== {table_name.upper()} ===\n"
        )

        for row in df.limit(20).collect():

            structured_context.append(
                str(row)
            )

    except Exception as e:

        structured_context.append(
            f"{table_name} Failed : {str(e)}"
        )

# ----------------------------------------------------------
# STEP 2 : INDEX STATUS
# ----------------------------------------------------------

print("\nSTEP 2 : VECTOR INDEX STATUS")

semantic_context = []

index_info = w.vector_search_indexes.get_index(
    index_name=INDEX_NAME
)

print(
    f"Ready : "
    f"{index_info.status.ready}"
)

print(
    f"Indexed Rows : "
    f"{index_info.status.indexed_row_count}"
)

semantic_context.append(

    f"""
INDEX STATUS

ready:
{index_info.status.ready}

indexed_rows:
{index_info.status.indexed_row_count}
"""
)

# ----------------------------------------------------------
# STEP 3 : QUERY EMBEDDING
# ----------------------------------------------------------

print("\nSTEP 3 : QUERY EMBEDDING")

response = w.serving_endpoints.query(

    name=EMBEDDING_ENDPOINT,

    input=QUESTION

)

query_vector = (
    response.data[0].embedding
)

print(
    f"Embedding Dimension : "
    f"{len(query_vector)}"
)

# ----------------------------------------------------------
# STEP 4 : SEMANTIC SEARCH
# ----------------------------------------------------------

print("\nSTEP 4 : SEMANTIC SEARCH")

try:

    results = (

        w.vector_search_indexes.query_index(

            index_name=INDEX_NAME,

            query_vector=query_vector,

            columns=[

                "chunk_id",
                "knowledge_id",
                "artifact_name",
                "artifact_type",
                "knowledge_type",
                "chunk_text"

            ],

            num_results=TOP_K

        )

    )

    semantic_context.append(
        "\n=== SEMANTIC SEARCH RESULTS ===\n"
    )

    for row in results.result.data_array:
        semantic_context.append(
            f"""
            Chunk ID       : {row[0]}
            Knowledge ID   : {row[1]}
            Artifact Name  : {row[2]}
            Artifact Type  : {row[3]}
            Knowledge Type : {row[4]}

            Content:
            {row[5]}
            """
        )

    print(
        "Semantic Search Successful"
    )

except Exception as e:

    semantic_context.append(
        f"""
Semantic Search Failed

{str(e)}
"""
    )

# ----------------------------------------------------------
# STEP 5 : CONTEXT BUILDING
# ----------------------------------------------------------

print("\nSTEP 5 : CONTEXT BUILDING")

context = []

context.append(

    f"""
QUESTION

{QUESTION}
"""
)

context.append(

"""
==================================================
STRUCTURED CONTEXT
==================================================
"""
)

context.extend(
    structured_context
)

context.append(

"""
==================================================
SEMANTIC CONTEXT
==================================================
"""
)

context.extend(
    semantic_context
)

final_context = "\n".join(
    context
)

# ----------------------------------------------------------
# DISPLAY CONTEXT
# ----------------------------------------------------------

print("\nFINAL CONTEXT")

print(
    final_context[:10000]
)

# ----------------------------------------------------------
# STEP 6 : SAVE CONTEXT
# ----------------------------------------------------------

print("\nSTEP 6 : SAVE CONTEXT")

context_df = spark.createDataFrame(

    [
        (
            QUESTION,
            final_context
        )
    ],

    [
        "question",
        "context"
    ]

)

spark.sql("""

DROP TABLE IF EXISTS
migration.repository.repo_search_context

""")

context_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "migration.repository.repo_search_context"
    )

print(
    "Context Saved Successfully"
)

print(
    "Table : migration.repository.repo_search_context"
)

# ----------------------------------------------------------
# COMPLETE
# ----------------------------------------------------------

print("\n" + "=" * 60)

print(
    "AI SEARCH AGENT COMPLETED"
)

print("=" * 60)