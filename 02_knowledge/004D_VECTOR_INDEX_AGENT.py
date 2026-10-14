# ==========================================================
# 005D_VECTOR_INDEX_AGENT.py
# Databricks AI Search Index Creator
# ==========================================================

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import *

print("=" * 60)
print("VECTOR INDEX AGENT STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

ENDPOINT_NAME = "codemod_poc_vs_endpoint"

INDEX_NAME = (
    "migration.repository.migration_knowledge_index"
)

SOURCE_TABLE = (
    "migration.repository.repo_knowledge_embeddings"
)

PRIMARY_KEY = "chunk_id"

# ----------------------------------------------------------
# CONNECT
# ----------------------------------------------------------

w = WorkspaceClient()

print(f"Endpoint : {ENDPOINT_NAME}")
print(f"Index    : {INDEX_NAME}")
print(f"Table    : {SOURCE_TABLE}")

# ----------------------------------------------------------
# CHECK EXISTING INDEX
# ----------------------------------------------------------

existing_indexes = []

for idx in w.vector_search_indexes.list_indexes(
    endpoint_name=ENDPOINT_NAME
):
    existing_indexes.append(idx.name)

print("\nExisting Indexes:")

for idx in existing_indexes:
    print(f" - {idx}")

# ----------------------------------------------------------
# CREATE INDEX
# ----------------------------------------------------------

if INDEX_NAME in existing_indexes:

    print(
        f"\nIndex Already Exists : {INDEX_NAME}"
    )

else:

    print(
        f"\nCreating Index : {INDEX_NAME}"
    )

    response = w.vector_search_indexes.create_index(

        name=INDEX_NAME,

        endpoint_name=ENDPOINT_NAME,

        primary_key=PRIMARY_KEY,

        index_type=VectorIndexType.DELTA_SYNC,

        delta_sync_index_spec=
        DeltaSyncVectorIndexSpecRequest(

            source_table=SOURCE_TABLE,

            pipeline_type=
            PipelineType.TRIGGERED,

            embedding_vector_columns=[

                EmbeddingVectorColumn(
                    name="embedding",
                    embedding_dimension=1024
                )

            ],

            columns_to_sync=[

                "chunk_id",
                "knowledge_id",
                "artifact_name",
                "artifact_type",
                "knowledge_type",
                "source_file",
                "chunk_number",
                "chunk_text"

            ]
        )
    )

    print("\nIndex Creation Submitted")
    print(response)

# ----------------------------------------------------------
# VERIFY INDEX
# ----------------------------------------------------------

print("\nVerifying Index...")

try:

    index_info = w.vector_search_indexes.get_index(
        index_name=INDEX_NAME
    )

    print(index_info)

except Exception as e:

    print("Verification Failed")
    print(str(e))

# ----------------------------------------------------------
# COMPLETE
# ----------------------------------------------------------

print("\n" + "=" * 60)
print("VECTOR INDEX AGENT COMPLETED")
print("=" * 60)