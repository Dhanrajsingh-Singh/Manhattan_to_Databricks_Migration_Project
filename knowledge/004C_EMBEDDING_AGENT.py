# ==========================================================
# 005C_EMBEDDING_AGENT.py
# Endpoint Based Embedding Agent
# ==========================================================

from databricks.sdk import WorkspaceClient
from pyspark.sql import Row
from pyspark.sql.types import *
from datetime import datetime

print("=" * 60)
print("EMBEDDING AGENT STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIG
# ----------------------------------------------------------

SOURCE_TABLE = (
    "migration.repository.repo_knowledge_chunks"
)

TARGET_TABLE = (
    "migration.repository.repo_knowledge_embeddings"
)

EMBEDDING_ENDPOINT = (
    "databricks-gte-large-en"
)

# ----------------------------------------------------------
# LOAD CHUNKS
# ----------------------------------------------------------

chunks_df = spark.table(
    SOURCE_TABLE
)

chunk_count = chunks_df.count()

print(
    f"Knowledge Chunks : {chunk_count}"
)

# ----------------------------------------------------------
# DATABRICKS CLIENT
# ----------------------------------------------------------

w = WorkspaceClient()

# ----------------------------------------------------------
# EMBEDDING GENERATION
# ----------------------------------------------------------

embedding_rows = []

processed = 0

for row in chunks_df.collect():

    chunk_text = str(row["chunk_text"])

    try:

        response = w.serving_endpoints.query(

            name=EMBEDDING_ENDPOINT,

            input=chunk_text

        )

        embedding = (
            response.data[0].embedding
        )

        embedding_rows.append(

            Row(

                chunk_id=row["chunk_id"],

                knowledge_id=row["knowledge_id"],

                artifact_name=row["artifact_name"],

                artifact_type=row["artifact_type"],

                knowledge_type=row["knowledge_type"],

                source_file=row["source_file"],

                chunk_number=row["chunk_number"],

                chunk_text=chunk_text,

                embedding=embedding,

                created_time=str(
                    datetime.now()
                )

            )

        )

        processed += 1

        print(
            f"Embedded : "
            f"{row['chunk_id']}"
        )

    except Exception as e:

        print(
            f"Embedding Failed : "
            f"{row['chunk_id']} : "
            f"{str(e)}"
        )

# ----------------------------------------------------------
# SCHEMA
# ----------------------------------------------------------

schema = StructType([

    StructField(
        "chunk_id",
        StringType(),
        True
    ),

    StructField(
        "knowledge_id",
        StringType(),
        True
    ),

    StructField(
        "artifact_name",
        StringType(),
        True
    ),

    StructField(
        "artifact_type",
        StringType(),
        True
    ),

    StructField(
        "knowledge_type",
        StringType(),
        True
    ),

    StructField(
        "source_file",
        StringType(),
        True
    ),

    StructField(
        "chunk_number",
        IntegerType(),
        True
    ),

    StructField(
        "chunk_text",
        StringType(),
        True
    ),

    StructField(
        "embedding",
        ArrayType(
            DoubleType()
        ),
        True
    ),

    StructField(
        "created_time",
        StringType(),
        True
    )

])

# ----------------------------------------------------------
# CREATE DATAFRAME
# ----------------------------------------------------------

embeddings_df = spark.createDataFrame(

    embedding_rows,

    schema=schema

)

# ----------------------------------------------------------
# SAVE TABLE
# ----------------------------------------------------------

spark.sql(f"""

DROP TABLE IF EXISTS
{TARGET_TABLE}

""")

embeddings_df.write \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        TARGET_TABLE
    )

# ----------------------------------------------------------
# ENABLE VECTOR SEARCH FEATURES
# ----------------------------------------------------------

spark.sql(f"""

ALTER TABLE {TARGET_TABLE}

SET TBLPROPERTIES (

    'delta.enableRowTracking'='true',

    'delta.enableChangeDataFeed'='true'

)

""")

# ----------------------------------------------------------
# VALIDATION
# ----------------------------------------------------------

print(
    f"\nEmbeddings Generated : "
    f"{processed}"
)

spark.sql(f"""

SELECT
    chunk_id,
    size(embedding) AS embedding_dimension
FROM {TARGET_TABLE}
LIMIT 5

""").show(truncate=False)

print("=" * 60)
print("EMBEDDING AGENT COMPLETED")
print("=" * 60)