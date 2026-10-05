# ==========================================================
# 005B_CHUNKING_AGENT.py
# Knowledge Chunking Agent
# ==========================================================

from pyspark.sql import Row
from datetime import datetime
import textwrap 

print("=" * 60)
print("KNOWLEDGE CHUNKING AGENT STARTED")
print("=" * 60)

# ----------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------

CHUNK_SIZE = 500

OVERLAP_SIZE = 100

# ----------------------------------------------------------
# READ KNOWLEDGE REPOSITORY
# ----------------------------------------------------------

knowledge_df = spark.table(
    "migration.repository.repo_knowledge"
)

print(
    f"Knowledge Records Found : "
    f"{knowledge_df.count()}"
)

display(knowledge_df)

# ----------------------------------------------------------
# CHUNK FUNCTION
# ----------------------------------------------------------

def create_chunks(
    text,
    chunk_size=500,
    overlap=100
):

    if text is None:

        return []

    text = str(text)

    if len(text.strip()) == 0:

        return []

    chunks = []

    start = 0

    while start < len(text):

        end = start + chunk_size

        chunk = text[start:end]

        chunks.append(chunk)

        start += (
            chunk_size - overlap
        )

    return chunks

# ----------------------------------------------------------
# PROCESS KNOWLEDGE RECORDS
# ----------------------------------------------------------

chunk_records = []

chunk_counter = 1

for row in knowledge_df.collect():

    content = row["content"]

    chunks = create_chunks(

        text=content,

        chunk_size=CHUNK_SIZE,

        overlap=OVERLAP_SIZE
    )

    for chunk_number, chunk in enumerate(
        chunks,
        start=1
    ):

        chunk_records.append(

            Row(

                chunk_id=f"KC{chunk_counter:05}",

                knowledge_id=row[
                    "knowledge_id"
                ],

                artifact_name=row[
                    "artifact_name"
                ],

                artifact_type=row[
                    "artifact_type"
                ],

                knowledge_type=row[
                    "knowledge_type"
                ],

                source_file=row[
                    "source_file"
                ],

                chunk_number=chunk_number,

                chunk_text=chunk,

                chunk_length=len(chunk),

                created_time=str(
                    datetime.now()
                )

            )
        )

        chunk_counter += 1

# ----------------------------------------------------------
# VALIDATION
# ----------------------------------------------------------

print(
    f"Total Chunks Created : "
    f"{len(chunk_records)}"
)

if len(chunk_records) == 0:

    raise Exception(
        "No knowledge chunks generated."
    )

# ----------------------------------------------------------
# CREATE SCHEMA
# ----------------------------------------------------------

from pyspark.sql.types import *

chunk_schema = StructType([

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
        "chunk_length",
        IntegerType(),
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

chunks_df = spark.createDataFrame(
    chunk_records,
    schema=chunk_schema
)

display(chunks_df)

# ----------------------------------------------------------
# DROP OLD TABLE
# ----------------------------------------------------------

spark.sql("""

DROP TABLE IF EXISTS
migration.repository.repo_knowledge_chunks

""")

# ----------------------------------------------------------
# SAVE CHUNKS
# ----------------------------------------------------------

chunks_df.write \
    .mode("overwrite") \
    .option(
        "overwriteSchema",
        "true"
    ) \
    .saveAsTable(
        "migration.repository.repo_knowledge_chunks"
    )

print(
    "repo_knowledge_chunks created successfully"
)

# ----------------------------------------------------------
# SUMMARY
# ----------------------------------------------------------

print("\nCHUNK SUMMARY")

spark.sql("""

SELECT

    knowledge_type,

    COUNT(*) AS total_chunks

FROM migration.repository.repo_knowledge_chunks

GROUP BY knowledge_type

ORDER BY total_chunks DESC

""").show()

# ----------------------------------------------------------
# SAMPLE CHUNKS
# ----------------------------------------------------------

display(

    spark.sql("""

    SELECT

        chunk_id,
        knowledge_id,
        knowledge_type,
        LEFT(chunk_text,100) AS preview

    FROM migration.repository.repo_knowledge_chunks

    ORDER BY chunk_id

    """)

)

print("=" * 60)
print("KNOWLEDGE CHUNKING COMPLETED")
print("=" * 60)