import os

import numpy as np
import psycopg2
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
from pgvector.psycopg2 import register_vector

# Connect to the database based on environment variables
load_dotenv(".env", override=True)
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_USERNAME = os.environ["POSTGRES_USERNAME"]
POSTGRES_DATABASE = os.environ["POSTGRES_DATABASE"]

if POSTGRES_HOST.endswith(".database.azure.com"):
    print("Authenticating to Azure Database for PostgreSQL using Azure Identity...")
    azure_credential = DefaultAzureCredential()
    token = azure_credential.get_token("https://ossrdbms-aad.database.windows.net/.default")
    POSTGRES_PASSWORD = token.token
else:
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

extra_params = {}
if POSTGRES_SSL := os.environ.get("POSTGRES_SSL"):
    extra_params["sslmode"] = POSTGRES_SSL

conn = psycopg2.connect(
    database=POSTGRES_DATABASE,
    user=POSTGRES_USERNAME,
    password=POSTGRES_PASSWORD,
    host=POSTGRES_HOST,
    **extra_params,
)

conn.autocommit = True
cur = conn.cursor()
# Create pgvector extension
cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
# Drop table defined in this model from the database, if already exists
cur.execute("DROP TABLE IF EXISTS items")
# Create table defined in this model in the database
cur.execute("CREATE TABLE items (id bigserial PRIMARY KEY, embedding vector(3))")
register_vector(conn)

# Define HNSW index to support vector similarity search through the vector_l2_ops access method (Euclidean distance). The SQL operator for Euclidean distance is written as <->.
cur.execute("CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)")

# Insert three vectors as three separate rows in the items table
embeddings = [
    np.array([1, 2, 3]),
    np.array([-1, 1, 3]),
    np.array([0, -1, -2]),
]
for embedding in embeddings:
    cur.execute("INSERT INTO items (embedding) VALUES (%s)", (embedding,))


# Find all vectors in table items
cur.execute("SELECT * FROM items")
all_items = cur.fetchall()
print("All vectors in table items:")
for item in all_items:
    print(f"\t{item[1]}")

# Find 2 closest vectors to [3, 1, 2]
embedding_predicate = np.array([3, 1, 2])
cur.execute("SELECT * FROM items ORDER BY embedding <-> %s LIMIT 2", (embedding_predicate,))
closest_items = cur.fetchall()
print("Two closest vectors to [3, 1, 2] in table items:")
for item in closest_items:
    print(f"\t{item[1]}")

# Calculate distance between [3, 1, 2] and the first vector
cur.execute(
    "SELECT embedding <-> %s AS distance FROM items ORDER BY embedding <-> %s LIMIT 1",
    (embedding_predicate, embedding_predicate),
)
distance = cur.fetchone()
print(f"Distance between [3, 1, 2] vector and the one closest to it: {distance[0]}")

# Find vectors within distance 5 from [3, 1, 2]
cur.execute("SELECT * FROM items WHERE embedding <-> %s < 5", (embedding_predicate,))
close_enough_items = cur.fetchall()
print("Vectors within a distance of 5 from [3, 1, 2]:")
for item in close_enough_items:
    print(f"\t{item[1]}")

# Calculate average of all vectors
cur.execute("SELECT avg(embedding) FROM items")
avg_embedding = cur.fetchone()
print(f"Average of all vectors: {avg_embedding}")

cur.close()
