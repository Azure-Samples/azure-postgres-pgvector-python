import os

from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
from pgvector.sqlalchemy import Vector
from sqlalchemy import Index, create_engine, func, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


# Define the models
class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "items"
    id: Mapped[int] = mapped_column(primary_key=True)
    embedding = mapped_column(Vector(3))


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

DATABASE_URI = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DATABASE}"
# Specify SSL mode if needed
if POSTGRES_SSL := os.environ.get("POSTGRES_SSL"):
    DATABASE_URI += f"?sslmode={POSTGRES_SSL}"

engine = create_engine(DATABASE_URI, echo=False)

# Create pgvector extension
with engine.begin() as conn:
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))

# Drop all tables defined in this model from the database, if they already exist
Base.metadata.drop_all(engine)
# Create all tables defined in this model in the database
Base.metadata.create_all(engine)

# Insert data and issue queries
with Session(engine) as session:
    # Define HNSW index to support vector similarity search through the vector_l2_ops access method (Euclidean distance). The SQL operator for Euclidean distance is written as <->.
    index = Index(
        "hnsw_index_for_euclidean_distance_similarity_search",
        Item.embedding,
        postgresql_using="hnsw",
        postgresql_with={"m": 16, "ef_construction": 64},
        postgresql_ops={"embedding": "vector_l2_ops"},
    )

    # Create the HNSW index
    index.create(engine)

    # Insert three vectors as three separate rows in the items table
    session.add_all(
        [
            Item(embedding=[1, 2, 3]),
            Item(embedding=[-1, 1, 3]),
            Item(embedding=[0, -1, -2]),
        ]
    )

    # Find all vectors in table items
    all_items = session.scalars(select(Item))
    print("All vectors in table items:")
    for item in all_items:
        print(f"\t{item.embedding}")

    # Find 2 closest vectors to [3, 1, 2]
    closest_items = session.scalars(select(Item).order_by(Item.embedding.l2_distance([3, 1, 2])).limit(2))
    print("Two closest vectors to [3, 1, 2] in table items:")
    for item in closest_items:
        print(f"\t{item.embedding}")

    # Calculate distance between [3, 1, 2] and the first vector
    distance = session.scalars(select(Item.embedding.l2_distance([3, 1, 2]))).first()
    print(f"Distance between [3, 1, 2] vector and the one closest to it: {distance}")

    # Find vectors within distance 5 from [3, 1, 2]
    close_enough_items = session.scalars(select(Item).filter(Item.embedding.l2_distance([3, 1, 2]) < 5))
    print("Vectors within a distance of 5 from [3, 1, 2]:")
    for item in close_enough_items:
        print(f"\t{item.embedding}")

    # Calculate average of all vectors
    avg_embedding = session.scalars(select(func.avg(Item.embedding))).first()
    print(f"Average of all vectors: {avg_embedding}")
