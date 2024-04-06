import json
import os
from pathlib import Path

from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
from pgvector.sqlalchemy import Vector
from sqlalchemy import Index, create_engine, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


# Define the models
class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "items"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    type: Mapped[str] = mapped_column()
    brand: Mapped[str] = mapped_column()
    name: Mapped[str] = mapped_column()
    description: Mapped[str] = mapped_column()
    price: Mapped[float] = mapped_column()
    embedding = mapped_column(Vector(1536))


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
    # Define HNSW index to support vector similarity search through the vector_cosine_ops access method (cosine distance). The SQL operator for cosine distance is written as <=>.
    index = Index(
        "hnsw_index_for_cosine_distance_similarity_search",
        Item.embedding,
        postgresql_using="hnsw",
        postgresql_with={"m": 16, "ef_construction": 64},
        postgresql_ops={"embedding": "vector_cosine_ops"},
    )

    # Create the HNSW index
    index.drop(engine, checkfirst=True)
    index.create(engine)

    # Insert the movies from the JSON file
    current_directory = Path(__file__).parent
    data_path = current_directory / "catalog.json"
    with open(data_path) as f:
        catalog_items = json.load(f)
        for catalog_item in catalog_items:
            item = Item(
                id=catalog_item["Id"],
                type=catalog_item["Type"],
                brand=catalog_item["Brand"],
                name=catalog_item["Name"],
                description=catalog_item["Description"],
                price=catalog_item["Price"],
                embedding=catalog_item["Embedding"],
            )
            session.add(item)
        session.commit()

    # Query for target movie, the one whose title matches "Winnie the Pooh"
    query = select(Item).where(Item.name == "LumenHead Headlamp")
    target_item = session.execute(query).scalars().first()
    if target_item is None:
        print("Movie not found")
        exit(1)

    # Find the 5 most similar movies to "Winnie the Pooh"
    most_similars = session.scalars(
        select(Item).order_by(Item.embedding.cosine_distance(target_item.embedding)).limit(5)
    )
    print(f"Five most similar items to '{target_item.name}':")
    for item in most_similars:
        print(f"\t{item.name}")
