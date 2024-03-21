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


class Movie(Base):
    __tablename__ = "movies"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column()
    title_vector = mapped_column(Vector(1536))  # ada-002 is 1536-dimensional


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
        Movie.title_vector,
        postgresql_using="hnsw",
        postgresql_with={"m": 16, "ef_construction": 64},
        postgresql_ops={"title_vector": "vector_cosine_ops"},
    )

    # Create the HNSW index
    index.create(engine)

    # Insert the movies from the JSON file
    current_directory = Path(__file__).parent
    data_path = current_directory / "movies_ada002.json"
    with open(data_path) as f:
        movies = json.load(f)
        for title, title_vector in movies.items():
            movie = Movie(title=title, title_vector=title_vector)
            session.add(movie)
        session.commit()

    # Query for target movie, the one whose title matches "Winnie the Pooh"
    query = select(Movie).where(Movie.title == "Winnie the Pooh")
    target_movie = session.execute(query).scalars().first()
    if target_movie is None:
        print("Movie not found")
        exit(1)

    # Find the 5 most similar movies to "Winnie the Pooh"
    most_similars = session.scalars(
        select(Movie).order_by(Movie.title_vector.cosine_distance(target_movie.title_vector)).limit(5)
    )
    print(f"Five most similar movies to '{target_movie.title}':")
    for movie in most_similars:
        print(f"\t{movie.title}")
