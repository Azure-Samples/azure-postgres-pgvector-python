from __future__ import annotations

import asyncio
import os

from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
from pgvector.sqlalchemy import Vector
from sqlalchemy import Index, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# Define the models
class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "items"
    id: Mapped[int] = mapped_column(primary_key=True)
    embedding = mapped_column(Vector(3))


# Define HNSW index to support vector similarity search through the vector_l2_ops access method (Euclidean distance). The SQL operator for Euclidean distance is written as <->.
index = Index(
    "hnsw_index_for_euclidean_distance_similarity_search",
    Item.embedding,
    postgresql_using="hnsw",
    postgresql_with={"m": 16, "ef_construction": 64},
    postgresql_ops={"embedding": "vector_l2_ops"},
)


async def insert_objects(async_session: async_sessionmaker[AsyncSession]) -> None:
    async with async_session() as session:
        async with session.begin():
            # Insert three vectors as three separate rows in the items table
            session.add_all(
                [
                    Item(embedding=[1, 2, 3]),
                    Item(embedding=[-1, 1, 3]),
                    Item(embedding=[0, -1, -2]),
                ]
            )


async def select_and_update_objects(
    async_session: async_sessionmaker[AsyncSession],
) -> None:
    async with async_session() as session:
        # Find 2 closest vectors to [3, 1, 2]
        closest_items = await session.scalars(select(Item).order_by(Item.embedding.l2_distance([3, 1, 2])).limit(2))
        print("Two closest vectors to [3, 1, 2] in table items:")
        for item in closest_items:
            print(f"\t{item.embedding}")

        # Calculate distance between [3, 1, 2] and the first vector
        distance = (await session.scalars(select(Item.embedding.l2_distance([3, 1, 2])))).first()
        print(f"Distance between [3, 1, 2] vector and the one closest to it: {distance}")

        # Find vectors within distance 5 from [3, 1, 2]
        close_enough_items = await session.scalars(select(Item).filter(Item.embedding.l2_distance([3, 1, 2]) < 5))
        print("Vectors within a distance of 5 from [3, 1, 2]:")
        for item in close_enough_items:
            print(f"\t{item.embedding}")

        # Calculate average of all vectors
        avg_embedding = (await session.scalars(select(func.avg(Item.embedding)))).first()
        print(f"Average of all vectors: {avg_embedding}")


async def async_main() -> None:
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

    DATABASE_URI = f"postgresql+asyncpg://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DATABASE}"
    # Specify SSL mode if needed
    if POSTGRES_SSL := os.environ.get("POSTGRES_SSL"):
        DATABASE_URI += f"?ssl={POSTGRES_SSL}"

    engine = create_async_engine(
        DATABASE_URI,
        echo=False,
    )

    # async_sessionmaker: a factory for new AsyncSession objects.
    # expire_on_commit - don't expire objects after transaction commit
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        # Create pgvector extension
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        # Drop all tables (and indexes) defined in this model from the database, if they already exist
        await conn.run_sync(Base.metadata.drop_all)
        # Create all tables (and indexes) defined in this model in the database
        await conn.run_sync(Base.metadata.create_all)

    await insert_objects(async_session)
    await select_and_update_objects(async_session)

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()


asyncio.run(async_main())
