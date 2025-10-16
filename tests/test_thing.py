import asyncpg
import pytest
from pytest_databases.docker.postgres import PostgresService

async def test_connection_with_service_details(postgres_service: PostgresService) -> None:
    conn_str = (
        f"postgresql://{postgres_service.user}:{postgres_service.password}@"
        f"{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )
    conn = await asyncpg.connect(conn_str)
    async with conn.transaction():
        assert await conn.fetchrow("SELECT 1") == (1,)


async def test_with_direct_connection(postgres_connection) -> None:
   # postgres_connection is often a configured client or connection object
   with postgres_connection.cursor() as cursor:
       cursor.execute("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT);")
       cursor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');")
       cursor.execute("SELECT name FROM users WHERE id = 1;")
       assert cursor.fetchone() == ('Alice',)
