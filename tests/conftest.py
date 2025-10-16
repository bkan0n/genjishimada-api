import asyncio
import glob
import os
from typing import Any, AsyncIterator, Generator
import asyncpg
from litestar import Litestar
from litestar.testing import AsyncTestClient
import pytest
from pytest_databases.docker.postgres import PostgresService

from app import create_app
pytest_plugins = [
    "pytest_databases.docker.postgres",
]

MIGRATIONS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "migrations"))

SEEDS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "seeds"))


def _apply_sql_dir(conn, directory: str) -> None:
    for path in sorted(glob.glob(os.path.join(directory, "*.sql"))):
        with open(path, "r", encoding="utf-8") as f:
            sql_text = f.read()
        conn.execute(sql_text, prepare=False)
    conn.commit()

@pytest.fixture(scope="session", autouse=True)
def setup_test_db(postgres_connection) -> Generator[None, Any, None]:

    _apply_sql_dir(postgres_connection, MIGRATIONS_DIR)
    _apply_sql_dir(postgres_connection, SEEDS_DIR)
    yield

@pytest.fixture(scope="function", autouse=False)
async def asyncpg_conn(postgres_service: PostgresService):
    conn = await asyncpg.connect(
        user=postgres_service.user,
        password=postgres_service.password,
        host=postgres_service.host,
        port=postgres_service.port,
        database=postgres_service.database
    )
    yield conn
    await conn.close()

@pytest.fixture
async def test_client(postgres_service: PostgresService) -> AsyncIterator[AsyncTestClient[Litestar]]:
    app = create_app(psql_dsn=f"postgresql://{postgres_service.user}:{postgres_service.password}@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}")
    async with AsyncTestClient(app=app) as client:
        client.headers.update({"x-pytest-enabled": "1", "X-API-KEY": "testing"},)
        yield client
