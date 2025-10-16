import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aio_pika
import litestar
import sentry_sdk
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool
from asyncpg import Connection
from litestar import Litestar, Request, Response, get
from litestar.exceptions import HTTPException
from litestar.logging.config import LoggingConfig
from litestar.middleware import DefineMiddleware
from litestar.openapi.config import OpenAPIConfig
from litestar.openapi.plugins import ScalarRenderPlugin
from litestar.openapi.spec import Server
from litestar.static_files.config import create_static_files_router
from litestar.status_codes import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_503_SERVICE_UNAVAILABLE
from litestar_asyncpg import AsyncpgConfig, AsyncpgConnection, AsyncpgPlugin, PoolConfig

from middleware.auth import CustomAuthenticationMiddleware
from routes import route_handlers
from utilities.errors import CustomHTTPException

DEFAULT_DSN = os.getenv("DEFAULT_DSN")
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")

log = logging.getLogger(__name__)


@asynccontextmanager
async def rabbitmq_connection(_app: Litestar) -> AsyncGenerator[None, None]:
    """Connect to RabbitMQ."""
    _conn = getattr(_app.state, "rabbitmq_connection", None)
    if _conn is None:

        async def get_connection() -> AbstractRobustConnection:
            return await aio_pika.connect_robust(f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}/")

        connection_pool: Pool = Pool(get_connection, max_size=2)

        async def get_channel() -> aio_pika.Channel:
            async with connection_pool.acquire() as connection:
                return await connection.channel()

        channel_pool: Pool = Pool(get_channel, max_size=10)

        _app.state.mq_channel_pool = channel_pool
    yield


def default_exception_handler(_: Request, exc: Exception) -> Response:
    """Handle errors."""
    status_code = getattr(exc, "status_code", HTTP_500_INTERNAL_SERVER_ERROR)
    detail = getattr(exc, "detail", "")
    extra = getattr(exc, "extra", {})
    print({"error": detail, "extra": extra})
    return Response(content={"error": detail, "extra": extra}, status_code=status_code)


def internal_server_error_handler(_: Request, exc: Exception) -> Response:
    """Handle internal server errors."""
    return Response(content={"error": str(exc)}, status_code=500)


async def _async_pg_init(conn: AsyncpgConnection) -> None:
    await conn.set_type_codec("numeric", encoder=str, decoder=float, schema="pg_catalog", format="text")


def create_app(psql_dsn: str | None = None) -> Litestar:
    """Create and configure a Litestar application.

    This function initializes a Litestar application by setting up a database plugin,
    configuring API routing, defining OpenAPI documentation, and setting application-wide
    exception handlers. It supports an optional PostgreSQL DSN (Data Source Name) parameter
    to customize the database configuration.

    Args:
        psql_dsn (Optional[str]): A PostgreSQL DSN to configure the database connection. If not provided,
            the function will use the DSN from the environment variable `PSQL_DSN` or fallback to the
            default DSN defined by `DEFAULT_DSN`.

    Returns:
        Litestar: An instance of the configured Litestar application.

    """
    dsn = psql_dsn or DEFAULT_DSN
    assert dsn
    asyncpg = AsyncpgPlugin(
        config=AsyncpgConfig(
            pool_config=PoolConfig(dsn=dsn, init=_async_pg_init),
            connection_dependency_key="conn",
        ),
    )

    v3_router = litestar.Router("/api/v3", route_handlers=route_handlers)

    @get("/healthcheck", tags=["Utilities"])
    async def _health_check(conn: Connection) -> bool:
        try:
            await conn.fetchval("SELECT 1;")
            return True
        except Exception:
            raise CustomHTTPException(
                detail="Health check failed.",
                status_code=HTTP_503_SERVICE_UNAVAILABLE,
                headers={"Retry-After": "30"},
            )

    openapi_config = OpenAPIConfig(
        title="Genji Shimada API",
        description="REST API for Genji Shimada project.",
        version="0.0.1",
        render_plugins=[ScalarRenderPlugin()],
        path="/docs",
        servers=[
            Server(
                url="https://api.youngnebula.com"
                if os.getenv("API_ENVIRONMENT") == "development"
                else "https://api.genji.pk",
                description="Default server",
            )
        ],
    )

    logging_config = LoggingConfig(
        root={"level": "INFO", "handlers": ["queue_listener"]},
        formatters={"standard": {"format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}},
        log_exceptions="always",
        # disable_stack_trace={404, ValidationError, ValidationException, CustomHTTPException},
    )

    auth_middleware = DefineMiddleware(CustomAuthenticationMiddleware, exclude=["docs"])

    sentry_sdk.init(
        dsn=os.getenv("SENTRY_DSN"),
        send_default_pii=True,
        enable_logs=True,
        traces_sample_rate=1.0,
        profile_session_sample_rate=1.0,
        profile_lifecycle="trace",
        environment=os.getenv("API_ENVIRONMENT"),
    )

    _app = Litestar(
        plugins=[asyncpg],
        route_handlers=[
            _health_check,
            v3_router,
            create_static_files_router(
                path="/",
                directories=["html"],
                html_mode=True,
                opt={"exclude_from_auth": True},
            ),
        ],
        openapi_config=openapi_config,
        exception_handlers={
            HTTPException: default_exception_handler,
            HTTP_500_INTERNAL_SERVER_ERROR: internal_server_error_handler,
        },
        lifespan=[rabbitmq_connection],
        logging_config=logging_config,
        middleware=[auth_middleware],
    )

    return _app


app = create_app()

