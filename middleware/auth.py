from typing import TYPE_CHECKING

import msgspec
from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException
from litestar.middleware.authentication import AbstractAuthenticationMiddleware, AuthenticationResult

if TYPE_CHECKING:
    from asyncpg import Pool


class AuthUser(msgspec.Struct):
    id: int
    username: str
    info: str | None


class AuthToken(msgspec.Struct):
    api_key: str


class CustomAuthenticationMiddleware(AbstractAuthenticationMiddleware):
    async def authenticate_request(self, connection: ASGIConnection) -> AuthenticationResult:
        """Authenticate request."""
        conn: Pool = connection.app.state["db_pool"]
        api_key = connection.headers.get("X-API-KEY")

        if not api_key:
            raise NotAuthorizedException("Missing API key")

        query = """
            SELECT u.id, u.username, u.info, t.api_key
            FROM public.api_tokens t
            JOIN public.auth_users u ON t.user_id = u.id
            WHERE t.api_key = $1
        """

        row = await conn.fetchrow(query, api_key)

        if not row:
            raise NotAuthorizedException("Invalid API key")

        user = AuthUser(id=row["id"], username=row["username"], info=row["info"])
        token = AuthToken(api_key=row["api_key"])
        return AuthenticationResult(user=user, auth=token)
