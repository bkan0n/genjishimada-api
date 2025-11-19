from __future__ import annotations

import msgspec
from asyncpg import Connection
from genjipk_sdk.change_requests import ChangeRequestCreateRequest, ChangeRequestResponse, StaleChangeRequestResponse
from genjipk_sdk.maps import OverwatchCode
from litestar.datastructures import State
from litestar.di import Provide

from .base import BaseService


class ChangeRequestsService(BaseService):
    async def check_permission_for_view_buttons(self, thread_id: int, user_id: int, code: OverwatchCode) -> bool:
        """Check whether a user has permission to view creator-only buttons.

        Args:
            thread_id (int): The Discord thread ID tied to the change request.
            user_id (int): The ID of the user to check.
            code (OverwatchCode): The Overwatch map code.

        Returns:
            bool: True if the user is included in the creator mentions, False otherwise.

        """
        query = """
            SELECT creator_mentions FROM change_requests
            WHERE thread_id = $1 AND code = $2;
        """
        val = await self._conn.fetchval(query, thread_id, code)
        return str(user_id) in val

    async def create_change_request(self, data: ChangeRequestCreateRequest) -> None:
        """Create a new change request.

        Args:
            data (ChangeRequest): A change request containing thread ID, map code,
                user ID, content, change request type, and creator mentions.

        Returns:
            None

        """
        query = """
        INSERT INTO change_requests (
            thread_id,
            code,
            user_id,
            content,
            creator_mentions,
            change_request_type
        )
        SELECT $1, $2, $3, $4, $5, $6
        FROM core.maps AS m
        WHERE m.code = $2;
        """
        await self._conn.execute(
            query,
            data.thread_id,
            data.code,
            data.user_id,
            data.content,
            data.creator_mentions,
            data.change_request_type,
        )

    async def resolve_change_request(self, thread_id: int) -> None:
        """Mark a change request as resolved.

        Args:
            thread_id (int): The Discord thread ID tied to the change request.

        Returns:
            None

        """
        query = """
            UPDATE change_requests
            SET resolved = TRUE
            WHERE thread_id = $1;
        """
        await self._conn.execute(query, thread_id)

    async def get_change_requests(self, code: OverwatchCode) -> list[ChangeRequestResponse]:
        """Retrieve unresolved change requests for a given map code.

        Args:
            code (OverwatchCode): The Overwatch map code to filter requests.

        Returns:
            list[ChangeRequest]: A list of unresolved change requests ordered by
                creation time (newest first).

        """
        query = """
            SELECT *
            FROM change_requests
            WHERE code = $1 AND resolved IS FALSE
            ORDER BY created_at DESC, resolved DESC;
        """
        rows = await self._conn.fetch(query, code)
        return msgspec.convert(rows, list[ChangeRequestResponse])

    async def get_stale_change_requests(self) -> list[StaleChangeRequestResponse]:
        """Retrieve stale change requests older than two weeks.

        Stale requests are those that have not been resolved or alerted.

        Returns:
            list[ChangeRequest]: A list of stale change requests containing thread ID,
                user ID, and creator mentions.

        """
        query = """
            SELECT thread_id, user_id, creator_mentions
            FROM change_requests
            WHERE created_at < NOW() - INTERVAL '2 weeks'
                AND alerted IS FALSE AND resolved IS FALSE;
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[StaleChangeRequestResponse])

    async def update_alerted_change_request(self, thread_id: int) -> None:
        """Mark a change request as alerted.

        Args:
            thread_id (int): The Discord thread ID tied to the change request.

        Returns:
            None

        """
        query = """
            UPDATE change_requests
            SET alerted = TRUE
            WHERE thread_id = $1;
        """
        await self._conn.execute(query, thread_id)


async def provide_change_requests_service(conn: Connection, state: State) -> ChangeRequestsService:
    """Litestar DI provider for ChangeRequestsService.

    Args:
        conn (Connection): Active asyncpg connection.
        state (State): App state.

    Returns:
        ChangeRequestsService: A new service instance bound to `conn`.

    """
    return ChangeRequestsService(conn=conn, state=state)


dependencies = {"svc": Provide(provide_change_requests_service)}
