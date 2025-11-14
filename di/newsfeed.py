from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING

import msgspec
from genjipk_sdk.models.jobs import CreatePublishNewsfeedReturnDTO
from genjipk_sdk.models.newsfeed import NewsfeedEvent, NewsfeedQueueMessage
from litestar.datastructures import Headers

from di.base import BaseService

if TYPE_CHECKING:
    from asyncpg import Connection
    from litestar.datastructures import State


log = getLogger(__name__)


class NewsfeedService(BaseService):
    async def create_and_publish(
        self,
        event: NewsfeedEvent,
        *,
        headers: Headers,
        use_pool: bool = False,
    ) -> CreatePublishNewsfeedReturnDTO:
        """Insert a newsfeed event and publish its ID to Rabbit.

        Args:
            event (NewsfeedEvent): The event payload to persist.
            routing_key (str | None, optional): Override for the publish routing key. Defaults to the service default.
            pytest_enabled (bool, optional): If True, publish using test mode in the publisher. Defaults to False.
            headers (dict | None, optional): Additional headers to include in the published message.
            use_pool (bool): Whether or not to use a pool for the connection.

        Returns:
            int: The newly created newsfeed event ID.

        """
        q = "INSERT INTO newsfeed (timestamp, payload) VALUES ($1, $2::jsonb) RETURNING id;"
        payload_obj = msgspec.to_builtins(event.payload)
        if use_pool:
            async with self._pool.acquire() as conn:
                new_id = await conn.fetchval(q, event.timestamp, payload_obj)
        else:
            new_id = await self._conn.fetchval(q, event.timestamp, payload_obj)
        idempotency_key = f"newsfeed:create:{new_id}"
        job_status = await self.publish_message(
            routing_key="api.newsfeed.create",
            data=NewsfeedQueueMessage(newsfeed_id=new_id),
            headers=headers,
            idempotency_key=idempotency_key,
            use_pool=use_pool,
        )
        return CreatePublishNewsfeedReturnDTO(job_status, new_id)

    async def get_event(self, id_: int) -> NewsfeedEvent | None:
        """Fetch a single newsfeed event by ID.

        Args:
            id_ (int): The newsfeed event ID.

        Returns:
            NewsfeedEvent: The resolved event.

        """
        row = await self._conn.fetchrow(
            "SELECT id, timestamp, payload, event_type FROM newsfeed WHERE id=$1",
            id_,
        )
        if not row:
            return None
        return msgspec.convert(row, NewsfeedEvent)

    async def list_events(
        self,
        *,
        limit: int,
        page_number: int,
        type_: str | None,
    ) -> list[NewsfeedEvent] | None:
        """List newsfeed events with offset/limit pagination and optional type filter.

        Args:
            limit (int): Page size to return (e.g., 10, 20, 25, 50).
            page_number (int): 1-based page number.
            type_ (str | None): Optional event type filter.

        Returns:
            list[NewsfeedEvent]: Events ordered by most recent first (timestamp DESC, id DESC).

        """
        offset = max(page_number - 1, 0) * limit
        q = """
            SELECT id, timestamp, payload, event_type, count(*) OVER () AS total_results
            FROM newsfeed
            WHERE ($1::text IS NULL OR event_type = $1)
            ORDER BY timestamp DESC, id DESC
            LIMIT $2 OFFSET $3
        """
        rows = await self._conn.fetch(q, type_, limit, offset)

        if not rows:
            return None
        log.debug(rows)
        return msgspec.convert(rows, list[NewsfeedEvent])


async def provide_newsfeed_service(conn: Connection, state: State) -> NewsfeedService:
    """Litestar DI provider for `NewsfeedService`.

    Args:
        conn (Connection): Active asyncpg connection scoped to the request.
        state (State): Application state instance.

    Returns:
        NewsfeedService: Service instance configured with the given connection and state.

    """
    return NewsfeedService(conn=conn, state=state)
