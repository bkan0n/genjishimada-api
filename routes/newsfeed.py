from __future__ import annotations

from logging import getLogger
from typing import Annotated, Literal

import litestar
from genjipk_sdk.models.newsfeed import NewsfeedEvent
from genjipk_sdk.utilities.types import NewsfeedEventType
from litestar import Controller, Request
from litestar.di import Provide
from litestar.params import Parameter

from di import NewsfeedService, provide_newsfeed_service

log = getLogger(__name__)


class NewsfeedController(Controller):
    path = "/newsfeed"
    tags = ["Newsfeed"]
    dependencies = {"svc": Provide(provide_newsfeed_service)}

    @litestar.post(
        "/",
        summary="Create Newsfeed Event",
        description=(
            "Insert a newsfeed event and immediately publish its ID to RabbitMQ. "
            "The request body must be a valid NewsfeedEvent; the response is the numeric ID of the newly created row."
        ),
    )
    async def create_newsfeed_event(self, request: Request, svc: NewsfeedService, data: NewsfeedEvent) -> int:
        """Create a newsfeed event and publish its ID.

        Args:
            request (Request): Request.
            svc (NewsfeedService): Injected service instance.
            data (NewsfeedEvent): Event payload to persist and publish.

        Returns:
            int: The newly created newsfeed event ID.

        """
        new_id = await svc.create_and_publish(data, headers=request.headers)
        return new_id

    @litestar.get(
        "/",
        summary="List Newsfeed Events",
        description=(
            "Return a paginated list of newsfeed events ordered by most recent first. "
            'Supports an optional type filter via the "type" query parameter and fixed page sizes (10, 20, 25, 50).'
        ),
    )
    async def get_newsfeed_events(
        self,
        svc: NewsfeedService,
        page_size: Annotated[Literal[10, 20, 25, 50], Parameter()] = 10,
        page_number: int = 1,
        event_type: Annotated[NewsfeedEventType | None, Parameter(query="type")] = None,
    ) -> list[NewsfeedEvent] | None:
        """List newsfeed events with pagination and optional type filter.

        Args:
            svc (NewsfeedService): Injected service instance.
            page_size (Literal[10, 20, 25, 50]): Number of rows per page.
            page_number (int): 1-based page number (default 1).
            event_type (NewsfeedEventType | None): Optional event type filter.

        Returns:
            list[NewsfeedEvent]: Events ordered by recency.

        """
        return await svc.list_events(limit=page_size, page_number=page_number, type_=event_type)

    @litestar.get(
        "/{newsfeed_id:int}",
        summary="Get Newsfeed Event",
        description="Fetch a single newsfeed event by its ID. Returns the event payload and metadata if present.",
        include_in_schema=False,
    )
    async def get_newsfeed_event(self, svc: NewsfeedService, newsfeed_id: int) -> NewsfeedEvent | None:
        """Fetch a single newsfeed event by ID.

        Args:
            svc (NewsfeedService): Injected service instance.
            newsfeed_id (int): The event ID.

        Returns:
            NewsfeedEvent: The resolved event.

        """
        return await svc.get_event(newsfeed_id)
