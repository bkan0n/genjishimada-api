import asyncio
import datetime as dt
import inspect
import re
from logging import getLogger
from typing import Any, Awaitable, Callable, Iterable, Literal

import litestar
import msgspec
from asyncpg import Connection
from genjipk_sdk.difficulties import DifficultyTop
from genjipk_sdk.internal import JobStatusResponse
from genjipk_sdk.maps import (
    ArchivalStatusPatchRequest,
    GuideFullResponse,
    GuideResponse,
    GuideURL,
    LinkMapsCreateRequest,
    MapCategory,
    MapCreateRequest,
    MapCreationJobResponse,
    MapMasteryCreateRequest,
    MapMasteryCreateResponse,
    MapMasteryResponse,
    MapPartialResponse,
    MapPatchRequest,
    MapResponse,
    Mechanics,
    OverwatchCode,
    OverwatchMap,
    PlaytestStatus,
    QualityValueRequest,
    Restrictions,
    SendToPlaytestRequest,
    TrendingMapResponse,
    UnlinkMapsCreateRequest,
)
from genjipk_sdk.newsfeed import (
    NewsfeedArchive,
    NewsfeedBulkArchive,
    NewsfeedBulkUnarchive,
    NewsfeedEvent,
    NewsfeedFieldChange,
    NewsfeedGuide,
    NewsfeedLegacyRecord,
    NewsfeedLinkedMap,
    NewsfeedMapEdit,
    NewsfeedUnarchive,
    NewsfeedUnlinkedMap,
)
from litestar.datastructures import Headers
from litestar.di import Provide
from litestar.response import Response, Stream
from litestar.status_codes import HTTP_200_OK, HTTP_400_BAD_REQUEST

from di.jobs import InternalJobsService, provide_internal_jobs_service
from di.maps import CompletionFilter, MapSearchFilters, MapService, MedalFilter, PlaytestFilter, provide_map_service
from di.newsfeed import NewsfeedService, provide_newsfeed_service
from di.users import UserService, provide_user_service
from utilities.errors import CustomHTTPException
from utilities.jobs import wait_for_job_completion

log = getLogger(__name__)

Friendly = str  # alias for readability

# Fields to skip entirely in the newsfeed
_EXCLUDED_FIELDS = {"hidden", "official", "archived", "playtesting"}

# Fields that are list-like and should be normalized/sorted for comparison
_LIST_FIELDS = {"creators", "mechanics", "restrictions"}


def _labelize(field: str) -> str:
    """Convert a snake_case field name into a human-friendly label.

    Transforms e.g. ``"map_name"`` into ``"Map Name"``.

    Args:
        field: The snake_case field name.

    Returns:
        A title-cased string suitable for display.

    Raises:
        TypeError: If ``field`` is not a string.
    """
    return field.replace("_", " ").title()


def _friendly_none(v: Any) -> Friendly:  # noqa: ANN401
    """Render a placeholder for ``None``-like values.

    Args:
        v: The value to render (ignored; only used for signature symmetry).

    Returns:
        A user-friendly placeholder string, e.g. ``"—"``.
    """
    return "Empty"


def _to_builtin(v: Any) -> Any:  # noqa: ANN401
    """Convert values to JSON-serializable Python builtins.

    This uses ``msgspec.to_builtins`` to normalize enums, msgspec structs,
    dataclasses, and other supported types into standard Python types.

    Args:
        v: The value to convert.

    Returns:
        A JSON-serializable representation of ``v``.

    Raises:
        Exception: If ``msgspec.to_builtins`` fails to convert the value.
    """
    return msgspec.to_builtins(v)


def _list_norm(items: Iterable[Any]) -> list:
    """Normalize an iterable of values for stable comparison and display.

    Converts items to builtins, sorts them with a string key for determinism,
    and returns a list. ``None`` is treated as an empty iterable.

    Args:
        items: The values to normalize (may be ``None``).

    Returns:
        A stably sorted list of normalized values.

    Raises:
        Exception: If element conversion to builtins fails unexpectedly.
    """
    lst = list(items or [])
    try:
        return sorted((_to_builtin(x) for x in lst), key=lambda x: (str(x)))
    except Exception:
        # Ultra-conservative fallback if items are not directly comparable
        return sorted([str(_to_builtin(x)) for x in lst])


async def _resolve_creator_name(
    resolver: Callable[[int], str | Awaitable[str]] | None,
    creator_id: int,
) -> str | None:
    """Resolve a creator's display name from an ID using a sync or async resolver.

    If ``resolver`` is async, this function awaits it; if sync, it calls directly.

    Args:
        resolver: Callable that returns a name (sync) or an awaitable name (async).
        creator_id: The numeric creator ID to resolve.

    Returns:
        The resolved display name, or ``None`` if unavailable.

    Raises:
        Exception: If the resolver raises unexpectedly. (Caught by caller in most flows.)
    """
    if resolver is None:
        return None
    try:
        result = resolver(creator_id)
        if inspect.isawaitable(result):
            return await result  # type: ignore[func-returns-value]
        return result  # type: ignore[return-value]
    except Exception:
        return None


async def _friendly_value(
    field: str,
    value: Any,  # noqa: ANN401
    *,
    get_creator_name: Callable[[int], str | Awaitable[str]] | None = None,
) -> Friendly:
    """Render a field value into a user-friendly string (async for optional lookup).

    Special-cases certain fields:
      * ``creators``: Shows creator names only; if names are missing on the
        new patch value, resolves via ``get_creator_name(id)`` (sync or async).
      * list fields (``mechanics``, ``restrictions``): Order-insensitive,
        comma-separated output.
      * ``None``: Rendered as a placeholder (e.g. ``"—"``).

    For all other fields, values are converted to builtins and stringified.

    Args:
        field: The map field name being rendered.
        value: The field value to render.
        get_creator_name: Optional resolver (sync or async) for creator names
            by ID when the patch omits names.

    Returns:
        A user-friendly string representation of ``value``.

    Raises:
        Exception: If builtin conversion fails unexpectedly.
    """
    if value is None:
        return _friendly_none(value)

    if field == "creators":
        names: list[str] = []
        for x in value or []:
            xb = _to_builtin(x)
            name = None
            if isinstance(xb, dict):
                name = xb.get("name")
                if not name and get_creator_name and "id" in xb:
                    resolved = await _resolve_creator_name(get_creator_name, int(xb["id"]))
                    name = resolved or None
            if not name:
                name = str(xb.get("name") or xb.get("id") or xb)
            names.append(name)
        names = sorted(set(names), key=str.casefold)
        return ", ".join(names) if names else _friendly_none(None)

    if field in _LIST_FIELDS:
        vals = _list_norm(value)
        return ", ".join(map(str, vals)) if vals else _friendly_none(None)

    if field == "medals":
        return (
            f"\n<a:_:1406302950443192320>: {value.gold}\n"
            f"<a:_:1406302952263782466>: {value.silver}\n"
            f"<a:_:1406300035624341604>: {value.bronze}\n"
        )

    b = _to_builtin(value)
    if b is None:
        return _friendly_none(None)

    return str(b)


def _values_equal(field: str, old: Any, new: Any) -> bool:  # noqa: ANN401
    """Check semantic equality of two values for a given field.

    For list fields (``creators``, ``mechanics``, ``restrictions``), compares
    order-insensitively after normalization. For all others, compares values
    after converting to builtins.

    Args:
        field: The field name being compared.
        old: The original value.
        new: The new/patch value.

    Returns:
        ``True`` if the values are semantically equal; otherwise ``False``.
    """
    if field in _LIST_FIELDS:
        return _list_norm(old) == _list_norm(new)
    return _to_builtin(old) == _to_builtin(new)


class BaseMapsController(litestar.Controller):
    """Maps."""

    tags = ["Maps"]
    path = "/"
    dependencies = {
        "svc": Provide(provide_map_service),
        "newsfeed": Provide(provide_newsfeed_service),
        "users": Provide(provide_user_service),
        "jobs": Provide(provide_internal_jobs_service),
    }
    linked_code_job_statuses = set()

    @litestar.get(
        "/",
        summary="Search Maps",
        description=(
            "Return maps matching the provided filters, including playtest status, visibility, categories, "
            "creators, mechanics, restrictions, difficulty, medals, and completion context. Supports pagination "
            "or returning all results."
        ),
    )
    async def get_maps(  # noqa: PLR0913
        self,
        svc: MapService,
        playtest_status: PlaytestStatus | None = None,
        archived: bool | None = None,
        hidden: bool | None = None,
        official: bool | None = None,
        playtest_thread_id: int | None = None,
        code: OverwatchCode | None = None,
        category: list[MapCategory] | None = None,
        map_name: list[OverwatchMap] | None = None,
        creator_ids: list[int] | None = None,
        creator_names: list[str] | None = None,
        mechanics: list[Mechanics] | None = None,
        restrictions: list[Restrictions] | None = None,
        difficulty_exact: DifficultyTop | None = None,
        difficulty_range_min: DifficultyTop | None = None,
        difficulty_range_max: DifficultyTop | None = None,
        minimum_quality: int | None = None,
        medal_filter: MedalFilter = "All",
        user_id: int | None = None,
        completion_filter: CompletionFilter = "All",
        finalized_playtests: bool | None = None,
        playtest_filter: PlaytestFilter = "All",
        return_all: bool = False,
        page_size: Literal[10, 20, 25, 50, 12] = 10,
        page_number: int = 1,
    ) -> list[MapResponse] | None:
        """Get maps with particular filters.

        Args:
            svc (MapService): Injected map service.
            playtest_status (PlaytestStatus | None): Filter by playtest status.
            archived (bool | None): Filter by archived state.
            hidden (bool | None): Filter by hidden state.
            official (bool | None): Filter by official flag.
            playtest_thread_id (int | None): Filter by playtest thread ID.
            code (OverwatchCode | None): Filter by exact map code.
            category (list[MapCategory] | None): Filter by one or more categories.
            map_name (list[OverwatchMap] | None): Filter by one or more map names.
            creator_ids (list[int] | None): Filter by creator user IDs.
            creator_names (list[str] | None): Filter by creator display names.
            mechanics (list[Mechanics] | None): Filter by mechanics.
            restrictions (list[Restrictions] | None): Filter by restrictions.
            difficulty_exact (DifficultyTop | None): Filter by exact normalized difficulty.
            difficulty_range_min (DifficultyTop | None): Minimum difficulty bound (inclusive).
            difficulty_range_max (DifficultyTop | None): Maximum difficulty bound (inclusive).
            minimum_quality (int | None): Filter by minimum quality score.
            medal_filter (MedalFilter): Medal presence filter.
            user_id (int | None): Filter by user specific completion context.
            completion_filter (CompletionFilter): Completion presence filter.
            playtest_filter (PlaytestFilter): Playtest filter.
            finalized_playtests (Bool | None): To return only finalized playtests.
            return_all (bool): If True, ignore pagination and return all results.
            page_size (Literal[10, 20, 25, 50]): Page size.
            page_number (int): Page number.

        Returns:
            list[MapReadDTO] | None: Paged results or all results, depending on `return_all`.

        """
        filters = MapSearchFilters(
            playtesting=playtest_status,
            archived=archived,
            hidden=hidden,
            official=official,
            playtest_thread_id=playtest_thread_id,
            code=code,
            category=category,
            map_name=map_name,
            creator_ids=creator_ids,
            creator_names=creator_names,
            mechanics=mechanics,
            restrictions=restrictions,
            difficulty_exact=difficulty_exact,
            difficulty_range_min=difficulty_range_min,
            difficulty_range_max=difficulty_range_max,
            finalized_playtests=finalized_playtests,
            minimum_quality=minimum_quality,
            medal_filter=medal_filter,
            playtest_filter=playtest_filter,
            user_id=user_id,
            completion_filter=completion_filter,
            page_size=page_size,
            page_number=page_number,
            return_all=return_all,
        )
        return await svc.fetch_maps(single=False, filters=filters)

    @litestar.get(
        path="/{code:str}/partial",
        summary="Get Partial Playtest Data",
        description=(
            "Fetch a minimal playtest view for initialization flows. "
            "This omits fields not required when starting a playtest."
        ),
        include_in_schema=False,
    )
    async def get_partial_playtest_map(self, svc: MapService, code: OverwatchCode) -> MapPartialResponse:
        """Fetch the partial playtest data.

        Args:
            svc (MapService): Injected map service.
            code (OverwatchCode): Map code.

        Returns:
            MapReadPartialDTO: Stripped playtest view suitable for initialization flows.

        """
        return await svc.fetch_partial_map(code)

    @litestar.post(
        path="/",
        summary="Submit Map",
        description=("Create a new map of any category. Accepts a structured payload and returns the created map."),
    )
    async def submit_map(
        self,
        request: litestar.Request,
        svc: MapService,
        data: MapCreateRequest,
        newsfeed: NewsfeedService,
        users: UserService,
    ) -> MapCreationJobResponse:
        """Submit a map of any type.

        Args:
            state (State): Application state for background work / publishing.
            svc (MapService): Injected map service.
            data (MapCreateDTO): New map payload.
            newsfeed (NewsfeedService): Service handling newsfeed.
            users (UserService): Service handling user data.
            request (Request): Request obj.

        Returns:
            MapReadDTO: Created map.

        """
        _data = await svc.create_map(data, request, newsfeed)
        return _data

    async def _generate_patch_newsfeed(  # noqa: PLR0913
        self,
        newsfeed: NewsfeedService,
        old_data: MapResponse,
        patch_data: MapPatchRequest,
        reason: str,
        request: litestar.Request,
        *,
        get_creator_name: Callable[[int], str | Awaitable[str]] | None = None,
    ) -> None:
        """Build and publish a user-friendly `NewsfeedMapEdit` from a map PATCH.

        Behavior:
          * Ignores fields set to ``msgspec.UNSET``.
          * Excludes internal/boring fields: ``hidden``, ``official``, ``archived``, ``playtesting``.
          * Emits changes only when values actually differ (order-insensitive for lists).
          * Renders creators by **name** only (resolving IDs via ``get_creator_name`` when needed).
          * Displays ``None`` as a friendly placeholder (e.g. ``"—"``).
          * Produces prettified field labels (e.g. ``"map_name"`` → ``"Map Name"``).

        Side Effects:
          Publishes a `NewsfeedEvent` via the provided `NewsfeedService` using
          `create_and_publish` if at least one material change is detected.

        Args:
            newsfeed: The newsfeed service used to persist and publish the event.
            old_data: The pre-patch map snapshot (`MapReadDTO`) for old values.
            patch_data: The incoming partial update (`MapPatchDTO`).
            reason: Human-readable explanation for the change (shown in feed).
            event_type: Event type label or enum value (defaults to ``"map_edit"``).
            get_creator_name: Optional resolver for creator names by ID when the
                patch omits names.
            request (Request): Request obj.

        Returns:
            None. Publishes an event only if there are material changes; otherwise no-op.

        Raises:
            Exception: If publishing fails or value conversion encounters unexpected errors.
        """
        patch_fields = msgspec.structs.asdict(patch_data)
        changes: list[NewsfeedFieldChange] = []

        for field, new_val in patch_fields.items():
            if new_val is msgspec.UNSET:
                continue
            if field in _EXCLUDED_FIELDS:
                continue

            old_val = getattr(old_data, field, None)

            if _values_equal(field, old_val, new_val):
                continue

            old_f = await _friendly_value(field, old_val, get_creator_name=get_creator_name)
            new_f = await _friendly_value(field, new_val, get_creator_name=get_creator_name)
            label = _labelize(field)

            changes.append(NewsfeedFieldChange(field=label, old=old_f, new=new_f))

        if not changes:
            return

        payload = NewsfeedMapEdit(
            code=old_data.code,
            changes=changes,
            reason=reason,
        )

        event = NewsfeedEvent(
            id=None,
            timestamp=dt.datetime.now(dt.timezone.utc),
            payload=payload,
            event_type="map_edit",
        )

        await newsfeed.create_and_publish(event, headers=request.headers)

    @litestar.patch(
        "/{code:str}",
        summary="Update Map",
        description=("Patch an existing map by code using a partial update payload. Returns the updated map."),
    )
    async def update_map(  # noqa: PLR0913
        self,
        code: OverwatchCode,
        data: MapPatchRequest,
        svc: MapService,
        newsfeed: NewsfeedService,
        users: UserService,
        request: litestar.Request,
    ) -> MapResponse:
        """Update map.

        Args:
            code (OverwatchCode): Map code to update.
            data (MapPatchDTO): Partial update payload.
            svc (MapService): Injected map service.
            newsfeed (NewsfeedService): Service handling newsfeed.
            users (UserService): Service handling user data.
            request (Request): Request obj.

        Returns:
            MapReadDTO: Updated map.

        """
        original_map = await svc.fetch_maps(filters=MapSearchFilters(code=code), single=True)
        patched_map = await svc.patch_map(code, data)

        async def _get_user_coalesced_name(user_id: int) -> str:
            d = await users.get_user(user_id)
            if d:
                return d.coalesced_name or "Unknown User"
            return "Unknown User"

        await self._generate_patch_newsfeed(
            newsfeed, original_map, data, "", request, get_creator_name=_get_user_coalesced_name
        )
        return patched_map

    @litestar.get(
        "/{code:str}/exists",
        summary="Check Map Code Exists",
        description=(
            "Validate the format of a map code and check if it exists. Useful for early validation during submission."
        ),
    )
    async def check_code_exists(self, conn: Connection, code: str) -> Response:
        """Validate the existence of a map code.

        This is useful for catching issues early in the submission process.

        Args:
            conn (Connection): Database connection.
            code (str): Overwatch share code.

        Returns:
            Response: Boolean response content indicating existence.

        Raises:
            CustomHTTPException: If the code fails the expected format validation.

        """
        if not re.match(r"^[A-Z0-9]{4,6}$", code):
            raise CustomHTTPException(
                detail="Provided code is not valid. Must follow regex ^[A-Z0-9]{4,6}$",
                status_code=HTTP_400_BAD_REQUEST,
                extra={"code": code},
            )

        if await conn.fetchval("SELECT EXISTS(SELECT 1 FROM core.maps WHERE code = $1)", code):
            return Response(content=True, status_code=HTTP_200_OK)

        return Response(content=False, status_code=HTTP_200_OK)

    @litestar.get(
        "/{code:str}/plot",
        summary="Get Playtest Plot",
        description=("Return a generated plot image stream for the specified map's playtest."),
        include_in_schema=False,
    )
    async def get_map_plot(
        self,
        svc: MapService,
        code: OverwatchCode,
    ) -> Stream:
        """Get plot image for a playtest.

        This is used when playtest metadata/playtest thread id has not yet been created.

        Args:
            svc (MapService): Injected map service.
            code (OverwatchCode): Map code.

        Returns:
            Stream: Streamed plot image content.

        """
        return await svc.get_playtest_plot(code=code)

    @litestar.get(
        path="/{code:str}/guides",
        summary="Get Guides",
        description="Fetch all guides associated with the specified map.",
    )
    async def get_guides(
        self, svc: MapService, code: OverwatchCode, include_records: bool = False
    ) -> list[GuideFullResponse]:
        """Fetch guides for a map.

        Args:
            svc (MapService): Injected map service.
            code (OverwatchCode): Map code.
            include_records (bool): Whether to include record videos.

        Returns:
            list[GuideFull]: Guides for the map.

        """
        return await svc.get_guides(code, include_records)

    @litestar.delete(
        path="/{code:str}/guides/{user_id:int}",
        summary="Delete Guide",
        description="Delete a guide for the given map and user.",
    )
    async def delete_guide(self, svc: MapService, code: OverwatchCode, user_id: int) -> None:
        """Delete a guide for a map and user.

        Args:
            svc (MapService): Injected map service.
            code (OverwatchCode): Map code.
            user_id (int): User who owns the guide.

        """
        await svc.delete_guide(code, user_id)

    @litestar.patch(
        path="/{code:str}/guides/{user_id:int}",
        summary="Edit Guide",
        description="Update the guide URL for the given map and user.",
    )
    async def edit_guide(self, svc: MapService, code: OverwatchCode, user_id: int, url: GuideURL) -> GuideResponse:
        """Edit a guide URL for a map and user.

        Args:
            svc (MapService): Injected map service.
            code (OverwatchCode): Map code.
            user_id (int): User who owns the guide.
            url (GuideURL): New guide URL.

        Returns:
            Guide: Updated guide.

        """
        return await svc.edit_guide(code, user_id, url)

    @litestar.post(
        path="/{code:str}/guides",
        summary="Create Guide",
        description="Create a new guide for the specified map.",
    )
    async def create_guide(  # noqa: PLR0913
        self,
        svc: MapService,
        code: OverwatchCode,
        data: GuideResponse,
        newsfeed: NewsfeedService,
        users: UserService,
        request: litestar.Request,
    ) -> GuideResponse:
        """Create a guide for a map.

        Args:
            svc (MapService): Injected map service.
            newsfeed (NewsfeedService): Service handling newsfeed.
            users (UserService): Service handling user data.
            code (OverwatchCode): Map code.
            data (Guide): Guide payload.
            request (Request): Request obj.

        Returns:
            Guide: Created guide.

        """
        guide = await svc.create_guide(code, data)
        user_data = await users.get_user(user_id=data.user_id)
        name = user_data.coalesced_name if user_data else None
        event_payload = NewsfeedGuide(code=code, guide_url=data.url, name=name or "Unknown User")
        event = NewsfeedEvent(
            id=None, timestamp=dt.datetime.now(dt.timezone.utc), payload=event_payload, event_type="guide"
        )
        await newsfeed.create_and_publish(event, headers=request.headers)
        return guide

    @litestar.get(
        path="/{code:str}/affected",
        summary="Get Affected Users",
        description=("Return user IDs that are impacted by changes to the specified map."),
    )
    async def get_affected_users(self, svc: MapService, code: OverwatchCode) -> list[int]:
        """Get IDs of users affected by a map change.

        Args:
            svc (MapService): Injected map service.
            code (OverwatchCode): Map code.

        Returns:
            list[int]: Affected user IDs.

        """
        return await svc.get_affected_users(code)

    @litestar.get(
        "/mastery",
        summary="Get Map Mastery",
        description=("Retrieve mastery data for a user, optionally scoped to a specific map."),
        tags=["Mastery"],
    )
    async def get_map_mastery_data(
        self, svc: MapService, user_id: int, map_name: OverwatchMap | None = None
    ) -> list[MapMasteryResponse]:
        """Get mastery data for a user, optionally scoped to a map.

        Args:
            svc (MapService): Injected map service.
            user_id (int): Target user ID.
            map_name (OverwatchMap | None): Optional map name filter.

        Returns:
            list[MapMasteryData]: Mastery rows for the user (and map if provided).

        """
        return await svc.get_map_mastery_data(user_id, map_name)

    @litestar.post(
        "/mastery",
        summary="Update Map Mastery",
        description=("Create or update a user's map mastery data and return the result."),
        tags=["Mastery"],
        include_in_schema=False,
    )
    async def update_mastery(self, svc: MapService, data: MapMasteryCreateRequest) -> MapMasteryCreateResponse | None:
        """Create or update mastery data.

        Args:
            svc (MapService): Injected map service.
            data (MapMasteryCreateDTO): Mastery payload.

        Returns:
            MapMasteryCreateReturnDTO: Result of the mastery operation.

        """
        return await svc.update_mastery(data)

    @litestar.post(
        "/{code:str}/legacy",
        summary="Convert To Legacy Map",
        description="Convert completions for a map to legacy and remove medals.",
        tags=["Maps"],
    )
    async def convert_to_legacy(
        self,
        svc: MapService,
        code: OverwatchCode,
        newsfeed: NewsfeedService,
        request: litestar.Request,
        reason: str = "",
    ) -> None:
        """Convert completions for a map to legacy and remove medals.

        Args:
            svc (MapService): Injected map service.
            newsfeed (NewsfeedService): Service handling newsfeed.
            code (OverwatchCode): Map code to convert.
            request (Request): Request obj.
            reason (str): Add a reason to why the map was converted.

        """
        affected_count = await svc.convert_map_to_legacy(code)
        event_payload = NewsfeedLegacyRecord(code=code, affected_count=affected_count, reason=reason)
        event = NewsfeedEvent(
            id=None, timestamp=dt.datetime.now(dt.timezone.utc), payload=event_payload, event_type="legacy_record"
        )
        await newsfeed.create_and_publish(event, headers=request.headers)

    @litestar.patch(
        "/archive",
        summary="Archive or unarchive maps (single or bulk)",
        description=(
            "Toggle archival status for one or more maps based on the request body. "
            'If `status` is `"Archive"`, the specified `codes` are archived; otherwise they are unarchived. '
            "Uses single-map operations when exactly one code is provided and "
            "bulk operations when multiple codes are provided. "
            "On success, publishes an `api.map.archive` message containing the request payload."
        ),
    )
    async def set_archive_status(
        self,
        request: litestar.Request,
        svc: MapService,
        data: ArchivalStatusPatchRequest,
        newsfeed: NewsfeedService,
    ) -> None:
        """Archive or unarchive one or more maps.

        Args:
            request (Request): Request obj.
            svc (MapService): Service handling archive and unarchive operations.
            newsfeed (NewsfeedService): Service handling newsfeed.
            data (ArchivalStatusPatchDTO): Patch payload containing the desired status
                and the list of map ``codes`` to act upon.

        Side Effects:
            Publishes an ``api.map.archive`` message with the provided payload after a successful update.
        """
        now = dt.datetime.now(dt.timezone.utc)
        is_single = len(data.codes) == 1
        is_archive = str(data.status).strip().lower() == "archive"

        if is_single:
            code = data.codes[0]
            map_data = await svc.fetch_maps(single=True, filters=MapSearchFilters(code=code))

            if is_archive:
                await svc.archive_map(code)
                payload = NewsfeedArchive(
                    code=code,
                    map_name=map_data.map_name,
                    creators=[c.name for c in map_data.creators],
                    difficulty=map_data.difficulty,
                    reason="",
                )
                event_type = "archive"
            else:
                await svc.unarchive_map(code)
                payload = NewsfeedUnarchive(
                    code=code,
                    map_name=map_data.map_name,
                    creators=[c.name for c in map_data.creators],
                    difficulty=map_data.difficulty,
                    reason="",
                )
                event_type = "unarchive"
        elif is_archive:
            await svc.bulk_archive_map(data.codes)
            payload = NewsfeedBulkArchive(codes=data.codes, reason="")
            event_type = "bulk_archive"
        else:
            await svc.bulk_unarchive_map(data.codes)
            payload = NewsfeedBulkUnarchive(codes=data.codes, reason="")
            event_type = "bulk_unarchive"

        event = NewsfeedEvent(
            id=None,
            timestamp=now,
            payload=payload,
            event_type=event_type,
        )

        await newsfeed.create_and_publish(event, headers=request.headers)

    @litestar.post(
        path="/{code:str}/quality",
        summary="Override Quality Votes",
        description="Overrides quality votes for a specific map.",
    )
    async def override_quality_votes(self, svc: MapService, code: OverwatchCode, data: QualityValueRequest) -> None:
        """Overwrite quality votes for a map.

        Args:
            svc (MapService): Map DI.
            code (OverwatchCode): The map code to overwrite.
            data (QualityVoteDTO): Data for overwriting.
        """
        return await svc.override_map_quality_votes(code, data)

    @litestar.post(
        path="/trending",
        summary="Trending Maps",
        description="View the trending maps.",
    )
    async def get_trending_maps(
        self,
        svc: MapService,
        limit: Literal[1, 3, 5, 10, 15, 20, 25],
    ) -> list[TrendingMapResponse]:
        """Get the trending maps.

        Args:
            svc (MapService): Map DI.
            limit (int): The number of maps to limit the query to.
        """
        return await svc.get_trending_maps(limit)

    @litestar.post(
        path="/{code:str}/playtest",
        summary="Send Map to Playtest",
        description="Send a currently approved map to playtesting.",
    )
    async def send_map_to_playtest(
        self,
        request: litestar.Request,
        code: OverwatchCode,
        svc: MapService,
        data: SendToPlaytestRequest,
    ) -> JobStatusResponse:
        """Send a map back to playtest."""
        return await svc.send_map_to_playtest(code=code, data=data, request=request)

    @litestar.post(
        path="/link-codes",
        summary="Link Maps.",
        description="Link an official and unofficial map and create a playtest and newsfeed if needed.",
    )
    async def link_map_codes(
        self,
        request: litestar.Request,
        svc: MapService,
        newsfeed: NewsfeedService,
        jobs: InternalJobsService,
        data: LinkMapsCreateRequest,
    ) -> JobStatusResponse | None:
        """Link an official and unofficial map and publish a newsfeed event.

        Links two map codes through the MapService. If a new playtest or clone needs
        to be created, this endpoint triggers the appropriate service logic and
        publishes a `linked_map` event to the newsfeed service.

        Args:
            request (litestar.Request): The current HTTP request context.
            jobs (InternalJobsService): Service providing `get_job` for polling.
            svc (MapService): Service responsible for map management and linking logic.
            newsfeed (NewsfeedService): Service for creating and publishing newsfeed events.
            data (LinkMapsCreateDTO): The payload containing `official_code` and `unofficial_code`.

        Returns:
            JobStatus | None: A job status object if a map clone or playtest was created;
                otherwise `None` if only a link was established.

        Raises:
            HTTPException: If linking or newsfeed publishing fails.
        """
        status, in_playtest = await svc.link_official_and_unofficial_map(
            request=request,
            official_code=data.official_code,
            unofficial_code=data.unofficial_code,
            newsfeed=newsfeed,
        )

        payload = NewsfeedLinkedMap(
            official_code=data.official_code,
            unofficial_code=data.unofficial_code,
        )
        event = NewsfeedEvent(id=None, timestamp=dt.datetime.now(dt.UTC), payload=payload, event_type="linked_map")

        if in_playtest:
            assert status
            task = asyncio.create_task(
                wait_and_publish_newsfeed(
                    svc=svc,
                    jobs=jobs,
                    newsfeed=newsfeed,
                    status=status,
                    event=event,
                    headers=request.headers,
                )
            )
            self.linked_code_job_statuses.add(task)
            task.add_done_callback(lambda t: self.linked_code_job_statuses.remove(t))

        else:
            await newsfeed.create_and_publish(event, headers=request.headers)

        return status

    @litestar.delete(
        path="/link-codes",
        summary="Unlink Maps.",
        description="Unlink an official and unofficial map.",
    )
    async def unlink_map_codes(
        self,
        request: litestar.Request,
        svc: MapService,
        newsfeed: NewsfeedService,
        data: UnlinkMapsCreateRequest,
    ) -> None:
        """Unlink two map codes."""
        await svc.unlink_two_map_codes(
            official_code=data.official_code,
            unofficial_code=data.unofficial_code,
        )
        payload = NewsfeedUnlinkedMap(
            official_code=data.official_code,
            unofficial_code=data.unofficial_code,
            reason=data.reason,
        )
        event = NewsfeedEvent(id=None, timestamp=dt.datetime.now(dt.UTC), payload=payload, event_type="unlinked_map")
        await newsfeed.create_and_publish(event, headers=request.headers)


async def wait_and_publish_newsfeed(  # noqa: PLR0913
    *,
    svc: MapService,
    jobs: InternalJobsService,
    newsfeed: NewsfeedService,
    status: JobStatusResponse,
    event: NewsfeedEvent,
    headers: Headers,
) -> None:
    """Wait for a job to complete, then publish a newsfeed event.

    Args:
        svc (MapService): Service responsible for map management and linking logic.
        jobs (InternalJobsService): Service providing `get_job` for polling.
        newsfeed (NewsfeedService): Service used to publish the newsfeed event.
        status (JobStatus): The initial job status returned from the map service.
        event (NewsfeedEvent): The event to publish once the job finishes.
        headers (Headers): HTTP headers to include when publishing.

    Returns:
        None
    """
    try:
        final_status = await wait_for_job_completion(
            job_id=status.id,
            fetch_status=jobs.get_job_using_pool,  # same signature as before
            timeout=90.0,
        )

        if final_status.status == "succeeded":
            assert isinstance(event.payload, NewsfeedLinkedMap)
            map_data = await svc.fetch_maps(
                single=True, filters=MapSearchFilters(code=event.payload.official_code), use_pool=True
            )
            assert map_data.playtest
            event.payload.playtest_id = map_data.playtest.thread_id
            await newsfeed.create_and_publish(event, headers=headers, use_pool=True)
        else:
            log.warning(
                "Skipping newsfeed publish for job %s (status=%s)",
                final_status.id,
                final_status.status,
            )
    except Exception:
        log.exception("Error while waiting for job completion for event publish.")
