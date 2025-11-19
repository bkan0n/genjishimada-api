import hashlib
import hmac
import os
from datetime import datetime
from logging import getLogger
from typing import Annotated, Any

import msgspec
from asyncpg import Connection
from genjipk_sdk.logs import LogCreateRequest, MapClickCreateRequest
from genjipk_sdk.maps import Mechanics, OverwatchCode, OverwatchMap, PlaytestStatus, Restrictions
from litestar import Controller, MediaType, get, post
from litestar.datastructures import UploadFile
from litestar.di import Provide
from litestar.enums import RequestEncodingType
from litestar.params import Body

from di import AutocompleteService, provide_autocomplete_service
from di.image_storage import ImageStorageService, provide_image_storage_service

log = getLogger(__name__)


class UtilitiesController(Controller):
    path = "/utilities"

    dependencies = {"autocomplete": Provide(provide_autocomplete_service)}

    @post(
        path="/image",
        tags=["Utilities"],
        dependencies={"svc": Provide(provide_image_storage_service)},
        summary="Upload Image",
        description="Upload an image or screenshot file to the CDN. The file must be sent as multipart/form-data.",
        sync_to_thread=False,
        request_max_body_size=1024 * 1024 * 25,
    )
    def upload_screenshot(
        self,
        data: Annotated[UploadFile, Body(media_type=RequestEncodingType.MULTI_PART)],
        svc: ImageStorageService,
    ) -> str:
        """Upload an image/screenshot to CDN.

        Args:
            data (UploadFile): Uploaded file received as multipart form-data.
            svc (ImageStorageService): Service responsible for handling CDN uploads.

        Returns:
            str: The public CDN URL of the uploaded screenshot.

        """
        content = data.file.read()

        return svc.upload_screenshot(content, data.content_type)

    @get(
        path="/autocomplete/names",
        tags=["Autocomplete"],
        summary="Autocomplete Map Names",
        description="Return a list of map names ordered by text similarity to the provided search string.",
    )
    async def get_similar_map_names(
        self, autocomplete: AutocompleteService, search: str, limit: int = 5
    ) -> list[OverwatchMap] | None:
        """Get similar map names.

        Args:
            autocomplete (AutocompleteService): Autocomplete and transform service.
            search (str): The input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[OverwatchMap] | None: A list of matching map names or `None` if no matches found.

        """
        return await autocomplete.get_similar_map_names(search, limit)

    @get(
        path="/transformers/names",
        tags=["Transformer"],
        media_type=MediaType.JSON,
        summary="Transform Map Name",
        description="Transform a free-form input string into the closest matching OverwatchMap name.",
    )
    async def transform_map_names(self, autocomplete: AutocompleteService, search: str) -> OverwatchMap | None:
        """Transform a map name into an OverwatchMap.

        Args:
            autocomplete (AutocompleteService): Autocomplete and transform service.
            search (str): Input string to transform.

        Returns:
            OverwatchMap | None: The closest matching map name, or `None` if no matches found.

        """
        return await autocomplete.transform_map_names(search)

    @get(
        path="/autocomplete/restrictions",
        tags=["Autocomplete"],
        summary="Autocomplete Map Restrictions",
        description="Return a list of map restrictions ordered by text similarity to the provided search string.",
    )
    async def get_similar_map_restrictions(
        self,
        autocomplete: AutocompleteService,
        search: str,
        limit: int = 5,
    ) -> list[Restrictions] | None:
        """Get similar map restrictions.

        Args:
            autocomplete (AutocompleteService): Autocomplete and transform service.
            search (str): Input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[Restrictions] | None: Matching restriction names, or `None` if none found.

        """
        return await autocomplete.get_similar_map_restrictions(search, limit)

    @get(
        path="/transformers/restrictions",
        tags=["Transformer"],
        media_type=MediaType.JSON,
        summary="Transform Map Restriction",
        description="Transform a free-form input string into the closest matching map restriction.",
    )
    async def transform_map_restrictions(self, autocomplete: AutocompleteService, search: str) -> OverwatchMap | None:
        """Transform a map name into a Restriction.

        Args:
            autocomplete (AutocompleteService): Autocomplete and transform service.
            search (str): Input string to transform.

        Returns:
            Restrictions | None: The closest matching restriction, or `None` if none found.

        """
        return await autocomplete.transform_map_restrictions(search)

    @get(
        path="/autocomplete/mechanics",
        tags=["Autocomplete"],
        summary="Autocomplete Map Mechanics",
        description="Return a list of mechanics ordered by similarity to the provided search string.",
    )
    async def get_similar_map_mechanics(
        self, autocomplete: AutocompleteService, search: str, limit: int = 5
    ) -> list[Mechanics] | None:
        """Get similar map mechanics.

        Args:
            autocomplete (AutocompleteService): Autocomplete and transform service.
            search (str): Input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[Mechanics] | None: Matching mechanics, or `None` if none found.

        """
        return await autocomplete.get_similar_map_mechanics(search, limit)

    @get(
        path="/transformers/mechanics",
        tags=["Transformer"],
        media_type=MediaType.JSON,
        summary="Transform Map Mechanic",
        description="Transform a free-form input string into the closest matching map mechanic.",
    )
    async def transform_map_mechanics(self, autocomplete: AutocompleteService, search: str) -> Mechanics | None:
        """Transform a map name into a Mechanic.

        Args:
            autocomplete (AutocompleteService): Autocomplete and transform service.
            search (str): Input string to transform.

        Returns:
            Mechanics | None: The closest matching mechanic, or `None` if none found.

        """
        return await autocomplete.transform_map_mechanics(search)

    @get(
        path="/autocomplete/codes",
        tags=["Autocomplete"],
        summary="Autocomplete Map Codes",
        description=(
            "Return a list of map codes ordered by exact match, prefix match, or similarity. "
            "Results can be filtered by archived/hidden status or playtest status."
        ),
    )
    async def get_similar_map_codes(  # noqa: PLR0913
        self,
        autocomplete: AutocompleteService,
        search: str,
        archived: bool | None = None,
        hidden: bool | None = None,
        playtesting: PlaytestStatus | None = None,
        limit: int = 5,
    ) -> list[OverwatchCode] | None:
        """Get similar map codes.

        Args:
            autocomplete (AutocompleteService): Autocomplete and transform service.
            search (str): Input string to compare.
            archived (bool | None, optional): Filter by archived flag, or `None` for no filter.
            hidden (bool | None, optional): Filter by hidden flag, or `None` for no filter.
            playtesting (PlaytestStatus | None, optional): Filter by playtesting status, or `None` for no filter.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[OverwatchCode] | None: Matching map codes, or `None` if none found.

        """
        return await autocomplete.get_similar_map_codes(search, archived, hidden, playtesting, limit)

    @get(
        path="/transformers/codes",
        tags=["Transformer"],
        media_type=MediaType.JSON,
        summary="Transform Map Code",
        description=(
            "Transform a free-form input string into the closest matching map code. "
            "Results may be filtered by archived, hidden, or playtest status."
        ),
    )
    async def transform_map_codes(
        self,
        autocomplete: AutocompleteService,
        search: str,
        archived: bool | None = None,
        hidden: bool | None = None,
        playtesting: PlaytestStatus | None = None,
    ) -> OverwatchCode | None:
        """Transform a map name into a code.

        Args:
            autocomplete (AutocompleteService): Autocomplete and transform service.
            search (str): Input string to transform.
            archived (bool | None, optional): Filter by archived flag, or `None` for no filter.
            hidden (bool | None, optional): Filter by hidden flag, or `None` for no filter.
            playtesting (PlaytestStatus | None, optional): Filter by playtesting status, or `None` for no filter.

        Returns:
            OverwatchCode | None: The closest matching map code, or `None` if none found.

        """
        return await autocomplete.transform_map_codes(search, archived, hidden, playtesting)

    @get(
        path="/autocomplete/users",
        tags=["Autocomplete"],
        summary="Autocomplete Users",
        description=(
            "Return a list of users ordered by text similarity to the provided search string. "
            "Considers nickname, global name, and Overwatch usernames."
        ),
    )
    async def get_similar_users(
        self,
        autocomplete: AutocompleteService,
        search: str,
        limit: int = 10,
        fake_users_only: bool = False,
    ) -> list[tuple[int, str]] | None:
        """Get similar users by nickname, global name, or Overwatch username.

        Args:
            autocomplete (AutocompleteService): Autocomplete and transform service.
            search (str): Input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 10.
            fake_users_only (bool): Filter out actualy discord users and display fake members only.

        Returns:
            list[tuple[int, str]] | None: A list of `(user_id, display_name)` tuples, or `None` if no matches found.

        """
        return await autocomplete.get_similar_users(search, limit, fake_users_only)

    @post(path="/log", include_in_schema=False)
    async def log_analytics(self, conn: Connection, data: LogCreateRequest) -> None:
        """Log Discord interaction command information."""
        query = """
            INSERT INTO public.analytics (command_name, user_id,  created_at, namespace)
            VALUES ($1, $2, $3, $4);
        """
        await conn.execute(query, data.command_name, data.user_id, data.created_at, data.namespace)

    @post(
        path="/log-map-click",
        summary="Log Map Code Clicks",
        description="Log when a user clicks ona Map Code Copy button.",
        tags=["Utilities"],
    )
    async def log_map_clicks(self, conn: Connection, data: MapClickCreateRequest) -> None:
        """Log the click on a 'copy code' button on the website."""
        secret = os.getenv("IP_HASH_SECRET", "").encode("utf-8")
        ip_hash = hmac.new(secret, data.ip_address.encode("utf-8"), hashlib.sha256).hexdigest()
        query = """
            WITH target_map AS (
                SELECT id AS map_id FROM core.maps WHERE code = $1
            )
            INSERT INTO maps.clicks (map_id, user_id, source, ip_hash)
            VALUES ((SELECT map_id FROM target_map), $2, $3, $4)
            ON CONFLICT ON CONSTRAINT u_click_unique_per_day DO NOTHING;
        """
        await conn.execute(query, data.code, data.user_id, data.source, ip_hash)

    @get(
        path="/log-map-click",
        tags=["Utilities"],
    )
    async def get_log_map_clicks(self, conn: Connection) -> Any:  # noqa: ANN401
        """Get log clicks. DEBUG ONLY."""
        query = """
            SELECT id, map_id, user_id, source, user_agent, ip_hash, inserted_at, day_bucket
            FROM maps.clicks ORDER BY inserted_at DESC LIMIT 100;
        """
        return msgspec.convert(await conn.fetch(query), list[LogClicksDebug] | None)


class LogClicksDebug(msgspec.Struct):
    id: int | None
    map_id: int | None
    user_id: int | None
    source: str | None
    user_agent: str | None
    ip_hash: str | None
    inserted_at: datetime
    day_bucket: int
