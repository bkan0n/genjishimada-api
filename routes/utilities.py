import hashlib
import hmac
import os
from logging import getLogger
from typing import Annotated, Any, cast

import msgspec
from asyncpg import Connection
from genjipk_sdk.models import LogCreateDTO
from genjipk_sdk.models.logging import MapClickCreateDTO
from genjipk_sdk.utilities._types import Mechanics, OverwatchCode, OverwatchMap, PlaytestStatus, Restrictions
from litestar import Controller, MediaType, get, post
from litestar.datastructures import UploadFile
from litestar.di import Provide
from litestar.enums import RequestEncodingType
from litestar.params import Body

from di.image_storage import ImageStorageService, provide_image_storage_service

log = getLogger(__name__)


class UtilitiesController(Controller):
    path = "/utilities"

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
    async def get_similar_map_names(self, conn: Connection, search: str, limit: int = 5) -> list[OverwatchMap] | None:
        """Get similar map names.

        Args:
            conn (Connection): Database connection.
            search (str): The input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[OverwatchMap] | None: A list of matching map names or `None` if no matches found.

        """
        query = "SELECT name FROM maps.names ORDER BY similarity(name, $1::text) DESC LIMIT $2;"
        res = await conn.fetch(query, search, limit)
        if not res:
            return None
        return [r["name"] for r in res]

    @get(
        path="/transformers/names",
        tags=["Transformer"],
        media_type=MediaType.JSON,
        summary="Transform Map Name",
        description="Transform a free-form input string into the closest matching OverwatchMap name.",
    )
    async def transform_map_names(self, conn: Connection, search: str) -> OverwatchMap | None:
        """Transform a map name into an OverwatchMap.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to transform.

        Returns:
            OverwatchMap | None: The closest matching map name, or `None` if no matches found.

        """
        query = "SELECT name FROM maps.names ORDER BY similarity(name, $1::text) DESC LIMIT 1;"
        res = cast("OverwatchMap", await conn.fetchval(query, search))
        if res is None:
            return None
        return f'"{res}"'  # type: ignore

    @get(
        path="/autocomplete/restrictions",
        tags=["Autocomplete"],
        summary="Autocomplete Map Restrictions",
        description="Return a list of map restrictions ordered by text similarity to the provided search string.",
    )
    async def get_similar_map_restrictions(
        self,
        conn: Connection,
        search: str,
        limit: int = 5,
    ) -> list[Restrictions] | None:
        """Get similar map restrictions.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[Restrictions] | None: Matching restriction names, or `None` if none found.

        """
        query = "SELECT name FROM maps.restrictions ORDER BY similarity(name, $1::text) DESC LIMIT $2;"
        res = await conn.fetch(query, search, limit)
        if not res:
            return None
        return [r["name"] for r in res]

    @get(
        path="/transformers/restrictions",
        tags=["Transformer"],
        media_type=MediaType.JSON,
        summary="Transform Map Restriction",
        description="Transform a free-form input string into the closest matching map restriction.",
    )
    async def transform_map_restrictions(self, conn: Connection, search: str) -> OverwatchMap | None:
        """Transform a map name into a Restriction.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to transform.

        Returns:
            Restrictions | None: The closest matching restriction, or `None` if none found.

        """
        query = "SELECT name FROM maps.restrictions ORDER BY similarity(name, $1::text) DESC LIMIT 1;"
        res = cast("Restrictions", await conn.fetchval(query, search))
        if res is None:
            return None
        return f'"{res}"'  # type: ignore

    @get(
        path="/autocomplete/mechanics",
        tags=["Autocomplete"],
        summary="Autocomplete Map Mechanics",
        description="Return a list of mechanics ordered by similarity to the provided search string.",
    )
    async def get_similar_map_mechanics(self, conn: Connection, search: str, limit: int = 5) -> list[Mechanics] | None:
        """Get similar map mechanics.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[Mechanics] | None: Matching mechanics, or `None` if none found.

        """
        query = "SELECT name FROM maps.mechanics ORDER BY similarity(name, $1::text) DESC LIMIT $2;"
        res = await conn.fetch(query, search, limit)
        if not res:
            return None
        return [r["name"] for r in res]

    @get(
        path="/transformers/mechanics",
        tags=["Transformer"],
        media_type=MediaType.JSON,
        summary="Transform Map Mechanic",
        description="Transform a free-form input string into the closest matching map mechanic.",
    )
    async def transform_map_mechanics(self, conn: Connection, search: str) -> Mechanics | None:
        """Transform a map name into a Mechanic.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to transform.

        Returns:
            Mechanics | None: The closest matching mechanic, or `None` if none found.

        """
        query = "SELECT name FROM maps.mechanics ORDER BY similarity(name, $1::text) DESC LIMIT 1;"
        res = cast("Mechanics", await conn.fetchval(query, search))
        if res is None:
            return None
        return f'"{res}"'  # type: ignore

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
        conn: Connection,
        search: str,
        archived: bool | None = None,
        hidden: bool | None = None,
        playtesting: PlaytestStatus | None = None,
        limit: int = 5,
    ) -> list[OverwatchCode] | None:
        """Get similar map codes.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to compare.
            archived (bool | None, optional): Filter by archived flag, or `None` for no filter.
            hidden (bool | None, optional): Filter by hidden flag, or `None` for no filter.
            playtesting (PlaytestStatus | None, optional): Filter by playtesting status, or `None` for no filter.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[OverwatchCode] | None: Matching map codes, or `None` if none found.

        """
        query = """
            SELECT code FROM core.maps
            WHERE ($2::bool IS NULL OR archived = $2) AND
            ($3::bool IS NULL OR hidden = $3) AND
            ($4::playtest_status IS NULL OR playtesting = $4)
            ORDER BY
                CASE
                    WHEN code = $1::text THEN 3
                    WHEN code ILIKE $1::text || '%' THEN 2
                    ELSE similarity(code, $1::text)
                END DESC
            LIMIT $5;
        """

        res = await conn.fetch(query, search, archived, hidden, playtesting, limit)
        if not res:
            return None
        return [r["code"] for r in res]

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
        conn: Connection,
        search: str,
        archived: bool | None = None,
        hidden: bool | None = None,
        playtesting: PlaytestStatus | None = None,
    ) -> OverwatchCode | None:
        """Transform a map name into a code.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to transform.
            archived (bool | None, optional): Filter by archived flag, or `None` for no filter.
            hidden (bool | None, optional): Filter by hidden flag, or `None` for no filter.
            playtesting (PlaytestStatus | None, optional): Filter by playtesting status, or `None` for no filter.

        Returns:
            OverwatchCode | None: The closest matching map code, or `None` if none found.

        """
        query = """
            SELECT code FROM core.maps
            WHERE ($2::bool IS NULL OR archived = $2) AND
            ($3::bool IS NULL OR hidden = $3) AND
            ($4::playtest_status IS NULL OR playtesting = $4)
            ORDER BY
                CASE
                    WHEN code = $1::text THEN 3
                    WHEN code ILIKE $1::text || '%' THEN 2
                    ELSE similarity(code, $1::text)
                END DESC
            LIMIT 1;
        """

        res = cast("OverwatchCode", await conn.fetchval(query, search, archived, hidden, playtesting))
        if res is None:
            return None
        return f'"{res}"'  # type: ignore

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
        conn: Connection,
        search: str,
        limit: int = 10,
        fake_users_only: bool = False,
    ) -> list[tuple[int, str]] | None:
        """Get similar users by nickname, global name, or Overwatch username.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 10.
            fake_users_only (bool): Filter out actualy discord users and display fake members only.

        Returns:
            list[tuple[int, str]] | None: A list of `(user_id, display_name)` tuples, or `None` if no matches found.

        """
        query = """
        WITH matches AS (
            SELECT u.id AS user_id, name, similarity(name, $1) AS sim
            FROM core.users u
            CROSS JOIN LATERAL (
                VALUES (u.nickname), (u.global_name)
            ) AS name_list(name)
            WHERE $3 IS FALSE OR id < 10000000000000

            UNION ALL

            SELECT o.user_id, o.username AS name, similarity(o.username, $1) AS sim
            FROM users.overwatch_usernames o
            WHERE $3 IS FALSE OR user_id < 10000000000000
        ),
        ranked_users AS (
            SELECT user_id, MAX(sim) AS sim
            FROM matches
            GROUP BY user_id
            ORDER BY sim DESC
            LIMIT $2
        ),
        user_names AS (
            SELECT
                u.id AS user_id,
                ARRAY_REMOVE(
                    ARRAY[u.nickname, u.global_name] || ARRAY_AGG(owu.username),
                    NULL
                ) AS all_usernames
            FROM ranked_users ru
            JOIN core.users u ON u.id = ru.user_id
            LEFT JOIN users.overwatch_usernames owu ON owu.user_id = u.id
            GROUP BY u.id, u.nickname, u.global_name, ru.sim
            ORDER BY ru.sim DESC
        )
        SELECT
            user_id,
            CASE
                WHEN array_length(all_usernames, 1) = 1 THEN all_usernames[1]
                ELSE
                    all_usernames[1] || ' (' ||
                    array_to_string(
                        ARRAY(SELECT DISTINCT unnest(all_usernames[2:array_length(all_usernames, 1)])), ', ') || ')'
            END AS name
        FROM user_names;
        """
        res = await conn.fetch(query, search, limit, fake_users_only)
        if not res:
            return None
        return [(r["user_id"], r["name"]) for r in res]

    @post(path="/log", include_in_schema=False)
    async def log_analytics(self, conn: Connection, data: LogCreateDTO) -> None:
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
    async def log_map_clicks(self, conn: Connection, data: MapClickCreateDTO) -> None:
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
    async def get_log_map_clicks(
        self,
        conn: Connection,
    ) -> Any:
        query = """
            SELECT * FROM maps.clicks LIMIT 100;
        """
        return msgspec.to_builtins(await conn.fetch(query))
