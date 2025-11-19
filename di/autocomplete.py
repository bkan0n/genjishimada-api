from __future__ import annotations

import logging
from typing import cast

from asyncpg import Connection
from genjipk_sdk.maps import Mechanics, OverwatchCode, OverwatchMap, PlaytestStatus, Restrictions
from litestar.datastructures import State

from di.base import BaseService

log = logging.getLogger(__name__)


class AutocompleteService(BaseService):
    async def get_similar_map_names(self, search: str, limit: int = 5) -> list[OverwatchMap] | None:
        """Get similar map names.

        Args:
            conn (Connection): Database connection.
            search (str): The input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[OverwatchMap] | None: A list of matching map names or `None` if no matches found.

        """
        query = "SELECT name FROM maps.names ORDER BY similarity(name, $1::text) DESC LIMIT $2;"
        res = await self._conn.fetch(query, search, limit)
        if not res:
            return None
        return [r["name"] for r in res]

    async def transform_map_names(self, search: str) -> OverwatchMap | None:
        """Transform a map name into an OverwatchMap.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to transform.

        Returns:
            OverwatchMap | None: The closest matching map name, or `None` if no matches found.

        """
        query = "SELECT name FROM maps.names ORDER BY similarity(name, $1::text) DESC LIMIT 1;"
        res = cast("OverwatchMap", await self._conn.fetchval(query, search))
        if res is None:
            return None
        return f'"{res}"'  # type: ignore

    async def get_similar_map_restrictions(
        self,
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
        res = await self._conn.fetch(query, search, limit)
        if not res:
            return None
        return [r["name"] for r in res]

    async def transform_map_restrictions(self, search: str) -> OverwatchMap | None:
        """Transform a map name into a Restriction.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to transform.

        Returns:
            Restrictions | None: The closest matching restriction, or `None` if none found.

        """
        query = "SELECT name FROM maps.restrictions ORDER BY similarity(name, $1::text) DESC LIMIT 1;"
        res = cast("Restrictions", await self._conn.fetchval(query, search))
        if res is None:
            return None
        return f'"{res}"'  # type: ignore

    async def get_similar_map_mechanics(self, search: str, limit: int = 5) -> list[Mechanics] | None:
        """Get similar map mechanics.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 5.

        Returns:
            list[Mechanics] | None: Matching mechanics, or `None` if none found.

        """
        query = "SELECT name FROM maps.mechanics ORDER BY similarity(name, $1::text) DESC LIMIT $2;"
        res = await self._conn.fetch(query, search, limit)
        if not res:
            return None
        return [r["name"] for r in res]

    async def transform_map_mechanics(self, search: str) -> Mechanics | None:
        """Transform a map name into a Mechanic.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to transform.

        Returns:
            Mechanics | None: The closest matching mechanic, or `None` if none found.

        """
        query = "SELECT name FROM maps.mechanics ORDER BY similarity(name, $1::text) DESC LIMIT 1;"
        res = cast("Mechanics", await self._conn.fetchval(query, search))
        if res is None:
            return None
        return f'"{res}"'  # type: ignore

    async def get_similar_map_codes(
        self,
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

        res = await self._conn.fetch(query, search, archived, hidden, playtesting, limit)
        if not res:
            return None
        return [r["code"] for r in res]

    async def transform_map_codes(
        self,
        search: str,
        archived: bool | None = None,
        hidden: bool | None = None,
        playtesting: PlaytestStatus | None = None,
        use_pool: bool = False,
    ) -> OverwatchCode | None:
        """Transform a map name into a code.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to transform.
            archived (bool | None, optional): Filter by archived flag, or `None` for no filter.
            hidden (bool | None, optional): Filter by hidden flag, or `None` for no filter.
            playtesting (PlaytestStatus | None, optional): Filter by playtesting status, or `None` for no filter.
            use_pool (bool): Use a pool instead of a route-based connection.

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
        if use_pool:
            async with self._pool.acquire() as conn:
                res = cast("OverwatchCode", await conn.fetchval(query, search, archived, hidden, playtesting))
        else:
            res = cast("OverwatchCode", await self._conn.fetchval(query, search, archived, hidden, playtesting))
        if res is None:
            return None
        return f'"{res}"'  # type: ignore

    async def get_similar_users(
        self,
        search: str,
        limit: int = 10,
        fake_users_only: bool = False,
        use_pool: bool = False,
    ) -> list[tuple[int, str]] | None:
        """Get similar users by nickname, global name, or Overwatch username.

        Args:
            conn (Connection): Database connection.
            search (str): Input string to compare.
            limit (int, optional): Maximum number of results. Defaults to 10.
            fake_users_only (bool): Filter out actualy discord users and display fake members only.
            use_pool (bool): Use a pool instead of a route-based connection.

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
        if use_pool:
            async with self._pool.acquire() as conn:
                res = await conn.fetch(query, search, limit, fake_users_only)
        else:
            res = await self._conn.fetch(query, search, limit, fake_users_only)
        if not res:
            return None
        return [(r["user_id"], r["name"]) for r in res]


async def provide_autocomplete_service(conn: Connection, state: State) -> AutocompleteService:
    """Litestar DI provider for AutocompleteService.

    Args:
        conn (asyncpg.Connection): Active asyncpg connection.
        state (State): Application state.

    Returns:
        UserService: A new service instance bound to the given connection.

    """
    return AutocompleteService(conn, state)
