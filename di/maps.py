import datetime as dt
import functools
import itertools
from logging import getLogger
from textwrap import dedent
from typing import Any, Awaitable, Callable, Iterator, Literal, ParamSpec, Sequence, TypeVar, overload

import asyncpg
import msgspec
from asyncpg import Connection
from genjipk_sdk.models import (
    Creator,
    Guide,
    GuideFull,
    JobStatus,
    MapCreateDTO,
    MapMasteryCreateDTO,
    MapMasteryCreateReturnDTO,
    MapMasteryData,
    MapPatchDTO,
    MapReadDTO,
    MapReadPartialDTO,
    Medals,
    MessageQueueCreatePlaytest,
    NewsfeedEvent,
    NewsfeedNewMap,
    PlaytestCreatePartialDTO,
    QualityValueDTO,
    SendToPlaytestDTO,
    TrendingMapReadDTO,
)
from genjipk_sdk.models.jobs import CreateMapReturnDTO
from genjipk_sdk.utilities import (
    DIFFICULTY_MIDPOINTS,
    DIFFICULTY_RANGES_ALL,
    DIFFICULTY_RANGES_TOP,
    DifficultyAll,
    DifficultyTop,
    convert_raw_difficulty_to_difficulty_all,
)
from genjipk_sdk.utilities._types import (
    GuideURL,
    MapCategory,
    Mechanics,
    OverwatchCode,
    OverwatchMap,
    PlaytestStatus,
    Restrictions,
)
from litestar import Request
from litestar.datastructures import State
from litestar.exceptions import HTTPException
from litestar.response import Stream
from litestar.status_codes import HTTP_400_BAD_REQUEST

from di.base import BaseService
from di.newsfeed import NewsfeedService
from utilities.errors import CustomHTTPException, parse_pg_detail
from utilities.playtest_plot import build_playtest_plot
from utilities.shared_queries import get_map_mastery_data

log = getLogger("__name__")

P = ParamSpec("P")
R = TypeVar("R")

_TriFilter = Literal["All", "With", "Without"]
CompletionFilter = _TriFilter
MedalFilter = _TriFilter
PlaytestFilter = Literal["All", "Only", "None"]


class QueryWithArgs(msgspec.Struct):
    query: str
    args: list[Any]

    def __iter__(self) -> Iterator[Any]:
        """Allow unpacking into `(query, args)`.

        Yields:
            str: The SQL query string.
            list[Any]: The list of parameter values.

        """
        yield self.query
        yield self.args


class MapSearchFilters(msgspec.Struct):
    playtesting: PlaytestStatus | None = None
    archived: bool | None = None
    hidden: bool | None = None
    official: bool | None = None
    playtest_thread_id: int | None = None
    code: OverwatchCode | None = None
    category: list[MapCategory] | None = None
    map_name: list[OverwatchMap] | None = None
    creator_ids: list[int] | None = None
    creator_names: list[str] | None = None
    mechanics: list[Mechanics] | None = None
    restrictions: list[Restrictions] | None = None
    difficulty_exact: DifficultyTop | None = None
    difficulty_range_min: DifficultyTop | None = None
    difficulty_range_max: DifficultyTop | None = None
    finalized_playtests: bool | None = None
    minimum_quality: int | None = None
    user_id: int | None = None
    medal_filter: MedalFilter = "All"
    completion_filter: CompletionFilter = "All"
    playtest_filter: PlaytestFilter = "All"
    return_all: bool = False
    page_size: Literal[10, 20, 25, 50, 12] = 10
    page_number: int = 1


class MapSearchSQLBuilder:
    def __init__(self, filters: MapSearchFilters) -> None:
        """Initialize the SQL builder with filters.

        Args:
            filters (MapSearchFilters): Filters to apply to the query.

        """
        self._filters: MapSearchFilters = filters
        self.validate()

        self._params: list[Any] = []
        self._counter: itertools.count[int] = itertools.count(1)
        self._cte_definitions: list[str] = []
        self._intersect_subqueries: list[str] = []
        self._where_clauses: list[str] = []

    def validate(self) -> None:
        """Validate the consistency of filter combinations.

        Raises:
            ValueError: If mutually exclusive filters are used together,
                such as `difficulty_exact` and difficulty range filters,
                or both `creator_ids` and `creator_names`.

        """
        if self._filters.difficulty_exact and (
            self._filters.difficulty_range_min or self._filters.difficulty_range_max
        ):
            raise ValueError("Cannot use exact difficulty with range-based filtering")

        if self._filters.creator_ids and self._filters.creator_names:
            raise ValueError("Cannot use creator_ids and creator_names simultaneously")

    def _add_cte_definition(self, name: str, sql: str, *param_values: Any) -> None:  # noqa: ANN401
        """Register a new Common Table Expression (CTE).

        Args:
            name (str): The CTE alias.
            sql (str): The SQL subquery for the CTE.
            *param_values (Any): Parameters to bind to the CTE query.

        """
        _sql = dedent(sql)
        self._cte_definitions.append(f"{name} AS (\n    {_sql.strip()}\n)")
        self._intersect_subqueries.append(f"    SELECT map_id FROM {name}")
        self._params.extend(param_values)

    def _generate_mechanics_cte(self) -> None:
        """Generate a CTE restricting results to maps containing given mechanics.

        Skips generation if no mechanics are provided in the filters.
        """
        if not self._filters.mechanics:
            return
        placeholders = ", ".join(f"${next(self._counter)}" for _ in self._filters.mechanics)
        mechanic_links_query = (
            "SELECT map_id FROM maps.mechanic_links ml "
            "JOIN maps.mechanics m ON ml.mechanic_id = m.id "
            f"WHERE m.name IN ({placeholders})"
        )
        self._add_cte_definition(
            "limited_mechanics",
            mechanic_links_query,
            *self._filters.mechanics,
        )

    def _generate_restrictions_cte(self) -> None:
        """Generate a CTE restricting results to maps containing given restrictions.

        Skips generation if no restrictions are provided in the filters.
        """
        if not self._filters.restrictions:
            return
        placeholders = ", ".join(f"${next(self._counter)}" for _ in self._filters.restrictions)
        restriction_links_query = (
            "SELECT map_id FROM maps.restriction_links rl "
            "JOIN maps.restrictions r ON rl.restriction_id = r.id "
            f"WHERE r.name IN ({placeholders})"
        )
        self._add_cte_definition(
            "limited_restrictions",
            restriction_links_query,
            *self._filters.restrictions,
        )

    def _generate_creator_ids_cte(self) -> None:
        """Generate a CTE restricting results to maps created by given user IDs.

        Skips generation if no creator IDs are provided in the filters.
        """
        if not self._filters.creator_ids:
            return
        placeholders = ", ".join(f"${next(self._counter)}" for _ in self._filters.creator_ids)
        creator_ids_query = f"SELECT map_id FROM maps.creators WHERE user_id IN ({placeholders})"
        self._add_cte_definition(
            "limited_creator_ids",
            creator_ids_query,
            *self._filters.creator_ids,
        )

    def _generate_creator_names_cte(self) -> None:
        """Generate one or more CTEs.

        Restricts results to maps created by users whose nickname,
        global name, or Overwatch usernames match the provided strings.

        Skips generation if no creator names are provided in the filters.
        """
        if not self._filters.creator_names:
            return
        for i, _name in enumerate(self._filters.creator_names):
            p = f"${next(self._counter)}"
            creator_name_query = (
                "SELECT DISTINCT c.map_id "
                "FROM maps.creators c "
                "JOIN core.users u ON c.user_id = u.id "
                "LEFT JOIN users.overwatch_usernames ow ON u.id = ow.user_id "
                f"WHERE u.nickname ILIKE '%' || {p} || '%' "
                f"OR u.global_name ILIKE '%' || {p} || '%' "
                f"OR ow.username ILIKE '%' || {p} || '%' "
            )
            self._add_cte_definition(
                f"creator_match_{i}",
                creator_name_query,
                _name,
            )

    def _generate_minimum_quality_cte(self) -> None:
        """Generate a CTE restricting results to maps with a minimum quality rating.

        Skips generation if no minimum quality is set.
        """
        if not self._filters.minimum_quality:
            return
        minimum_quality_query = f"""
            SELECT map_id FROM
                (SELECT map_id, avg(quality) as avg_quality FROM maps.ratings GROUP BY map_id)
            WHERE avg_quality >= ${next(self._counter)}
        """
        self._add_cte_definition(
            "limited_quality",
            minimum_quality_query,
            self._filters.minimum_quality,
        )

    def _generate_medals_cte(self) -> None:
        """Generate a CTE restricting results by medal presence.

        "With" → only maps with medals.
        "Without" → only maps without medals.
        "All" → no filter applied.
        """
        if not self._filters.medal_filter:
            return
        match self._filters.medal_filter:
            case "With":
                query = "SELECT map_id FROM maps.medals"
            case "Without":
                query = (
                    "SELECT m.id AS map_id "
                    "FROM core.maps m "
                    "WHERE NOT EXISTS ("
                    "    SELECT 1 FROM maps.medals med WHERE med.map_id = m.id)"
                )
            case _:
                return

        self._add_cte_definition("limited_medals", query)

    def _generate_completions_cte(self) -> None:
        """Generate a CTE restricting results by user completion status.

        "With" → maps the user has verified completions on.
        "Without" → maps the user has no verified completions on.
        "All" → no filter applied.

        Skips generation if no `user_id` is set.
        """
        if not self._filters.user_id:
            return

        match self._filters.completion_filter:
            case "Without":
                query = (
                    "SELECT m.id AS map_id "
                    "FROM core.maps m "
                    "WHERE m.id NOT IN ( "
                    "    SELECT c.map_id"
                    "    FROM core.completions c"
                    f"    WHERE c.user_id = ${next(self._counter)}"
                    "      AND c.verified AND NOT c.legacy)"
                )
            case "With":
                query = f"""
                    SELECT map_id
                    FROM core.completions
                    WHERE user_id = ${next(self._counter)}
                      AND verified AND NOT legacy
                """
            case _:
                return

        self._add_cte_definition("limited_user_completion", query, self._filters.user_id)

    def _generate_cte_definitions(self) -> str:
        """Generate Common Table Expressions (CTEs) based on the filters.

        If a `code` filter is present, all other filters are ignored.

        Returns:
            str: A `WITH ...` clause containing applicable CTEs,
            or an empty string if none are required.

        """
        if self._filters.code:
            return ""
        self._generate_mechanics_cte()
        self._generate_restrictions_cte()
        self._generate_creator_ids_cte()
        self._generate_creator_names_cte()
        self._generate_minimum_quality_cte()
        self._generate_medals_cte()
        self._generate_completions_cte()

        if self._intersect_subqueries:
            joined_subqueries = f"intersection_map_ids AS (\n{'\n    INTERSECT\n'.join(self._intersect_subqueries)}\n)"
            self._cte_definitions.append(joined_subqueries)

        if self._cte_definitions:
            joined_ctes = ", ".join(self._cte_definitions)
            return f"WITH {joined_ctes}"
        return ""

    def _generate_where_clauses(self) -> str:  # noqa: PLR0912, PLR0915
        """Construct the `WHERE` clause from applicable filters.

        Returns:
            str: A `WHERE ...` clause, or an empty string if none apply.

        """
        if self._filters.code:
            self._params.append(self._filters.code)
            return f"WHERE m.code = ${next(self._counter)}"

        if self._filters.playtesting:
            self._where_clauses.append(f"m.playtesting = ${next(self._counter)}")
            self._params.append(self._filters.playtesting)

        if self._filters.playtest_filter:
            print(self._filters.playtest_filter)
            match self._filters.playtest_filter:
                case "None":
                    self._where_clauses.append("pm.thread_id IS NULL")
                case "Only":
                    self._where_clauses.append("pm.thread_id IS NOT NULL")
                case _:
                    pass

        if self._filters.difficulty_range_min or self._filters.difficulty_range_max:
            raw_min, raw_max = self._get_raw_difficulty_bounds(
                self._filters.difficulty_range_min,
                self._filters.difficulty_range_max,
            )
            self._where_clauses.append(f"m.raw_difficulty BETWEEN ${next(self._counter)} AND ${next(self._counter)}")
            self._params.extend([raw_min, raw_max])

        if self._filters.difficulty_exact:
            top = self._filters.difficulty_exact
            if top == "Hell":
                self._where_clauses.append("m.difficulty = 'Hell'")
            else:
                lo_key = f"{top} -"
                hi_key = f"{top} +"
                raw_min = DIFFICULTY_RANGES_ALL[lo_key][0]  # pyright: ignore[reportArgumentType]
                raw_max = DIFFICULTY_RANGES_ALL[hi_key][1]  # pyright: ignore[reportArgumentType]

                p1 = f"${next(self._counter)}"
                p2 = f"${next(self._counter)}"
                self._where_clauses.append(f"(m.raw_difficulty >= {p1} AND m.raw_difficulty < {p2})")
                self._params.extend([raw_min, raw_max])

        if self._filters.archived is not None:
            self._where_clauses.append(f"m.archived = ${next(self._counter)}")
            self._params.append(self._filters.archived)

        if self._filters.hidden is not None:
            self._where_clauses.append(f"m.hidden = ${next(self._counter)}")
            self._params.append(self._filters.hidden)

        if self._filters.official is not None:
            self._where_clauses.append(f"m.official = ${next(self._counter)}")
            self._params.append(self._filters.official)

        if self._filters.map_name:
            self._where_clauses.append(f"m.map_name = ANY(${next(self._counter)})")
            self._params.append(self._filters.map_name)

        if self._filters.category:
            self._where_clauses.append(f"m.category = ANY(${next(self._counter)})")
            self._params.append(self._filters.category)

        if self._filters.playtest_thread_id:
            self._where_clauses.append(f"pm.thread_id = ${next(self._counter)}")
            self._params.append(self._filters.playtest_thread_id)

        if self._filters.finalized_playtests:
            self._where_clauses.append("pm.verification_id IS NOT NULL AND m.playtesting='In Progress'")

        if self._where_clauses:
            print(self._where_clauses)
            print(self._where_clauses)
            print(self._where_clauses)
            joined_where_clauses = " AND ".join(self._where_clauses)
            return f"WHERE {joined_where_clauses}"

        return ""

    @staticmethod
    def _get_raw_difficulty_bounds(
        min_difficulty: DifficultyTop | None, max_difficulty: DifficultyTop | None
    ) -> tuple[float, float]:
        """Convert difficulty labels into raw numeric bounds.

        Args:
            min_difficulty (DifficultyTop | None): Lower difficulty bound.
            max_difficulty (DifficultyTop | None): Upper difficulty bound.

        Returns:
            tuple[float, float]: Numeric `(min, max)` bounds.

        """
        min_key = min_difficulty or "Easy"
        max_key = max_difficulty or "Hell"
        raw_min = DIFFICULTY_RANGES_TOP.get(min_key, (0.0, 0.0))[0]
        raw_max = DIFFICULTY_RANGES_TOP.get(max_key, (10.0, 10.0))[1]
        return raw_min, raw_max

    def _user_completion_time_col(self) -> str:
        """Return a SELECT fragment + params for the user's verified completion time.

        - Returns latest verified non-legacy completion time if user_id is set.
        - Returns NULL if user_id is None.
        """
        if self._filters.user_id is None:
            frag = "NULL AS time,"
            return frag

        frag = f"""
        (
            SELECT c.time
            FROM core.completions c
            WHERE c.map_id = m.id
              AND c.user_id = ${next(self._counter)}
              AND c.verified
              AND c.legacy = FALSE
            ORDER BY c.inserted_at DESC
            LIMIT 1
        ) AS time,
        """
        self._params.append(self._filters.user_id)
        return frag

    def _generate_added_columns(self) -> str:
        res = self._user_completion_time_col()
        return res

    def _generate_full_query(
        self,
        columns: str,
        joined_cte_definitions: str,
        joined_where_clauses: str,
    ) -> str:
        """Assemble the final SQL query.

        Includes CTEs, SELECT fields, joins, WHERE clauses,
        ORDER BY, and pagination.

        Args:
            columns (str): Additional columns.
            joined_cte_definitions (str): The WITH clause.
            joined_where_clauses (str): The WHERE clause.

        Returns:
            str: The complete parameterized SQL query string.

        """
        limit_offset = ""
        if not self._filters.return_all:
            limit_offset = f"LIMIT ${next(self._counter)} OFFSET ${next(self._counter)}"

        main_query = dedent(f"""
{joined_cte_definitions}
SELECT
    m.id,
    m.code,
    m.map_name,
    m.category,
    m.checkpoints,
    m.official,
    m.playtesting,
    m.archived,
    m.hidden,
    m.created_at,
    m.updated_at,
    pm.thread_id,
    {columns}
    (SELECT avg(quality)::float FROM maps.ratings r WHERE r.map_id = m.id) AS ratings,
    CASE WHEN playtesting::text = 'In Progress' and pm.thread_id IS NOT NULL
    THEN
    jsonb_build_object(
        'thread_id', pm.thread_id,
        'initial_difficulty', pm.initial_difficulty,
        'verification_id', pm.verification_id,
        'completed', pm.completed,
        'vote_average', (
            SELECT avg(difficulty)::float
            FROM playtests.votes v
            WHERE v.map_id = m.id
        ),
        'vote_count', (
            SELECT count(*)
            FROM playtests.votes v
            WHERE v.map_id = m.id
        ),
        'voters', (
            SELECT array_agg(DISTINCT v.user_id)
            FROM playtests.votes v
            WHERE v.map_id = m.id
        )
    ) END AS playtest,
    (
        SELECT jsonb_agg(
            DISTINCT jsonb_build_object(
               'id', c.user_id,
               'is_primary', c.is_primary,
               'name', coalesce(ow.username, u.nickname, u.global_name, 'Unknown Username')
           )
        )
        FROM maps.creators c
        JOIN core.users u ON c.user_id = u.id
        LEFT JOIN users.overwatch_usernames ow ON c.user_id = ow.user_id AND ow.is_primary
        WHERE c.map_id = m.id
    ) AS creators,
    (SELECT array_agg(DISTINCT g.url) FROM maps.guides g WHERE g.map_id = m.id) AS guides,
    (
       SELECT jsonb_build_object(
           'gold', med.gold,
           'silver', med.silver,
           'bronze', med.bronze
       )
       FROM maps.medals med WHERE med.map_id = m.id
    ) AS medals,
    COALESCE((
        SELECT array_agg(DISTINCT mech.name)
        FROM maps.mechanic_links ml
        JOIN maps.mechanics mech ON mech.id = ml.mechanic_id
        WHERE ml.map_id = m.id
    ), ARRAY[]::text[]) AS mechanics,

    COALESCE((
        SELECT array_agg(DISTINCT res.name)
        FROM maps.restriction_links rl
        JOIN maps.restrictions res ON res.id = rl.restriction_id
        WHERE rl.map_id = m.id
    ), ARRAY[]::text[]) AS restrictions,
    m.description,
    m.raw_difficulty,
    m.difficulty,
    m.title,
    m.linked_code,
    m.custom_banner AS map_banner,
    COUNT(*) OVER() AS total_results
{"FROM intersection_map_ids i" if self._intersect_subqueries else ""}
{"JOIN core.maps m ON m.id = i.map_id" if self._intersect_subqueries else "FROM core.maps m"}
LEFT JOIN LATERAL (
    SELECT
        thread_id,
        initial_difficulty,
        verification_id,
        created_at,
        updated_at,
        completed
    FROM playtests.meta
    WHERE map_id = m.id AND completed IS FALSE
    ORDER BY created_at DESC
    LIMIT 1
) pm ON TRUE
{joined_where_clauses}
ORDER BY raw_difficulty
{limit_offset}
""")
        return main_query

    def build(self) -> QueryWithArgs:
        """Build the full parameterized SQL query.

        Returns:
            QueryWithArgs: The query string with its parameters.

        """
        columns = self._generate_added_columns()
        ctes = self._generate_cte_definitions()
        where_clauses = self._generate_where_clauses()
        if not self._filters.return_all:
            limit = self._filters.page_size
            offset = (self._filters.page_number - 1) * self._filters.page_size
            self._params.append(limit)
            self._params.append(offset)
        query = self._generate_full_query(columns, ctes, where_clauses)
        return QueryWithArgs(query, self._params)


def _handle_exceptions(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Catch asyncpg constraint violations via decorator.

    Catches `UniqueViolationError` and `ForeignKeyViolationError`,
    converting them into `CustomHTTPException` with meaningful messages.

    Args:
        func (Callable): Async function to wrap.

    Returns:
        Callable: Wrapped function with exception handling applied.

    Raises:
        CustomHTTPException: For known constraint violations (duplicate codes,
            mechanics, restrictions, creators, or invalid user IDs).
        Exception: Any other unhandled exceptions are re-raised.

    """

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await func(*args, **kwargs)
        except asyncpg.exceptions.UniqueViolationError as e:
            if e.constraint_name == "maps_code_key":
                raise CustomHTTPException(
                    detail="Provided code already exists.",
                    status_code=HTTP_400_BAD_REQUEST,
                    extra=parse_pg_detail(e.detail),
                )
            elif e.constraint_name == "mechanic_links_pkey":
                raise CustomHTTPException(
                    detail="You have a duplicate mechanic.",
                    status_code=HTTP_400_BAD_REQUEST,
                )
            elif e.constraint_name == "restriction_links_pkey":
                raise CustomHTTPException(
                    detail="You have a duplicate restriction.",
                    status_code=HTTP_400_BAD_REQUEST,
                )
            elif e.constraint_name == "creators_pkey":
                raise CustomHTTPException(
                    detail="You have a duplicate creator ID.",
                    status_code=HTTP_400_BAD_REQUEST,
                )
            log.info(
                (
                    "Playtest submission failed with a Unique key error. This needs to be caught.\n"
                    f"Constraint name: {e.constraint_name}\ndetail: {e.detail}"
                ),
                exc_info=e,
            )
            raise HTTPException

        except asyncpg.exceptions.ForeignKeyViolationError as e:
            if e.constraint_name == "creators_user_id_fkey":
                raise CustomHTTPException(
                    detail="There is no user associated with supplied ID.",
                    status_code=HTTP_400_BAD_REQUEST,
                    extra=parse_pg_detail(e.detail),
                )

            log.info(
                (
                    "Playtest submission failed with a Foreign key error. This needs to be caught.\n"
                    f"Constraint name: {e.constraint_name}\ndetail: {e.detail}"
                ),
                exc_info=e,
            )
            raise HTTPException
        except Exception as e:
            log.info("Playtest submission failed. This needs to be caught.", exc_info=e)
            raise e

    return wrapper


class MapService(BaseService):
    @_handle_exceptions
    async def create_map(
        self,
        data: MapCreateDTO,
        request: Request,
        newsfeed_service: NewsfeedService,
    ) -> CreateMapReturnDTO:
        """Create a map.

        Within a transaction, inserts the core map row and all related data (creators, guide,
        mechanics, restrictions, medals). If `playtesting` is set, creates a partial playtest
        meta row and publishes a queue message. Returns the newly created map via `fetch_maps`.

        Args:
            data (MapCreateDTO): Map creation payload.
            request (Request): Request.
            newsfeed_service (NewsfeedService): Manages newsfeed events

        Returns:
            MapReadDTO: The created map.

        """
        async with self._conn.transaction():
            if not data.official and data.playtesting != "Approved":
                data.playtesting = "Approved"
            map_id = await self._insert_core_map_data(data)
            await self._insert_creators(map_id, data.creators, remove_existing=False)
            await self._insert_guide(map_id, data.guide_url, data.primary_creator_id)
            await self._insert_mechanics(map_id, data.mechanics, remove_existing=False)
            await self._insert_restrictions(map_id, data.restrictions, remove_existing=False)
            await self._insert_medals(map_id, data.medals, remove_existing=False)
            job_status = None
            if data.playtesting == "In Progress":
                metadata = PlaytestCreatePartialDTO(data.code, data.difficulty)
                playtest_id = await self.create_playtest_meta_partial(metadata)
                message_data = MessageQueueCreatePlaytest(data.code, playtest_id)
                idempotency_key = f"map:submit:{map_id}"
                job_status = await self.publish_message(
                    routing_key="api.playtest.create",
                    data=message_data,
                    headers=request.headers,
                    idempotency_key=idempotency_key,
                )

        map_data = await self.fetch_maps(single=True, filters=MapSearchFilters(code=data.code))

        if data.playtesting == "Approved" and newsfeed_service:
            event_payload = NewsfeedNewMap(
                code=map_data.code,
                map_name=map_data.map_name,
                difficulty=map_data.difficulty,
                creators=[x.name for x in map_data.creators],
                banner_url=map_data.map_banner,
                official=data.official,
                title=data.title,
            )

            event = NewsfeedEvent(
                id=None,
                timestamp=dt.datetime.now(dt.timezone.utc),
                payload=event_payload,
                event_type="new_map",
            )
            await newsfeed_service.create_and_publish(event, headers=request.headers, use_pool=True)

        return CreateMapReturnDTO(job_status, map_data)

    @_handle_exceptions
    async def patch_map(self, code: OverwatchCode, data: MapPatchDTO) -> MapReadDTO:
        """Edit a map.

        Looks up the map by code, then updates the core row and replaces related data
        (creators, mechanics, restrictions, medals) as provided. Returns the updated map.

        Args:
            code (OverwatchCode): Map code to edit.
            data (MapPatchDTO): Partial update payload.

        Returns:
            MapReadDTO: The updated map.

        """
        map_id = await self._lookup_id(code)
        async with self._conn.transaction():
            await self._edit_core_map_data(code, data)
            await self._insert_creators(map_id, data.creators, remove_existing=True)
            await self._insert_mechanics(map_id, data.mechanics, remove_existing=True)
            await self._insert_restrictions(map_id, data.restrictions, remove_existing=True)
            await self._insert_medals(map_id, data.medals, remove_existing=True)
            final_code = data.code if data.code is not msgspec.UNSET else code
            return await self.fetch_maps(single=True, filters=MapSearchFilters(code=final_code))

    async def send_map_to_playtest(
        self,
        *,
        code: OverwatchCode,
        data: SendToPlaytestDTO,
        request: Request,
    ) -> JobStatus:
        """Send a map back to playtest."""
        map_id = await self._lookup_id(code)
        current_map_data = await self.fetch_maps(single=True, filters=MapSearchFilters(code=code))
        if current_map_data.playtesting == "In Progress":
            raise CustomHTTPException(detail="Map is already in playtest", status_code=HTTP_400_BAD_REQUEST)
        async with self._conn.transaction():
            await self.convert_map_to_legacy(code)
            await self.patch_map(code, MapPatchDTO(playtesting="In Progress"))
            payload = PlaytestCreatePartialDTO(code, data.initial_difficulty)
            playtest_id = await self.create_playtest_meta_partial(payload)
        message_data = MessageQueueCreatePlaytest(code, playtest_id)
        idempotency_key = f"map:send-to-playtest:{map_id}:{playtest_id}"
        job_status = await self.publish_message(
            routing_key="api.playtest.create",
            data=message_data,
            headers=request.headers,
            idempotency_key=idempotency_key,
        )
        return job_status

    async def create_playtest_meta_partial(self, data: PlaytestCreatePartialDTO) -> int:
        """Create Playtest Meta Partial.

        Inserts a `playtests.meta` record using the map's ID and the midpoint of the
        provided initial difficulty.

        Args:
            data (PlaytestCreatePartialDTO): Partial playtest metadata.

        Returns:
            int: The new playtest meta ID.

        """
        map_id = await self._lookup_id(data.code)
        query = """
            INSERT INTO playtests.meta (
                map_id, initial_difficulty
            ) VALUES ($1, $2)
            RETURNING id;
        """
        return await self._conn.fetchval(query, map_id, DIFFICULTY_MIDPOINTS[data.initial_difficulty])

    async def fetch_partial_map(self, code: OverwatchCode) -> MapReadPartialDTO:
        """Fetch a partial of a Map.

        Useful when initializing a playtest where a subset of fields is sufficient.

        Args:
            code (OverwatchCode): Map code.

        Returns:
            MapReadPartialDTO: Minimal map data for playtest setup.

        Raises:
            CustomHTTPException: If the map is not found.

        """
        map_id = await self._lookup_id(code)
        query = """
            SELECT
                m.id,
                m.code,
                m.map_name,
                m.checkpoints,
                pm.initial_difficulty AS difficulty,
                array_agg(DISTINCT u.nickname) AS creator_names
            FROM core.maps AS m
            LEFT JOIN maps.creators AS c ON c.map_id = m.id AND c.is_primary
            LEFT JOIN core.users AS u ON c.user_id = u.id
            LEFT JOIN playtests.meta AS pm ON m.id = pm.map_id
            WHERE m.id = $1
            GROUP BY m.id,
                m.code,
                m.map_name,
                m.checkpoints,
                pm.initial_difficulty,
                u.id;

        """
        row = await self._conn.fetchrow(query, map_id)
        if not row:
            raise CustomHTTPException(detail="Map not found", status_code=404)
        return MapReadPartialDTO(
            map_id=row["id"],
            code=row["code"],
            map_name=row["map_name"],
            checkpoints=row["checkpoints"],
            difficulty=convert_raw_difficulty_to_difficulty_all(row["difficulty"]),
            creator_name=row["creator_names"][0],
        )

    @overload
    async def fetch_maps(
        self, *, single: Literal[True], filters: MapSearchFilters, use_pool: bool = False
    ) -> MapReadDTO: ...

    @overload
    async def fetch_maps(
        self, *, single: Literal[False], filters: MapSearchFilters, use_pool: bool = False
    ) -> list[MapReadDTO]: ...

    async def fetch_maps(
        self, *, single: bool, filters: MapSearchFilters, use_pool: bool = False
    ) -> list[MapReadDTO] | MapReadDTO | None:
        """Fetch maps from the database with any filter.

        Builds SQL with `MapSearchSQLBuilder`, executes it, converts rows to `MapReadDTO`,
        and returns either a single item or a list based on `single`.

        Args:
            single (bool): If True, return the first result only.
            filters (MapSearchFilters): All supported search filters and pagination.
            use_pool (bool): Whether or not to use a pool for the connection.

        Returns:
            list[MapReadDTO] | MapReadDTO | None: Matching maps (or first map when `single=True`).

        """
        builder = MapSearchSQLBuilder(filters)
        query, args = builder.build()
        if use_pool:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(query, *args)
        else:
            rows = await self._conn.fetch(query, *args)
        _models = msgspec.convert(rows, list[MapReadDTO])
        if not _models:
            return _models
        if single:
            return _models[0]
        return _models

    @overload
    async def get_playtest_plot(self, *, thread_id: int) -> Stream: ...

    @overload
    async def get_playtest_plot(self, *, code: OverwatchCode) -> Stream: ...

    async def get_playtest_plot(
        self,
        *,
        thread_id: int | None = None,
        code: OverwatchCode | None = None,
    ) -> Stream:
        """Get plot image for a playtest.

        When `code` is provided (and `thread_id` is omitted), the initial difficulty may be
        the only datapoint—intended for early initialization before votes exist.

        Args:
            thread_id (int | None): Playtest thread ID.
            code (OverwatchCode | None): Map code.

        Returns:
            Stream: PNG image stream of the difficulty distribution plot.

        Raises:
            CustomHTTPException: If neither `code` nor `thread_id` is provided, or if no rows are found.

        """
        if code and not thread_id:
            rows = await self._conn.fetch(
                """
                    WITH target_map AS (
                        SELECT id FROM core.maps WHERE code = $1
                    )
                    SELECT initial_difficulty AS difficulty, 1 AS amount
                    FROM playtests.meta
                    WHERE map_id = (SELECT id FROM target_map) AND completed=FALSE
                    ORDER BY created_at DESC
                    LIMIT 1;
                """,
                code,
            )
        elif thread_id:
            rows = await self._conn.fetch(
                """
                    SELECT difficulty, count(*) AS amount
                    FROM playtests.votes
                    WHERE playtest_thread_id = $1
                    GROUP BY difficulty
                    UNION ALL
                    SELECT initial_difficulty AS difficulty, 1 AS amount
                    FROM playtests.meta
                    WHERE thread_id = $1
                      AND NOT EXISTS (
                        SELECT 1 FROM playtests.votes WHERE playtest_thread_id = $1
                    );
                """,
                thread_id,
            )

        else:
            raise CustomHTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="At least one of code or thread_id is required",
            )

        if not rows:
            raise CustomHTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="No difficulty or votes found for the given thread or map.",
            )

        data: dict[DifficultyAll, int] = {
            convert_raw_difficulty_to_difficulty_all(row["difficulty"]): row["amount"] for row in rows
        }

        buffer = await build_playtest_plot(data)

        return Stream(
            buffer,
            headers={
                "content-type": "image/png",
                "content-disposition": 'attachment; filename="playtest.png"',
            },
        )

    async def _insert_core_map_data(self, data: MapCreateDTO) -> int:
        """Insert the core map row and return its ID.

        Args:
            data (MapCreateDTO): Map creation payload.

        Returns:
            int: Newly created map ID.

        """
        query = """
            INSERT INTO core.maps (
                code, map_name, category, checkpoints, description, difficulty,
                raw_difficulty, hidden, archived, official, playtesting, title, custom_banner
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            RETURNING id
        """
        raw_difficulty = DIFFICULTY_MIDPOINTS[data.difficulty]
        _id = await self._conn.fetchval(
            query,
            data.code,
            data.map_name,
            data.category,
            data.checkpoints,
            data.description,
            data.difficulty,
            raw_difficulty,
            data.hidden,
            data.archived,
            data.official,
            data.playtesting,
            data.title,
            data.custom_banner,
        )
        return _id

    async def _edit_core_map_data(self, code: OverwatchCode, data: MapPatchDTO) -> None:
        """Update core map columns from a patch payload.

        Converts `difficulty` to `raw_difficulty` when present and performs a dynamic
        `SET` based on provided fields (excluding related collections).

        Args:
            code (OverwatchCode): Code of the map to update.
            data (MapPatchDTO): Partial update payload.

        """
        ignore = ["creators", "mechanics", "restrictions", "medals"]
        cleaned: dict[str, Any] = {
            k: v for k, v in msgspec.structs.asdict(data).items() if v is not msgspec.UNSET and k not in ignore
        }

        if "difficulty" in cleaned:
            cleaned["raw_difficulty"] = DIFFICULTY_MIDPOINTS[cleaned["difficulty"]]

        if cleaned:
            args = [code, *list(cleaned.values())]

            set_clauses = [f"{col} = ${idx}" for idx, col in enumerate(cleaned.keys(), start=2)]

            query = f"UPDATE core.maps SET {', '.join(set_clauses)} WHERE code = $1"
            await self._conn.execute(query, *args)

    async def _lookup_id(self, code: OverwatchCode) -> int:
        """Look up a map's internal ID by code.

        Args:
            code (OverwatchCode): Map code.

        Returns:
            int: Internal map ID.

        Raises:
            CustomHTTPException: If the map code does not exist.

        """
        query = "SELECT id FROM core.maps WHERE code=$1"
        map_id = await self._conn.fetchval(query, code)
        if not map_id:
            raise CustomHTTPException("Map not found", status_code=404)
        return map_id

    async def _insert_creators(
        self,
        map_id: int,
        creators: Sequence[Creator] | msgspec.UnsetType,
        *,
        remove_existing: bool,
    ) -> None:
        """Insert or replace creators linked to a map.

        Args:
            map_id (int): Target map ID.
            creators (Sequence[Creator] | msgspec.UnsetType): Creators to persist; skipped if UNSET.
            remove_existing (bool): If True, clears existing rows before inserting.

        """
        if creators is msgspec.UNSET:
            return

        if remove_existing:
            remove_query = "DELETE FROM maps.creators WHERE map_id=$1"
            await self._conn.execute(remove_query, map_id)

        query = "INSERT INTO maps.creators (map_id, user_id, is_primary) VALUES ($1, $2, $3)"
        for c in creators:
            await self._conn.execute(query, map_id, c.id, c.is_primary)

    async def _insert_mechanics(
        self,
        map_id: int,
        mechanics: Sequence[Mechanics] | msgspec.UnsetType | None,
        *,
        remove_existing: bool,
    ) -> None:
        """Insert or replace mechanics linked to a map.

        Args:
            map_id (int): Target map ID.
            mechanics (Sequence[Mechanics] | msgspec.UnsetType | None): Mechanics to persist; skipped if UNSET.
                If empty/None, nothing is inserted.
            remove_existing (bool): If True, clears existing rows before inserting.

        """
        if mechanics is msgspec.UNSET:
            return

        if remove_existing:
            remove_query = "DELETE FROM maps.mechanic_links WHERE map_id=$1"
            await self._conn.execute(remove_query, map_id)

        if not mechanics:
            return

        query = """
            INSERT INTO maps.mechanic_links (map_id, mechanic_id)
            SELECT $1, m.id AS mechanic_id
            FROM maps.mechanics m WHERE m.name = $2;
        """
        for m in mechanics:
            await self._conn.execute(query, map_id, m)

    async def _insert_restrictions(
        self,
        map_id: int,
        restrictions: Sequence[Restrictions] | msgspec.UnsetType | None,
        *,
        remove_existing: bool,
    ) -> None:
        """Insert or replace restrictions linked to a map.

        Args:
            map_id (int): Target map ID.
            restrictions (Sequence[Restrictions] | msgspec.UnsetType | None): Restrictions to persist; skipped if UNSET.
                If empty/None, nothing is inserted.
            remove_existing (bool): If True, clears existing rows before inserting.

        """
        if restrictions is msgspec.UNSET:
            return

        if remove_existing:
            remove_query = "DELETE FROM maps.restriction_links WHERE map_id=$1"
            await self._conn.execute(remove_query, map_id)

        if not restrictions:
            return

        query = """
            INSERT INTO maps.restriction_links (map_id, restriction_id)
            SELECT $1, m.id AS restriction_id
            FROM maps.restrictions m WHERE m.name = $2;
        """
        for m in restrictions:
            await self._conn.execute(query, map_id, m)

    async def _insert_medals(
        self,
        map_id: int,
        medals: Medals | msgspec.UnsetType | None,
        *,
        remove_existing: bool,
    ) -> None:
        """Insert or replace medal thresholds for a map.

        Args:
            map_id (int): Target map ID.
            medals (Medals | msgspec.UnsetType | None): Medal thresholds; skipped if UNSET.
                If None, nothing is inserted.
            remove_existing (bool): If True, clears existing rows before inserting.

        """
        if medals is msgspec.UNSET:
            return

        if remove_existing:
            remove_query = "DELETE FROM maps.medals WHERE map_id=$1"
            await self._conn.execute(remove_query, map_id)

        if not medals:
            return

        query = "INSERT INTO maps.medals (map_id, gold, silver, bronze) VALUES ($1, $2, $3, $4)"
        await self._conn.execute(query, map_id, medals.gold, medals.silver, medals.bronze)

    async def _insert_guide(self, map_id: int, guide: GuideURL | None, creator_id: int | None) -> None:
        """Insert a guide URL for a map and user if both are provided.

        Args:
            map_id (int): Target map ID.
            guide (GuideURL | None): Guide URL to add.
            creator_id (int | None): User ID who owns the guide.

        """
        if not guide:
            return
        if not creator_id:
            return
        query = """
            INSERT INTO maps.guides (
                map_id, url, user_id
            ) VALUES ($1, $2, $3);
        """
        await self._conn.execute(query, map_id, guide, creator_id)

    async def _archival_helper(self, code: OverwatchCode, archive: bool) -> None:
        """Set a map's archived status.

        Helper used by archive/unarchive operations to update the `archived` flag.

        Args:
            code: Overwatch map code to update.
            archive: Whether the map should be archived (True) or unarchived (False).
        """
        data = MapPatchDTO(archived=archive)
        await self._edit_core_map_data(code, data)

    async def archive_map(self, code: OverwatchCode) -> None:
        """Archive a map.

        Marks the map identified by `code` as archived.

        Args:
            code: Overwatch map code to archive.
        """
        await self._archival_helper(code, True)

    async def bulk_archive_map(self, codes: list[OverwatchCode]) -> None:
        """Archive multiple maps.

        Iterates over the provided codes and marks each as archived.

        Args:
            codes: List of Overwatch map codes to archive.
        """
        for code in codes:
            await self._archival_helper(code, True)

    async def unarchive_map(self, code: OverwatchCode) -> None:
        """Unarchive a map.

        Marks the map identified by `code` as not archived.

        Args:
            code: Overwatch map code to unarchive.
        """
        await self._archival_helper(code, False)

    async def bulk_unarchive_map(self, codes: list[OverwatchCode]) -> None:
        """Unarchive multiple maps.

        Iterates over the provided codes and marks each as not archived.

        Args:
            codes: List of Overwatch map codes to unarchive.
        """
        for code in codes:
            await self._archival_helper(code, False)

    async def get_guides(self, code: OverwatchCode, include_records: bool = False) -> list[GuideFull]:
        """Fetch guides for a map with resolved username list.

        Args:
            code (OverwatchCode): Map code.
            include_records (bool): Whether or not to include record videos.

        Returns:
            list[GuideFull]: Guides with owner info.

        """
        query = """
        WITH m AS (
            SELECT id
            FROM core.maps
            WHERE code = $1
        )
        SELECT
            g.user_id,
            g.url,
            ARRAY_REMOVE(
                ARRAY[u.nickname, u.global_name]::text[]
                    || ARRAY(
                        SELECT owu.username::text
                        FROM users.overwatch_usernames owu
                        WHERE owu.user_id = g.user_id
                       ),
                NULL
            ) AS usernames
        FROM m
        JOIN maps.guides g ON g.map_id = m.id
        LEFT JOIN core.users u ON u.id = g.user_id
        UNION ALL
        SELECT
            c.user_id,
            c.video AS url,
            ARRAY_REMOVE(
                ARRAY[u.nickname, u.global_name]::text[]
                    || ARRAY(
                        SELECT owu.username::text
                        FROM users.overwatch_usernames owu
                        WHERE owu.user_id = c.user_id
                       ),
                NULL
            ) AS usernames
        FROM m
        JOIN LATERAL (
            SELECT DISTINCT ON (c.user_id)
                c.user_id, c.video, c.inserted_at, c.id
            FROM core.completions c
            WHERE c.map_id = m.id
              AND c.verified = TRUE
              AND c.completion = FALSE
              AND c.video IS NOT NULL
            ORDER BY c.user_id, c.inserted_at DESC, c.id DESC
            ) c ON TRUE
        LEFT JOIN core.users u ON u.id = c.user_id
        WHERE $2::bool IS TRUE;
        """
        res = await self._conn.fetch(query, code, include_records)
        log.info(res)
        return msgspec.convert(res, list[GuideFull])

    async def delete_guide(self, code: OverwatchCode, user_id: int) -> None:
        """Delete a guide for the given map code and user.

        Args:
            code (OverwatchCode): Map code.
            user_id (int): Guide owner's user ID.

        """
        query = """
            DELETE FROM maps.guides g
            USING core.maps m
            WHERE g.map_id = m.id
            AND m.code = $1
            AND g.user_id = $2;
        """
        await self._conn.execute(query, code, user_id)

    async def edit_guide(self, code: OverwatchCode, user_id: int, url: GuideURL) -> Guide:
        """Edit a guide URL for a given map code and user.

        Args:
            code (OverwatchCode): Map code.
            user_id (int): Guide owner's user ID.
            url (GuideURL): New URL to replace.

        Returns:
            Guide: The updated guide.

        """
        query = """
            WITH target_map AS (
                SELECT id AS map_id FROM core.maps WHERE code = $1
            )
            UPDATE maps.guides g
            SET url = $3
            FROM target_map
            WHERE g.map_id = target_map.map_id AND g.user_id = $2
            RETURNING g.user_id, g.url;
        """
        res = await self._conn.fetchrow(query, code, user_id, url)
        return msgspec.convert(res, Guide)

    async def create_guide(self, code: OverwatchCode, data: Guide) -> Guide:
        """Create a guide for a given map.

        Args:
            code (OverwatchCode): Map code.
            data (Guide): Guide payload with user ID and URL.

        Returns:
            Guide: The created guide.

        """
        query = """
        WITH target_map AS (
            SELECT id AS map_id
            FROM core.maps
            WHERE code = $1
        )
        INSERT INTO maps.guides (map_id, user_id, url)
        SELECT target_map.map_id, $2, $3
        FROM target_map
        RETURNING user_id, url;
        """
        res = await self._conn.fetchrow(query, code, data.user_id, data.url)
        return msgspec.convert(res, Guide)

    async def get_affected_users(self, code: OverwatchCode) -> list[int]:
        """Get IDs of users affected by a map change.

        Args:
            code (OverwatchCode): Map code.

        Returns:
            list[int]: Affected user IDs.

        """
        query = """
        WITH target_map AS (
            SELECT id AS map_id
            FROM core.maps
            WHERE code = $1
        )
        SELECT DISTINCT c.user_id
        FROM core.completions AS c
        JOIN target_map AS t ON c.map_id = t.map_id
        WHERE c.legacy IS FALSE;
        """
        rows = await self._conn.fetch(query, code)
        return msgspec.convert(rows, list[int])

    async def get_map_mastery_data(self, user_id: int, map_name: OverwatchMap | None = None) -> list[MapMasteryData]:
        """Get mastery data for a user, optionally scoped to a map.

        Args:
            user_id (int): Target user ID.
            map_name (OverwatchMap | None): Optional map filter.

        Returns:
            list[MapMasteryData]: Mastery rows for the user (and map if provided).

        """
        return await get_map_mastery_data(self._conn, user_id, map_name)

    async def update_mastery(self, data: MapMasteryCreateDTO) -> MapMasteryCreateReturnDTO:
        """Create or update mastery data.

        Inserts a new mastery record or updates the existing one if different.

        Args:
            data (MapMasteryCreateDTO): Mastery payload.

        Returns:
            MapMasteryCreateReturnDTO: Result of the mastery operation.

        """
        query = """
            INSERT INTO maps.mastery (user_id, map_name, medal)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, map_name)
            DO UPDATE
            SET medal = excluded.medal
            WHERE maps.mastery.medal IS DISTINCT FROM excluded.medal
            RETURNING
                map_name,
                medal,
                CASE
                    WHEN xmax::text::int = 0 THEN 'inserted'
                    ELSE 'updated'
                END AS operation_status;
        """
        row = await self._conn.fetchrow(query, data.user_id, data.map_name, data.level)
        return msgspec.convert(row, MapMasteryCreateReturnDTO | None)

    async def _check_if_any_pending_verifications(self, code: OverwatchCode) -> bool:
        query = """
        WITH target_map AS (
            SELECT
                id AS map_id
            FROM core.maps
            WHERE code = $1
        )
        SELECT EXISTS (
            SELECT 1 FROM core.completions
            WHERE map_id = (SELECT map_id FROM target_map) AND verified IS FALSE AND verification_id IS NOT NULL
        );
        """
        return await self._conn.fetchval(query, code)

    async def _remove_map_medal_entries(self, code: OverwatchCode) -> None:
        query = """
        WITH target_map AS (
            SELECT
                id AS map_id
            FROM core.maps
            WHERE code = $1
        )
        DELETE FROM maps.medals WHERE map_id = (SELECT map_id FROM target_map);
        """
        await self._conn.execute(query, code)

    async def _convert_completions_to_legacy(self, code: OverwatchCode) -> int:
        query = """
        WITH target_map AS (
          SELECT id AS map_id
          FROM core.maps
          WHERE code = $1
        ),
        all_completions AS (
          SELECT
            CASE
              WHEN c.verified = TRUE AND c.time <= mm.gold   THEN 'Gold'
              WHEN c.verified = TRUE AND c.time <= mm.silver AND c.time > mm.gold  THEN 'Silver'
              WHEN c.verified = TRUE AND c.time <= mm.bronze AND c.time > mm.silver THEN 'Bronze'
            END AS legacy_medal,
            tm.map_id,
            c.user_id,
            c.inserted_at
          FROM target_map tm
          LEFT JOIN core.completions c ON tm.map_id = c.map_id
          LEFT JOIN maps.medals mm ON tm.map_id = mm.map_id
          WHERE legacy IS FALSE
        ),
        updated AS (
          UPDATE core.completions AS cc
          SET
            completion = ac.legacy_medal IS NULL,
            legacy = TRUE,
            legacy_medal = ac.legacy_medal
          FROM all_completions AS ac
          WHERE cc.map_id = ac.map_id
            AND cc.user_id = ac.user_id
            AND cc.inserted_at = ac.inserted_at
          RETURNING 1
        )
        SELECT COUNT(*) AS affected_rows
        FROM updated;
        """
        return await self._conn.fetchval(query, code)

    async def convert_map_to_legacy(self, code: OverwatchCode) -> int:
        """Convert a map to legacy.

        This converts all completions for a map that aren't already marked as `legacy`.
        Additionally, it will remove medals associated with the map code.

        Args:
            code (OverwatchCode): The map to convert.

        Raises:
            ValueError: If any pending verifications exist.
        """
        if await self._check_if_any_pending_verifications(code):
            raise CustomHTTPException(
                detail="Pending verifications exist for this map code.", status_code=HTTP_400_BAD_REQUEST
            )
        async with self._conn.transaction():
            await self._remove_map_medal_entries(code)
            return await self._convert_completions_to_legacy(code)

    async def override_map_quality_votes(self, code: OverwatchCode, data: QualityValueDTO) -> None:
        """Override the map quality votes for a particular map code.

        Args:
            code (OverwatchCode): The map to override.
            data (QualityValueDTO): The data for overriding.
        """
        min_quality = 1
        max_quality = 6
        if not min_quality <= data.value <= max_quality:
            raise ValueError("Quality must be between 1 and 6 (inclusive).")
        map_id = await self._lookup_id(code)
        query = """
            UPDATE maps.ratings SET quality=$2 WHERE map_id=$1
        """
        await self._conn.execute(query, map_id, data.value)

    async def get_trending_maps(self, limit: Literal[1, 3, 5, 10, 15, 20, 25]) -> list[TrendingMapReadDTO]:
        """Get trending maps."""
        query = """
        WITH
            params AS (
                SELECT
                    ($1)::int AS window_days,
                    ($2)::numeric AS half_life_days,
                    (ln(2) / NULLIF(($2)::numeric, 0))::numeric AS lambda
            ),
            bounds AS (
                SELECT
                    now() - make_interval(days => p.window_days)     AS curr_start,
                    now()                                            AS curr_end,
                    now() - make_interval(days => 2 * p.window_days) AS prev_start,
                    now() - make_interval(days => p.window_days)     AS prev_end,
                    p.lambda
                FROM params p
            ),
            maps_base AS (
                SELECT m.id, m.code, m.map_name, m.created_at
                FROM core.maps m
                WHERE m.hidden IS NOT TRUE AND m.archived IS NOT TRUE
            ),

            -- ---- CURRENT WINDOW (decayed weights, per map) ----
            clicks_curr AS (
                WITH last_clicks AS (
                    SELECT
                        c.map_id,
                        c.ip_hash,
                        MAX(c.inserted_at) AS last_at
                    FROM maps.clicks c
                    CROSS JOIN bounds b
                    WHERE c.inserted_at >= b.curr_start
                      AND c.inserted_at <  b.curr_end
                    GROUP BY c.map_id, c.ip_hash
                )
                SELECT
                    lc.map_id,
                    SUM(
                            exp(-b.lambda * EXTRACT(EPOCH FROM (b.curr_end - lc.last_at)) / 86400.0)
                    )::numeric AS w_clicks
                FROM last_clicks lc
                CROSS JOIN bounds b
                GROUP BY lc.map_id
            ),

            comps_curr AS (
                SELECT c.map_id,
                    SUM(exp(-b.lambda * EXTRACT(EPOCH FROM (b.curr_end - c.inserted_at)) / 86400.0))::numeric
                    AS w_completions
                FROM core.completions c
                CROSS JOIN bounds b
                WHERE c.inserted_at >= b.curr_start AND c.inserted_at < b.curr_end
                  AND c.verified = TRUE
                  AND COALESCE(c.legacy, FALSE) = FALSE
                -- AND c.completion = TRUE  -- add back if reliably set
                GROUP BY c.map_id
            ),
            ups_curr AS (
                SELECT c.map_id,
                    SUM(exp(-b.lambda * EXTRACT(EPOCH FROM (b.curr_end - u.inserted_at)) / 86400.0))::numeric
                    AS w_upvotes
                FROM completions.upvotes u
                JOIN core.completions c ON c.message_id = u.message_id
                CROSS JOIN bounds b
                WHERE u.inserted_at >= b.curr_start AND u.inserted_at < b.curr_end
                GROUP BY c.map_id
            ),
            -- Quality: all-time with decay (no time filter)
            q_curr AS (
                SELECT r.map_id,
                    SUM(r.quality * exp(-b.lambda * EXTRACT(
                        EPOCH FROM (b.curr_end - GREATEST(r.updated_at, r.created_at))) / 86400.0)
                    )::numeric AS num,
                    SUM(exp(-b.lambda * EXTRACT(EPOCH FROM (
                        b.curr_end - GREATEST(r.updated_at, r.created_at))) / 86400.0)
                    )::numeric AS den
                FROM maps.ratings r
                CROSS JOIN bounds b
                GROUP BY r.map_id
            ),

            -- ---- CURRENT WINDOW (RAW COUNTS to return) ----
            clicks_cnt_curr AS (
                SELECT
                    c.map_id,
                    COUNT(DISTINCT c.ip_hash)::int AS clicks_count
                FROM maps.clicks c
                CROSS JOIN bounds b
                WHERE c.inserted_at >= b.curr_start AND c.inserted_at < b.curr_end
                GROUP BY c.map_id
            ),
            comps_cnt_curr AS (
                SELECT c.map_id, COUNT(*)::int AS completions_count
                FROM core.completions c
                CROSS JOIN bounds b
                WHERE c.inserted_at >= b.curr_start AND c.inserted_at < b.curr_end
                  AND c.verified = TRUE
                  AND COALESCE(c.legacy, FALSE) = FALSE
                -- AND c.completion = TRUE
                GROUP BY c.map_id
            ),
            ups_cnt_curr AS (
                SELECT c.map_id, COUNT(u.id)::int AS upvotes_count
                FROM completions.upvotes u
                JOIN core.completions c ON c.message_id = u.message_id
                CROSS JOIN bounds b
                WHERE u.inserted_at >= b.curr_start AND u.inserted_at < b.curr_end
                GROUP BY c.map_id
            ),

            -- ---- PREVIOUS WINDOW (for momentum) ----
            clicks_prev AS (
                WITH last_clicks AS (
                    SELECT
                        c.map_id,
                        c.ip_hash,
                        MAX(c.inserted_at) AS last_at
                    FROM maps.clicks c
                    CROSS JOIN bounds b
                    WHERE c.inserted_at >= b.prev_start
                      AND c.inserted_at <  b.prev_end
                    GROUP BY c.map_id, c.ip_hash
                )
                SELECT
                    lc.map_id,
                    SUM(
                            exp(-b.lambda * EXTRACT(EPOCH FROM (b.prev_end - lc.last_at)) / 86400.0)
                    )::numeric AS w_clicks_prev
                FROM last_clicks lc
                CROSS JOIN bounds b
                GROUP BY lc.map_id
            ),

            comps_prev AS (
                SELECT c.map_id,
                    SUM(exp(-b.lambda * EXTRACT(EPOCH FROM (b.prev_end - c.inserted_at)) / 86400.0))::numeric
                    AS w_completions_prev
                FROM core.completions c
                CROSS JOIN bounds b
                WHERE c.inserted_at >= b.prev_start AND c.inserted_at < b.prev_end
                  AND c.verified = TRUE
                  AND COALESCE(c.legacy, FALSE) = FALSE
                GROUP BY c.map_id
            ),
            ups_prev AS (
                SELECT c.map_id,
                    SUM(exp(-b.lambda * EXTRACT(EPOCH FROM (b.prev_end - u.inserted_at)) / 86400.0))::numeric
                    AS w_upvotes_prev
                FROM completions.upvotes u
                JOIN core.completions c ON c.message_id = u.message_id
                CROSS JOIN bounds b
                WHERE u.inserted_at >= b.prev_start AND u.inserted_at < b.prev_end
                GROUP BY c.map_id
            ),

            -- ---- Assemble current & previous per map ----
            agg_curr AS (
                SELECT
                    mb.id AS map_id,
                    mb.code,
                    mb.map_name,
                    mb.created_at,
                    COALESCE(cc.w_clicks, 0)      AS w_clicks,
                    COALESCE(co.w_completions, 0) AS w_completions,
                    COALESCE(uu.w_upvotes, 0)     AS w_upvotes,
                    CASE WHEN qc.den > 0 THEN qc.num / qc.den ELSE NULL END AS w_quality,
                    COALESCE(cct.clicks_count, 0)      AS clicks_count,
                    COALESCE(cot.completions_count, 0) AS completions_count,
                    COALESCE(uut.upvotes_count, 0)     AS upvotes_count
                FROM maps_base mb
                LEFT JOIN clicks_curr     cc  ON cc.map_id  = mb.id
                LEFT JOIN comps_curr      co  ON co.map_id  = mb.id
                LEFT JOIN ups_curr        uu  ON uu.map_id  = mb.id
                LEFT JOIN q_curr          qc  ON qc.map_id  = mb.id
                LEFT JOIN clicks_cnt_curr cct ON cct.map_id = mb.id
                LEFT JOIN comps_cnt_curr  cot ON cot.map_id = mb.id
                LEFT JOIN ups_cnt_curr    uut ON uut.map_id = mb.id
            ),
            agg_prev AS (
                SELECT
                    mb.id AS map_id,
                    COALESCE(cc.w_clicks_prev, 0)      AS w_clicks_prev,
                    COALESCE(co.w_completions_prev, 0) AS w_completions_prev,
                    COALESCE(uu.w_upvotes_prev, 0)     AS w_upvotes_prev
                FROM maps_base mb
                LEFT JOIN clicks_prev cc ON cc.map_id = mb.id
                LEFT JOIN comps_prev  co ON co.map_id = mb.id
                LEFT JOIN ups_prev    uu ON uu.map_id = mb.id
            ),

            -- ---- p95s for normalization (current window only) ----
            pcts AS (
                SELECT
                            percentile_disc(0.95) WITHIN GROUP (ORDER BY a.w_clicks)      AS p95_clicks,
                            percentile_disc(0.95) WITHIN GROUP (ORDER BY a.w_completions) AS p95_completions,
                            percentile_disc(0.95) WITHIN GROUP (ORDER BY a.w_upvotes)     AS p95_upvotes,
                            percentile_disc(0.95) WITHIN GROUP (ORDER BY a.w_quality)     AS p95_quality
                FROM agg_curr a
            ),

            -- ---- Normalize + momentum (for scoring only) ----
            scored AS (
                SELECT
                    a.map_id, a.code, a.map_name, a.created_at,
                    a.w_clicks, a.w_completions, a.w_upvotes, a.w_quality,
                    a.clicks_count, a.completions_count, a.upvotes_count,
                    LEAST(1.0, CASE WHEN p.p95_clicks      > 0 THEN log(1 + a.w_clicks)      / log(1 + p.p95_clicks)
                    ELSE 0 END) AS n_clicks,
                    LEAST(1.0, CASE WHEN p.p95_completions > 0 THEN log(1 + a.w_completions) / log(1 + p.p95_completions
                    )
                    ELSE 0 END) AS n_completions,
                    LEAST(1.0, CASE WHEN p.p95_upvotes     > 0 THEN log(1 + a.w_upvotes)     / log(1 + p.p95_upvotes)
                    ELSE 0 END) AS n_upvotes,
                    LEAST(1.0, CASE WHEN p.p95_quality     > 0 AND a.w_quality IS NOT NULL
                                        THEN log(1 + a.w_quality)     / log(1 + p.p95_quality)     ELSE 0 END
                            ) AS n_quality
                FROM agg_curr a CROSS JOIN pcts p
            ),
            momentum AS (
                SELECT
                    s.map_id,
                    (s.w_clicks + s.w_completions + s.w_upvotes) AS curr_vol,
                    (
                        COALESCE(pv.w_clicks_prev,0) + COALESCE(pv.w_completions_prev,0) + COALESCE(pv.w_upvotes_prev,0)
                    ) AS prev_vol
                FROM scored s
                LEFT JOIN agg_prev pv ON pv.map_id = s.map_id
            ),
            final AS (
                SELECT
                    s.*,
                    CASE WHEN m.prev_vol > 0
                    THEN (m.curr_vol - m.prev_vol) / m.prev_vol ELSE NULL END AS momentum_ratio,
                    1.0 + 0.30 * exp(-EXTRACT(EPOCH FROM (now() - s.created_at)) / 86400.0 / 30.0)     AS new_map_boost
                FROM scored s
                JOIN momentum m ON m.map_id = s.map_id
            )

        SELECT
            code,
            map_name,
            -- raw counts for the current window
            clicks_count       AS clicks,
            completions_count  AS completions,
            upvotes_count      AS upvotes,
            -- extra context
            GREATEST(0, COALESCE(momentum_ratio, 0)) AS momentum,
            (
                ($3 * n_clicks) +
                ($4 * n_completions) +
                ($5 * n_upvotes) +
                ($6 * n_quality) +
                ($7 * GREATEST(0, COALESCE(momentum_ratio, 0)))
                ) * new_map_boost AS trend_score
        FROM final
        ORDER BY trend_score DESC
        LIMIT $8;
        """
        window_days = 14
        half_life_days = 3.0
        w_clicks = 0.35
        w_completions = 0.35
        w_upvotes = 0.20
        w_quality = 0.10
        w_momentum = 0.20
        rows = await self._conn.fetch(
            query,
            window_days,
            half_life_days,
            w_clicks,
            w_completions,
            w_upvotes,
            w_quality,
            w_momentum,
            limit,
        )
        return msgspec.convert(rows, list[TrendingMapReadDTO])

    async def _link_two_map_codes(
        self,
        *,
        code_1: OverwatchCode,
        code_2: OverwatchCode,
    ) -> None:
        """Establish a bidirectional link between two map codes.

        Updates both map records so that each one's `linked_code` field references
        the other, ensuring a symmetrical relationship in the database.

        Args:
            code_1 (OverwatchCode): The first map code to link.
            code_2 (OverwatchCode): The second map code to link.

        """
        query = "UPDATE core.maps SET linked_code=$2 WHERE code=$1;"
        async with self._conn.transaction():
            await self._conn.execute(query, code_1, code_2)
            await self._conn.execute(query, code_2, code_1)

    def _create_cloned_map_data_payload(
        self,
        *,
        map_data: MapReadDTO,
        code: OverwatchCode,
        is_official: bool,
    ) -> MapCreateDTO:
        """Create a map creation payload by cloning an existing map.

        Generates a `MapCreateDTO` from an existing `MapReadDTO`, preserving all
        core fields such as creators, category, mechanics, and medals, while assigning
        a new map code. The clone is marked as hidden, unofficial, and playtesting-approved.

        Args:
            map_data (MapReadDTO): The source map data to clone.
            code (OverwatchCode): The new map code to assign to the cloned map.

        Returns:
            MapCreateDTO: The fully prepared DTO for creating the cloned map.
        """
        creators = [Creator(c.id, c.is_primary) for c in map_data.creators]
        guide_url = map_data.guides[0] if map_data.guides else ""
        create_map_payload = MapCreateDTO(
            code=code,
            map_name=map_data.map_name,
            category=map_data.category,
            creators=creators,
            checkpoints=map_data.checkpoints,
            difficulty=map_data.difficulty,
            official=is_official,
            hidden=is_official,
            playtesting="In Progress" if is_official else "Approved",
            archived=False,
            mechanics=map_data.mechanics,
            restrictions=map_data.restrictions,
            description=map_data.description,
            medals=map_data.medals,
            guide_url=guide_url,
            title=map_data.title,
            custom_banner=map_data.map_banner,
        )
        return create_map_payload

    async def link_official_and_unofficial_map(
        self,
        *,
        request: Request,
        official_code: OverwatchCode,
        unofficial_code: OverwatchCode,
        newsfeed: NewsfeedService,
    ) -> tuple[JobStatus | None, bool]:
        """Link an official and unofficial map, cloning as needed.

        Determines which maps exist and performs the appropriate operation:
        - Clone the official map if only it exists.
        - Clone the unofficial map and initiate playtesting if only it exists.
        - Link both directly if both exist.

        Args:
            request (Request): The active HTTP request context.
            official_code (OverwatchCode): The official map code.
            unofficial_code (OverwatchCode): The unofficial map code.
            newsfeed (NewsfeedService): Manages newsfeed events.

        Returns:
            A tuple where the first item is:
                JobStatus | None: The resulting job status if a clone or playtest was created;
                    otherwise `None` when only a link operation was performed.
            The second item is:
                in_playtest: bool

        Raises:
            CustomHTTPException: If neither an official nor an unofficial map is provided.
        """
        official_map = await self.fetch_maps(single=True, filters=MapSearchFilters(code=official_code))
        unofficial_map = await self.fetch_maps(single=True, filters=MapSearchFilters(code=unofficial_code))

        if not official_map and not unofficial_map:
            raise CustomHTTPException(
                detail="You must have submit with at least one of official_code or unofficial_code.",
                status_code=HTTP_400_BAD_REQUEST,
            )

        if official_map.linked_code or unofficial_map.linked_code:
            raise CustomHTTPException(
                detail=(
                    "One or both maps already have a linked map code.\n"
                    f"Official ({official_code}): {official_map.linked_code}\n"
                    f"Unofficial CN ({unofficial_code}): {unofficial_map.linked_code}"
                ),
                status_code=HTTP_400_BAD_REQUEST,
            )

        needs_clone_only = official_map and not unofficial_map
        needs_clone_and_playtest = not official_map and unofficial_map
        needs_link_only = official_map and unofficial_map

        if needs_clone_only:
            log.debug("needs clone only is TRUE")
            payload = self._create_cloned_map_data_payload(
                map_data=official_map, code=unofficial_code, is_official=False
            )
            res = await self.create_map(payload, request, newsfeed)
            await self._link_two_map_codes(code_1=official_code, code_2=unofficial_code)
            return res.job_status, False

        if needs_clone_and_playtest:
            log.debug("needs clone AND playtest is TRUE")
            payload = self._create_cloned_map_data_payload(
                map_data=unofficial_map, code=official_code, is_official=True
            )
            res = await self.create_map(payload, request, newsfeed)
            await self._link_two_map_codes(code_1=official_code, code_2=unofficial_code)
            return res.job_status, True

        if needs_link_only:
            log.debug("needs link only is TRUE")
            await self._link_two_map_codes(code_1=official_code, code_2=unofficial_code)
            return None, False

        return None, False

    async def unlink_two_map_codes(
        self,
        official_code: OverwatchCode,
        unofficial_code: OverwatchCode,
    ) -> None:
        """Unlink two map codes.

        Args:
            official_code (OverwatchCode): The official map code.
            unofficial_code (OverwatchCode): The unofficial map code.
        """
        official_map = await self.fetch_maps(single=True, filters=MapSearchFilters(code=official_code))
        unofficial_map = await self.fetch_maps(single=True, filters=MapSearchFilters(code=unofficial_code))
        if not official_map or not unofficial_map:
            raise CustomHTTPException(
                detail=(
                    "One or both codes found no matching maps.\n"
                    f"Official ({official_code}): {'FOUND' if official_map else 'NOT FOUND'}\n"
                    f"Unofficial CN ({unofficial_code}): {'FOUND' if unofficial_map else 'NOT FOUND'}"
                ),
                status_code=HTTP_400_BAD_REQUEST,
            )
        if not official_map.linked_code or not unofficial_map.linked_code:
            raise CustomHTTPException(
                detail=(
                    "One or both codes have no linked map.\n"
                    f"Official ({official_code}): Linked to {official_map.linked_code}\n"
                    f"Unofficial CN ({unofficial_code}): Linked to {unofficial_map.linked_code}"
                ),
                status_code=HTTP_400_BAD_REQUEST,
            )

        if official_map.linked_code != unofficial_code and unofficial_map.linked_code != official_code:
            raise CustomHTTPException(
                detail=(
                    "The two maps given do not link to each other. "
                    f"Official ({official_code}): Linked to {official_map.linked_code} | "
                    f"Unofficial CN ({unofficial_code}): Linked to {unofficial_map.linked_code}"
                ),
                status_code=HTTP_400_BAD_REQUEST,
            )

        query = "UPDATE core.maps SET linked_code=NULL WHERE code=$1;"
        async with self._conn.transaction():
            await self._conn.execute(query, official_code)
            await self._conn.execute(query, unofficial_code)


async def provide_map_service(conn: Connection, state: State) -> MapService:
    """Provide MapService DI.

    Args:
        conn (Connection): Active asyncpg connection.
        state: Application state.

    Returns:
        MapService: New service instance.

    """
    return MapService(conn, state)
