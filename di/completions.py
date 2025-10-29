from logging import getLogger
from typing import Any

import asyncpg
import msgspec
from asyncpg import Connection
from genjipk_sdk.models import (
    CompletionCreateDTO,
    CompletionPatchDTO,
    CompletionReadDTO,
    CompletionSubmissionReadDTO,
    MessageQueueCompletionsCreate,
    MessageQueueVerificationChange,
    UpvoteUpdateDTO,
)
from genjipk_sdk.models.completions import (
    CompletionVerificationPutDTO,
    PendingVerification,
    SuspiciousCompletionReadDTO,
    SuspiciousCompletionWriteDTO,
    UpvoteCreateDTO,
)
from genjipk_sdk.models.jobs import JobStatus, SubmitCompletionReturnDTO, UpvoteSubmissionReturnDTO
from genjipk_sdk.utilities import DifficultyAll
from genjipk_sdk.utilities._types import OverwatchCode
from litestar import Request
from litestar.datastructures import State
from litestar.status_codes import HTTP_400_BAD_REQUEST

from di.base import BaseService
from utilities.errors import CustomHTTPException

log = getLogger(__name__)


class CompletionsService(BaseService):
    async def get_completions_for_user(
        self,
        user_id: int,
        difficulty: DifficultyAll | None = None,
        page_size: int = 10,
        page_number: int = 1,
    ) -> list[CompletionReadDTO]:
        """Retrieve verified completions for a user.

        Args:
            user_id (int): ID of the user to fetch completions for.
            difficulty (DifficultyAll | None): Optional difficulty filter.
            page_size (int): Page size; one of 10, 20, 25, 50.
            page_number (int): 1-based page number.

        Returns:
            list[CompletionReadDTO]: A list of completions including map metadata,
            times, verification status, ranks, medals, and display names.

        """
        query = """
        WITH latest_per_user_per_map AS (
            -- Latest verified submission per (user, map) across ALL users
            SELECT DISTINCT ON (c.user_id, c.map_id)
                c.user_id,
                c.map_id,
                c.time,
                c.completion,
                c.verified,
                c.screenshot,
                c.video,
                c.legacy,
                c.legacy_medal,
                c.message_id,
                c.inserted_at
            FROM core.completions c
            WHERE c.verified = TRUE
            ORDER BY c.user_id, c.map_id, c.inserted_at DESC
        ),
            ranked AS (
                -- Compute true global rank per map (older wins ties)
                SELECT
                    l.*,
                    CASE
                        WHEN l.completion = FALSE THEN
                                    rank() OVER (
                                PARTITION BY l.map_id
                                ORDER BY l.time ASC, l.inserted_at ASC
                                )
                        ELSE NULL::int
                    END AS rank
                FROM latest_per_user_per_map l
            ),
            with_map AS (
                -- Join map metadata and medals; normalize difficulty to top-level buckets
                SELECT
                    m.code,
                    m.map_name,
                    m.difficulty,
                    m.raw_difficulty,
                    regexp_replace(m.difficulty, ' (\\+|\\-)$', '') AS top_difficulty,
                    r.user_id,
                    r.time,
                    r.completion,
                    r.verified,
                    r.screenshot,
                    r.video,
                    r.legacy,
                    r.legacy_medal,
                    r.message_id,
                    r.inserted_at,
                    r.rank,
                    (r.rank IS NULL) AS is_nonrankable,
                    md.gold,
                    md.silver,
                    md.bronze
                FROM ranked r
                JOIN core.maps m ON m.id = r.map_id
                LEFT JOIN maps.medals md ON md.map_id = r.map_id
            ),
            user_names AS (
                SELECT
                    u.id AS user_id,
                            MAX(owu.username) FILTER (WHERE owu.is_primary) AS primary_ow,
                    array_remove(array_agg(DISTINCT owu.username), NULL) AS all_ow_names,
                    u.nickname,
                    u.global_name
                FROM core.users u
                LEFT JOIN users.overwatch_usernames owu ON owu.user_id = u.id
                WHERE u.id = $1
                GROUP BY u.id, u.nickname, u.global_name
            ),
            name_split AS (
                SELECT
                    un.user_id,
                    COALESCE(
                            NULLIF(un.primary_ow, ''),
                            NULLIF(un.nickname, ''),
                            NULLIF(un.global_name, ''),
                            'Unknown User'
                    ) AS name,
                    NULLIF((
                                SELECT string_agg(DISTINCT v, ', ')
                                FROM unnest(
                                        ARRAY[
                                            NULLIF(un.global_name, ''),
                                            NULLIF(un.nickname, '')
                                            ] || COALESCE(un.all_ow_names, '{}')
                                    ) AS v
                                WHERE v IS NOT NULL
                                AND v <> ''
                                AND v <> COALESCE(
                                        NULLIF(un.primary_ow, ''),
                                        NULLIF(un.nickname, ''),
                                        NULLIF(un.global_name, ''),
                                        'Unknown User'
                                        )
                            ), '') AS also_known_as
                FROM user_names un
            )
        SELECT
            wm.code,
            wm.user_id,
            ns.name,
            ns.also_known_as,
            wm.map_name,
            wm.difficulty,
            wm.raw_difficulty,
            wm.time,
            wm.screenshot,
            wm.video,
            wm.completion,
            wm.verified,
            wm.rank,
            CASE
                WHEN wm.rank IS NOT NULL AND wm.gold   IS NOT NULL AND wm.time <= wm.gold   THEN 'Gold'
                WHEN wm.rank IS NOT NULL AND wm.silver IS NOT NULL AND wm.time <= wm.silver THEN 'Silver'
                WHEN wm.rank IS NOT NULL AND wm.bronze IS NOT NULL AND wm.time <= wm.bronze THEN 'Bronze'
            END AS medal,
            wm.legacy,
            wm.legacy_medal,
            wm.message_id,
            FALSE as suspicious,
            (SELECT COUNT(*) FROM completions.upvotes WHERE message_id=wm.message_id) AS upvotes,
            COUNT(*) OVER() AS total_results
        FROM with_map wm
        JOIN name_split ns ON ns.user_id = wm.user_id
        WHERE wm.user_id = $1
        AND ($2::text IS NULL OR wm.top_difficulty = $2::text)
        ORDER BY
            wm.raw_difficulty,
            (wm.rank IS NULL),
            wm.time,
            wm.inserted_at
        LIMIT $3 OFFSET $4;
        """
        offset = (page_number - 1) * page_size
        rows = await self._conn.fetch(query, user_id, difficulty, page_size, offset)
        models = msgspec.convert(rows, list[CompletionReadDTO])
        return models

    async def submit_completion(self, request: Request, data: CompletionCreateDTO) -> SubmitCompletionReturnDTO:
        """Submit a new completion record and publish an event.

        Args:
            request (Request): Request obj.
            data (CompletionCreateDTO): DTO containing completion details.

        Returns:
            int: ID of the newly inserted completion record.

        """
        query = """
            WITH target_map AS (
                SELECT id AS map_id FROM core.maps WHERE code = $1
            )
            INSERT INTO core.completions (
                map_id,
                user_id,
                time,
                screenshot,
                video,
                completion
            )
            VALUES (
                (SELECT map_id FROM target_map), $2, $3, $4, $5, $6
            )
            RETURNING id;
        """
        in_playtest = await self._conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM core.maps WHERE playtesting='In Progress' AND code=$1);",
            data.code,
        )

        is_official = await self._conn.fetchval(
            "SELECT official FROM core.maps WHERE code=$1;",
            data.code,
        )
        completion = in_playtest or not data.video or not is_official
        try:
            res = await self._conn.fetchval(
                query,
                data.code,
                data.user_id,
                data.time,
                data.screenshot,
                data.video,
                completion,
            )
        except asyncpg.exceptions.CheckViolationError as e:
            raise CustomHTTPException(status_code=HTTP_400_BAD_REQUEST, detail=e.detail or "")

        job_status = await self.publish_message(
            routing_key="api.completion.submission",
            data=MessageQueueCompletionsCreate(res),
            headers=request.headers,
        )

        return SubmitCompletionReturnDTO(job_status, res)

    def build_completion_patch_query(self, patch: CompletionPatchDTO) -> tuple[str, list[Any]]:
        """Build a dynamic SQL UPDATE query for patching a completion.

        Args:
            patch (CompletionPatchDTO): DTO containing optional fields to update.

        Returns:
            tuple[str, list[Any]]: Query string and its ordered parameters.

        Raises:
            ValueError: If no fields are set for update.

        """
        set_clauses = []
        values = []

        index = 2  # Start at $2 because $1 is `id`
        for field_name, value in msgspec.structs.asdict(patch).items():
            if value is not msgspec.UNSET:
                set_clauses.append(f"{field_name.lower()} = ${index}")
                values.append(value)
                index += 1

        if not set_clauses:
            raise ValueError("No fields to update")

        set_clause = ", ".join(set_clauses)
        query = f"""
            UPDATE core.completions
            SET {set_clause}
            WHERE id = $1
        """

        return query.strip(), values

    async def edit_completion(self, state: State, record_id: int, data: CompletionPatchDTO) -> None:
        """Apply partial updates to a completion record.

        Args:
            state (State): Application state (unused, for consistency).
            record_id (int): Completion record ID to update.
            data (CompletionPatchDTO): Fields to patch.

        """
        query, args = self.build_completion_patch_query(data)
        await self._conn.execute(query, record_id, *args)

    async def check_for_previous_world_record(self, code: OverwatchCode, user_id: int) -> bool:
        """Check if a record submitted by this user has ever received World Record XP.

        This is used to stop potential abuse e.g. incremental completion submission spam resulting in XP gain.
        """
        query = """
        WITH target_map AS (
            SELECT id AS map_id FROM core.maps WHERE code = $1
        )
        SELECT EXISTS(
            SELECT 1 FROM core.completions c
            LEFT JOIN target_map tm ON c.map_id = tm.map_id
            WHERE user_id=$2 AND NOT legacy AND wr_xp_check
        )
        """
        return await self._conn.fetchval(query, code, user_id)

    async def get_completion_submission(self, record_id: int) -> CompletionSubmissionReadDTO:
        """Retrieve detailed submission info for a completion.

        Includes ranking, medal eligibility, usernames, and flags.

        Args:
            record_id (int): Completion record ID.

        Returns:
            CompletionSubmissionReadDTO: Enriched submission details.

        """
        query = """
        WITH hypothetical_target AS (
            SELECT
                c.*,
                m.code        AS code,
                m.difficulty  AS difficulty,
                m.map_name
            FROM core.completions c
            JOIN core.maps m ON m.id = c.map_id
            WHERE c.id = $1
        ),
        latest_per_user AS (
            -- Latest leaderboard-eligible run per user for the same map
            SELECT DISTINCT ON (c.user_id)
                c.user_id,
                c.time,
                c.inserted_at
            FROM core.completions c
            WHERE c.map_id = (SELECT map_id FROM hypothetical_target)
              AND c.verified = TRUE
              AND c.completion = FALSE
            ORDER BY c.user_id, c.inserted_at DESC
        ),
        eligible_ranked AS (
            SELECT user_id, time
            FROM latest_per_user

            UNION ALL

            SELECT ht.user_id, ht.time
            FROM hypothetical_target ht
            WHERE ht.completion = FALSE
                AND NOT EXISTS (
                SELECT 1
                FROM latest_per_user l
                WHERE l.user_id = ht.user_id
                    AND l.time = ht.time
                )
        ),
        ranked AS (
            SELECT
                user_id,
                time,
                RANK() OVER (ORDER BY time ASC) AS rank
            FROM eligible_ranked
        ),
        final AS (
            SELECT
                ht.id,
                ht.user_id,
                ht.time,
                ht.screenshot,
                ht.video,
                ht.verified,
                ht.completion,
                ht.inserted_at,
                ht.code,
                ht.difficulty,
                ht.map_name,
                -- Match rank entry for THIS completion (by user + time)
                r.rank AS hypothetical_rank,
                md.gold,
                md.silver,
                md.bronze,
                ht.verified_by,
                ht.verification_id,
                ht.message_id
            FROM hypothetical_target ht
            LEFT JOIN ranked r
              ON r.user_id = ht.user_id AND r.time = ht.time
            LEFT JOIN maps.medals md
              ON md.map_id = ht.map_id
        ),
        user_names AS (
            SELECT
                u.id AS user_id,
                -- Put OW usernames first, with primary first; then append nickname/global_name.
                ARRAY_REMOVE(
                    COALESCE(
                        ARRAY_AGG(owu.username ORDER BY owu.is_primary DESC, owu.username) ,  -- primary first
                        ARRAY[]::text[]
                    )
                    || ARRAY[u.nickname, u.global_name],
                    NULL
                ) AS all_usernames
            FROM final f
            JOIN core.users u ON u.id = f.user_id
            LEFT JOIN users.overwatch_usernames owu ON owu.user_id = u.id
            GROUP BY u.id, u.nickname, u.global_name
        ),
        name_split AS (
            SELECT
                un.user_id,
                -- The first element is now the primary OW name if present; otherwise next OW, else nickname/global_name
                un.all_usernames[1] AS name,
                COALESCE(
                    array_to_string(
                        ARRAY(
                            SELECT DISTINCT x
                            FROM unnest(un.all_usernames[2:array_length(un.all_usernames, 1)]) AS x
                            WHERE x IS NOT NULL AND x <> ''          -- optional: drop empties
                        ),
                        ', '
                    ),
                    ''
                ) AS also_known_as
            FROM user_names un
        ),
        medal_eval AS (
            SELECT
                f.*,
                ns.name,
                ns.also_known_as,
                CASE
                    WHEN f.completion = FALSE AND f.gold   IS NOT NULL AND f.time <= f.gold   THEN 'Gold'
                    WHEN f.completion = FALSE AND f.silver IS NOT NULL AND f.time <= f.silver THEN 'Silver'
                    WHEN f.completion = FALSE AND f.bronze IS NOT NULL AND f.time <= f.bronze THEN 'Bronze'
                END AS hypothetical_medal
            FROM final f
            JOIN name_split ns ON ns.user_id = f.user_id
        )
        SELECT
            id,
            user_id,
            time,
            screenshot,
            video,
            verified,
            completion,
            inserted_at,
            code,
            difficulty,
            map_name,
            hypothetical_rank,
            hypothetical_medal,
            name,
            also_known_as,
            verified_by,
            verification_id,
            message_id,
            EXISTS (
              SELECT 1
              FROM users.suspicious_flags sf
              JOIN core.completions c2
                ON c2.id = sf.completion_id
              WHERE c2.user_id = me.user_id
            ) AS suspicious
        FROM medal_eval me;

        """
        row = await self._conn.fetchrow(query, record_id)
        return msgspec.convert(row, CompletionSubmissionReadDTO)

    async def get_pending_verifications(self) -> list[PendingVerification]:
        """Retrieve completions awaiting verification.

        Returns:
            list[PendingVerification]: Records that have a verification ID but are unverified.

        """
        query = """
            SELECT id, verification_id FROM core.completions
            WHERE verified=FALSE AND verified_by IS NULL AND verification_id IS NOT NULL
            ORDER BY inserted_at DESC;
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[PendingVerification])

    async def verify_completion(
        self,
        request: Request,
        record_id: int,
        data: CompletionVerificationPutDTO,
    ) -> JobStatus:
        """Update verification status for a completion and publish an event.

        Args:
            request (Request): Request.
            record_id (int): Completion record ID.
            data (CompletionVerificationPutDTO): Verification details.

        """
        query = "UPDATE core.completions SET verified=$2, verified_by=$3, reason=$4 WHERE id=$1;"
        await self._conn.execute(query, record_id, data.verified, data.verified_by, data.reason)
        message_data = MessageQueueVerificationChange(
            completion_id=record_id,
            verified=data.verified,
            verified_by=data.verified_by,
            reason=data.reason,
        )
        job_status = await self.publish_message(
            routing_key="api.completion.verification",
            data=message_data,
            headers=request.headers,
        )
        return job_status

    async def get_completions_leaderboard(self, code: str, page_number: int, page_size: int) -> list[CompletionReadDTO]:
        """Retrieve the leaderboard for a map.

        Args:
            code (str): Overwatch map code.
            page_size (int): Page size.
            page_number (int): Page number.

        Returns:
            list[CompletionReadDTO]: Ranked completions for the map,
            including medal eligibility and user display names.

        """
        query = """
        WITH target_map AS (
            SELECT
                id AS map_id,
                code,
                map_name,
                difficulty
            FROM core.maps
            WHERE code = $1
        ), latest_per_user_all AS (
            SELECT DISTINCT ON (c.user_id)
                c.user_id,
                c.map_id,
                c.time,
                c.completion,
                c.verified,
                c.screenshot,
                c.video,
                c.legacy,
                c.legacy_medal,
                c.message_id,
                c.inserted_at
            FROM core.completions c
            JOIN target_map tm ON tm.map_id = c.map_id
            WHERE c.verified = TRUE
            ORDER BY c.user_id,
                c.inserted_at DESC
        ), split AS (
            SELECT
                l.user_id,
                l.map_id,
                l.time,
                l.completion,
                l.verified,
                l.screenshot,
                l.video,
                l.legacy,
                l.legacy_medal,
                l.message_id,
                l.inserted_at
            FROM latest_per_user_all l
        ), rankable AS (
            SELECT
                s.*,
                rank() OVER (ORDER BY s.time) AS rank -- keep ties (same time => same rank)
            FROM split s
            WHERE s.completion = FALSE
        ), nonrankable AS (
            SELECT
                s.*,
                NULL::integer AS rank
            FROM split s
            WHERE s.completion = TRUE
        ), combined AS (
            SELECT *
            FROM rankable
            UNION ALL
            SELECT *
            FROM nonrankable
        ), with_map AS (
            SELECT
                tm.code,
                tm.map_name,
                tm.difficulty,
                cb.user_id,
                cb.time,
                cb.completion,
                cb.verified,
                cb.screenshot,
                cb.video,
                cb.legacy,
                cb.legacy_medal,
                cb.inserted_at,
                cb.rank,
                cb.message_id,
                (cb.rank IS NULL) AS is_nonrankable,
                md.gold,
                md.silver,
                md.bronze
            FROM combined cb
            JOIN target_map tm ON tm.map_id = cb.map_id
            LEFT JOIN maps.medals md ON md.map_id = cb.map_id
        ), user_names AS (
            SELECT
                u.id AS user_id,
                max(owu.username) FILTER (WHERE owu.is_primary) AS primary_ow,
                array_remove(array_agg(owu.username), NULL) AS all_ow_names,
                u.nickname,
                u.global_name
            FROM (
                SELECT DISTINCT
                    user_id
                FROM with_map
            ) um
            JOIN core.users u ON u.id = um.user_id
            LEFT JOIN users.overwatch_usernames owu ON owu.user_id = u.id
            GROUP BY u.id,
                u.nickname,
                u.global_name
        ), name_split AS (
            SELECT
                un.user_id,
                coalesce(nullif(un.primary_ow, ''), nullif(un.nickname, ''), nullif(un.global_name, ''),
                         'Unknown User') AS name,
                nullif(array_to_string(array(SELECT DISTINCT
                                                 x
                                             FROM unnest(un.all_ow_names) x
                                             WHERE x IS NOT NULL
                                               AND x <> coalesce(un.primary_ow, '')), ', '), '') AS also_known_as
            FROM user_names un
        )
        SELECT
            wm.code AS code,
            wm.user_id AS user_id,
            ns.name AS name,
            ns.also_known_as AS also_known_as,
            wm.time AS time,
            wm.screenshot AS screenshot,
            wm.video AS video,
            wm.completion AS completion,
            wm.verified AS verified,
            wm.message_id,
            (SELECT COUNT(*) FROM completions.upvotes WHERE message_id=wm.message_id) AS upvotes,
            wm.rank AS rank,
            CASE
                WHEN wm.rank IS NOT NULL AND wm.gold IS NOT NULL AND wm.time <= wm.gold
                    THEN 'Gold'
                WHEN wm.rank IS NOT NULL AND wm.silver IS NOT NULL AND wm.time <= wm.silver
                    THEN 'Silver'
                WHEN wm.rank IS NOT NULL AND wm.bronze IS NOT NULL AND wm.time <= wm.bronze
                    THEN 'Bronze'
            END AS medal,
            wm.map_name AS map_name,
            wm.difficulty AS difficulty,
            wm.legacy AS legacy,
            wm.legacy_medal AS legacy_medal,
            FALSE AS suspicious,
            COUNT(*) OVER() AS total_results
        FROM with_map wm
        JOIN name_split ns ON ns.user_id = wm.user_id
        ORDER BY wm.code,
            (wm.rank IS NULL), -- rankable first
            wm.time,           -- best time first
            wm.inserted_at    -- tie-breaker - older submission first
        LIMIT $2
        OFFSET $3;
        """
        offset = (page_number - 1) * page_size
        rows = await self._conn.fetch(query, code, page_size, offset)
        models = msgspec.convert(rows, list[CompletionReadDTO])

        return models

    async def get_world_records_per_user(self, user_id: int) -> list[CompletionReadDTO]:
        """Get all world records (rank 1 verified non-legacy runs) for a specific user.

        Computes true global ranks per map using each user's latest verified non-legacy
        submission, breaking ties by older `inserted_at`. Filters to rank-1, non-completion
        (i.e., timing-based) results for the target user and enriches with map metadata and
        display names.

        Args:
            user_id: The user whose world records should be returned.

        Returns:
            list[CompletionReadDTO]: World-record rows including code, user identity
            (name/also_known_as), time, media, rank, inferred medal at rank 1, map
            metadata, and flags.
        """
        query = """
        WITH latest_per_user_per_map AS (
            -- Latest verified submission per (user, map) across ALL users
            SELECT DISTINCT ON (c.user_id, c.map_id)
                c.id,
                c.user_id,
                c.map_id,
                c.time,
                c.completion,
                c.verified,
                c.screenshot,
                c.video,
                c.legacy,
                c.legacy_medal,
                c.message_id,
                c.inserted_at
            FROM core.completions c
            WHERE c.verified = TRUE AND c.legacy = FALSE
            ORDER BY c.user_id, c.map_id, c.inserted_at DESC
        ),
            ranked AS (
                -- Compute true global rank per map (older wins ties)
                SELECT
                    l.*,
                    CASE
                        WHEN l.completion = FALSE THEN
                                    RANK() OVER (
                                PARTITION BY l.map_id
                                ORDER BY l.time, l.inserted_at
                                )
                        ELSE NULL::int
                    END AS rank
                FROM latest_per_user_per_map l
            ),
            with_map AS (
                -- Join map metadata & medal thresholds
                SELECT
                    r.id,
                    m.code,
                    m.map_name,
                    m.difficulty,
                    m.raw_difficulty,
                    r.user_id,
                    r.time,
                    r.completion,
                    r.verified,
                    r.screenshot,
                    r.video,
                    r.message_id,
                    r.inserted_at,
                    r.legacy,
                    r.legacy_medal,
                    r.rank,
                    md.gold,
                    md.silver,
                    md.bronze
                FROM ranked r
                JOIN core.maps m ON m.id = r.map_id
                LEFT JOIN maps.medals md ON md.map_id = r.map_id
            ),
            user_names AS (
                -- Build display name + also_known_as for the target user
                SELECT
                    u.id AS user_id,
                            MAX(owu.username) FILTER (WHERE owu.is_primary) AS primary_ow,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT owu.username), NULL) AS all_ow_names,
                    u.nickname,
                    u.global_name
                FROM core.users u
                LEFT JOIN users.overwatch_usernames owu ON owu.user_id = u.id
                WHERE u.id = $1
                GROUP BY u.id, u.nickname, u.global_name
            ),
            name_split AS (
                SELECT
                    un.user_id,
                    COALESCE(
                            NULLIF(un.primary_ow, ''),
                            NULLIF(un.nickname, ''),
                            NULLIF(un.global_name, ''),
                            'Unknown User'
                    ) AS name,
                    NULLIF((
                               SELECT string_agg(DISTINCT v, ', ')
                               FROM unnest(
                                       ARRAY[
                                           NULLIF(un.global_name, ''),
                                           NULLIF(un.nickname, '')
                                           ] || COALESCE(un.all_ow_names, '{}')
                                    ) AS v
                               WHERE v IS NOT NULL
                                 AND v <> ''
                                 AND v <> COALESCE(
                                       NULLIF(un.primary_ow, ''),
                                       NULLIF(un.nickname, ''),
                                       NULLIF(un.global_name, ''),
                                       'Unknown User'
                                          )
                           ), '') AS also_known_as
                FROM user_names un
            )
        SELECT
            wm.code                             AS code,
            wm.user_id                          AS user_id,
            ns.name                             AS name,
            ns.also_known_as                    AS also_known_as,
            wm.time                             AS time,
            wm.screenshot                       AS screenshot,
            wm.video                            AS video,
            wm.completion                       AS completion,
            wm.verified                         AS verified,
            wm.rank                             AS rank,
            CASE
                WHEN wm.rank = 1 AND wm.gold   IS NOT NULL AND wm.time <= wm.gold   THEN 'Gold'
                WHEN wm.rank = 1 AND wm.silver IS NOT NULL AND wm.time <= wm.silver THEN 'Silver'
                WHEN wm.rank = 1 AND wm.bronze IS NOT NULL AND wm.time <= wm.bronze THEN 'Bronze'
            END                                  AS medal,
            wm.map_name                          AS map_name,
            wm.difficulty                        AS difficulty,
            wm.message_id,
            (SELECT COUNT(*) FROM completions.upvotes WHERE message_id=wm.message_id) AS upvotes,
            wm.legacy,
            wm.legacy_medal,
            FALSE AS suspicious
        FROM with_map wm
        JOIN name_split ns ON ns.user_id = wm.user_id
        WHERE wm.user_id = $1
          AND wm.completion = FALSE
          AND wm.rank = 1
        ORDER BY
            wm.raw_difficulty,          -- easiest → hardest (adjust if you prefer)
            wm.time,                -- faster first
            wm.inserted_at;         -- older first among identical times
        """
        rows = await self._conn.fetch(query, user_id)
        return msgspec.convert(rows, list[CompletionReadDTO])

    async def get_legacy_completions_per_map(
        self,
        code: OverwatchCode,
        page_number: int,
        page_size: int,
    ) -> list[CompletionReadDTO]:
        """Get the latest legacy completion per user for a given map code with ranks.

        For the target map, selects each user's latest legacy submission, ranks only
        time-based (non-completion) results globally (ties by older `inserted_at`),
        and returns enriched rows with display names and map metadata.

        Args:
            code: Overwatch map code to fetch legacy completions for.
            page_number (int): Page nubmer for pagination.
            page_size (int): Page size for pagination.

        Returns:
            list[CompletionReadDTO]: Legacy completion rows (latest per user) for the
            map, including time, media, legacy medal, global rank (if rankable), and
            identity fields.
        """
        query = """
        WITH target_map AS (
            SELECT
                id AS map_id,
                code,
                map_name,
                difficulty
            FROM core.maps
            WHERE code = $1
        ),
            latest_legacy_per_user AS (
                -- Latest legacy submission per (user, target map)
                SELECT DISTINCT ON (c.user_id)
                    c.user_id,
                    c.map_id,
                    c.time,
                    c.completion,
                    c.verified,
                    c.screenshot,
                    c.video,
                    c.legacy,
                    c.legacy_medal,
                    c.message_id,
                    c.inserted_at
                FROM core.completions c
                JOIN target_map tm ON tm.map_id = c.map_id
                WHERE c.legacy = TRUE
                ORDER BY c.user_id, c.inserted_at DESC
            ),
            ranked AS (
                -- Rank only rankable legacy rows; keep ties, older first within ties
                SELECT
                    l.*,
                    CASE
                        WHEN l.completion = FALSE THEN
                                    RANK() OVER (ORDER BY l.time, l.inserted_at)
                        ELSE NULL::int
                    END AS rank
                FROM latest_legacy_per_user l
            ),
            with_map AS (
                SELECT
                    tm.code,
                    tm.map_name,
                    tm.difficulty,
                    r.user_id,
                    r.time,
                    r.screenshot,
                    r.video,
                    r.completion,
                    r.verified,
                    r.rank,
                    r.message_id,
                    r.inserted_at,
                    r.legacy,
                    r.legacy_medal
                FROM ranked r
                JOIN target_map tm ON tm.map_id = r.map_id
            ),
            user_names AS (
                SELECT
                    u.id AS user_id,
                            MAX(owu.username) FILTER (WHERE owu.is_primary) AS primary_ow,
                    ARRAY_REMOVE(ARRAY_AGG(DISTINCT owu.username), NULL) AS all_ow_names,
                    u.nickname,
                    u.global_name
                FROM (SELECT DISTINCT user_id FROM with_map) um
                JOIN core.users u ON u.id = um.user_id
                LEFT JOIN users.overwatch_usernames owu ON owu.user_id = u.id
                GROUP BY u.id, u.nickname, u.global_name
            ),
            name_split AS (
                SELECT
                    un.user_id,
                    COALESCE(
                            NULLIF(un.primary_ow, ''),
                            NULLIF(un.nickname, ''),
                            NULLIF(un.global_name, ''),
                            'Unknown User'
                    ) AS name,
                    NULLIF((
                               SELECT string_agg(DISTINCT v, ', ')
                               FROM unnest(
                                       ARRAY[
                                           NULLIF(un.global_name, ''),
                                           NULLIF(un.nickname, '')
                                           ] || COALESCE(un.all_ow_names, '{}')
                                    ) AS v
                               WHERE v IS NOT NULL
                                 AND v <> ''
                                 AND v <> COALESCE(
                                       NULLIF(un.primary_ow, ''),
                                       NULLIF(un.nickname, ''),
                                       NULLIF(un.global_name, ''),
                                       'Unknown User'
                                          )
                           ), '') AS also_known_as
                FROM user_names un
            )
        SELECT
            wm.code                               AS code,
            wm.user_id                            AS user_id,
            ns.name                               AS name,
            ns.also_known_as                      AS also_known_as,
            wm.time                               AS time,
            wm.screenshot                         AS screenshot,
            wm.video                              AS video,
            wm.completion                         AS completion,
            wm.verified                           AS verified,
            wm.rank                               AS rank,
            wm.legacy_medal                       AS medal,        -- legacy uses stored medal
            wm.map_name                           AS map_name,
            wm.difficulty                         AS difficulty,
            wm.message_id                         AS message_id,
            (SELECT COUNT(*) FROM completions.upvotes WHERE message_id=wm.message_id) AS upvotes,
            wm.legacy                             AS legacy,
            wm.legacy_medal                       AS legacy_medal,
            FALSE                                 AS suspicious    -- per requirement
        FROM with_map wm
        JOIN name_split ns ON ns.user_id = wm.user_id
        ORDER BY
            wm.time,          -- fastest first
            wm.inserted_at    -- older first within ties
        LIMIT $2
        OFFSET $3;
        """
        offset = (page_number - 1) * page_size
        rows = await self._conn.fetch(query, code, page_size, offset)
        return msgspec.convert(rows, list[CompletionReadDTO])

    async def get_suspicious_flags(self, user_id: int) -> list[SuspiciousCompletionReadDTO]:
        """Retrieve suspicious flags associated with a user.

        Args:
            user_id (int): ID of the user.

        Returns:
            list[SuspiciousCompletionReadDTO]: List of suspicious flags tied to user completions.

        """
        query = """
            SELECT
                usf.id, u.id AS user_id, usf.context, usf.flag_type, cc.message_id, cc.verification_id, usf.flagged_by
            FROM users.suspicious_flags usf
            LEFT JOIN core.completions cc ON cc.id = usf.completion_id
            LEFT JOIN core.users u ON cc.user_id = u.id
            WHERE u.id = $1
        """
        rows = await self._conn.fetch(query, user_id)
        return msgspec.convert(rows, list[SuspiciousCompletionReadDTO])

    async def set_suspicious_flags(self, data: SuspiciousCompletionWriteDTO) -> None:
        """Insert a suspicious flag for a completion.

        Args:
            data (SuspiciousCompletionWriteDTO): Suspicious flag details including context,
                type, and reporter.

        """
        query = """
            WITH message_to_completion_id AS (
            SELECT id
            FROM core.completions
            WHERE
                ($1::bigint IS NOT NULL AND message_id = $1::bigint) OR
                ($1::bigint IS NULL     AND verification_id = $2::bigint)
            LIMIT 1
            )
            INSERT INTO users.suspicious_flags (completion_id, context, flag_type, flagged_by)
            SELECT id, $3, $4, $5
            FROM message_to_completion_id;
        """
        await self._conn.execute(
            query,
            data.message_id,
            data.verification_id,
            data.context,
            data.flag_type,
            data.flagged_by,
        )

    async def upvote_submission(self, request: Request, data: UpvoteCreateDTO) -> UpvoteSubmissionReturnDTO:
        """Upvote a completion submission.

        Args:
            data (UpvoteCreateDTO): Upvote details including user and message ID.
            request: Request.

        Returns:
            int: The total upvote count after insertion.

        """
        query = """
            INSERT INTO completions.upvotes (user_id, message_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            RETURNING (
                SELECT COUNT(*) + 1
                FROM completions.upvotes
                WHERE message_id = $2
            ) AS count;
        """
        count = await self._conn.fetchval(query, data.user_id, data.message_id)
        upvote_channel_amount_breakpoint = 10
        if count is None:
            raise CustomHTTPException(
                detail="User has already upvoted this completion.", status_code=HTTP_400_BAD_REQUEST
            )
        job_status = None
        if count != 0 and count % upvote_channel_amount_breakpoint == 0:
            messsage_data = UpvoteUpdateDTO(
                data.user_id,
                data.message_id,
            )
            job_status = await self.publish_message(
                routing_key="api.completion.upvote", data=messsage_data, headers=request.headers
            )
        return UpvoteSubmissionReturnDTO(job_status, count)

    async def get_all_completions(self, page_size: int, page_number: int) -> list[CompletionReadDTO]:
        """Get all completions from most recent.

        Args:
            page_size (int): The size of the pagination pages.
            page_number (int): The page number.
        """
        query = """
        WITH latest_per_user_per_map AS (
            SELECT DISTINCT ON (c.user_id, c.map_id)
                c.id,
                c.user_id,
                c.map_id,
                c.time,
                c.completion,
                c.inserted_at
            FROM core.completions c
            WHERE c.verified
              AND c.legacy = FALSE
            ORDER BY c.user_id,
                c.map_id,
                c.inserted_at DESC
        ), current_ranks AS (
            SELECT
                l.map_id,
                l.user_id,
                CASE
                    WHEN l.completion = FALSE
                        THEN rank() OVER (PARTITION BY l.map_id ORDER BY l.time, l.inserted_at)
                    ELSE NULL::int
                END AS current_rank
            FROM latest_per_user_per_map l
        )
        SELECT
            m.code,
            c.user_id,
            coalesce(ow.username, u.nickname, u.global_name, 'Unknown Username') AS name,
            (
                SELECT
                    ou.username
                FROM users.overwatch_usernames ou
                WHERE ou.user_id = c.user_id
                  AND NOT ou.is_primary
                ORDER BY c.inserted_at DESC NULLS LAST
                LIMIT 1
            ) AS also_known_as,
            c.time,
            c.screenshot,
            c.video,
            c.completion,
            c.verified,
            CASE WHEN lp.id = c.id THEN r.current_rank END AS rank, -- only for latest rows
            CASE
                WHEN med.gold IS NOT NULL AND c.time <= med.gold
                    THEN 'Gold'
                WHEN med.silver IS NOT NULL AND c.time <= med.silver
                    THEN 'Silver'
                WHEN med.bronze IS NOT NULL AND c.time <= med.bronze
                    THEN 'Bronze'
            END AS medal,
            m.map_name,
            m.difficulty,
            c.message_id,
            (SELECT COUNT(*) FROM completions.upvotes WHERE message_id=c.message_id) AS upvotes,
            c.legacy,
            c.legacy_medal,
            FALSE AS suspicious,
            count(*) OVER () AS total_results
        FROM core.completions c
        JOIN core.maps m ON m.id = c.map_id
        JOIN core.users u ON u.id = c.user_id
        LEFT JOIN users.overwatch_usernames ow ON ow.user_id = u.id AND ow.is_primary
        LEFT JOIN maps.medals med ON med.map_id = m.id
        LEFT JOIN latest_per_user_per_map lp ON lp.user_id = c.user_id AND lp.map_id = c.map_id
        LEFT JOIN current_ranks r ON r.user_id = c.user_id AND r.map_id = c.map_id
        LEFT JOIN LATERAL (
            WITH ow AS (
                SELECT username, is_primary
                FROM users.overwatch_usernames
                WHERE user_id = c.user_id
            ),
                display AS (
                    SELECT COALESCE(
                            (SELECT username FROM ow WHERE is_primary LIMIT 1),
                            u.nickname,
                            u.global_name,
                            'Unknown Username'
                           ) AS name
                ),
                candidates AS (
                    SELECT u.global_name AS n
                    UNION ALL SELECT u.nickname
                    UNION ALL SELECT username FROM ow
                ),
                dedup AS (
                    SELECT DISTINCT ON (lower(btrim(n))) btrim(n) AS n
                    FROM candidates
                    WHERE n IS NOT NULL AND btrim(n) <> ''
                    ORDER BY lower(btrim(n))
                )
            SELECT
                (SELECT name FROM display) AS name,
                NULLIF(
                        array_to_string(
                                ARRAY(
                                        SELECT n
                                        FROM dedup
                                        WHERE lower(n) <> lower((SELECT name FROM display))
                                        ORDER BY n
                                ),
                                ', '
                        ),
                        ''
                ) AS also_known_as
            ) names ON TRUE

        WHERE TRUE
          AND c.verified
          AND c.legacy = FALSE
        ORDER BY c.inserted_at DESC
        LIMIT $1 OFFSET $2;
        """
        offset = (page_number - 1) * page_size
        rows = await self._conn.fetch(query, page_size, offset)
        return msgspec.convert(rows, list[CompletionReadDTO])

    async def set_quality_vote_for_map_code(self, code: OverwatchCode, user_id: int, quality: int) -> None:
        """Set the quality vote for a map code per user."""
        query = """
        WITH target_map AS (
            SELECT id AS map_id FROM core.maps WHERE code = $1
        )
        INSERT INTO maps.ratings (map_id, user_id, quality, verified)
        SELECT tm.map_id, $2, $3, FALSE
        FROM target_map AS tm
        ON CONFLICT (map_id, user_id)
        DO UPDATE
          SET quality = EXCLUDED.quality;
        """
        await self._conn.execute(query, code, user_id, quality)


async def provide_completions_service(conn: Connection, state: State) -> CompletionsService:
    """Litestar DI provider for CompletionsService.

    Args:
        conn (asyncpg.Connection): Active asyncpg connection.
        state: App state.

    Returns:
        CompletionsService: A new service instance bound to the given connection.

    """
    return CompletionsService(conn=conn, state=state)
