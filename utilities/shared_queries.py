import msgspec
from asyncpg import Connection
from genjipk_sdk.models import MapMasteryData, RankDetailReadDTO
from genjipk_sdk.utilities.types import OverwatchMap


async def get_map_mastery_data(
    conn: Connection, user_id: int, map_name: OverwatchMap | None = None
) -> list[MapMasteryData]:
    """Get mastery data for a user, optionally scoped to a map.

    Args:
        conn (Connection): Asyncpg connection
        user_id (int): Target user ID.
        map_name (OverwatchMap | None): Optional map filter.

    Returns:
        list[MapMasteryData]: Mastery rows for the user (and map if provided).

    """
    query = """
        WITH minimized_records AS (
            SELECT DISTINCT ON (c.map_id, m.map_name)
                map_name
            FROM core.completions c
            LEFT JOIN core.maps m ON c.map_id = m.id
            WHERE c.user_id = $1
        ),
        map_counts AS (
            SELECT
                map_name,
                count(map_name) AS amount
            FROM minimized_records
            GROUP BY map_name
        )
        SELECT
            amn.name AS map_name,
            coalesce(mc.amount, 0) AS amount
        FROM maps.names amn
        LEFT JOIN map_counts mc ON mc.map_name = amn.name
        WHERE ($2::text IS NULL OR amn.name = $2) AND amn.name != 'Adlersbrunn'
        ORDER BY amn.name;
    """
    rows = await conn.fetch(query, user_id, map_name)
    return msgspec.convert(rows, list[MapMasteryData])


async def get_user_rank_data(conn: Connection, user_id: int) -> list[RankDetailReadDTO]:
    """Compute rank details for a user based on verified completions and medal thresholds.

    Args:
        conn (Connection): Asyncpg connection
        user_id (int): The ID of the user.

    Returns:
        list[RankDetailReadDTO]: Per-difficulty counts and rank-met flags.

    """
    query = r"""
    WITH user_completions AS (
        SELECT DISTINCT ON (map_id, user_id)
            map_id,
            user_id,
            time,
            screenshot,
            video,
            verified,
            message_id,
            completion,
            legacy_medal AS medal
        FROM core.completions
        WHERE verified
        ORDER BY map_id, user_id, inserted_at DESC
    ),
    thresholds AS (
        SELECT * FROM (
            VALUES
                ('Easy', 10),
                ('Medium', 10),
                ('Hard', 10),
                ('Very Hard', 10),
                ('Extreme', 7),
                ('Hell', 3)
        ) AS t(name, threshold)
    ),
    map_data AS (
        SELECT
            regexp_replace(trim(m.difficulty), '\s*[+-]\s*$', '') AS difficulty,
            uc.video IS NOT NULL AND (
                time <= gold OR medal LIKE 'Gold'
            ) AS gold,
            uc.video IS NOT NULL AND (
                time <= silver AND time > gold OR medal LIKE 'Silver'
            ) AS silver,
            uc.video IS NOT NULL AND (
                time <= bronze AND time > silver OR medal LIKE 'Bronze'
            ) AS bronze
            FROM user_completions uc
            LEFT JOIN core.maps m ON uc.map_id = m.id
            LEFT JOIN maps.medals mm ON uc.map_id = mm.map_id
            WHERE uc.user_id = $1
              AND m.official = TRUE
    ),
    counts_data AS (
        SELECT
            difficulty,
            count(difficulty) AS completions,
            count(CASE WHEN gold THEN 1 END) AS gold,
            count(CASE WHEN silver THEN 1 END) AS silver,
            count(CASE WHEN bronze THEN 1 END) AS bronze,
            -- Use threshold for rank comparison
            count(difficulty) >= t.threshold AS rank_met,
            count(CASE WHEN gold THEN 1 END) >= t.threshold AS gold_rank_met,
            count(CASE WHEN silver THEN 1 END) >= t.threshold AS silver_rank_met,
            count(CASE WHEN bronze THEN 1 END) >= t.threshold AS bronze_rank_met
        FROM map_data md
        INNER JOIN thresholds t ON difficulty = t.name
        GROUP BY difficulty, t.threshold
    )
    SELECT
        name AS difficulty,
        coalesce(completions, 0) AS completions,
        coalesce(gold, 0) AS gold,
        coalesce(silver, 0) AS silver,
        coalesce(bronze, 0) AS bronze,
        coalesce(rank_met, FALSE) AS rank_met,
        coalesce(gold_rank_met, FALSE) AS gold_rank_met,
        coalesce(silver_rank_met, FALSE) AS silver_rank_met,
        coalesce(bronze_rank_met, FALSE) AS bronze_rank_met
    FROM thresholds t
    LEFT JOIN counts_data cd ON t.name = cd.difficulty
    ORDER BY
        CASE name
            WHEN 'Easy' THEN 1
            WHEN 'Medium' THEN 2
            WHEN 'Hard' THEN 3
            WHEN 'Very Hard' THEN 4
            WHEN 'Extreme' THEN 5
            WHEN 'Hell' THEN 6
        END;
    """
    rows = await conn.fetch(query, user_id)
    return msgspec.convert(rows, list[RankDetailReadDTO])
