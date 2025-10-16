from typing import Literal

import msgspec
from asyncpg import Connection
from genjipk_sdk.models import (
    CommunityLeaderboardReadDTO,
    MapCompletionStatisticsResponse,
    MapCountsResponse,
    MapPerDifficultyStatisticsResponse,
    MapRecordProgressionResponse,
    PlayersPerSkillTierResponse,
    PlayersPerXPTierResponse,
    PopularMapsStatisticsResponse,
    TimePlayedPerRankResponse,
    TopCreatorsResponse,
)
from genjipk_sdk.utilities.types import OverwatchCode
from litestar.datastructures import State

from .base import BaseService


class CommunityService(BaseService):
    async def get_community_leaderboard(  # noqa: PLR0913
        self,
        name: str | None = None,
        tier_name: str | None = None,
        skill_rank: str | None = None,
        sort_column: Literal[
            "xp_amount",
            "nickname",
            "prestige_level",
            "wr_count",
            "map_count",
            "playtest_count",
            "discord_tag",
            "skill_rank",
        ] = "xp_amount",
        sort_direction: Literal["asc", "desc"] = "asc",
        page_size: Literal[10, 20, 25, 50] = 10,
        page_number: int = 1,
    ) -> list[CommunityLeaderboardReadDTO]:
        """Fetch the community leaderboard with filtering, sorting, and pagination.

        Filters by optional `name` (nickname/global name ILIKE), `tier_name` (XP tier),
        and `skill_rank` (derived rank: Ninja → God). Sorts by the given column and
        direction; when `sort_column='skill_rank'` a fixed rank ordering is applied
        (God > Grandmaster > Master > Pro > Skilled > Jumper > Ninja).

        Args:
            name: Optional search string for nickname or global name.
            tier_name: Exact XP tier label to match (e.g., "Bronze II").
            skill_rank: Exact derived skill rank to match (e.g., "Master").
            sort_column: Column to sort by. One of:
                "xp_amount", "nickname", "prestige_level", "wr_count", "map_count",
                "playtest_count", "discord_tag", "skill_rank".
            sort_direction: Sort direction, "asc" or "desc".
            page_size: Page size; one of 10, 20, 25, 50.
            page_number: 1-based page number.

        Returns:
            list[CommunityLeaderboardReadDTO]: Paged leaderboard rows including XP, tiers,
            WR count, map count, playtest count, Discord tag, and derived skill rank.
        """
        if sort_column == "skill_rank":
            sort_values = """
                CASE
                    WHEN rank_name = 'Ninja' THEN 7
                    WHEN rank_name = 'Jumper' THEN 6
                    WHEN rank_name = 'Skilled' THEN 5
                    WHEN rank_name = 'Pro' THEN 4
                    WHEN rank_name = 'Master' THEN 3
                    WHEN rank_name = 'Grandmaster' THEN 2
                    WHEN rank_name = 'God' THEN 1
                END
            """
        else:
            sort_values = sort_column

        query = f"""
        WITH unioned_records AS (
            SELECT DISTINCT ON (map_id, user_id)
                map_id,
                user_id,
                time,
                screenshot,
                video,
                verified,
                message_id,
                completion,
                NULL AS medal
            FROM core.completions
            ORDER BY map_id,
                user_id,
                inserted_at DESC
        ), thresholds AS (
            SELECT *
            FROM (
                VALUES ('Easy',10),
                       ('Medium', 10),
                       ('Hard', 10),
                       ('Very Hard', 10),
                       ('Extreme', 7),
                       ('Hell', 3)
            ) AS t(name, threshold)
        ), map_data AS (
            SELECT DISTINCT ON (m.id, r.user_id)
                r.user_id,
                difficulty
            FROM unioned_records r
            LEFT JOIN core.maps m ON r.map_id = m.id
            WHERE m.official = TRUE
        ), skill_rank_data AS (
            SELECT
                difficulty,
                md.user_id,
                coalesce(sum(CASE WHEN md.difficulty IS NOT NULL THEN 1 ELSE 0 END), 0) AS completions,
                coalesce(sum(CASE WHEN md.difficulty IS NOT NULL THEN 1 ELSE 0 END), 0) >= t.threshold AS rank_met
            FROM map_data md
            LEFT JOIN thresholds t ON difficulty=t.name
            GROUP BY difficulty,
                t.threshold,
                md.user_id
        ), first_rank AS (
            SELECT
                difficulty,
                user_id,
                CASE
                    WHEN difficulty = 'Easy' THEN 'Jumper'
                    WHEN difficulty = 'Medium' THEN 'Skilled'
                    WHEN difficulty = 'Hard' THEN 'Pro'
                    WHEN difficulty = 'Very Hard' THEN 'Master'
                    WHEN difficulty = 'Extreme' THEN 'Grandmaster'
                    WHEN difficulty = 'Hell' THEN 'God'
                END AS rank_name,
                row_number() OVER (
                    PARTITION BY user_id ORDER BY CASE difficulty
                        WHEN 'Easy' THEN 1
                        WHEN 'Medium' THEN 2
                        WHEN 'Hard' THEN 3
                        WHEN 'Very Hard' THEN 4
                        WHEN 'Extreme' THEN 5
                        WHEN 'Hell' THEN 6
                END DESC ) AS rank_order
            FROM skill_rank_data
            WHERE rank_met
        ), all_users AS (
            SELECT DISTINCT
                user_id
            FROM unioned_records
        ), highest_ranks AS (
            SELECT
                u.user_id,
                coalesce(fr.rank_name, 'Ninja') AS rank_name
            FROM all_users u
            LEFT JOIN first_rank fr ON u.user_id = fr.user_id AND fr.rank_order = 1
        ), ranks AS (
            SELECT
                r.user_id,
                r.map_id,
                rank() OVER (PARTITION BY r.map_id ORDER BY time) AS rank_num
            FROM core.completions r
            JOIN core.users u ON r.user_id = u.id
            WHERE u.id > 1000
              AND r.time < 99999999
              AND r.verified = TRUE
        ), world_records AS (
            SELECT
                r.user_id,
                count(r.user_id) AS amount
            FROM ranks r
            WHERE rank_num = 1
            GROUP BY r.user_id
        ), map_counts AS (
            SELECT
                user_id,
                count(*) AS amount
            FROM maps.creators
            GROUP BY user_id
        ), xp_tiers AS (
            SELECT
                u.id,
                coalesce(own.username, nickname) AS nickname,
                u.global_name,
                coalesce(xp.amount, 0) AS xp,
                (coalesce(xp.amount, 0) / 100) AS raw_tier,               -- Integer division for raw tier
                ((coalesce(xp.amount, 0) / 100) % 100) AS normalized_tier,-- Normalized tier, resetting every 100 tiers
                (coalesce(xp.amount, 0) / 100) / 100 AS prestige_level,-- Prestige level based on multiples of 100 tiers
                x.name AS main_tier_name,                                 -- Main tier label without sub-tier levels
                s.name AS sub_tier_name,
                x.name || ' ' || s.name AS full_tier_name                 -- Sub-tier label
            FROM core.users u
            LEFT JOIN users.overwatch_usernames own ON u.id = own.user_id AND own.is_primary = TRUE
            LEFT JOIN lootbox.xp xp ON u.id = xp.user_id
            LEFT JOIN lootbox.main_tiers x ON (((coalesce(xp.amount, 0) / 100) % 100)) / 5 = x.threshold
            LEFT JOIN lootbox.sub_tiers s ON (coalesce(xp.amount, 0) / 100) % 5 = s.threshold

            WHERE u.id > 100000
        ),
        playtest_counts AS (
            SELECT user_id, count(*) AS amount
            FROM playtests.votes
            GROUP BY user_id
        )
        SELECT
            u.id as user_id,
            u.nickname AS nickname,
            xp AS xp_amount,
            raw_tier,
            normalized_tier,
            prestige_level,
            full_tier_name AS tier_name,
            coalesce(wr.amount, 0) AS wr_count,
            coalesce(mc.amount, 0) AS map_count,
            coalesce(ptc.amount, 0) AS playtest_count,
            coalesce(u.global_name, 'Unknown Username') AS discord_tag,
            coalesce(rank_name, 'Ninja') AS skill_rank,
            count(*) OVER () AS total_results
        FROM xp_tiers u
        LEFT JOIN playtest_counts ptc ON u.id = ptc.user_id
        LEFT JOIN map_counts mc ON u.id = mc.user_id
        LEFT JOIN world_records wr ON u.id = wr.user_id
        LEFT JOIN highest_ranks hr ON u.id = hr.user_id
        WHERE ($3::text IS NULL OR (nickname ILIKE $3::text OR u.global_name ILIKE $3::text))
          AND ($4::text IS NULL OR full_tier_name = $4::text)
          AND ($5::text IS NULL OR rank_name = $5::text)
        ORDER BY {sort_values} {sort_direction}
        LIMIT $1::int OFFSET $2::int
        """
        offset = (page_number - 1) * page_size
        _name = f"%{name}%" if name else name
        rows = await self._conn.fetch(query, page_size, offset, _name, tier_name, skill_rank)
        return msgspec.convert(rows, list[CommunityLeaderboardReadDTO])

    async def get_players_per_xp_tier(self) -> list[PlayersPerXPTierResponse]:
        """Compute player counts per main XP tier.

        Groups users into XP main tiers and returns the number of players in each tier.

        Returns:
            list[PlayersPerXPTierResponse]: Count of players for every main XP tier.
        """
        query = """
        WITH player_xp AS (
            SELECT
                x.name AS tier,
                x.threshold
            FROM core.users u
            LEFT JOIN lootbox.xp xp ON u.id = xp.user_id
            LEFT JOIN lootbox.main_tiers x ON ((coalesce(xp.amount, 0) / 100) % 100) / 5 = x.threshold
            WHERE xp.amount > 500
        ),
            tier_counts AS (
                SELECT
                    tier,
                    threshold,
                    COUNT(*) AS amount
                FROM player_xp
                GROUP BY tier, threshold
            )
        SELECT
            mxt.name AS tier,
            COALESCE(tc.amount, 0) AS amount
        FROM lootbox.main_tiers mxt
        LEFT JOIN tier_counts tc ON mxt.name = tc.tier
        ORDER BY mxt.threshold;
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[PlayersPerXPTierResponse])

    async def get_players_per_skill_tier(self) -> list[PlayersPerSkillTierResponse]:
        """Compute player counts per derived skill tier.

        Derives a player's highest skill rank (Ninja → God) from official map
        completions versus thresholds, then returns counts by rank.

        Returns:
            list[PlayersPerSkillTierResponse]: Count of players per skill rank.
        """
        query = """
        WITH all_completions AS (
            SELECT DISTINCT ON (map_id, user_id)
                map_id,
                user_id,
                time,
                screenshot,
                video,
                verified,
                message_id,
                completion,
                NULL AS medal
            FROM core.completions
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
            SELECT DISTINCT ON (m.id, c.user_id)
                c.user_id,
                m.difficulty
            FROM all_completions c
            LEFT JOIN core.maps m ON c.map_id = m.id
            WHERE m.official = TRUE
        ),
        skill_rank_data AS (
            SELECT
                md.difficulty,
                md.user_id,
                COALESCE(SUM(CASE WHEN md.difficulty IS NOT NULL THEN 1 ELSE 0 END), 0) AS completions,
                COALESCE(SUM(CASE WHEN md.difficulty IS NOT NULL THEN 1 ELSE 0 END), 0) >= t.threshold AS rank_met
            FROM map_data md
            LEFT JOIN thresholds t ON md.difficulty = t.name
            GROUP BY md.difficulty, t.threshold, md.user_id
        ),
        first_rank AS (
            SELECT
                difficulty,
                user_id,
                CASE
                    WHEN difficulty = 'Easy' THEN 'Jumper'
                    WHEN difficulty = 'Medium' THEN 'Skilled'
                    WHEN difficulty = 'Hard' THEN 'Pro'
                    WHEN difficulty = 'Very Hard' THEN 'Master'
                    WHEN difficulty = 'Extreme' THEN 'Grandmaster'
                    WHEN difficulty = 'Hell' THEN 'God'
                END AS rank_name,
                        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY
                    CASE difficulty
                        WHEN 'Easy' THEN 1
                        WHEN 'Medium' THEN 2
                        WHEN 'Hard' THEN 3
                        WHEN 'Very Hard' THEN 4
                        WHEN 'Extreme' THEN 5
                        WHEN 'Hell' THEN 6
                    END DESC
                    ) AS rank_order
            FROM skill_rank_data
            WHERE rank_met
        ),
        all_users AS (
            SELECT DISTINCT id FROM core.users
        ),
        highest_ranks AS (
            SELECT coalesce(fr.rank_name, 'Ninja') AS rank_name
            FROM all_users u
            LEFT JOIN first_rank fr ON u.id = fr.user_id AND fr.rank_order = 1
        )
        SELECT count(*) AS amount, rank_name as tier FROM highest_ranks GROUP BY rank_name
        ORDER BY CASE
            WHEN rank_name = 'Ninja' THEN 7
            WHEN rank_name = 'Jumper' THEN 6
            WHEN rank_name = 'Skilled' THEN 5
            WHEN rank_name = 'Pro' THEN 4
            WHEN rank_name = 'Master' THEN 3
            WHEN rank_name = 'Grandmaster' THEN 2
            WHEN rank_name = 'God' THEN 1
        END DESC;
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[PlayersPerSkillTierResponse])

    async def get_map_completion_statistics(self, code: OverwatchCode) -> list[MapCompletionStatisticsResponse]:
        """Get min, max, and average verified completion times for a map.

        Filters verified runs for the target code and aggregates summary stats.

        Args:
            code: Overwatch map code.

        Returns:
            list[MapCompletionStatisticsResponse]: Single-row summary with min/max/avg.
        """
        query = """
        WITH target_map AS (
            SELECT
                id AS map_id
            FROM core.maps
            WHERE code = $1
        ),
        filtered_completions AS (
            SELECT *
            FROM core.completions
            WHERE map_id = (SELECT map_id FROM target_map) AND time < 99999999.99 AND verified = TRUE
        )
        SELECT round(min(r.time), 2) AS min, round(max(r.time), 2) AS max, round(avg(r.time), 2) AS avg
        FROM core.maps m
        LEFT JOIN filtered_completions r ON m.id = r.map_id
        WHERE m.id = (SELECT map_id FROM target_map)
        GROUP BY m.id
        """
        rows = await self._conn.fetch(query, code)
        return msgspec.convert(rows, list[MapCompletionStatisticsResponse])

    async def get_maps_per_difficulty(self) -> list[MapPerDifficultyStatisticsResponse]:
        """Count official, visible maps by base difficulty.

        Strips trailing '+'/'-' from difficulty (e.g., 'Hard +' → 'Hard') and returns
        counts per base difficulty in canonical order.

        Returns:
            list[MapPerDifficultyStatisticsResponse]: Counts per base difficulty.
        """
        query = r"""
        WITH filtered AS (
            SELECT
                regexp_replace(m.difficulty, '\s*[-+]\s*$', '', '') AS base_difficulty
            FROM core.maps m
            WHERE m.official IS TRUE
              AND m.archived IS FALSE
              AND m.hidden   IS FALSE
        )
        SELECT
            base_difficulty AS difficulty,
            COUNT(*) AS amount
        FROM filtered
        GROUP BY base_difficulty
        ORDER BY
            array_position(ARRAY['Easy','Medium','Hard','Very Hard','Extreme','Hell'], base_difficulty) NULLS LAST;
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[MapPerDifficultyStatisticsResponse])
        return [MapPerDifficultyStatisticsResponse(**row) for row in rows]

    async def get_popular_maps(self) -> list[PopularMapsStatisticsResponse]:
        """Return top maps per difficulty by completions (tiebreaker: quality).

        For each base difficulty, ranks maps by completion volume, breaking ties by
        average quality (desc) and a deterministic fallback, and returns the top 5.

        Returns:
            list[PopularMapsStatisticsResponse]: Top maps per difficulty with rank.
        """
        query = r"""
        WITH eligible_maps AS (
            SELECT
                m.id,
                code,
                regexp_replace(m.difficulty, '\s*[-+]\s*$', '', '') AS base_difficulty
            FROM core.maps m
            WHERE m.official IS TRUE
              AND m.archived IS FALSE
              AND m.hidden   IS FALSE
        ),
        completion_data AS (
            SELECT
                c.map_id,
                COUNT(*) AS completions
            FROM core.completions c
            JOIN eligible_maps em ON em.id = c.map_id
            GROUP BY c.map_id
        ),
        rating_data AS (
            SELECT
                em.id AS map_id,
                code,
                em.base_difficulty,
                AVG(mr.quality) AS quality
            FROM eligible_maps em
            LEFT JOIN maps.ratings mr ON mr.map_id = em.id
            WHERE mr.verified
            GROUP BY em.id, code, em.base_difficulty
        ),
        map_data AS (
            SELECT
                em.id AS map_id,
                em.code,
                COALESCE(cd.completions, 0) AS completions,
                rd.base_difficulty          AS difficulty,
                rd.quality
            FROM eligible_maps em
            LEFT JOIN completion_data cd ON cd.map_id = em.id
            LEFT JOIN rating_data     rd ON rd.map_id = em.id
        ),
        ranked_maps AS (
            SELECT
                md.map_id,
                code,
                md.completions,
                round(md.quality, 2) AS quality,
                md.difficulty,
                        ROW_NUMBER() OVER (
                    PARTITION BY md.difficulty
                    ORDER BY md.completions DESC,
                        md.quality DESC NULLS LAST,
                        md.map_id           -- deterministic tiebreaker; swap if you prefer updated_at, code, etc.
                    ) AS pos
            FROM map_data md
        )
        SELECT code, completions, quality, difficulty, pos AS ranking
        FROM ranked_maps
        WHERE pos <= 5
        ORDER BY
            array_position(ARRAY['Easy','Medium','Hard','Very Hard','Extreme','Hell'], difficulty),
            pos;
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[PopularMapsStatisticsResponse])

    async def get_popular_creators(self) -> list[TopCreatorsResponse]:
        """Return top creators by average map quality (min 3 maps).

        Aggregates average quality per creator across their maps and filters to
        creators with at least three rated maps.

        Returns:
            list[TopCreatorsResponse]: Creators with map count and average quality.
        """
        query = """
        WITH map_creator_data AS (
            SELECT m.code, mc.user_id, round(avg(quality), 2) AS quality
            FROM core.maps m
            LEFT JOIN maps.creators mc ON m.id = mc.map_id
            LEFT JOIN maps.ratings mr ON m.id = mr.map_id
            WHERE quality IS NOT NULL AND mr.verified
            GROUP BY mc.user_id, m.code
        ), quality_data AS (
            SELECT
                count(code) AS map_count,
                coalesce(own.username, u.nickname) AS name,
                avg(quality) AS average_quality
            FROM map_creator_data mcd
            LEFT JOIN core.users u ON mcd.user_id = u.id
            LEFT JOIN users.overwatch_usernames own ON u.id = own.user_id
            GROUP BY mcd.user_id, own.username, u.nickname
            ORDER BY average_quality DESC
        )
        SELECT * FROM quality_data WHERE map_count >= 3
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[TopCreatorsResponse])

    async def get_unarchived_map_count(self) -> list[MapCountsResponse]:
        """Count visible, unarchived maps grouped by map name.

        Returns:
            list[MapCountsResponse]: Per-name counts for non-archived, non-hidden maps.
        """
        query = """
            SELECT
                name as map_name,
                count(m.map_name) as amount
            FROM maps.names amn
            LEFT JOIN core.maps m ON amn.name = m.map_name
            WHERE m.archived IS FALSE AND m.hidden IS FALSE
            GROUP BY name
            ORDER BY amount DESC
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[MapCountsResponse])

    async def get_total_map_count(self) -> list[MapCountsResponse]:
        """Count all maps grouped by map name, regardless of archive/visibility.

        Returns:
            list[MapCountsResponse]: Per-name counts for all maps.
        """
        query = """
            SELECT
                name as map_name,
                count(m.map_name) as amount
            FROM maps.names amn
            LEFT JOIN core.maps m ON amn.name = m.map_name
            GROUP BY name
            ORDER BY amount DESC
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[MapCountsResponse])

    async def get_map_record_progression(self, user_id: int, code: OverwatchCode) -> list[MapRecordProgressionResponse]:
        """Get a user's record progression over time for a specific map.

        Returns all historical record entries (time vs. inserted_at) for the user and map.

        Args:
            user_id: Target user ID.
            code: Overwatch map code.

        Returns:
            list[MapRecordProgressionResponse]: Time-series of record improvements.
        """
        query = """
        WITH target_map AS (
            SELECT
                id AS map_id
            FROM core.maps
            WHERE code = $1
        )
        SELECT
            time,
            inserted_at
        FROM core.completions
        WHERE user_id = $2
            AND map_id = (SELECT map_id FROM target_map)
            AND time < 99999999.99
        ORDER BY time;
        """
        rows = await self._conn.fetch(query, code, user_id)
        return msgspec.convert(rows, list[MapRecordProgressionResponse])

    async def get_time_played_per_rank(self) -> list[TimePlayedPerRankResponse]:
        """Sum verified playtime by base difficulty.

        Aggregates total verified run time across all maps, normalized to base
        difficulty (stripping '+'/'-'), and returns totals per difficulty.

        Returns:
            list[TimePlayedPerRankResponse]: Total seconds played per base difficulty.
        """
        query = r"""
        WITH record_sum_by_map_code AS (
            SELECT
                SUM(c.time) AS total_seconds,
                c.map_id
            FROM core.completions c
            WHERE c.verified
              AND c.time < 99999999.99
            GROUP BY c.map_id
        ),
            difficulty_norm AS (
                SELECT
                    rs.total_seconds,
                    regexp_replace(m.difficulty, '\s*[-+]\s*$', '', '') AS base_difficulty
                FROM record_sum_by_map_code rs
                JOIN core.maps m ON m.id = rs.map_id
            )
        SELECT
            SUM(total_seconds) AS total_seconds,
            base_difficulty    AS difficulty
        FROM difficulty_norm
        WHERE base_difficulty IS NOT NULL
        GROUP BY base_difficulty
        ORDER BY array_position(ARRAY['Easy','Medium','Hard','Very Hard','Extreme','Hell'], base_difficulty);
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[TimePlayedPerRankResponse])


async def provide_community_service(conn: Connection, state: State) -> CommunityService:
    """Litestar DI provider for CommunityService.

    Args:
        conn (asyncpg.Connection): Active asyncpg connection.

    Returns:
        CommunityService: A new service instance bound to the given connection.

    """
    return CommunityService(conn=conn, state=state)
