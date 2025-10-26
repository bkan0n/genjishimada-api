from typing import NamedTuple

import asyncpg
from genjipk_sdk.models import (
    AvatarResponse,
    BackgroundResponse,
    RankCardData,
    RankDetailReadDTO,
)
from genjipk_sdk.models.rank_card import RankCardBadgeSettings
from genjipk_sdk.utilities import DIFFICULTY_TO_RANK_MAP
from genjipk_sdk.utilities.lootbox import sanitize_string
from genjipk_sdk.utilities.types import Rank
from litestar.datastructures import State

from utilities.shared_queries import get_map_mastery_data, get_user_rank_data

from .base import BaseService


class Avatar(NamedTuple):
    """Represents a user avatar.

    Attributes:
        skin (str): The name of the equipped avatar skin.
        pose (str): The name of the equipped avatar pose.
    """

    skin: str
    pose: str


class XPData(NamedTuple):
    """Represents XP and prestige information for a user.

    Attributes:
        xp (float): Total accumulated XP for the user.
        prestige_level (float): Calculated prestige level derived from XP.
        community_rank (str): The user's current community rank string.
    """

    xp: float
    prestige_level: float
    community_rank: str


class RankCardService(BaseService):
    async def set_background(
        self,
        user_id: int,
        background: str,
    ) -> BackgroundResponse:
        """Assign a background to a user's rank card.

        Args:
            user_id (int): The ID of the user.
            background (str): The background name to set.

        Returns:
            BackgroundResponse: The newly set background.
        """
        query = """
            INSERT INTO rank_card.background (user_id, name) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET name = EXCLUDED.name;
        """
        await self._conn.execute(query, user_id, background)
        return BackgroundResponse(name=background)

    async def get_background(self, user_id: int) -> BackgroundResponse:
        """Retrieve the current background for a user.

        Args:
            user_id (int): The ID of the user.

        Returns:
            BackgroundResponse: The background currently set for the user.
        """
        background = await self._get_background_choice(user_id)
        return BackgroundResponse(name=background)

    async def set_avatar_skin(self, user_id: int, skin: str) -> AvatarResponse:
        """Set the skin for a user's avatar.

        Args:
            user_id (int): The ID of the user.
            skin (str): The skin name to set.

        Returns:
            AvatarResponse: The updated avatar containing the new skin.
        """
        query = """
            INSERT INTO rank_card.avatar (user_id, skin) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET skin = EXCLUDED.skin;
        """
        await self._conn.execute(query, user_id, skin)
        return AvatarResponse(skin=skin)

    async def get_avatar_skin(self, user_id: int) -> AvatarResponse:
        """Retrieve the skin for a user's avatar.

        Args:
            user_id (int): The ID of the user.

        Returns:
            AvatarResponse: The avatar containing the user's skin.
        """
        skin, _ = await self._get_avatar(user_id)
        return AvatarResponse(skin=skin)

    async def set_avatar_pose(self, user_id: int, pose: str) -> AvatarResponse:
        """Set the pose for a user's avatar.

        Args:
            user_id (int): The ID of the user.
            pose (str): The pose name to set.

        Returns:
            AvatarResponse: The updated avatar containing the new pose.
        """
        query = """
            INSERT INTO rank_card.avatar (user_id, pose) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET pose = EXCLUDED.pose;
        """
        await self._conn.execute(query, user_id, pose)
        return AvatarResponse(pose=pose)

    async def get_avatar_pose(self, user_id: int) -> AvatarResponse:
        """Retrieve the pose for a user's avatar.

        Args:
            user_id (int): The ID of the user.

        Returns:
            AvatarResponse: The avatar containing the user's pose.
        """
        _, pose = await self._get_avatar(user_id)
        return AvatarResponse(pose=pose)

    async def fetch_badges_settings(self, user_id: int) -> RankCardBadgeSettings:
        """Fetch badge settings for a user, resolving mastery and spray icons.

        Args:
            user_id (int): The ID of the user.

        Returns:
            RankCardBadgeSettings: Badge settings with resolved URLs.
        """
        query = "SELECT * FROM rank_card.badges WHERE user_id = $1;"
        row = await self._conn.fetchrow(query, user_id)
        print(row)
        if not row:
            return RankCardBadgeSettings()
        row_d = {**row}
        row_d.pop("user_id")
        for num in range(1, 7):
            type_col = f"badge_type{num}"
            name_col = f"badge_name{num}"
            url_col = f"badge_url{num}"
            if row_d[type_col] == "mastery":
                mastery = await get_map_mastery_data(self._conn, user_id, row_d[name_col])
                if mastery:
                    cur = mastery[0]
                    row_d[url_col] = cur.icon_url
            elif row_d[type_col] == "spray":
                _sanitized = sanitize_string(row_d[name_col])
                row_d[url_col] = f"assets/rank_card/spray/{_sanitized}.webp"
        return RankCardBadgeSettings(**row_d)

    async def set_badges_settings(self, data: RankCardBadgeSettings, user_id: int) -> None:
        """Persist badge settings for a user.

        Args:
            data (RankCardBadgeSettings): Badge settings to store.
            user_id (int): The ID of the user.
        """
        query = """
            INSERT INTO rank_card.badges (
                user_id,
                badge_name1, badge_type1,
                badge_name2, badge_type2,
                badge_name3, badge_type3,
                badge_name4, badge_type4,
                badge_name5, badge_type5,
                badge_name6, badge_type6
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (user_id) DO UPDATE SET
                badge_name1 = excluded.badge_name1,
                badge_type1 = excluded.badge_type1,
                badge_name2 = excluded.badge_name2,
                badge_type2 = excluded.badge_type2,
                badge_name3 = excluded.badge_name3,
                badge_type3 = excluded.badge_type3,
                badge_name4 = excluded.badge_name4,
                badge_type4 = excluded.badge_type4,
                badge_name5 = excluded.badge_name5,
                badge_type5 = excluded.badge_type5,
                badge_name6 = excluded.badge_name6,
                badge_type6 = excluded.badge_type6
        """
        await self._conn.execute(
            query,
            user_id,
            data.badge_name1,
            data.badge_type1,
            data.badge_name2,
            data.badge_type2,
            data.badge_name3,
            data.badge_type3,
            data.badge_name4,
            data.badge_type4,
            data.badge_name5,
            data.badge_type5,
            data.badge_name6,
            data.badge_type6,
        )

    async def _fetch_community_rank_xp(self, user_id: int) -> XPData:
        """Fetch XP and prestige data for a user.

        Args:
            user_id (int): The ID of the user.

        Returns:
            XPData: The user's XP, prestige, and community rank.
        """
        query = """
            SELECT
                coalesce(xp.amount, 0) AS xp,
                (coalesce(xp.amount, 0) / 100) / 100 AS prestige_level,
                x.name || ' ' || s.name AS community_rank
            FROM core.users u
            LEFT JOIN lootbox.xp xp ON u.id = xp.user_id
            LEFT JOIN lootbox.main_tiers x ON (((coalesce(xp.amount, 0) / 100) % 100)) / 5 = x.threshold
            LEFT JOIN lootbox.sub_tiers s ON (coalesce(xp.amount, 0) / 100) % 5 = s.threshold
            WHERE u.id = $1
        """
        row = await self._conn.fetchrow(query, user_id)
        assert row
        return XPData(row["xp"], row["prestige_level"], row["community_rank"])

    def _find_highest_rank(self, data: list[RankDetailReadDTO]) -> Rank:
        """Determine the highest rank achieved by a user.

        Args:
            data (list[RankDetailReadDTO]): Rank detail entries for the user.

        Returns:
            Rank: The name of the highest achieved rank.
        """
        highest = "Ninja"
        for row in data:
            if row.rank_met:
                highest = DIFFICULTY_TO_RANK_MAP[row.difficulty]
        return highest

    async def _get_avatar(self, user_id: int) -> Avatar:
        """Retrieve avatar skin and pose for a user.

        Args:
            user_id (int): The ID of the user.

        Returns:
            Avatar: The user's avatar with skin and pose.
        """
        query = "SELECT skin, pose FROM rank_card.avatar WHERE user_id = $1;"
        row = await self._conn.fetchrow(query, user_id)
        if row:
            return Avatar(row["skin"], row["pose"])
        return Avatar("Overwatch 1", "Heroic")

    async def _fetch_nickname(self, user_id: int) -> str:
        """Retrieve the nickname or primary Overwatch username for a user.

        Args:
            user_id (int): The ID of the user.

        Returns:
            str: The user's nickname or primary username.
        """
        nickname_query = """
                WITH default_name AS (
                    SELECT nickname, id as user_id
                    FROM core.users
                )
                SELECT coalesce(own.username, dn.nickname) AS nickname
                FROM default_name dn
                LEFT JOIN users.overwatch_usernames own ON own.user_id = dn.user_id AND own.is_primary = TRUE
                WHERE dn.user_id = $1;
            """
        return await self._conn.fetchval(nickname_query, user_id)

    async def _get_map_totals(self) -> list[asyncpg.Record]:
        """Get the total count of official, non-archived maps by difficulty.

        Returns:
            list[asyncpg.Record]: A list of records containing difficulty and count.
        """
        query = r"""
            SELECT
                regexp_replace(m.difficulty::text, '\s*[-+]\s*$', '') AS base_difficulty,
                count(*) AS total
            FROM core.maps AS m
            WHERE m.official = TRUE
                AND m.archived = FALSE
            GROUP BY base_difficulty
            ORDER BY base_difficulty;
        """
        return await self._conn.fetch(query)

    async def _get_world_record_count(self, user_id: int) -> int:
        """Count how many world records a user currently holds.

        Args:
            user_id (int): The ID of the user.

        Returns:
            int: The number of world records held.
        """
        query = """
            WITH all_records AS (
                SELECT
                    user_id,
                    m.code,
                    time,
                    rank() OVER (
                        PARTITION BY m.code
                        ORDER BY time
                    ) as pos
                FROM core.completions c
                LEFT JOIN core.maps m on c.map_id = m.id
                WHERE m.official = TRUE AND time < 99999999 AND video IS NOT NULL AND completion IS FALSE
            )
            SELECT count(*) FROM all_records WHERE user_id = $1 AND pos = 1
        """
        return await self._conn.fetchval(query, user_id)

    async def _get_maps_created_count(self, user_id: int) -> int:
        """Count how many official maps a user has created.

        Args:
            user_id (int): The ID of the user.

        Returns:
            int: The total maps created by the user.
        """
        query = """
            SELECT count(*)
            FROM core.maps m
            LEFT JOIN maps.creators mc ON m.id = mc.map_id
            WHERE user_id = $1 AND official = TRUE
        """
        return await self._conn.fetchval(query, user_id)

    async def _get_playtests_voted_count(self, user_id: int) -> int:
        """Count how many playtests a user has voted in.

        Args:
            user_id (int): The ID of the user.

        Returns:
            int: The number of playtest votes.
        """
        query = "SELECT count(*) FROM playtests.votes WHERE user_id=$1;"
        return await self._conn.fetchval(query, user_id) or 0

    async def _get_background_choice(self, user_id: int) -> str:
        """Retrieve a user's background choice, falling back to a placeholder.

        Args:
            user_id (int): The ID of the user.

        Returns:
            str: The name of the chosen background.
        """
        query = "SELECT name FROM rank_card.background WHERE user_id = $1"
        return await self._conn.fetchval(query, user_id) or "placeholder"

    async def fetch_rank_card_data(self, user_id: int) -> RankCardData:
        """Assemble all rank card data for a user.

        Args:
            user_id (int): The ID of the user.

        Returns:
            RankCardData: The user's complete rank card information including
                ranks, nickname, avatar, badges, stats, and XP.
        """
        rank_data = await get_user_rank_data(self._conn, user_id)
        rank = self._find_highest_rank(rank_data)
        nickname = await self._fetch_nickname(user_id)
        background = await self._get_background_choice(user_id)
        maps = await self._get_maps_created_count(user_id)
        playtests = await self._get_playtests_voted_count(user_id)
        world_records = await self._get_world_record_count(user_id)
        avatar = await self._get_avatar(user_id)
        badges = await self.fetch_badges_settings(user_id)
        totals = await self._get_map_totals()
        xp_data = await self._fetch_community_rank_xp(user_id)

        data = {
            "rank_name": rank,
            "nickname": nickname,
            "background": background,
            "total_maps_created": maps,
            "total_playtests": playtests,
            "world_records": world_records,
            "difficulties": {},
            "avatar_skin": avatar.skin,
            "avatar_pose": avatar.pose,
            "badges": badges,
            "xp": xp_data.xp,
            "prestige_level": xp_data.prestige_level,
            "community_rank": xp_data.community_rank,
        }
        for row in rank_data:
            data["difficulties"][row.difficulty] = {
                "completed": row.completions,
                "gold": row.gold,
                "silver": row.silver,
                "bronze": row.bronze,
            }

        for total in totals:
            data["difficulties"][total["base_difficulty"]]["total"] = total["total"]
        _d = RankCardData(**data)
        return _d


async def provide_rank_card_service(conn: asyncpg.Connection, state: State) -> RankCardService:
    """Litestar DI provider for RankCardService.

    Args:
        conn (asyncpg.Connection): Active asyncpg connection.
        state (State): Application state.

    Returns:
        RankCardService: A new service instance bound to the given connection.

    """
    return RankCardService(conn=conn, state=state)
