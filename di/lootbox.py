import random

import msgspec
from asyncpg import Connection
from genjipk_sdk.models import (
    LootboxKeyType,
    LootboxKeyTypeResponse,
    RewardTypeResponse,
    TierChange,
    UserLootboxKeyAmountsResponse,
    UserRewardsResponse,
    XpGrant,
    XpGrantResult,
)
from genjipk_sdk.models.xp import XpGrantMQ
from litestar import Request
from litestar.datastructures import State
from litestar.datastructures.headers import Headers
from litestar.exceptions import HTTPException

from .base import BaseService

WEIGHTS = {
    "Legendary": {
        "weight": 3,
    },
    "Epic": {
        "weight": 5,
    },
    "Rare": {
        "weight": 25,
    },
    "Common": {
        "weight": 65,
    },
}


def gacha(amount: int) -> list[str]:
    """Perform a weighted gacha roll.

    Uses predefined rarity weights to randomly select items.

    Args:
        amount (int): Number of pulls to perform.

    Returns:
        list[str]: List of rarity strings (e.g., "Common", "Rare", "Epic", "Legendary").

    """
    pulls = random.choices(list(WEIGHTS.keys()), [x["weight"] for x in WEIGHTS.values()], k=amount)
    return pulls


class LootboxService(BaseService):
    async def view_all_rewards(
        self, reward_type: str | None = None, key_type: LootboxKeyType | None = None, rarity: str | None = None
    ) -> list[RewardTypeResponse]:
        """View all possible rewards.

        Args:
            reward_type (str | None): Optional filter by reward type.
            key_type (LootboxKeyType | None): Optional filter by key type.
            rarity (str | None): Optional filter by rarity.

        Returns:
            list[RewardTypeResponse]: All rewards matching filters.

        """
        query = """
            SELECT *
            FROM lootbox.reward_types
            WHERE
                ($1::text IS NULL OR type = $1::text) AND
                ($2::text IS NULL OR key_type = $2::text) AND
                ($3::text IS NULL OR rarity = $3::text)
            ORDER BY key_type, name
        """
        rows = await self._conn.fetch(query, reward_type, key_type, rarity)
        return msgspec.convert(rows, list[RewardTypeResponse])

    async def view_all_keys(self, key_type: LootboxKeyType | None = None) -> list[LootboxKeyTypeResponse]:
        """View all possible key types.

        Args:
            key_type (LootboxKeyType | None): Optional filter by key type name.

        Returns:
            list[LootboxKeyTypeResponse]: All keys matching filter.

        """
        query = """
            SELECT *
            FROM lootbox.key_types
            WHERE
                ($1::text IS NULL OR name = $1::text)
            ORDER BY name
        """
        rows = await self._conn.fetch(query, key_type)
        return msgspec.convert(rows, list[LootboxKeyTypeResponse])

    async def view_user_rewards(
        self,
        user_id: int,
        reward_type: str | None = None,
        key_type: LootboxKeyType | None = None,
        rarity: str | None = None,
    ) -> list[UserRewardsResponse]:
        """View all rewards earned by a specific user.

        Args:
            user_id (int): Target user ID.
            reward_type (str | None): Optional filter by reward type.
            key_type (LootboxKeyType | None): Optional filter by key type.
            rarity (str | None): Optional filter by rarity.

        Returns:
            list[UserRewardsResponse]: User reward data.

        """
        query = """
            SELECT DISTINCT ON (rt.name, rt.key_type, rt.type)
                ur.user_id,
                ur.earned_at,
                rt.name,
                rt.type,
                NULL as medal,
                rt.rarity
            FROM lootbox.user_rewards ur
            LEFT JOIN lootbox.reward_types rt ON ur.reward_name = rt.name
                AND ur.reward_type = rt.type
                AND ur.key_type = rt.key_type
            WHERE
                ur.user_id = $1::bigint AND
                ($2::text IS NULL OR rt.type = $2::text) AND
                ($3::text IS NULL OR ur.key_type = $3::text) AND
                ($4::text IS NULL OR rarity = $4::text)

            UNION ALL

            SELECT
                user_id,
                now() as earned_at,
                map_name as name,
                'mastery' as type,
                medal,
                'common' as rarity
            FROM maps.mastery
            WHERE user_id = $1::bigint AND medal != 'Placeholder' AND ($2::text IS NULL OR medal = $2::text)
        """
        rows = await self._conn.fetch(query, user_id, reward_type, key_type, rarity)
        return msgspec.convert(rows, list[UserRewardsResponse])

    async def view_user_keys(
        self,
        user_id: int,
        key_type: LootboxKeyType | None = None,
    ) -> list[UserLootboxKeyAmountsResponse]:
        """View keys owned by a user.

        Args:
            user_id (int): Target user ID.
            key_type (LootboxKeyType | None): Optional filter by key type.

        Returns:
            list[UserLootboxKeyAmountsResponse]: Keys and amounts per type.

        """
        query = """
            SELECT count(*) as amount, key_type
            FROM lootbox.user_keys
            WHERE
                ($1::bigint = user_id) AND
                ($2::text IS NULL OR key_type = $2::text)
            GROUP BY key_type
        """

        rows = await self._conn.fetch(query, user_id, key_type)
        return msgspec.convert(rows, list[UserLootboxKeyAmountsResponse])

    async def _get_user_key_count(self, user_id: int, key_type: LootboxKeyType) -> int:
        """Get the number of keys a user has of a given type.

        Args:
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type.

        Returns:
            int: Number of keys.

        """
        query = "SELECT count(*) as keys FROM lootbox.user_keys WHERE key_type = $1 AND user_id = $2"
        return await self._conn.fetchval(query, key_type, user_id)

    async def _use_user_key(self, user_id: int, key_type: LootboxKeyType) -> None:
        """Consume the oldest key of a given type for a user.

        Args:
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type.

        """
        query = """
            DELETE FROM lootbox.user_keys
            WHERE earned_at = (
                SELECT MIN(earned_at)
                FROM lootbox.user_keys
                WHERE user_id = $1::bigint AND key_type = $2::text
            ) AND user_id = $1::bigint AND key_type = $2::text;
        """
        await self._conn.execute(query, user_id, key_type)

    async def get_random_items(
        self,
        request: Request,
        user_id: int,
        key_type: LootboxKeyType,
        amount: int = 3,
    ) -> list[RewardTypeResponse]:
        """Draw random rewards for a user using gacha.

        Validates key ownership unless in test mode.
        Rewards may be converted into coins if duplicates are rolled.

        Args:
            request (Request): Current request (for test-mode header).
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type consumed.
            amount (int): Number of rewards to pull.

        Returns:
            list[RewardTypeResponse]: Selected rewards.

        Raises:
            HTTPException: If the user has insufficient keys.

        """
        key_count = await self._get_user_key_count(user_id, key_type)
        if key_count <= 0 and not request.headers.get("x-test-mode"):
            raise HTTPException(detail="User does not have enough keys for this action.", status_code=400)

        rarities = gacha(amount)
        items = []
        query = """
            WITH selected_rewards AS (
                SELECT *
                FROM lootbox.reward_types
                WHERE
                    rarity = $1::text AND
                    key_type = $2::text
                ORDER BY random()
                LIMIT 1
            )
            SELECT
                sr.*,
                EXISTS(
                    SELECT 1
                    FROM lootbox.user_rewards ur
                    WHERE ur.user_id = $3::bigint AND
                        ur.reward_name = sr.name AND
                        ur.reward_type = sr.type AND
                        ur.key_type = $2::text
                ) AS duplicate,
                CASE
                    WHEN EXISTS(
                        SELECT 1
                        FROM lootbox.user_rewards ur
                        WHERE ur.user_id = $3::bigint AND
                            ur.reward_name = sr.name AND
                            ur.reward_type = sr.type AND
                            ur.key_type = $2::text
                    )
                    THEN CASE
                        WHEN sr.rarity = 'common' THEN 100
                        WHEN sr.rarity = 'rare' THEN 250
                        WHEN sr.rarity = 'epic' THEN 500
                        WHEN sr.rarity = 'legendary' THEN 1000
                        ELSE 0
                    END
                ELSE 0
                END AS coin_amount
            FROM selected_rewards sr;
        """
        for rarity in rarities:
            reward = await self._conn.fetchrow(query, rarity.lower(), key_type, user_id)
            items.append(reward)

        return msgspec.convert(items, list[RewardTypeResponse])

    async def grant_reward_to_user(
        self,
        request: Request,
        user_id: int,
        key_type: LootboxKeyType,
        reward_type: str,
        reward_name: str,
    ) -> None:
        """Grant a specific reward to a user.

        Converts duplicates into coin rewards. Consumes a key unless in test mode.

        Args:
            request (Request): Current request (for test-mode header).
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type to consume.
            reward_type (str): Type of reward to grant.
            reward_name (str): Name of the reward or coin amount.

        Raises:
            HTTPException: If the user has insufficient keys.

        """
        key_count = await self._get_user_key_count(user_id, key_type)
        if key_count <= 0 and not request.headers.get("x-test-mode"):
            raise HTTPException(detail="User does not have enough keys for this action.", status_code=400)

        query = """
            SELECT rt.rarity
            FROM lootbox.user_rewards ur
            JOIN lootbox.reward_types rt ON ur.reward_name = rt.name
                AND ur.reward_type = rt.type
                AND ur.key_type = rt.key_type
            WHERE ur.user_id = $1::bigint AND
              ur.reward_type = $2::text AND
              ur.key_type = $3::text AND
              ur.reward_name = $4::text
        """
        is_duplicate = await self._conn.fetchval(query, user_id, reward_type, key_type, reward_name)
        if is_duplicate:
            reward_type = "coins"
            coin_convert = {
                "common": 100,
                "rare": 250,
                "epic": 500,
                "legendary": 1000,
            }
            reward_name = str(coin_convert[is_duplicate])

        async with self._conn.transaction():
            if not request.headers.get("x-test-mode"):
                await self._use_user_key(user_id, key_type)
            if reward_type != "coins":
                query = """
                    INSERT INTO lootbox.user_rewards (user_id, reward_type, key_type, reward_name)
                    VALUES ($1, $2, $3, $4)
                """
                await self._conn.execute(query, user_id, reward_type, key_type, reward_name)
            else:
                query = """
                    INSERT INTO core.users (id, coins) VALUES ($1, $2)
                    ON CONFLICT (id) DO UPDATE SET coins = users.coins + excluded.coins
                """
                await self._conn.execute(query, user_id, int(reward_name))

    async def grant_key_to_user(self, user_id: int, key_type: LootboxKeyType) -> None:
        """Grant a lootbox key to a user.

        Args:
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type to grant.

        """
        query = "INSERT INTO lootbox.user_keys (user_id, key_type) VALUES ($1, $2)"
        await self._conn.execute(query, user_id, key_type)

    async def grant_active_key_to_user(self, user_id: int) -> None:
        """Grant the currently active lootbox key to a user.

        Args:
            user_id (int): Target user ID.

        """
        query = """
            INSERT INTO lootbox.user_keys (user_id, key_type)
            SELECT $1, key FROM lootbox.active_key LIMIT 1;
        """
        await self._conn.execute(query, user_id)

    async def debug_grant_reward_no_key(
        self,
        user_id: int,
        key_type: LootboxKeyType,
        reward_type: str,
        reward_name: str,
    ) -> None:
        """DEBUG ONLY: Grant a reward to a user without consuming a key.

        Args:
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type.
            reward_type (str): Reward type.
            reward_name (str): Reward name or coin amount.

        """
        if reward_type != "coins":
            query = """
                INSERT INTO lootbox.user_rewards (user_id, reward_type, key_type, reward_name)
                VALUES ($1, $2, $3, $4)
            """
            await self._conn.execute(query, user_id, reward_type, key_type, reward_name)
        else:
            query = """
                INSERT INTO core.users (id, coins) VALUES ($1, $2)
                ON CONFLICT (id) DO UPDATE SET coins = users.coins + excluded.coins
            """
            await self._conn.execute(query, user_id, int(reward_name))

    async def set_active_key(self, request: Request, key_type: LootboxKeyType) -> None:
        """Set the globally active lootbox key.

        Args:
            request (Request): Current request (for test-mode header).
            key_type (LootboxKeyType): Key type to set as active.

        """
        if request.headers.get("x-test-mode"):
            return
        query = "UPDATE lootbox.active_key SET key = $1;"
        await self._conn.execute(query, key_type)

    async def get_user_coins_amount(
        self,
        user_id: int,
    ) -> int:
        """Get the number of coins a user has.

        Args:
            user_id (int): Target user ID.

        Returns:
            int: Coin amount. Returns 0 if no record exists.

        """
        query = "SELECT coins FROM core.users WHERE id = $1;"
        amount = await self._conn.fetchval(query, user_id)
        if amount is None:
            return 0
        return amount

    async def grant_user_xp(self, headers: Headers, user_id: int, data: XpGrant) -> XpGrantResult:
        """Grant XP to a user.

        Performs an upsert into the XP table, summing new and existing values.

        Args:
            user_id (int): Target user ID.
            data (XpGrant): XP grant payload.

        Returns:
            XpGrantResult: Previous and new XP amounts.

        """
        query = """
        WITH mult AS (
          SELECT COALESCE((SELECT value FROM lootbox.xp_multiplier LIMIT 1), 1)::numeric AS m
        ),
        old_values AS (
          SELECT amount
          FROM lootbox.xp
          WHERE user_id = $1
        ),
        upsert_result AS (
          INSERT INTO lootbox.xp (user_id, amount)
          SELECT $1, floor($2::numeric * (SELECT m FROM mult))::bigint
          ON CONFLICT (user_id) DO UPDATE
          SET amount = lootbox.xp.amount + EXCLUDED.amount
          RETURNING lootbox.xp.amount
        )
        SELECT
          COALESCE((SELECT amount FROM old_values), 0) AS previous_amount,
          (SELECT amount FROM upsert_result)           AS new_amount;
        """
        row = await self._conn.fetchrow(query, user_id, data.amount)
        result = msgspec.convert(row, XpGrantResult)
        message = XpGrantMQ(
            user_id=user_id,
            amount=data.amount,
            type=data.type,
            previous_amount=result.previous_amount,
            new_amount=result.new_amount,
        )
        await self.publish_message(routing_key="api.xp.grant", data=message, headers=headers)
        return result

    async def get_xp_tier_change(self, old_xp: int, new_xp: int) -> TierChange:
        """Calculate tier change when XP is updated.

        Determines whether the user has ranked up, sub-ranked up,
        or achieved a prestige level change.

        Args:
            old_xp (int): Previous XP amount.
            new_xp (int): New XP amount.

        Returns:
            TierChange: Tier and prestige change details.

        """
        query = """
            WITH old_tier AS (
                SELECT
                    $1::int AS old_xp,
                    (($1 / 100) % 100) AS old_normalized_tier,
                    (($1 / 100) / 100) AS old_prestige_level,
                    x.name AS old_main_tier_name,
                    s.name AS old_sub_tier_name
                FROM lootbox.main_tiers x
                LEFT JOIN lootbox.sub_tiers s ON (($1 / 100) % 5) = s.threshold
                WHERE (($1 / 100) % 100) / 5 = x.threshold
            ),
            new_tier AS (
                SELECT
                    $2::int AS new_xp,
                    (($2 / 100) % 100) AS new_normalized_tier,
                    (($2 / 100) / 100) AS new_prestige_level,
                    x.name AS new_main_tier_name,
                    s.name AS new_sub_tier_name
                FROM lootbox.main_tiers x
                LEFT JOIN lootbox.sub_tiers s ON (($2 / 100) % 5) = s.threshold
                WHERE (($2 / 100) % 100) / 5 = x.threshold
            )
            SELECT
                o.old_xp,
                n.new_xp,
                o.old_main_tier_name,
                n.new_main_tier_name,
                o.old_sub_tier_name,
                n.new_sub_tier_name,
                old_prestige_level,
                new_prestige_level,
                CASE
                    WHEN o.old_main_tier_name != n.new_main_tier_name THEN 'Main Tier Rank Up'
                    WHEN o.old_sub_tier_name != n.new_sub_tier_name THEN 'Sub-Tier Rank Up'
                END AS rank_change_type,
                o.old_prestige_level != n.new_prestige_level AS prestige_change
            FROM old_tier o
            JOIN new_tier n ON TRUE;
        """
        row = await self._conn.fetchrow(query, old_xp, new_xp)
        return msgspec.convert(row, TierChange)

    async def edit_xp_multiplier(self, multiplier: float) -> None:
        """Edit the XP multiplier.

        Args:
            multiplier (float): Value to multiply the XP by.
        """
        query = "UPDATE lootbox.xp_multiplier SET value=$1;"
        await self._conn.execute(query, multiplier)

    async def get_xp_multiplier(self) -> float:
        """Get the XP multiplier that is currently set.

        Returns:
            float: The XP multiplier.
        """
        query = "SELECT * FROM lootbox.xp_multiplier LIMIT 1;"
        return await self._conn.fetchval(query)


async def provide_lootbox_service(conn: Connection, state: State) -> LootboxService:
    """Provide LootboxService DI.

    Args:
        conn (Connection): Active asyncpg connection.

    Returns:
        LootboxService: New service instance.

    """
    return LootboxService(conn, state=state)
