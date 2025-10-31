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
from genjipk_sdk.models.maps import XPMultiplierDTO
from litestar import Controller, Request, get, patch, post
from litestar.di import Provide

from di import LootboxService, provide_lootbox_service


class LootboxController(Controller):
    """Controller exposing endpoints for lootbox rewards, keys, coins, and XP progression."""

    path = "/lootbox"
    tags = ["Lootbox"]
    dependencies = {"svc": Provide(provide_lootbox_service)}

    @get(
        path="/rewards",
        summary="List All Rewards",
        description="Retrieve all available rewards, optionally filtered by type, key type, or rarity.",
    )
    async def view_all_rewards(
        self,
        svc: LootboxService,
        reward_type: str | None = None,
        key_type: LootboxKeyType | None = None,
        rarity: str | None = None,
    ) -> list[RewardTypeResponse]:
        """Retrieve all available rewards.

        Args:
            svc (LootboxService): Lootbox service dependency.
            reward_type (str | None): Optional filter by reward type.
            key_type (LootboxKeyType | None): Optional filter by key type.
            rarity (str | None): Optional filter by rarity.

        Returns:
            list[RewardTypeResponse]: Rewards matching filters.

        """
        return await svc.view_all_rewards(reward_type, key_type, rarity)

    @get(
        path="/keys",
        summary="List All Key Types",
        description="Retrieve all possible lootbox key types, optionally filtered by key type name.",
    )
    async def view_all_keys(
        self,
        svc: LootboxService,
        key_type: LootboxKeyType | None = None,
    ) -> list[LootboxKeyTypeResponse]:
        """Retrieve all possible key types.

        Args:
            svc (LootboxService): Lootbox service dependency.
            key_type (LootboxKeyType | None): Optional filter by key type.

        Returns:
            list[LootboxKeyTypeResponse]: Key types matching filter.

        """
        return await svc.view_all_keys(key_type)

    @get(
        path="/users/{user_id:int}/rewards",
        summary="Get User Rewards",
        description=(
            "Retrieve all rewards owned by a specific user, with optional filters for type, key type, or rarity."
        ),
    )
    async def view_user_rewards(
        self,
        svc: LootboxService,
        user_id: int,
        reward_type: str | None = None,
        key_type: LootboxKeyType | None = None,
        rarity: str | None = None,
    ) -> list[UserRewardsResponse]:
        """Retrieve rewards owned by a user.

        Args:
            svc (LootboxService): Lootbox service dependency.
            user_id (int): Target user ID.
            reward_type (str | None): Optional filter by reward type.
            key_type (LootboxKeyType | None): Optional filter by key type.
            rarity (str | None): Optional filter by rarity.

        Returns:
            list[UserRewardsResponse]: Rewards the user has earned.

        """
        return await svc.view_user_rewards(user_id, reward_type, key_type, rarity)

    @get(
        path="/users/{user_id:int}/keys",
        summary="Get User Keys",
        description="Retrieve all lootbox keys owned by a specific user, with optional filtering by key type.",
    )
    async def view_user_keys(
        self,
        svc: LootboxService,
        user_id: int,
        key_type: LootboxKeyType | None = None,
    ) -> list[UserLootboxKeyAmountsResponse]:
        """Retrieve keys owned by a user.

        Args:
            svc (LootboxService): Lootbox service dependency.
            user_id (int): Target user ID.
            key_type (LootboxKeyType | None): Optional filter by key type.

        Returns:
            list[UserLootboxKeyAmountsResponse]: Keys and amounts per type.

        """
        return await svc.view_user_keys(user_id, key_type)

    @get(
        path="/users/{user_id:int}/keys/{key_type:str}",
        summary="Draw Random Rewards",
        description=(
            "Consume lootbox keys to draw random rewards for a user. Duplicates are converted into coin rewards."
        ),
    )
    async def get_random_items(
        self,
        request: Request,
        svc: LootboxService,
        user_id: int,
        key_type: LootboxKeyType,
        amount: int = 3,
    ) -> list[RewardTypeResponse]:
        """Draw random rewards for a user using lootbox keys.

        Args:
            request (Request): Current request (for test-mode header).
            svc (LootboxService): Lootbox service dependency.
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type being consumed.
            amount (int): Number of rewards to pull.

        Returns:
            list[RewardTypeResponse]: Selected rewards.

        """
        return await svc.get_random_items(request, user_id, key_type, amount)

    @post(
        path="/users/{user_id:int}/{key_type:str}/{reward_type:str}/{reward_name:str}",
        summary="Grant Reward to User",
        description=(
            "Grant a specific reward to a user. Duplicates may be converted into coins. "
            "Consumes a key unless in test mode."
        ),
    )
    async def grant_reward_to_user(  # noqa: PLR0913
        self,
        request: Request,
        svc: LootboxService,
        user_id: int,
        key_type: LootboxKeyType,
        reward_type: str,
        reward_name: str,
    ) -> None:
        """Grant a specific reward to a user.

        Args:
            request (Request): Current request (for test-mode header).
            svc (LootboxService): Lootbox service dependency.
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type to consume.
            reward_type (str): Type of reward to grant.
            reward_name (str): Name of the reward or coin value.

        """
        return await svc.grant_reward_to_user(request, user_id, key_type, reward_type, reward_name)

    @post(
        path="/users/{user_id:int}/keys/{key_type:str}",
        summary="Grant Key to User",
        description="Grant a specific lootbox key to a user.",
    )
    async def grant_key_to_user(self, svc: LootboxService, user_id: int, key_type: LootboxKeyType) -> None:
        """Grant a lootbox key to a user.

        Args:
            svc (LootboxService): Lootbox service dependency.
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type to grant.

        """
        return await svc.grant_key_to_user(user_id, key_type)

    @post(
        path="/users/{user_id:int}/keys",
        summary="Grant Active Key to User",
        description="Grant the globally active key to a user.",
    )
    async def grant_active_key_to_user(self, svc: LootboxService, user_id: int) -> None:
        """Grant the currently active key to a user.

        Args:
            svc (LootboxService): Lootbox service dependency.
            user_id (int): Target user ID.

        """
        return await svc.grant_active_key_to_user(user_id)

    @post(
        path="/users/debug/{user_id:int}/{key_type:str}/{reward_type:str}/{reward_name:str}",
        summary="DEBUG: Grant Reward Without Key",
        description="For debugging only. Grants a reward to a user without consuming a key.",
    )
    async def debug_grant_reward_no_key(
        self,
        svc: LootboxService,
        user_id: int,
        key_type: LootboxKeyType,
        reward_type: str,
        reward_name: str,
    ) -> None:
        """DEBUG ONLY: Grant a reward to a user without consuming a key.

        Args:
            svc (LootboxService): Lootbox service dependency.
            user_id (int): Target user ID.
            key_type (LootboxKeyType): Key type.
            reward_type (str): Reward type.
            reward_name (str): Reward name or coin amount.

        """
        return await svc.debug_grant_reward_no_key(user_id, key_type, reward_type, reward_name)

    @patch(
        path="/keys/{key_type:str}",
        summary="Set Active Key",
        description="Set the globally active lootbox key type.",
    )
    async def set_active_key(self, svc: LootboxService, request: Request, key_type: LootboxKeyType) -> None:
        """Set the globally active lootbox key.

        Args:
            svc (LootboxService): Lootbox service dependency.
            request (Request): Current request (for test-mode header).
            key_type (LootboxKeyType): Key type to set as active.

        """
        return await svc.set_active_key(request, key_type)

    @get(
        path="/users/{user_id:int}/coins",
        summary="Get User Coin Balance",
        description="Retrieve the current coin balance for a user.",
    )
    async def get_user_coins_amount(self, svc: LootboxService, user_id: int) -> int:
        """Retrieve the coin balance for a user.

        Args:
            svc (LootboxService): Lootbox service dependency.
            user_id (int): Target user ID.

        Returns:
            int: Amount of coins.

        """
        return await svc.get_user_coins_amount(user_id)

    @post(
        path="/users/{user_id:int}/xp",
        summary="Grant XP to User",
        description="Add XP to a user and return their previous and new totals.",
    )
    async def grant_user_xp(self, request: Request, svc: LootboxService, user_id: int, data: XpGrant) -> XpGrantResult:
        """Grant XP to a user.

        Args:
            svc (LootboxService): Lootbox service dependency.
            user_id (int): Target user ID.
            data (XpGrant): XP grant payload.
            request (Request): Request obj.

        Returns:
            XpGrantResult: Previous and new XP totals.

        """
        return await svc.grant_user_xp(request.headers, user_id, data)

    @get(
        path="/xp/tier",
        summary="Get XP Tier Change",
        description="Calculate whether a user's tier or prestige has changed after gaining XP.",
    )
    async def get_xp_tier_change(self, svc: LootboxService, old_xp: int, new_xp: int) -> TierChange:
        """Calculate a user's tier change after an XP update.

        Args:
            svc (LootboxService): Lootbox service dependency.
            old_xp (int): Previous XP total.
            new_xp (int): New XP total.

        Returns:
            TierChange: Tier progression details.

        """
        return await svc.get_xp_tier_change(old_xp, new_xp)

    @post(
        path="/xp/multiplier",
        summary="Change XP Multiplier",
        description="Change the XP multiplier, e.g. double XP weekends.",
    )
    async def edit_xp_multiplier(self, svc: LootboxService, data: XPMultiplierDTO) -> None:
        """Change the XP Multiplier.

        Args:
            svc (LootboxService): Lootbox DI service.
            data (XPMultiplierDTO): Data.
        """
        await svc.edit_xp_multiplier(data.value)

    @get(
        path="/xp/multiplier",
        summary="Get XP Multiplier",
        description="Get the XP multiplier, e.g. double XP weekends.",
    )
    async def get_xp_multiplier(self, svc: LootboxService) -> float:
        """Get the XP multiplier, e.g. double XP weekends.

        Args:
            svc (LootboxService): Lootbox DI service.
        """
        return await svc.get_xp_multiplier()
