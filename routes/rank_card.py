from genjipk_sdk.rank_card import AvatarResponse, BackgroundResponse, RankCardBadgeSettings, RankCardResponse
from litestar import Controller, get, put
from litestar.di import Provide
from msgspec import Struct

from di import provide_rank_card_service
from di.rank_card import RankCardService


class BackgroundBody(Struct):
    name: str


class AvatarSkinBody(Struct):
    skin: str


class AvatarPoseBody(Struct):
    pose: str


class RankCardController(Controller):
    path = "/users/{user_id:int}/rank-card"
    tags = ["Rank Card"]
    dependencies = {"svc": Provide(provide_rank_card_service)}

    @get(
        "/",
        summary="Get rank card data",
        description="Return full rank card payload including rank, avatar, badges, map totals, and XP.",
    )
    async def get_rank_card(self, svc: RankCardService, user_id: int) -> RankCardResponse:
        """Get the full rank card payload for a user.

        Aggregates rank, nickname, avatar, background, badge settings, per-difficulty
        progress, map/playtest counts, world records, and XP/prestige info.

        Args:
            svc (RankCardService): Injected rank card service.
            user_id (int): Target user ID from the URL path.

        Returns:
            RankCardResponse: The complete rank card model ready for rendering.
        """
        return await svc.fetch_rank_card_data(user_id)

    @get(
        "/background",
        summary="Get background",
        description="Return the user's current rank-card background.",
    )
    async def get_background(self, svc: RankCardService, user_id: int) -> BackgroundResponse:
        """Get the user's current rank-card background.

        Args:
            svc (RankCardService): Injected rank card service.
            user_id (int): Target user ID from the URL path.

        Returns:
            BackgroundResponse: The background name and resolved asset URL.
        """
        return await svc.get_background(user_id)

    @put(
        "/background",
        summary="Set background",
        description="Set the user's rank-card background by name.",
    )
    async def set_background(self, svc: RankCardService, user_id: int, data: BackgroundBody) -> BackgroundResponse:
        """Set the user's rank-card background.

        Args:
            svc (RankCardService): Injected rank card service.
            user_id (int): Target user ID from the URL path.
            data (BackgroundBody): Payload containing the background name.

        Returns:
            BackgroundResponse: The updated background name and resolved asset URL.
        """
        return await svc.set_background(user_id, data.name)

    @get(
        "/avatar/skin",
        summary="Get avatar skin",
        description="Return the user's current avatar skin.",
    )
    async def get_avatar_skin(self, svc: RankCardService, user_id: int) -> AvatarResponse:
        """Get the user's current avatar skin.

        Args:
            svc (RankCardService): Injected rank card service.
            user_id (int): Target user ID from the URL path.

        Returns:
            AvatarResponse: The avatar skin and resolved asset URL.
        """
        return await svc.get_avatar_skin(user_id)

    @put(
        "/avatar/skin",
        summary="Set avatar skin",
        description="Set the user's avatar skin by name.",
    )
    async def set_avatar_skin(self, svc: RankCardService, user_id: int, data: AvatarSkinBody) -> AvatarResponse:
        """Set the user's avatar skin.

        Args:
            svc (RankCardService): Injected rank card service.
            user_id (int): Target user ID from the URL path.
            data (AvatarSkinBody): Payload containing the skin name.

        Returns:
            AvatarResponse: The updated avatar skin and resolved asset URL.
        """
        return await svc.set_avatar_skin(user_id, data.skin)

    @get(
        "/avatar/pose",
        summary="Get avatar pose",
        description="Return the user's current avatar pose.",
    )
    async def get_avatar_pose(self, svc: RankCardService, user_id: int) -> AvatarResponse:
        """Get the user's current avatar pose.

        Args:
            svc (RankCardService): Injected rank card service.
            user_id (int): Target user ID from the URL path.

        Returns:
            AvatarResponse: The avatar pose and resolved asset URL.
        """
        return await svc.get_avatar_pose(user_id)

    @put(
        "/avatar/pose",
        summary="Set avatar pose",
        description="Set the user's avatar pose by name.",
    )
    async def set_avatar_pose(self, svc: RankCardService, user_id: int, data: AvatarPoseBody) -> AvatarResponse:
        """Set the user's avatar pose.

        Args:
            svc (RankCardService): Injected rank card service.
            user_id (int): Target user ID from the URL path.
            data (AvatarPoseBody): Payload containing the pose name.

        Returns:
            AvatarResponse: The updated avatar pose and resolved asset URL.
        """
        return await svc.set_avatar_pose(user_id, data.pose)

    @get(
        "/badges",
        summary="Get badge settings",
        description="Return the user's badge settings with resolved URLs (e.g., mastery, spray).",
    )
    async def get_badges(self, svc: RankCardService, user_id: int) -> RankCardBadgeSettings:
        """Get the user's badge settings.

        Resolves URLs for supported badge types (e.g., mastery and spray).

        Args:
            svc (RankCardService): Injected rank card service.
            user_id (int): Target user ID from the URL path.

        Returns:
            RankCardBadgeSettings: Badge names/types/URLs for slots 1-6.
        """
        return await svc.fetch_badges_settings(user_id)

    @put(
        "/badges",
        summary="Set badge settings",
        description="Set all badge slots (1-6) for the user. The user_id in the payload is ignored if provided.",
    )
    async def set_badges(self, svc: RankCardService, user_id: int, data: RankCardBadgeSettings) -> None:
        """Set the user's badge settings for slots 1-6.

        All slots are upserted atomically. To clear a slot, set its ``badge_name``
        and ``badge_type`` to ``null``. Any ``user_id`` field included in the payload
        is ignored; the path parameter is authoritative.

        Args:
            svc (RankCardService): Injected rank card service.
            user_id (int): Target user ID from the URL path.
            data (RankCardBadgeSettings): Badge settings payload for slots 1-6.
        """
        return await svc.set_badges_settings(data, user_id)
