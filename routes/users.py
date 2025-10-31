from __future__ import annotations

import logging
from typing import Annotated

import litestar
from genjipk_sdk.models import (
    NOTIFICATION_TYPES,
    OverwatchUsernamesReadDTO,
    OverwatchUsernamesUpdate,
    SettingsUpdate,
    UserCreateDTO,
    UserReadDTO,
    UserUpdateDTO,
)
from genjipk_sdk.models.users import RankDetailReadDTO
from litestar.di import Provide
from litestar.exceptions import HTTPException
from litestar.params import Body
from litestar.response import Response
from litestar.status_codes import HTTP_200_OK, HTTP_400_BAD_REQUEST
from msgspec import UNSET

from di import UserService, provide_user_service

log = logging.getLogger(__name__)


class UsersController(litestar.Controller):
    """Users."""

    tags = ["Users"]
    path = "/users"
    dependencies = {"svc": Provide(provide_user_service)}

    @litestar.get(
        path="/{user_id:int}/creator",
        summary="Check If User Is Creator",
        description="Check if user is a creator.",
    )
    async def check_if_user_is_creator(self, svc: UserService, user_id: int) -> bool:
        """Check if user is a creator.

        Args:
            svc (UserService): Service DI.
            user_id (int): The id of the user to check.

        Returns:
            Response[bool]: True if user is a creator.
        """
        return await svc.check_if_user_is_creator(user_id)

    @litestar.patch(
        path="/{user_id:int}",
        summary="Update User Names",
        description="Update the global name and nickname for a user.",
    )
    async def update_user_names(self, user_id: int, data: UserUpdateDTO, svc: UserService) -> None:
        """Update user names.

        Args:
            user_id (int): The user id to edit.
            data (UserUpdateDTO): The payload for updating user names.
            svc (UserService): UserService DI.

        Raises:
            HTTPException: If data has no set values.

        """
        if data.global_name == UNSET and data.nickname == UNSET:
            raise HTTPException(detail="You must set either nickname or global_name.", status_code=HTTP_400_BAD_REQUEST)
        return await svc.update_user_names(user_id, data)

    @litestar.get(
        path="/",
        summary="List Users",
        description=("Fetch all users with their basic fields and aggregated Overwatch usernames."),
    )
    async def get_users(self, svc: UserService) -> list[UserReadDTO] | None:
        """Get user(s).

        Returns:
            list[UserReadDTO] | None: A list of users with aggregated Overwatch usernames; `None` only if no rows.

        """
        return await svc.list_users()

    @litestar.get(
        path="/{user_id:int}",
        summary="Get User",
        description=(
            "Fetch a single user by ID. Returns nickname, global name, coins, aggregated Overwatch usernames, "
            "and a `coalesced_name` preferring primary OW username, then nickname, then global name."
        ),
    )
    async def get_user(self, svc: UserService, user_id: int) -> UserReadDTO | None:
        """Get user.

        Args:
            svc (UserService): UserService DI.
            user_id (int): The user ID.

        Returns:
            UserReadDTO | None: The user if found; otherwise `None`.

        """
        return await svc.get_user(user_id)

    @litestar.get(
        path="/{user_id:int}/exists",
        summary="Check User Exists",
        description="Return a boolean indicating whether a user with the given ID exists.",
    )
    async def check_user_exists(self, svc: UserService, user_id: int) -> bool:
        """Check if a user exists.

        Args:
            svc (UserService): UserService DI.
            user_id (int): The user ID.

        Returns:
            bool: `True` if the user exists; otherwise `False`.

        """
        return await svc.user_exists(user_id)

    @litestar.post(
        path="/",
        summary="Create User",
        description=(
            "Create a new user with the provided ID, nickname, and global name. "
            "If the user already exists, this is a no-op. Duplicate primary keys are reported with a 400."
        ),
    )
    async def create_user(self, svc: UserService, data: UserCreateDTO) -> UserReadDTO:
        """Create new user.

        Args:
            svc (UserService): UserService DI.
            data (UserCreateDTO): The user payload.

        Returns:
            UserReadDTO: The created (or existing) user with default fields.

        """
        return await svc.create_user(data)

    @litestar.put(
        path="/{user_id:int}/overwatch",
        summary="Replace Overwatch Usernames",
        description=(
            "Replace the Overwatch usernames for a user. "
            "This clears all existing entries and inserts the provided list. "
            "Use `is_primary` on exactly one entry to mark it as primary."
        ),
    )
    async def update_overwatch_usernames(
        self,
        svc: UserService,
        user_id: int,
        data: Annotated[OverwatchUsernamesUpdate, Body(title="User Overwatch Usernames")],
    ) -> Response:
        """Update the Overwatch usernames for a specific user.

        Args:
            svc (UserService): The user service.
            user_id (int): The user ID.
            data (OverwatchUsernamesUpdate): The new usernames payload.

        Returns:
            Response: `{"success": true}` on success; otherwise an error payload with HTTP 400.

        """
        try:
            log.info("Set Overwatch usernames for user %s: %s", user_id, data.usernames)
            await svc.set_overwatch_usernames(user_id, data.usernames)
            return Response({"success": True}, status_code=HTTP_200_OK)
        except Exception as e:
            log.error("Error updating Overwatch usernames for user %s: %s", user_id, e)
            return Response({"error": str(e)}, status_code=HTTP_400_BAD_REQUEST)

    @litestar.get(
        path="/{user_id:int}/overwatch",
        summary="Get Overwatch Usernames",
        description=(
            "Retrieve Overwatch usernames for a user. Responds with 404 if the user does not exist. "
            "Includes `username` and `is_primary` fields."
        ),
    )
    async def get_overwatch_usernames(self, svc: UserService, user_id: int) -> OverwatchUsernamesReadDTO:
        """Retrieve the Overwatch usernames for a specific user.

        Args:
            svc (UserService): The user service.
            user_id (int): The user ID.

        Returns:
            OverwatchUsernamesReadDTO: The user's Overwatch usernames.

        """
        return await svc.get_overwatch_usernames_response(user_id)

    @litestar.get(
        path="/{user_id:int}/notifications",
        summary="Get Notification Settings",
        description=(
            "Get the user's notification settings. If `to_bitmask=true`, returns the raw integer bitmask. "
            "Otherwise returns an array of enabled notification names."
        ),
    )
    async def get_user_notifications(
        self,
        svc: UserService,
        user_id: int,
        to_bitmask: bool = False,
    ) -> Response:
        """Retrieve the settings for a specific user.

        Args:
            svc (UserService): The user service.
            user_id (int): The user ID.
            to_bitmask (bool, optional): If `True`, return the bitmask; otherwise names. Defaults to `False`.

        Returns:
            Response: The response containing either `bitmask` or `notifications`.

        """
        payload = await svc.get_user_notifications_payload(user_id, to_bitmask=to_bitmask)
        return Response(payload, status_code=HTTP_200_OK)

    @litestar.put(
        path="/{user_id:int}/notifications",
        summary="Bulk Update Notifications",
        description=(
            "Replace all notification settings for the user using the provided settings payload. "
            "If the `X-Test-Mode` header is present, the update is short-circuited with success."
        ),
    )
    async def bulk_update_notifications(
        self,
        svc: UserService,
        request: litestar.Request,
        data: Annotated[SettingsUpdate, Body(title="User Notifications")],
        user_id: int,
    ) -> Response:
        """Update the settings for a specific user.

        Args:
            svc (UserService): The user service.
            request (Request): The current request (checks `X-Test-Mode` header).
            data (SettingsUpdate): The settings to apply.
            user_id (int): The user ID.

        Returns:
            Response: Success with new bitmask or error with HTTP 400.

        """
        if request.headers.get("x-test-mode"):
            return Response({"status": "success"}, status_code=HTTP_200_OK)

        ok, bitmask, error = await svc.apply_notifications_bulk(user_id, data)
        if ok:
            return Response({"status": "success", "bitmask": bitmask}, status_code=HTTP_200_OK)
        return Response({"error": error or "Update failed"}, status_code=HTTP_400_BAD_REQUEST)

    @litestar.patch(
        path="/{user_id:int}/notifications/{notification_type:str}",
        summary="Toggle Single Notification",
        description=(
            "Enable or disable a single notification flag by name (e.g., `DM_ON_VERIFICATION`). "
            "The request body must be a boolean: `true` to enable, `false` to disable."
        ),
    )
    async def update_notification(
        self,
        svc: UserService,
        user_id: int,
        notification_type: NOTIFICATION_TYPES,
        data: Annotated[bool, Body(title="Enable Notification")],
    ) -> Response:
        """Update a single notification flag for a user.

        Args:
            svc (UserService): The user service.
            user_id (int): The user ID.
            notification_type (NOTIFICATION_TYPES): The notification name to toggle.
            data (bool): `True` to enable, `False` to disable.

        Returns:
            Response: Success with new bitmask or error payload with HTTP 400.

        """
        ok, bitmask, error = await svc.apply_single_notification(user_id, notification_type, data)
        if ok:
            return Response({"status": "success", "bitmask": bitmask}, status_code=HTTP_200_OK)
        return Response({"error": error or "Update failed"}, status_code=HTTP_400_BAD_REQUEST)

    @litestar.get(
        path="/{user_id:int}/rank",
        summary="Get User Rank Details",
        description=(
            "Compute per-difficulty completion counts and medal thresholds for the given user. "
            "Uses verified, latest-per-user runs and official maps only."
        ),
    )
    async def get_user_rank_data(self, svc: UserService, user_id: int) -> list[RankDetailReadDTO]:
        """Get rank details for a user.

        Args:
            svc (UserService): The user service.
            user_id (int): The user ID.

        Returns:
            list[RankDetailReadDTO]: A list of rank detail rows by difficulty.

        """
        return await svc.get_user_rank_data(user_id)

    @litestar.post(
        "/fake",
        summary="Create fake member",
        description="Create a placeholder user with an auto-generated ID and return the new user ID.",
    )
    async def create_fake_member(self, svc: UserService, name: str) -> int:
        """Create a placeholder (fake) member and return the new user ID.

        Delegates to the user service to create a fake user whose nickname/global_name
        are set to the provided `name`.

        Args:
            svc: User service dependency.
            name: Display name to assign to the fake user.

        Returns:
            int: The newly created fake user ID.
        """
        return await svc.create_fake_member(name)

    @litestar.put(
        "/fake/{fake_user_id:int}/link/{real_user_id:int}",
        summary="Link fake member to real user",
        description="Reassign references from the fake user to the real user and delete the fake user row.",
    )
    async def link_fake_member_id_to_real_user_id(self, svc: UserService, fake_user_id: int, real_user_id: int) -> None:
        """Link a fake member to a real user and remove the fake user.

        Delegates to the user service to reassign references from `fake_user_id`
        to `real_user_id`, then delete the fake user.

        Args:
            svc: User service dependency.
            fake_user_id: The placeholder user ID to migrate from and delete.
            real_user_id: The real user ID to migrate references to.
        """
        return await svc.link_fake_member_id_to_real_user_id(fake_user_id, real_user_id)
