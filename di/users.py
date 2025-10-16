# services/user_service.py
from __future__ import annotations

import logging

import asyncpg
import msgspec
from asyncpg import Connection
from genjipk_sdk.models import (
    NOTIFICATION_TYPES,
    Notification,
    OverwatchUsernameItem,
    OverwatchUsernamesReadDTO,
    SettingsUpdate,
    UserCreateDTO,
    UserReadDTO,
    UserUpdateDTO,
)
from genjipk_sdk.models.users import RankDetailReadDTO
from litestar.datastructures import State
from litestar.status_codes import HTTP_400_BAD_REQUEST

from utilities.errors import CustomHTTPException, parse_pg_detail
from utilities.shared_queries import get_user_rank_data

from .base import BaseService

log = logging.getLogger(__name__)


class UserService(BaseService):
    async def check_if_user_is_creator(self, user_id: int) -> bool:
        """Check if user is a creator.

        Args:
            user_id (int): The id of the user to check.

        Returns:
            bool: True if user is a creator.
        """
        query = "SELECT EXISTS(SELECT 1 FROM maps.creators WHERE user_id=$1);"
        return await self._conn.fetchval(query, user_id)

    async def update_user_names(self, user_id: int, data: UserUpdateDTO) -> None:
        """Update user names.

        Args:
            user_id (int): The user id to edit.
            data (UserUpdateDTO): The payload for updating user names.

        """
        is_nick_set = data.nickname is not msgspec.UNSET
        nick_val = data.nickname if is_nick_set else None

        is_glob_set = data.global_name is not msgspec.UNSET
        glob_val = data.global_name if is_glob_set else None

        if not (is_nick_set or is_glob_set):
            return

        q = """
        UPDATE core.users AS u
        SET
            nickname    = CASE WHEN $2 THEN $3::text ELSE u.nickname END,
            global_name = CASE WHEN $4 THEN $5::text ELSE u.global_name END
        WHERE u.id = $1
          AND (
                ($2 AND u.nickname    IS DISTINCT FROM $3::text) OR
                ($4 AND u.global_name IS DISTINCT FROM $5::text)
              )
        RETURNING u.nickname, u.global_name;
        """
        await self._conn.execute(q, user_id, is_nick_set, nick_val, is_glob_set, glob_val)

    async def list_users(self) -> list[UserReadDTO] | None:
        """Return all users.

        Returns:
            list[UserReadDTO] | None: A list of User models with overwatch_usernames populated

        """
        query = """
            SELECT
                u.id,
                u.nickname,
                coalesce(u.global_name, 'Unknown Username') AS global_name,
                u.coins,
                NULLIF(array_agg(owu.username), '{NULL}') AS overwatch_usernames
            FROM core.users u
            LEFT JOIN users.overwatch_usernames owu ON u.id = owu.user_id
            GROUP BY u.id, u.nickname, u.global_name, u.coins
            ;
        """
        rows = await self._conn.fetch(query)
        return msgspec.convert(rows, list[UserReadDTO])

    async def get_user(self, user_id: int) -> UserReadDTO | None:
        """Return a single user with coalesced display name.

        Args:
            user_id (int): The ID of the user.

        Returns:
            UserReadDTO | None: The requested User if found; otherwise None.

        """
        query = """
        SELECT
            u.id,
            u.nickname,
            u.global_name,
            u.coins,
            NULLIF(array_agg(owu.username ORDER BY owu.is_primary DESC), '{NULL}') AS overwatch_usernames,
            COALESCE(
                (array_remove(array_agg(owu.username ORDER BY owu.is_primary DESC), NULL))[1], -- primary first
                u.nickname,
                u.global_name,
                'Unknown User'
            ) AS coalesced_name
        FROM core.users u
        LEFT JOIN users.overwatch_usernames owu
            ON u.id = owu.user_id
        WHERE u.id = $1
        GROUP BY u.id, u.nickname, u.global_name, u.coins;
        """
        row = await self._conn.fetchrow(query, user_id)
        if not row:
            return None
        return UserReadDTO(
            id=row["id"],
            nickname=row["nickname"],
            global_name=row["global_name"],
            coins=row["coins"],
            overwatch_usernames=row["overwatch_usernames"],
            coalesced_name=row["coalesced_name"],
        )

    async def user_exists(self, user_id: int) -> bool:
        """Check if a user exists.

        Args:
            user_id (int): The ID of the user.

        Returns:
            bool: True if the user exists; otherwise False.

        """
        query = """
            SELECT EXISTS(
                SELECT 1 FROM core.users WHERE id = $1
            );
        """
        return await self._conn.fetchval(query, user_id)

    async def create_user(self, data: UserCreateDTO) -> UserReadDTO:
        """Create a user (no-op if already exists) and return the basic model.

        Args:
            data (UserCreateDTO): The payload containing id, nickname, and global_name.

        Returns:
            UserReadDTO: The created (or existing) user representation with coins=0 and empty overwatch_usernames.

        Raises:
            CustomHTTPException: If a duplicate primary key (users_pkey) is detected.

        """
        fake_user_id_limit = 1000000000000000
        if data.id < fake_user_id_limit:
            raise CustomHTTPException(
                detail="Please use create fake member endpoint for user ids less than 1000000000000000.",
                status_code=HTTP_400_BAD_REQUEST,
            )
        query = "INSERT INTO core.users (id, nickname, global_name) VALUES ($1, $2, $3);"
        try:
            await self._conn.execute(query, data.id, data.nickname, data.global_name)
        except asyncpg.exceptions.UniqueViolationError as e:
            if e.constraint_name == "users_pkey":
                raise CustomHTTPException(
                    detail="Provided user_id already exists.",
                    status_code=HTTP_400_BAD_REQUEST,
                    extra=parse_pg_detail(e.detail),
                )
            raise
        return UserReadDTO(
            id=data.id,
            nickname=data.nickname,
            global_name=data.global_name,
            coins=0,
            overwatch_usernames=[],
            coalesced_name=data.nickname,
        )

    async def set_overwatch_usernames(self, user_id: int, new_usernames: list[OverwatchUsernameItem]) -> None:
        """Replace all Overwatch usernames for a user.

        Args:
            user_id (int): The ID of the user.
            new_usernames (list[OverwatchUsernameItem]): New usernames to persist; replaces all prior values.

        """
        await self._conn.execute("DELETE FROM users.overwatch_usernames WHERE user_id = $1", user_id)

        for item in new_usernames:
            await self._conn.execute(
                """
                INSERT INTO users.overwatch_usernames (user_id, username, is_primary) VALUES ($1, $2, $3)
                """,
                user_id,
                item.username,
                item.is_primary,
            )

    async def fetch_overwatch_usernames(self, user_id: int) -> list[OverwatchUsernameItem]:
        """Fetch Overwatch usernames for a user.

        Args:
            user_id (int): The ID of the user.

        Returns:
            list[OverwatchUsernameItem]: The list of username records for the user.

        Raises:
            CustomHTTPException: If the user is not found (HTTP 400).

        """
        query = """
            SELECT username, is_primary
            FROM core.users u
            LEFT JOIN users.overwatch_usernames owu ON u.id = owu.user_id
            WHERE user_id = $1
            ORDER BY is_primary DESC;
        """
        rows = await self._conn.fetch(query, user_id)
        return msgspec.convert(rows, list[OverwatchUsernameItem])

    async def get_overwatch_usernames_response(self, user_id: int) -> OverwatchUsernamesReadDTO:
        """Build an OverwatchUsernamesReadDTO for a user.

        Args:
            user_id (int): The ID of the user.

        Returns:
            OverwatchUsernamesReadDTO: The response containing user_id and usernames.

        """
        usernames = await self.fetch_overwatch_usernames(user_id)
        primary = usernames[0].username if usernames else None
        secondary = usernames[1].username if len(usernames) > 1 else None
        tertiary = usernames[2].username if len(usernames) > 2 else None  # noqa: PLR2004

        return OverwatchUsernamesReadDTO(user_id=user_id, primary=primary, secondary=secondary, tertiary=tertiary)

    async def fetch_user_notifications(self, user_id: int) -> int | None:
        """Get the current notification bitmask for a user.

        Args:
            user_id (int): The ID of the user.

        Returns:
            int | None: The bitmask if present; otherwise None.

        """
        query = "SELECT flags FROM users.notification_settings WHERE user_id = $1;"
        return await self._conn.fetchval(query, user_id)

    async def update_user_notifications(self, user_id: int, notifications_bitmask: int) -> bool:
        """Upsert the notification bitmask for a user.

        Args:
            user_id (int): The ID of the user.
            notifications_bitmask (int): The new bitmask value.

        Returns:
            bool: True if the operation succeeded; False otherwise.

        """
        log.debug(f"Updating user {user_id} settings to bitmask: {notifications_bitmask}")
        query = """
            INSERT INTO users.notification_settings (user_id, flags) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET flags = $2;
        """
        try:
            await self._conn.execute(query, user_id, notifications_bitmask)
        except Exception:
            return False
        return True

    async def get_user_notifications_payload(self, user_id: int, to_bitmask: bool = False) -> dict:
        """Return the notifications payload for a user.

        Args:
            user_id (int): The ID of the user.
            to_bitmask (bool, optional): If True, return the raw bitmask.
                If False, return names of enabled notifications. Defaults to False.

        Returns:
            dict: Either {"user_id": int, "bitmask": int} or {"user_id": int, "notifications": list[str]}.

        """
        bitmask = await self.fetch_user_notifications(user_id)
        if bitmask is None:
            log.debug("User %s not found.", user_id)
            bitmask = 0

        if to_bitmask:
            return {"user_id": user_id, "bitmask": bitmask}

        notifications = [notif.name for notif in Notification if bitmask & notif]
        log.debug("User %s settings: %s", user_id, notifications)
        return {"user_id": user_id, "notifications": notifications}

    async def apply_notifications_bulk(self, user_id: int, data: SettingsUpdate) -> tuple[bool, int | None, str | None]:
        """Apply a bulk notifications update.

        Args:
            user_id (int): The ID of the user.
            data (SettingsUpdate): The update object whose to_bitmask() will be applied.

        Returns:
            tuple[bool, int | None, str | None]: (success, bitmask_or_none, error_message_or_none).

        """
        try:
            bitmask = data.to_bitmask()
            log.debug(f"User {user_id} notifications bitmask: {bitmask}")
            ok = await self.update_user_notifications(user_id, bitmask)
            if ok:
                return True, bitmask, None
            return False, None, "Update failed"
        except ValueError as ve:
            log.error(f"Validation error: {ve}")
            return False, None, str(ve)

    async def apply_single_notification(
        self,
        user_id: int,
        notification_type: NOTIFICATION_TYPES,
        enable: bool,
    ) -> tuple[bool, int | None, str | None]:
        """Toggle a single notification flag for a user.

        Args:
            user_id (int): The ID of the user.
            notification_type (NOTIFICATION_TYPES): The notification name (e.g., "DM_ON_VERIFICATION").
            enable (bool): True to enable; False to disable.

        Returns:
            tuple[bool, int | None, str | None]: (success, new_bitmask_or_none, error_message_or_none).

        """
        valid_notification_names = {flag.name for flag in Notification}
        if notification_type not in valid_notification_names:
            return False, None, f"Invalid notification type: {notification_type}"

        try:
            current_bitmask = await self.fetch_user_notifications(user_id)
            if current_bitmask is None:
                current_bitmask = 0
            current_flags = Notification(current_bitmask)
            notification_flag = Notification[notification_type]

            new_flags = current_flags | notification_flag if enable else current_flags & ~notification_flag

            log.debug(
                "User %s: updating %s to %s, bitmask: %s -> %s",
                user_id,
                notification_type,
                "enabled" if enable else "disabled",
                current_flags.value,
                new_flags.value,
            )

            ok = await self.update_user_notifications(user_id=user_id, notifications_bitmask=new_flags.value)  # type: ignore[call-arg]
            if ok:
                return True, new_flags.value, None
            return False, None, "Update failed"
        except Exception as e:
            log.error("Error updating single notification: %s", e)
            return False, None, str(e)

    async def get_user_rank_data(self, user_id: int) -> list[RankDetailReadDTO]:
        """Compute rank details for a user based on verified completions and medal thresholds.

        Args:
            user_id (int): The ID of the user.

        Returns:
            list[RankDetailReadDTO]: Per-difficulty counts and rank-met flags.

        """
        return await get_user_rank_data(self._conn, user_id)

    async def create_fake_member(self, name: str) -> int:
        """Create a placeholder (fake) user and return the new user ID.

        Creates a new row in `core.users` with the next available ID below the
        1,000,000,000,000,000 threshold, setting both nickname and global_name to `name`.

        Args:
            name: Display name to assign to the fake user.

        Returns:
            int: The newly created fake user ID.
        """
        query = """
        WITH next_id AS (
          SELECT COALESCE(MAX(id) + 1, 1) AS id
          FROM core.users
          WHERE id < 1000000000000000
        )
        INSERT INTO core.users (id, nickname, global_name)
        SELECT id, $1, $1
        FROM next_id
        RETURNING id;
        """
        return await self._conn.fetchval(query, name)

    async def link_fake_member_id_to_real_user_id(self, fake_user_id: int, real_user_id: int) -> None:
        """Link a fake user to a real user and remove the fake user.

        Reassigns references (e.g., in `maps.creators`) from `fake_user_id` to `real_user_id`,
        then deletes the fake user's row from `core.users`.

        Args:
            fake_user_id: The placeholder user ID to migrate from and delete.
            real_user_id: The real user ID to migrate references to.
        """
        async with self._conn.transaction():
            update_query = "UPDATE maps.creators SET user_id=$2 WHERE user_id=$1"
            await self._conn.execute(update_query, fake_user_id, real_user_id)
            delete_query = "DELETE FROM core.users WHERE id=$1"
            await self._conn.execute(delete_query, fake_user_id)


async def provide_user_service(conn: Connection, state: State) -> UserService:
    """Litestar DI provider for UserService.

    Args:
        conn (asyncpg.Connection): Active asyncpg connection.

    Returns:
        UserService: A new service instance bound to the given connection.

    """
    return UserService(conn, state)
