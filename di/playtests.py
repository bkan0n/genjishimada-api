from typing import Any

import msgspec
from asyncpg import Connection
from genjipk_sdk.models import (
    JobStatus,
    PlaytestAssociateIDThread,
    PlaytestPatchDTO,
    PlaytestVote,
    PlaytestVoteCastMQ,
    PlaytestVoteRemovedMQ,
    PlaytestVotesAll,
    PlaytestVoteWithUser,
)
from genjipk_sdk.models.maps import (
    PlaytestApproveMQ,
    PlaytestForceAcceptMQ,
    PlaytestForceDenyMQ,
    PlaytestReadDTO,
    PlaytestResetMQ,
)
from genjipk_sdk.utilities import DIFFICULTY_MIDPOINTS, DifficultyAll
from litestar import Request
from litestar.datastructures import State

from utilities.errors import CustomHTTPException

from .base import BaseService


class PlaytestService(BaseService):
    async def get_playtest(self, thread_id: int) -> PlaytestReadDTO:
        """Fetch playtest meta (by thread_id).

        Args:
            thread_id: Forum thread ID.

        Returns:
            PlaytestReadDTO: Playtest metadata.

        Raises:
            ValueError: If not found.

        """
        q = """
            SELECT
                me.id,
                me.thread_id,
                ma.code,
                me.verification_id,
                me.initial_difficulty,
                me.created_at,
                me.updated_at,
                me.completed
            FROM playtests.meta me
            LEFT JOIN core.maps ma ON me.map_id = ma.id
            WHERE me.thread_id = $1
        """
        row = await self._conn.fetchrow(q, thread_id)
        if not row:
            raise ValueError("Playtest not found.")
        return msgspec.convert(row, PlaytestReadDTO, from_attributes=True)

    async def get_votes(self, thread_id: int) -> PlaytestVotesAll:
        """Return all votes and the average for a playtest.

        Args:
            thread_id: Forum thread ID.

        Returns:
            PlaytestVotesAll: Aggregated votes and average difficulty.

        """
        q = """
        SELECT
            v.difficulty,
            v.user_id,
            coalesce(
                (
                    SELECT ou.username
                    FROM users.overwatch_usernames ou
                    WHERE ou.user_id = u.id AND ou.is_primary = TRUE
                    LIMIT 1
                ),
                u.nickname,
                u.global_name,
                'Unknown Name'
            ) AS name
        FROM playtests.votes v
        JOIN core.maps m ON m.id = v.map_id
        JOIN core.users u ON u.id = v.user_id
        WHERE v.playtest_thread_id = $1;
        """
        rows = await self._conn.fetch(q, thread_id)
        player_votes = msgspec.convert(rows, list[PlaytestVoteWithUser] | None) or []
        values = [x.difficulty for x in player_votes]
        average = round(sum(values) / len(values), 2) if values else 0
        return PlaytestVotesAll(player_votes, average)

    async def cast_vote(self, *, request: Request, thread_id: int, user_id: int, data: PlaytestVote) -> JobStatus:
        """Cast or update a vote, then publish MQ.

        Args:
            state: App state (for MQ).
            thread_id: Forum thread ID.
            user_id: Voter's user ID.
            data: Vote payload.

        """
        q = """
            WITH target_map AS (
                SELECT id AS map_id FROM core.maps WHERE code = $4
            )
            INSERT INTO playtests.votes (user_id, playtest_thread_id, difficulty, map_id)
            SELECT $1, $2, $3, target_map.map_id
            FROM target_map
            ON CONFLICT (user_id, map_id, playtest_thread_id) DO UPDATE
            SET difficulty = EXCLUDED.difficulty, updated_at = now();
        """
        async with self._conn.transaction():
            await self._conn.execute(q, user_id, thread_id, data.difficulty, data.code)

        payload = PlaytestVoteCastMQ(
            thread_id=thread_id,
            voter_id=user_id,
            difficulty_value=data.difficulty,
        )
        return await self.publish_message(routing_key="api.playtest.vote.cast", data=payload, headers=request.headers)

    async def delete_vote(self, *, request: Request, thread_id: int, user_id: int) -> JobStatus:
        """Remove a user's vote, then publish MQ.

        Args:
            state: App state (for MQ).
            thread_id: Forum thread ID.
            user_id: Voter's user ID.

        """
        q = "DELETE FROM playtests.votes WHERE playtest_thread_id = $1 AND user_id = $2"
        await self._conn.execute(q, thread_id, user_id)

        payload = PlaytestVoteRemovedMQ(thread_id=thread_id, voter_id=user_id)
        return await self.publish_message(routing_key="api.playtest.vote.remove", data=payload, headers=request.headers)

    async def delete_all_votes(self, *, state: State, thread_id: int) -> None:
        """Remove all votes for a playtest (used by moderators).

        Args:
            state: App state (for MQ).
            thread_id: Forum thread ID.

        """
        q = "DELETE FROM playtests.votes WHERE playtest_thread_id = $1"
        await self._conn.execute(q, thread_id)

    async def edit_playtest_meta(self, *, thread_id: int, data: PlaytestPatchDTO) -> None:
        """Patch playtest meta row (dynamic SET).

        Args:
            thread_id: Forum thread ID.
            data: Patch DTO (UNSET fields are ignored).

        Raises:
            ValueError: If all fields are UNSET.

        """
        cleaned: dict[str, Any] = {k: v for k, v in msgspec.structs.asdict(data).items() if v is not msgspec.UNSET}
        if not cleaned:
            raise ValueError("All fields cannot be UNSET.")

        args = [thread_id, *list(cleaned.values())]
        set_clauses = [f"{col} = ${idx}" for idx, col in enumerate(cleaned.keys(), start=2)]
        q = f"UPDATE playtests.meta SET {', '.join(set_clauses)} WHERE thread_id = $1"

        await self._conn.execute(q, *args)

    async def associate_playtest_meta(self, *, data: PlaytestAssociateIDThread) -> PlaytestReadDTO:
        """Associate a playtest meta row with a discord thread_id.

        Args:
            data: Association payload (playtest_id, thread_id).

        Returns:
            PlaytestReadDTO: The updated playtest row.

        """
        await self._conn.execute("UPDATE playtests.meta SET thread_id=$2 WHERE id=$1", data.playtest_id, data.thread_id)
        row = await self._conn.fetchrow(
            """
            SELECT
                me.id,
                me.thread_id,
                ma.code,
                me.verification_id,
                me.initial_difficulty,
                me.created_at,
                me.updated_at,
                me.completed
            FROM playtests.meta me
            LEFT JOIN core.maps ma ON me.map_id = ma.id
            WHERE me.thread_id = $1
            """,
            data.thread_id,
        )
        if not row:
            raise ValueError("Association failed.")
        return msgspec.convert(row, PlaytestReadDTO, from_attributes=True)

    async def approve(
        self,
        request: Request,
        *,
        thread_id: int,
        verifier_id: int,
    ) -> JobStatus:
        """Approve a map's playtest.

        Marks the map as approved, updates its difficulty, and completes the playtest metadata.
        After the transaction commits, publishes a `PlaytestApprove` message to the queue.

        Args:
            state (State): Application state for publishing.
            code (str): Map code being approved.
            thread_id (int): Associated playtest thread ID.
            difficulty (DifficultyAll): Finalized difficulty rating.
            verifier_id (int): ID of the verifier.
            primary_creator_id (int | None): Primary creator's user ID, if known.

        """
        async with self._conn.transaction():
            row = await self._conn.fetchrow(
                "SELECT map_id, code FROM playtests.meta WHERE thread_id=$1;",
                thread_id,
            )
            if not row:
                raise CustomHTTPException("A map was not found that is associated with the given thread id.")
            map_id = row["map_id"]
            code = row["code"]
            difficulty = await self._conn.fetchval(
                "SELECT avg(difficulty) FROM playtests.votes WHERE playtest_thread_id=$1;",
                thread_id,
            )
            await self._conn.execute(
                "UPDATE core.maps SET playtesting='Approved'::playtest_status, raw_difficulty=$1 WHERE id=$2;",
                difficulty,
                map_id,
            )
            await self._conn.execute(
                "UPDATE playtests.meta SET completed=TRUE WHERE thread_id=$1;",
                thread_id,
            )
            primary_creator_id = await self._conn.fetchval(
                "SELECT user_id FROM maps.creators WHERE map_id=$1 AND is_primary;"
            )
        payload = PlaytestApproveMQ(
            code=code,
            thread_id=thread_id,
            difficulty=difficulty,
            verifier_id=verifier_id,
            primary_creator_id=primary_creator_id,
        )
        return await self.publish_message(
            headers=request.headers,
            routing_key="api.playtest.approve",
            data=payload,
        )

    async def force_accept(
        self,
        request: Request,
        *,
        thread_id: int,
        difficulty: DifficultyAll,
        verifier_id: int,
    ) -> JobStatus:
        """Force accept a playtest regardless of normal flow.

        Sets the map as approved, updates difficulty, and marks the playtest as completed.
        After the transaction commits, publishes a `PlaytestForceAccept` message.

        Args:
            state (State): Application state for publishing.
            code (str): Map code being force-accepted.
            thread_id (int): Associated playtest thread ID.
            difficulty (DifficultyAll): Finalized difficulty rating.
            verifier_id (int): ID of the verifier.
            primary_creator_id (int | None): Primary creator's user ID, if known.

        """
        async with self._conn.transaction():
            map_id = await self._conn.fetchval(
                "SELECT map_id FROM playtests.meta WHERE thread_id=$1",
                thread_id,
            )
            raw_difficulty = DIFFICULTY_MIDPOINTS[difficulty]
            await self._conn.execute(
                "UPDATE core.maps SET playtesting='Approved'::playtest_status, raw_difficulty=$1 WHERE id=$2",
                raw_difficulty,
                map_id,
            )
            await self._conn.execute(
                "UPDATE playtests.meta SET completed=TRUE WHERE thread_id=$1",
                thread_id,
            )
        payload = PlaytestForceAcceptMQ(
            thread_id=thread_id,
            difficulty=difficulty,
            verifier_id=verifier_id,
        )
        return await self.publish_message(
            headers=request.headers,
            routing_key="api.playtest.force_accept",
            data=payload,
        )

    async def force_deny(
        self,
        request: Request,
        *,
        thread_id: int,
        verifier_id: int,
        reason: str,
    ) -> JobStatus:
        """Force deny a playtest.

        Marks the map as rejected, hides it, and completes the playtest metadata.
        After the transaction commits, publishes a `PlaytestForceDeny` message.

        Args:
            state (State): Application state for publishing.
            thread_id (int): Associated playtest thread ID.
            verifier_id (int): ID of the verifier.
            reason (str): Explanation for denial.

        """
        async with self._conn.transaction():
            map_id = await self._conn.fetchval(
                "SELECT map_id FROM playtests.meta WHERE thread_id=$1",
                thread_id,
            )
            await self._conn.execute(
                "UPDATE core.maps SET playtesting='Rejected'::playtest_status, hidden=TRUE WHERE id=$1",
                map_id,
            )
            await self._conn.execute(
                "UPDATE playtests.meta SET completed=TRUE WHERE thread_id=$1",
                thread_id,
            )
        payload = PlaytestForceDenyMQ(
            thread_id=thread_id,
            verifier_id=verifier_id,
            reason=reason,
        )
        return await self.publish_message(
            headers=request.headers,
            routing_key="api.playtest.force_deny",
            data=payload,
        )

    async def reset(  # noqa: PLR0913
        self,
        request: Request,
        *,
        thread_id: int,
        verifier_id: int,
        reason: str,
        remove_votes: bool,
        remove_completions: bool,
    ) -> JobStatus:
        """Reset a playtest.

        Optionally removes votes and/or completions, while leaving the map entry intact.
        After cleanup, publishes a `PlaytestReset` message.

        Args:
            state (State): Application state for publishing.
            thread_id (int): Associated playtest thread ID.
            verifier_id (int): ID of the verifier initiating the reset.
            reason (str): Explanation for the reset.
            remove_votes (bool): Whether to delete associated votes.
            remove_completions (bool): Whether to delete associated completions.

        """
        async with self._conn.transaction():
            if remove_votes:
                await self._conn.execute("DELETE FROM playtests.votes WHERE thread_id=$1", thread_id)
            if remove_completions:
                await self._conn.execute("DELETE FROM core.completions WHERE playtest_thread_id=$1", thread_id)

        payload = PlaytestResetMQ(
            thread_id=thread_id,
            verifier_id=verifier_id,
            reason=reason,
            remove_votes=remove_votes,
            remove_completions=remove_completions,
        )
        return await self.publish_message(
            headers=request.headers,
            routing_key="api.playtest.reset",
            data=payload,
        )


async def provide_playtest_service(conn: Connection, state: State) -> PlaytestService:
    """Provide PlaytestService DI."""
    return PlaytestService(conn, state)
