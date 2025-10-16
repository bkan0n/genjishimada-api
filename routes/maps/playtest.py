import litestar
from genjipk_sdk.models import PlaytestAssociateIDThread, PlaytestPatchDTO, PlaytestVote, PlaytestVotesAll
from genjipk_sdk.models.maps import (
    PlaytestApproveCreate,
    PlaytestForceAcceptCreate,
    PlaytestForceDenyCreate,
    PlaytestReadDTO,
    PlaytestResetCreate,
)
from litestar.datastructures import State
from litestar.di import Provide
from litestar.response import Stream

from di import MapService, PlaytestService, provide_map_service, provide_playtest_service


class PlaytestController(litestar.Controller):
    tags = ["Playtest"]
    path = "/playtests"
    dependencies = {"map_svc": Provide(provide_map_service), "playtest_svc": Provide(provide_playtest_service)}

    @litestar.get(
        "/{thread_id:int}",
        summary="Get Playtest Data",
        description="Retrieve the full playtest metadata and related details for a specific thread.",
    )
    async def get_playtest(self, thread_id: int, playtest_svc: PlaytestService) -> PlaytestReadDTO:
        """Retrieve a playtest by its thread ID.

        Args:
            thread_id: ID of the playtest thread.
            playtest_svc: Service layer for playtest operations.

        Returns:
            PlaytestReadDTO: The full playtest metadata object.

        """
        return await playtest_svc.get_playtest(thread_id)

    @litestar.get(
        "/{thread_id:int}/plot",
        summary="Get Playtest Plot Image",
        description="Generate and stream the difficulty distribution plot image for a playtest.",
        include_in_schema=False,
    )
    async def get_playtest_plot(
        self,
        map_svc: MapService,
        thread_id: int,
    ) -> Stream:
        """Retrieve the plot image for a playtest.

        Args:
            map_svc: Service layer for map-related operations.
            thread_id: ID of the playtest thread.

        Returns:
            Stream: A stream of the generated plot image.

        """
        return await map_svc.get_playtest_plot(thread_id=thread_id)

    @litestar.post(
        "/{thread_id:int}/vote/{user_id:int}",
        summary="Cast Playtest Vote",
        description="Submit a vote for a specific playtest thread on behalf of a user.",
    )
    async def cast_vote(
        self, request: litestar.Request, thread_id: int, user_id: int, data: PlaytestVote, playtest_svc: PlaytestService
    ) -> None:
        """Cast a vote for a playtest.

        Args:
            state: Application state container.
            thread_id: ID of the playtest thread.
            user_id: ID of the user casting the vote.
            data: The vote payload.
            playtest_svc: Service layer for playtest operations.

        """
        await playtest_svc.cast_vote(
            request=request,
            thread_id=thread_id,
            user_id=user_id,
            data=data,
        )

    @litestar.delete(
        "/{thread_id:int}/vote/{user_id:int}",
        summary="Delete Playtest Vote",
        description="Remove an individual user's vote for a specific playtest thread.",
    )
    async def delete_vote(
        self, request: litestar.Request, thread_id: int, user_id: int, playtest_svc: PlaytestService
    ) -> None:
        """Delete a user's vote for a playtest.

        Args:
            state: Application state container.
            thread_id: ID of the playtest thread.
            user_id: ID of the user whose vote should be removed.
            playtest_svc: Service layer for playtest operations.

        """
        await playtest_svc.delete_vote(request=request, thread_id=thread_id, user_id=user_id)

    @litestar.delete(
        "/{thread_id:int}/vote",
        summary="Delete All Playtest Votes",
        description="Remove all votes associated with a specific playtest thread.",
    )
    async def delete_all_votes(self, state: State, thread_id: int, playtest_svc: PlaytestService) -> None:
        """Delete all votes for a playtest.

        Args:
            state: Application state container.
            thread_id: ID of the playtest thread.
            playtest_svc: Service layer for playtest operations.

        """
        await playtest_svc.delete_all_votes(state=state, thread_id=thread_id)

    @litestar.get(
        "/{thread_id:int}/votes",
        summary="Get Playtest Votes",
        description="Retrieve all votes currently associated with a specific playtest thread.",
    )
    async def get_votes(self, thread_id: int, playtest_svc: PlaytestService) -> PlaytestVotesAll:
        """Retrieve all votes for a playtest.

        Args:
            thread_id: ID of the playtest thread.
            playtest_svc: Service layer for playtest operations.

        Returns:
            PlaytestVotesAll: The collection of votes for the thread.

        """
        return await playtest_svc.get_votes(thread_id)

    @litestar.patch(
        "/{thread_id:int}",
        summary="Edit Playtest Metadata",
        description="Update playtest metadata such as verification ID or message references.",
        include_in_schema=False,
    )
    async def edit_playtest_meta(self, thread_id: int, data: PlaytestPatchDTO, playtest_svc: PlaytestService) -> None:
        """Update metadata for a playtest.

        Args:
            thread_id: ID of the playtest thread.
            data: The patch payload for metadata updates.
            playtest_svc: Service layer for playtest operations.

        """
        await playtest_svc.edit_playtest_meta(thread_id=thread_id, data=data)

    @litestar.patch(
        "/",
        summary="Associate Playtest Metadata With Map",
        description="Associate a playtest thread with PlaytestMeta for a map.",
        include_in_schema=False,
    )
    async def associate_playtest_meta(
        self,
        data: PlaytestAssociateIDThread,
        playtest_svc: PlaytestService,
    ) -> PlaytestReadDTO:
        """Associate a playtest thread with PlaytestMeta.

        Args:
            data: Association payload containing map and thread details.
            playtest_svc: Service layer for playtest operations.

        Returns:
            PlaytestReadDTO: The updated playtest metadata object.

        """
        return await playtest_svc.associate_playtest_meta(data=data)

    @litestar.post(
        "/{thread_id:int}/approve",
        summary="Approve Playtest",
        description="Approve a playtest, marking it as verified and setting its difficulty rating.",
    )
    async def approve_playtest(
        self, request: litestar.Request, thread_id: int, data: PlaytestApproveCreate, playtest_svc: PlaytestService
    ) -> None:
        """Approve a playtest.

        Args:
            state: Application state container.
            thread_id: ID of the playtest thread.
            data: Approval payload containing code, difficulty, and verifier details.
            playtest_svc: Service layer for playtest operations.

        """
        await playtest_svc.approve(
            request=request,
            thread_id=thread_id,
            verifier_id=data.verifier_id,
        )

    @litestar.post(
        "/{thread_id:int}/force_accept",
        summary="Force Accept Playtest",
        description="Forcefully accept a playtest regardless of votes, assigning difficulty and verifier.",
    )
    async def force_accept_playtest(
        self, request: litestar.Request, thread_id: int, data: PlaytestForceAcceptCreate, playtest_svc: PlaytestService
    ) -> None:
        """Force accept a playtest.

        Args:
            state: Application state container.
            thread_id: ID of the playtest thread.
            data: Force-accept payload containing code, difficulty, and verifier details.
            playtest_svc: Service layer for playtest operations.

        """
        await playtest_svc.force_accept(
            request=request,
            thread_id=thread_id,
            verifier_id=data.verifier_id,
            difficulty=data.difficulty,
        )

    @litestar.post(
        "/{thread_id:int}/force_deny",
        summary="Force Deny Playtest",
        description="Forcefully deny a playtest regardless of votes, recording the reason for rejection.",
    )
    async def force_deny_playtest(
        self, request: litestar.Request, thread_id: int, data: PlaytestForceDenyCreate, playtest_svc: PlaytestService
    ) -> None:
        """Force deny a playtest.

        Args:
            state: Application state container.
            thread_id: ID of the playtest thread.
            data: Force-deny payload containing code, reason, and verifier details.
            playtest_svc: Service layer for playtest operations.

        """
        await playtest_svc.force_deny(
            request=request,
            thread_id=thread_id,
            verifier_id=data.verifier_id,
            reason=data.reason,
        )

    @litestar.post(
        "/{thread_id:int}/reset",
        summary="Reset Playtest",
        description="Reset a playtest to its initial state, optionally removing votes and completions.",
    )
    async def reset_playtest(
        self, request: litestar.Request, thread_id: int, data: PlaytestResetCreate, playtest_svc: PlaytestService
    ) -> None:
        """Reset a playtest to its initial state.

        Args:
            state: Application state container.
            thread_id: ID of the playtest thread.
            data: Reset payload containing removal options and reset reason.
            playtest_svc: Service layer for playtest operations.

        """
        await playtest_svc.reset(
            request=request,
            thread_id=thread_id,
            verifier_id=data.verifier_id,
            reason=data.reason,
            remove_votes=data.remove_votes,
            remove_completions=data.remove_completions,
        )
