from genjipk_sdk.models import ChangeRequestCreateDTO, ChangeRequestReadDTO, StaleChangeRequestReadDTO
from genjipk_sdk.utilities._types import OverwatchCode
from litestar import Controller, get, patch, post
from litestar.di import Provide

from di import ChangeRequestsService, provide_change_requests_service


class ChangeRequestsController(Controller):
    """Endpoints for map change-requests (create, list, resolve, alerts, permissions)."""

    path = "/change-requests"
    tags = ["Change Requests"]
    dependencies = {"svc": Provide(provide_change_requests_service)}

    @get(
        path="/permission",
        summary="Check Creator-Only Button Permission",
        description="Return whether the given `user_id` is included in the creator mentions for the thread and code.",
    )
    async def check_permission_for_view_buttons(
        self,
        svc: ChangeRequestsService,
        thread_id: int,
        user_id: int,
        code: OverwatchCode,
    ) -> bool:
        """Check whether a user can see creator-only UI actions.

        Args:
            svc (ChangeRequestsService): Change-requests service dependency.
            thread_id (int): Discord thread ID associated with the change request.
            user_id (int): The user to check for permission.
            code (OverwatchCode): The Overwatch map code.

        Returns:
            bool: `True` if the user is included in `creator_mentions`, else `False`.

        """
        return await svc.check_permission_for_view_buttons(thread_id, user_id, code)

    @post(
        path="/",
        summary="Create Change Request",
        description="Create a change request for a specific map code and discussion thread.",
    )
    async def create_change_request(self, svc: ChangeRequestsService, data: ChangeRequestCreateDTO) -> None:
        """Create a new change request.

        Args:
            svc (ChangeRequestsService): Change-requests service dependency.
            data (ChangeRequest): Payload containing thread, code, author, content, type, and mentions.

        """
        return await svc.create_change_request(data)

    @patch(
        path="/{thread_id:int}/resolve",
        summary="Resolve Change Request",
        description="Mark the change request associated with the given thread as resolved.",
    )
    async def resolve_change_request(self, svc: ChangeRequestsService, thread_id: int) -> None:
        """Resolve a change request by thread.

        Args:
            svc (ChangeRequestsService): Change-requests service dependency.
            thread_id (int): Discord thread ID to resolve.

        """
        return await svc.resolve_change_request(thread_id)

    @get(
        path="/",
        summary="List Open Change Requests by Code",
        description="List all unresolved change requests for the specified map code, newest first.",
    )
    async def get_change_requests(self, svc: ChangeRequestsService, code: OverwatchCode) -> list[ChangeRequestReadDTO]:
        """Get unresolved change requests for a map.

        Args:
            svc (ChangeRequestsService): Change-requests service dependency.
            code (OverwatchCode): The Overwatch map code.

        Returns:
            list[ChangeRequest]: Unresolved change requests for the code.

        """
        return await svc.get_change_requests(code)

    @get(
        path="/stale",
        summary="List Stale Change Requests",
        description="Return change requests older than two weeks that are neither alerted nor resolved.",
    )
    async def get_stale_change_requests(self, svc: ChangeRequestsService) -> list[StaleChangeRequestReadDTO]:
        """Get stale change requests needing follow-up.

        Args:
            svc (ChangeRequestsService): Change-requests service dependency.

        Returns:
            list[ChangeRequest]: Stale change requests (older than 2 weeks, not alerted, not resolved).

        """
        return await svc.get_stale_change_requests()

    @patch(
        path="/{thread_id:int}/alerted",
        summary="Mark Change Request as Alerted",
        description="Mark the change request associated with the given thread as having been alerted.",
    )
    async def update_alerted_change_request(self, svc: ChangeRequestsService, thread_id: int) -> None:
        """Set a change request to alerted state.

        Args:
            svc (ChangeRequestsService): Change-requests service dependency.
            thread_id (int): Discord thread ID to mark as alerted.

        """
        return await svc.update_alerted_change_request(thread_id)
