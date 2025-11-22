import asyncio
import os
from logging import getLogger
from typing import Literal

import aiohttp
import msgspec
from asyncpg import Connection
from genjipk_sdk.completions import (
    CompletionCreatedEvent,
    CompletionCreateRequest,
    CompletionPatchRequest,
    CompletionResponse,
    CompletionSubmissionJobResponse,
    CompletionSubmissionResponse,
    CompletionVerificationUpdateRequest,
    FailedAutoverifyEvent,
    OcrResponse,
    PendingVerificationResponse,
    QualityUpdateRequest,
    SuspiciousCompletionCreateRequest,
    SuspiciousCompletionResponse,
    UpvoteCreateRequest,
    UpvoteSubmissionJobResponse,
)
from genjipk_sdk.difficulties import DifficultyAll
from genjipk_sdk.internal import JobStatusResponse
from genjipk_sdk.maps import OverwatchCode
from litestar import Controller, Request, get, patch, post, put
from litestar.datastructures import State
from litestar.di import Provide
from litestar.status_codes import HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND

from di import (
    AutocompleteService,
    CompletionsService,
    provide_autocomplete_service,
    provide_completions_service,
    provide_map_service,
    provide_user_service,
)
from utilities.errors import CustomHTTPException

log = getLogger(__name__)


class CompletionsController(Controller):
    """Completions."""

    _tasks = set()

    tags = ["Completions"]
    path = "/completions"
    dependencies = {
        "svc": Provide(provide_completions_service),
        "users": Provide(provide_user_service),
        "maps": Provide(provide_map_service),
        "autocomplete": Provide(provide_autocomplete_service),
    }

    @get(
        path="/",
        summary="Get User Completions",
        description="Retrieve all verified completions for a given user, optionally filtered by difficulty.",
    )
    async def get_completions_for_user(
        self,
        svc: CompletionsService,
        user_id: int,
        difficulty: DifficultyAll | None = None,
        page_size: Literal[10, 20, 25, 50] = 10,
        page_number: int = 1,
    ) -> list[CompletionResponse]:
        """Get completions for a specific user.

        Args:
            svc (CompletionsService): Service layer for completions.
            user_id (int): ID of the user to fetch completions for.
            difficulty (DifficultyAll | None): Optional difficulty filter.
            page_size (int): Page size; one of 10, 20, 25, 50.
            page_number (int): 1-based page number.

        Returns:
            list[CompletionReadDTO]: List of the user's verified completions.

        """
        return await svc.get_completions_for_user(user_id, difficulty, page_size, page_number)

    @get(
        path="/world-records",
        summary="Get User World Records",
        description="Retrieve all verified World Records for a given user.",
    )
    async def get_world_records_per_user(
        self,
        svc: CompletionsService,
        user_id: int,
    ) -> list[CompletionResponse]:
        """Get completions for a specific user.

        Args:
            svc (CompletionsService): Service layer for completions.
            user_id (int): ID of the user to fetch completions for.

        Returns:
            list[CompletionReadDTO]: List of the user's World Records.

        """
        return await svc.get_world_records_per_user(user_id)

    @post(path="/", summary="Submit Completion", description="Submit a new completion record and publish an event.")
    async def submit_completion(
        self,
        svc: CompletionsService,
        request: Request,
        data: CompletionCreateRequest,
        autocomplete: AutocompleteService,
        conn: Connection,
    ) -> CompletionSubmissionJobResponse:
        """Submit a new completion.

        Args:
            svc (CompletionsService): Service layer for completions.
            request (Request): Request.
            data (CompletionCreateDTO): DTO with completion details.
            autocomplete (AutocompleteService): Service for autocomplete.

        Returns:
            int: ID of the newly inserted completion.

        """
        query = """
            SELECT EXISTS(SELECT 1 FROM core.maps WHERE code=$1);
        """
        if not await conn.fetchval(query, data.code):
            raise CustomHTTPException(status_code=HTTP_404_NOT_FOUND, detail="This map code does not exist.")

        completion_id = await svc.submit_completion(data)
        if not completion_id:
            raise ValueError("Some how completion ID is null?")

        if not data.video:
            task = asyncio.create_task(
                _attempt_auto_verify(
                    request=request,
                    svc=svc,
                    autocomplete=autocomplete,
                    completion_id=completion_id,
                    data=data,
                )
            )
            self._tasks.add(task)
            task.add_done_callback(lambda t: self._tasks.remove(t))

            return CompletionSubmissionJobResponse(None, completion_id)

        idempotency_key = f"completion:submission:{data.user_id}:{completion_id}"
        job_status = await svc.publish_message(
            routing_key="api.completion.submission",
            data=CompletionCreatedEvent(completion_id),
            headers=request.headers,
            idempotency_key=idempotency_key,
            use_pool=True,
        )
        return CompletionSubmissionJobResponse(job_status, completion_id)

    @patch(
        path="/{record_id:int}",
        summary="Edit Completion",
        description="Apply partial updates to a completion record.",
        include_in_schema=False,
    )
    async def edit_completion(
        self,
        svc: CompletionsService,
        state: State,
        record_id: int,
        data: CompletionPatchRequest,
    ) -> None:
        """Patch an existing completion.

        Args:
            svc (CompletionsService): Service layer for completions.
            state (State): Application state (unused, for consistency).
            record_id (int): Completion record ID.
            data (CompletionPatchDTO): Patch data for the completion.

        """
        return await svc.edit_completion(state, record_id, data)

    @get(
        path="/{record_id:int}/submission",
        summary="Get Completion Submission",
        description=(
            "Retrieve enriched submission details for a specific completion, "
            "including ranks, medals, and display names."
        ),
    )
    async def get_completion_submission(
        self,
        svc: CompletionsService,
        record_id: int,
    ) -> CompletionSubmissionResponse:
        """Get a detailed view of a completion submission.

        Args:
            svc (CompletionsService): Service layer for completions.
            record_id (int): Completion record ID.

        Returns:
            CompletionSubmissionResponse: Detailed submission information.

        """
        return await svc.get_completion_submission(record_id)

    @get(
        path="/pending",
        summary="Get Pending Verifications",
        description="Retrieve all completions that are awaiting verification.",
    )
    async def get_pending_verifications(
        self,
        svc: CompletionsService,
    ) -> list[PendingVerificationResponse]:
        """Get completions waiting for verification.

        Args:
            svc (CompletionsService): Service layer for completions.

        Returns:
            list[PendingVerificationResponse]: List of unverified completions.

        """
        return await svc.get_pending_verifications()

    @put(
        path="/{record_id:int}/verification",
        summary="Verify Completion",
        description="Update the verification status of a completion and publish an event.",
    )
    async def verify_completion(
        self,
        svc: CompletionsService,
        request: Request,
        record_id: int,
        data: CompletionVerificationUpdateRequest,
    ) -> JobStatusResponse:
        """Verify or reject a completion.

        Args:
            svc (CompletionsService): Service layer for completions.
            request (Request): Request.
            record_id (int): Completion record ID.
            data (CompletionVerificationUpdateRequest): Verification details.

        """
        return await svc.verify_completion(request, record_id, data)

    @get(
        path="/{code:str}",
        summary="Get Map Leaderboard",
        description="Retrieve the leaderboard for a given map, including ranks and medals.",
    )
    async def get_completions_leaderboard(
        self,
        svc: CompletionsService,
        code: str,
        page_size: Literal[10, 20, 25, 50, 0] = 10,
        page_number: int = 1,
    ) -> list[CompletionResponse]:
        """Get the leaderboard for a map.

        Args:
            svc (CompletionsService): Service layer for completions.
            code (str): Overwatch map code.
            page_size (int): Page size; one of 10, 20, 25, 50.
            page_number: 1-based page number.

        Returns:
            list[CompletionResponse]: Ranked completions for the map.

        """
        return await svc.get_completions_leaderboard(code, page_number, page_size)

    @get(
        path="/suspicious",
        summary="Get Suspicious Flags",
        description="Retrieve suspicious flags associated with a user's completions.",
    )
    async def get_suspicious_flags(
        self,
        svc: CompletionsService,
        user_id: int,
    ) -> list[SuspiciousCompletionResponse]:
        """Get suspicious flags for a user.

        Args:
            svc (CompletionsService): Service layer for completions.
            user_id (int): ID of the user.

        Returns:
            list[SuspiciousCompletionResponse]: List of suspicious flags.

        """
        return await svc.get_suspicious_flags(user_id)

    @post(
        path="/suspicious", summary="Set Suspicious Flag", description="Insert a new suspicious flag for a completion."
    )
    async def set_suspicious_flags(
        self,
        svc: CompletionsService,
        data: SuspiciousCompletionCreateRequest,
    ) -> None:
        """Add a suspicious flag to a completion.

        Args:
            svc (CompletionsService): Service layer for completions.
            data (SuspiciousCompletionCreateRequest): Flag details.

        """
        if not data.message_id and not data.verification_id:
            raise CustomHTTPException(
                detail="One of message_id or verification_id must be used.", status_code=HTTP_400_BAD_REQUEST
            )
        return await svc.set_suspicious_flags(data)

    @post(
        path="/upvoting",
        summary="Upvote Submission",
        description="Upvote a completion submission. Returns the updated count.",
    )
    async def upvote_submission(
        self,
        svc: CompletionsService,
        request: Request,
        data: UpvoteCreateRequest,
    ) -> UpvoteSubmissionJobResponse:
        """Upvote a completion submission.

        Args:
            svc (CompletionsService): Service layer for completions.
            request (Request): Request.
            data (UpvoteCreateRequest): Upvote details.

        Returns:
            int: Updated upvote count.

        """
        return await svc.upvote_submission(request, data)

    @get(
        path="/all",
        summary="Get All Completions",
        description="Get all completions that are verified sorted by most recent.",
    )
    async def get_all_completions(
        self,
        svc: CompletionsService,
        page_size: Literal[10, 20, 25, 50] = 10,
        page_number: int = 1,
    ) -> list[CompletionResponse]:
        """Get all completions that are verified sorted by most recent.

        Args:
            svc (CompletionsService): Service layer for completions.
            page_size (int): Page size; one of 10, 20, 25, 50.
            page_number: 1-based page number.

        Returns:
            list[CompletionResponse]: Completion data.

        """
        return await svc.get_all_completions(page_size, page_number)

    @get(path="/{code:str}/wr-xp-check", include_in_schema=False)
    async def check_for_previous_world_record_xp(
        self,
        svc: CompletionsService,
        code: OverwatchCode,
        user_id: int,
    ) -> bool:
        """Check if a record submitted by this user has ever received World Record XP.

        This is used to stop potential abuse e.g. incremental completion submission spam resulting in XP gain.

        Args:
            svc (CompletionsService): Service layer for completions.
            code (OverwatchCode): Map code.
            user_id: The user id.
        """
        return await svc.check_for_previous_world_record(code, user_id)

    @get(
        path="/{code:str}/legacy",
        summary="Get Legacy Completions Per Map",
        description="Get all legacy completions for a particular map code.",
    )
    async def get_legacy_completions_per_map(
        self,
        svc: CompletionsService,
        code: str,
        page_number: int = 1,
        page_size: Literal[10, 20, 25, 50] = 10,
    ) -> list[CompletionResponse]:
        """Get the legacy completions for a map code."""
        return await svc.get_legacy_completions_per_map(code, page_number, page_size)

    @post(
        path="/{code:str}/quality",
        summary="Set Quality Vote",
        description="Set the quality vote for a user for a map code.",
    )
    async def set_quality_vote_for_map_code(
        self,
        svc: CompletionsService,
        code: OverwatchCode,
        data: QualityUpdateRequest,
    ) -> None:
        """Set the quality vote for a map code for a user."""
        return await svc.set_quality_vote_for_map_code(code, data.user_id, data.quality)

    @get(path="/upvoting/{message_id:int}")
    async def get_upvotes_from_message_id(self, svc: CompletionsService, message_id: int) -> int:
        """Get upvote count from a message id."""
        return await svc.get_upvotes_from_message_id(message_id)


async def _attempt_auto_verify(
    request: Request,
    svc: CompletionsService,
    autocomplete: AutocompleteService,
    completion_id: int,
    data: CompletionCreateRequest,
) -> None:
    hostname = "genjishimada-ocr" if os.getenv("API_ENVIRONMENT") == "production" else "genjishimada-ocr-dev"
    async with (
        aiohttp.ClientSession() as session,
        session.post(f"http://{hostname}:8000/extract", json={"image_url": data.screenshot}) as resp,
    ):
        resp.raise_for_status()
        raw_ocr_data = await resp.read()
        ocr_data = msgspec.json.decode(raw_ocr_data, type=OcrResponse)

    extracted = ocr_data.extracted
    extracted_user_cleaned = await autocomplete.get_similar_users(
        extracted.name or "", use_pool=True, ignore_fake_users=True
    )
    extracted_code_cleaned = await autocomplete.transform_map_codes(extracted.code or "", use_pool=True)
    if extracted_code_cleaned:
        extracted_code_cleaned = extracted_code_cleaned.replace('"', "")
        extracted_user_id: int | None = extracted_user_cleaned[0][0] if extracted_user_cleaned else None

    # --- Compute condition flags once ---
    code_match = data.code == extracted_code_cleaned
    time_match = data.time == extracted.time
    user_match = extracted_user_id == data.user_id

    log.debug(f"extracted: {extracted}")
    log.debug(f"data: {data}")
    log.debug(f"extracted_code_cleaned: {extracted_code_cleaned}")
    log.debug(f"code_match: {code_match} ({data.code=} vs {extracted_code_cleaned=})")
    log.debug(f"time_match: {time_match} ({data.time=} vs {extracted.time=})")
    log.debug(f"user_match: {user_match} ({data.user_id=} vs {extracted_user_id=})")

    if code_match and time_match and user_match:
        verification_data = CompletionVerificationUpdateRequest(
            verified_by=969632729643753482,
            verified=True,
            reason="Auto Verified by Genji Shimada.",
        )
        await svc.verify_completion(request, completion_id, verification_data, use_pool=True)
        return

    await svc.publish_message(
        routing_key="api.completion.autoverification.failed",
        data=FailedAutoverifyEvent(
            submitted_code=data.code,
            submitted_time=data.time,
            user_id=data.user_id,
            extracted=extracted,
            code_match=code_match,
            time_match=time_match,
            user_match=user_match,
            extracted_code_cleaned=extracted_code_cleaned,
            extracted_time=extracted.time,
            extracted_user_id=extracted_user_id,
        ),
        headers=request.headers,
        idempotency_key=None,
        use_pool=True,
    )

    idempotency_key = f"completion:submission:{data.user_id}:{completion_id}"
    await svc.publish_message(
        routing_key="api.completion.submission",
        data=CompletionCreatedEvent(completion_id),
        headers=request.headers,
        idempotency_key=idempotency_key,
        use_pool=True,
    )
    return
