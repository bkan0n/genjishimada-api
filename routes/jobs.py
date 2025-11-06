import uuid

from asyncpg import Connection
from genjipk_sdk.models import JobStatus, JobUpdate
from genjipk_sdk.models.jobs import ClaimRequest, ClaimResponse
from litestar import Controller, get, patch, post
from litestar.di import Provide

from di.jobs import InternalJobsService, provide_internal_jobs_service


class InternalJobsController(Controller):
    path = "/internal"
    tags = ["Internal"]
    dependencies = {"svc": Provide(provide_internal_jobs_service)}

    @get("/jobs/{job_id:str}")
    async def get_job(self, svc: InternalJobsService, job_id: uuid.UUID) -> JobStatus:
        """Get job status."""
        return await svc.get_job(job_id)

    @patch("/jobs/{job_id:str}", include_in_schema=False)
    async def update_job(self, svc: InternalJobsService, job_id: uuid.UUID, data: JobUpdate) -> None:
        """Update pending job."""
        return await svc.update_job(job_id, data)

    @post("/idempotency/claim")
    async def claim_idempotency(self, conn: Connection, data: ClaimRequest) -> ClaimResponse:
        tag = await conn.execute(
            """
            INSERT INTO public.processed_messages (idempotency_key)
            VALUES ($1)
            ON CONFLICT DO NOTHING
            """,
            data.key,
        )
        claimed = tag.endswith("INSERT 0 1")
        return ClaimResponse(claimed=claimed)
