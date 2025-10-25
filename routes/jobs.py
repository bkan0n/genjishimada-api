import uuid

from genjipk_sdk.models import JobStatus, JobUpdate
from litestar import Controller, get, patch
from litestar.di import Provide

from di.jobs import InternalJobsService, provide_internal_jobs_service


class InternalJobsController(Controller):
    path = "/internal/jobs"
    tags = ["Internal"]
    dependencies = {"svc": Provide(provide_internal_jobs_service)}

    @get("/{job_id:str}")
    async def get_job(self, svc: InternalJobsService, job_id: uuid.UUID) -> JobStatus:
        return await svc.get_job(job_id)

    @patch("/{job_id:str}", include_in_schema=False)
    async def update_job(self, svc: InternalJobsService, job_id: uuid.UUID, data: JobUpdate) -> None:
        return await svc.update_job(job_id, data)
