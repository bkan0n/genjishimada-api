from __future__ import annotations

import uuid
from datetime import datetime, timezone

from asyncpg import Connection
from genjipk_sdk.models import JobStatus, JobUpdate
from litestar.datastructures import State
from litestar.exceptions import HTTPException

from .base import BaseService


class InternalJobsService(BaseService):
    async def get_job(self, job_id: uuid.UUID) -> JobStatus:
        row = await self._conn.fetchrow(
            "SELECT id, status::text, error_code, error_msg FROM public.jobs WHERE id=$1",
            job_id,
        )
        if not row:
            raise HTTPException(404, "Job not found.")
        return JobStatus(id=row["id"], status=row["status"], error_code=row["error_code"], error_msg=row["error_msg"])

    async def update_job(self, job_id: uuid.UUID, data: JobUpdate) -> None:
        now = datetime.now(timezone.utc)
        sets = {
            "processing": ("status='processing', started_at=COALESCE(started_at,$2)", (job_id, now)),
            "succeeded": ("status='succeeded', finished_at=$2, error_code=NULL, error_msg=NULL", (job_id, now)),
            "failed": (
                "status='failed', finished_at=$2, error_code=$3, error_msg=$4",
                (job_id, now, data.error_code, data.error_msg),
            ),
        }
        sql_set, params = sets[data.status]
        await self._conn.execute(f"UPDATE public.jobs SET {sql_set} WHERE id=$1", *params)


async def provide_internal_jobs_service(conn: Connection, state: State) -> InternalJobsService:
    """Litestar DI provider for InternalJobsService.

    Args:
        conn (Connection): Active asyncpg connection.
        state (State): Used for RabbitMQ.

    Returns:
        InternalJobsService: A new service instance bound to `conn`.

    """
    return InternalJobsService(conn=conn, state=state)
