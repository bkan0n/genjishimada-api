import asyncio
import logging
import random
from typing import Awaitable, Callable
from uuid import UUID

from genjipk_sdk.internal import JobStatusResponse

log = logging.getLogger(__name__)


async def wait_for_job_completion(
    job_id: UUID,
    fetch_status: Callable[[UUID], Awaitable[JobStatusResponse | None]],
    *,
    timeout: float = 60.0,  # noqa: ASYNC109
    base_interval: float = 0.25,
    max_interval: float = 2.0,
) -> JobStatusResponse:
    """Poll the database until a JobStatus reaches a terminal state.

    Args:
        job_id (UUID): The ID of the job to monitor.
        fetch_status (Callable[[UUID], Awaitable[JobStatus | None]]):
            Async function that retrieves the current job status from the database.
        timeout (float, optional): Max time in seconds to wait for completion.
        base_interval (float, optional): Initial polling interval.
        max_interval (float, optional): Max interval between polls.

    Returns:
        JobStatus: The final job status object when the job finishes.

    Raises:
        TimeoutError: If the job did not complete within the timeout window.
        RuntimeError: If the job failed.
    """
    start_time = asyncio.get_running_loop().time()
    interval = base_interval

    while True:
        job = await fetch_status(job_id)

        if job is not None and job.status in {"succeeded", "failed", "timeout"}:
            if job.status == "succeeded":
                return job
            raise RuntimeError(
                f"Job {job.id} ended with status={job.status}, code={job.error_code}, msg={job.error_msg}"
            )

        if asyncio.get_running_loop().time() - start_time >= timeout:
            raise TimeoutError(f"Timed out waiting for job {job_id}")

        await asyncio.sleep(interval * (1.0 + random.random() * 0.2))  # jitter
        interval = min(interval * 1.5, max_interval)
