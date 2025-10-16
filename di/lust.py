import asyncio
import logging
import random
import time
from collections.abc import AsyncGenerator

import aiohttp
from litestar.status_codes import HTTP_400_BAD_REQUEST

logger = logging.getLogger(__name__)


class LustService:
    def __init__(self) -> None:
        """Initialize LustService.

        Creates an aiohttp client session with a 30-second per-request timeout.
        """
        timeout = aiohttp.ClientTimeout(total=30.0)
        self._client = aiohttp.ClientSession(timeout=timeout)

    async def upload_screenshot(self, image: bytes, content_type: str) -> str:
        """Upload a screenshot image to the CDN.

        Retries failed attempts with exponential backoff until either the upload succeeds
        or the 10-minute global timeout is exceeded.

        Args:
            image (bytes): Raw screenshot image data.
            content_type (str): MIME type of the image (e.g., "image/png").

        Returns:
            str: Public CDN URL of the uploaded image.

        Raises:
            TimeoutError: If all retries fail and the 10-minute timeout is exceeded.
            aiohttp.ClientResponseError: If the CDN responds with a 4xx/5xx status code.
            aiohttp.ClientError: For other client-related errors.
            asyncio.TimeoutError: If the request times out.

        """
        start_time = time.monotonic()
        attempts = 0
        total_timeout = 600.0  # 10 minutes
        backoff_base = 1.5

        while True:
            try:
                attempts += 1
                logger.info("Uploading screenshot (attempt %d)...", attempts)

                async with self._client.post(
                    "http://genjishimada-lust:8000/v1/images/genji-parkour-images",
                    params={"format": content_type.split("/")[1]},
                    headers={
                        "content-length": str(len(image)),
                        "content-type": "application/octet-stream",
                    },
                    data=image,
                ) as r:
                    if r.status >= HTTP_400_BAD_REQUEST:
                        raise aiohttp.ClientResponseError(
                            request_info=r.request_info,
                            history=r.history,
                            status=r.status,
                            message=await r.text(),
                            headers=r.headers,
                        )

                    data = await r.json()
                    logger.info("Screenshot uploaded successfully.")
                    return f"https://cdn.bkan0n.com/{data['bucket_id']}/{data['images'][0]['sizing_id']}/{data['image_id']}.png"

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                elapsed = time.monotonic() - start_time
                if elapsed >= total_timeout:
                    logger.error("Upload failed after %.2f seconds (%d attempts)", elapsed, attempts)
                    raise TimeoutError("Screenshot upload exceeded 10-minute limit.") from e

                delay = min(backoff_base**attempts + random.uniform(0, 1.0), 60.0)
                logger.warning("Upload attempt %d failed (%s). Retrying in %.2fs", attempts, type(e).__name__, delay)
                await asyncio.sleep(delay)

    async def close(self) -> None:
        """Close the underlying aiohttp client session.

        Should be called when the service is no longer needed
        to release network resources.
        """
        await self._client.close()


async def provide_lust_service() -> AsyncGenerator[LustService, None]:
    """Dependency provider for LustService.

    Ensures that the LustService is properly created and closed
    within the lifecycle of a request.

    Yields:
        LustService: Instance of the service for the current request.

    Finalizes:
        Closes the aiohttp client session when the request ends.

    """
    service = LustService()
    try:
        yield service
    finally:
        await service.close()
