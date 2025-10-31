import typing
import uuid
from logging import getLogger
from uuid import uuid4

import aio_pika
import msgspec
from asyncpg import Connection
from genjipk_sdk.models import JobStatus
from litestar.datastructures import Headers, State

log = getLogger(__name__)


class RabbitMessageBody(msgspec.Struct):
    type: str
    data: typing.Any


class BaseService:
    def __init__(self, conn: Connection, state: State) -> None:
        """Initialize a BaseService.

        Args:
            conn (Connection): asyncpg connection.
            state (State): App state.

        """
        self._conn = conn
        self._state = state

    async def publish_message(
        self,
        *,
        routing_key: str,
        data: msgspec.Struct | list[msgspec.Struct],
        headers: Headers,
    ) -> JobStatus:
        """Publish a message to RabbitMQ.

        Args:
            data (msgspec.Struct | list[msgspec.Struct]): The message data.
            routing_key (str, optional): The RabbitMQ message routing key.
            headers (dict, optional): Headers.
            correlation_id (UUID): A job id.
        """
        message_body = msgspec.json.encode(data)

        if headers.get("X-PYTEST-ENABLED") == "1":
            log.debug("Pytest in progress, skipping queue.")
            return JobStatus(uuid4(), "succeeded")

        log.info("[→] Preparing to publish RabbitMQ message")
        log.debug("Routing key: %s", routing_key)
        log.debug("Headers: %s", headers)
        log.debug("Payload: %s", message_body.decode("utf-8", errors="ignore"))

        async with self._state.mq_channel_pool.acquire() as channel:
            try:
                job_id = uuid.uuid4()
                await self._conn.execute(
                    "INSERT INTO public.jobs (id, action) VALUES ($1, $2);",
                    job_id,
                    routing_key,
                )
                message = aio_pika.Message(
                    message_body,
                    correlation_id=str(job_id),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers=headers.dict(),  # pyright: ignore[reportArgumentType]
                )
                await channel.default_exchange.publish(
                    message,
                    routing_key=routing_key,
                )
                log.info("[✓] Published RabbitMQ message to queue '%s'", routing_key)
                return JobStatus(id=job_id, status="queued")
            except Exception:
                log.exception("[!] Failed to publish message to RabbitMQ queue '%s'", routing_key)
                return JobStatus(job_id, "failed", "", "Failed to send message.")
