import typing
from logging import getLogger

import aio_pika
import msgspec
from asyncpg import Connection
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

        """
        self._conn = conn
        self._state = state

    async def publish_message(
        self,
        *,
        routing_key: str,
        data: msgspec.Struct | list[msgspec.Struct],
        headers: Headers,
    ) -> None:
        """Publish a message to RabbitMQ.

        Args:
            state (State): The app State.
            pytest_enabled (bool): Whether pytest is enabled.
            data (msgspec.Struct | list[msgspec.Struct]): The message data.
            routing_key (str, optional): The RabbitMQ message routing key.
            extra_headers (dict, optional): Additional headers.

        """
        message_body = msgspec.json.encode(data)

        if headers.get("X-PYTEST-ENABLED") == "1":
            log.debug("Pytest in progress, skipping queue.")
            return

        log.info("[→] Preparing to publish RabbitMQ message")
        log.debug("Routing key: %s", routing_key)
        log.debug("Headers: %s", headers)
        log.debug("Payload: %s", message_body.decode("utf-8", errors="ignore"))

        async with self._state.mq_channel_pool.acquire() as channel:
            try:
                message = aio_pika.Message(
                    message_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    headers=headers.dict(),  # pyright: ignore[reportArgumentType]
                )
                await channel.default_exchange.publish(
                    message,
                    routing_key=routing_key,
                )
                log.info("[✓] Published RabbitMQ message to queue '%s'", routing_key)
            except Exception:
                log.exception("[!] Failed to publish message to RabbitMQ queue '%s'", routing_key)
