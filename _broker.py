import asyncio
import logging
from typing import Type, Awaitable

import aioamqp
import functools

logger = logging.getLogger(__name__)


class IQueueConfig:
    queue_name = "dontcare"
    exchange_name = "dontcare"
    exchange_type = "topic"
    routing_key = "dontcare"


def _get_queue_config(queue: str) -> Type[IQueueConfig]:
    class QueueConfig(IQueueConfig):
        queue_name = queue
        exchange_name = queue
        routing_key = queue

    return QueueConfig


class Broker:
    def __init__(self, **options):
        self._options = options
        self._transport = None
        self._protocol = None
        self._channel = None

    @staticmethod
    async def heartbeat(protocol):
        while True:
            await asyncio.sleep(10)
            await protocol.send_heartbeat()

    @staticmethod
    async def handle_closed(protocol):
        await protocol.wait_closed()
        asyncio.get_event_loop().stop()
        exit(0)

    async def connect(self, reconnect=False, will_close=False):
        logger.info("Trying to establish connection to RabbitMQ")

        async def consumer_cancelled(*args):
            logger.warning(
                "The worker has received the basic.cancel from the broker. Exiting"
            )
            asyncio.get_event_loop().stop()
            exit(0)

        try:
            try:
                transport, protocol = await aioamqp.connect(
                    heartbeat=800, **self._options
                )

                channel = await protocol.channel()
                channel.add_cancellation_callback(consumer_cancelled)

                self._transport = transport
                self._protocol = protocol
                self._channel = channel

                if not will_close:
                    asyncio.ensure_future(self.handle_closed(protocol))
                    asyncio.ensure_future(self.heartbeat(protocol))

            except:
                logger.exception("Error on connect!")
                asyncio.get_event_loop().stop()
                exit(0)

        except (aioamqp.AmqpClosedConnection, OSError) as exc:
            if reconnect:
                logger.info("Trying to reconnect to RabbitMQ at after 1 second...")
                await asyncio.sleep(1)
                await self.connect(reconnect=True)
            else:
                logger.critical("Connection to RabbitMQ cannot be established: %r", exc)
                raise exc
        else:
            logger.info("Connection to RabbitMQ has been established")

    async def close(self):
        if self._protocol:
            await self._protocol.close()

        if self._transport:
            self._transport.close()

        logger.info("Connection to RabbitMQ has been closed")

    async def send(
        self, payload: bytes, exchange_name: str, routing_key: str, priority: int = 0
    ) -> None:
        try:
            await self._channel.basic_publish(
                payload=payload,
                exchange_name=exchange_name,
                routing_key=routing_key,
                properties={"delivery_mode": 2, "priority": priority},
            )
        except aioamqp.AioamqpException:
            logger.error("It seems that the connection to RabbitMQ has been lost")

            await self.connect(reconnect=True)
            asyncio.get_event_loop().call_later(
                1,
                functools.partial(
                    self.send,
                    payload=payload,
                    exchange_name=exchange_name,
                    routing_key=routing_key,
                    priority=priority,
                ),
            )

    async def declare_queue_and_exchange(self, queue_config) -> None:
        await self._channel.queue_declare(
            queue_name=queue_config.queue_name,
            durable=True,
            arguments={"x-max-priority": 255},
        )
        await self._channel.exchange_declare(
            queue_config.exchange_name, queue_config.exchange_type, durable=True
        )
        await self._channel.queue_bind(
            queue_name=queue_config.queue_name,
            exchange_name=queue_config.exchange_name,
            routing_key=queue_config.routing_key,
        )

    async def attach_callback_to_queue(self, queue_config, callback) -> None:
        await self.declare_queue_and_exchange(queue_config)

        if callback is not None:
            await self._channel.basic_consume(
                queue_name=queue_config.queue_name, callback=callback
            )
            logger.info(
                "Waiting for messages on queue={} routing_key={}".format(
                    queue_config.queue_name, queue_config.routing_key
                )
            )


def _get_broker(**options) -> Broker:
    options = {
        "host": "127.0.0.1",
        "port": 5672,
        "login": "guest",
        "password": "guest",
        "virtualhost": "/",
    }
    logger.debug("AMQP connection options: %r", options)

    return Broker(**options)
