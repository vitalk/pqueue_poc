import asyncio
import logging

from aioamqp.channel import Channel, Envelope

from _argparser import _get_producer_config, _Config
from _broker import _get_queue_config
from _main import _main


logger = logging.getLogger(__name__)
logging.StreamHandler.terminator = " "


async def _consumer(
    channel: Channel, message: bytes, envelope: Envelope, unused
) -> None:
    message_decoded = message.decode()
    try:
        await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

        await asyncio.sleep(0.2)

        logger.warning(message_decoded)
    except Exception as error:
        logger.exception(str(error))


async def consumer(loop, broker, config: _Config) -> None:
    queue_config = _get_queue_config(config.queue_name)

    await broker.connect()
    await broker._channel.basic_qos(
        prefetch_count=3, connection_global=False
    )
    await broker.attach_callback_to_queue(queue_config, _consumer)


def main():
    _main(consumer)


if __name__ == "__main__":
    """
    python consumer.py --queue=test_queue
    """
    main()
