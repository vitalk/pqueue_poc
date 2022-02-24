import asyncio
import argparse
import logging
from typing import Type

from _argparser import _Config
from _broker import _get_broker, _get_queue_config
from _main import _main


logger = logging.getLogger(__name__)


async def producer(loop, broker, config: _Config) -> None:
    queue_config = _get_queue_config(config.queue_name)

    await broker.connect(will_close=True)
    await broker.declare_queue_and_exchange(queue_config)

    try:
        i = 1

        while i <= config.number_of_records:
            logger.warning("published %d of %d records", i, config.number_of_records)
            await broker.send(
                payload=config.payload,
                exchange_name=queue_config.exchange_name,
                routing_key=queue_config.routing_key,
                priority=config.priority,
            )

            i += 1
            await asyncio.sleep(.1)

        logger.warning("done")

    finally:
        await broker.close()


def main():
    _main(producer)


if __name__ == "__main__":
    """
    python producer.py --queue=test_queue --payload="/" -n 1000 -nice 10
    python producer.py --queue=test_queue --payload="@" -n 100 -nice 50
    """
    main()
