import asyncio
import logging

from _argparser import _get_producer_config, _Config
from _broker import _get_broker, _get_queue_config


logger = logging.getLogger(__name__)


def _main(coro):
    config = _get_producer_config()

    loop = asyncio.get_event_loop()
    broker = _get_broker()

    loop.run_until_complete(coro(loop=loop, broker=broker, config=config))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Ctrl-C received. Cleaning up and exiting...")
    finally:
        pending = asyncio.Task.all_tasks()
        loop.run_until_complete(asyncio.gather(broker.close(), *pending))

    loop.close()
