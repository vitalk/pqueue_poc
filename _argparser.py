import argparse

import attr


def _get_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-q",
        "--queue",
        dest="queue_name",
        type=str,
        required=True,
        help="Name of the rabbitmq queue",
    )

    parser.add_argument(
        "-p",
        "--payload",
        dest="payload",
        type=str,
        default="/ ",
        help="Payload to publish into the queue",
    )

    parser.add_argument(
        "-n",
        dest="number_of_records",
        type=int,
        default=1_000,
        help="Number of records to publish",
    )

    parser.add_argument(
        "-nice", dest="nice", type=int, default=0, help="Message priority (0-255)",
    )

    return parser


@attr.s
class _Config:
    number_of_records: int = attr.ib()
    queue_name: str = attr.ib()
    payload: str = attr.ib()
    priority: int = attr.ib()


def _get_producer_config() -> _Config:
    argparser = _get_argparser()
    args = argparser.parse_args()

    return _Config(
        number_of_records=args.number_of_records,
        queue_name=args.queue_name,
        payload=args.payload.encode(),
        priority=args.nice,
    )
