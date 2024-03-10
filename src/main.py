import os
import argparse
import logging
import inspect

from datetime import datetime, timezone
from enum import Enum, IntEnum

from client import Client
from messages import SingleChannelMessageStream, MultiChannelMessageStream, ServerMessageStream
from analysis import MessageAnalysis


class _EnumArg(Enum):
    @classmethod
    def argtype(cls, s: str) -> Enum:
        try:
            return cls[s]
        except KeyError:
            raise argparse.ArgumentTypeError(
                f"{s!r} is not a valid {cls.__name__}")

    def __str__(self):
        return self.name


class Server(_EnumArg, IntEnum):
    rocketpool = 405159462932971535


class Channel(_EnumArg, IntEnum):
    general = 704196071881965589
    trading = 405163713063288832
    support = 468923220607762485


async def main() -> None:
    args.start = args.start.replace(tzinfo=timezone.utc) if args.start else None
    args.end = args.end.replace(tzinfo=timezone.utc) if args.end else None

    if args.server:
        stream = await ServerMessageStream(client.get_guild(args.server))
    elif len(args.channels) > 1:
        stream = MultiChannelMessageStream(list(map(client.get_channel, args.channels)))
    else:
        stream = SingleChannelMessageStream(client.get_channel(args.channels[0]))

    analysis = args.analysis(args.log_interval)
    result = await analysis.run(stream, args.start, args.end)
    await analysis.present(result, client, stream, args)


def get_subclasses(cls: type) -> set[type]:
    classes = [cls]
    i = 0
    while i < len(classes):
        classes.extend(classes[i].__subclasses__())
        i += 1
    return {c for c in classes if not inspect.isabstract(c)}


def parse_args():
    parser = argparse.ArgumentParser(prog='RocketScrape')

    analysis_types = {cls.subcommand(): cls for cls in get_subclasses(MessageAnalysis)}
    parser.add_argument('analysis', choices=analysis_types.keys())

    parser.add_argument('-s', '--start', type=datetime.fromisoformat)
    parser.add_argument('-e', '--end', type=datetime.fromisoformat)
    parser.add_argument('-r', '--max-results', type=int, default=10)
    parser.add_argument('-l', '--log-interval', type=float, default=1.0)

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--channels', type=Channel.argtype, choices=Channel, nargs='+')
    source.add_argument('--server', type=Server.argtype, choices=Server)

    _args = parser.parse_args()
    _args.analysis = analysis_types[_args.analysis]
    return _args


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    args = parse_args()
    client = Client(main)
    client.run(os.environ['DISCORD_USER_TOKEN'])
