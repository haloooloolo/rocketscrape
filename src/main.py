import os
import argparse
import logging
import inspect

from datetime import datetime, timezone
from enum import IntEnum

from client import Client
from messages import SingleChannelMessageStream, MultiChannelMessageStream, ServerMessageStream, Message
from analysis import MessageAnalysis, CustomArgument, CustomOption


class _EnumArg(IntEnum):
    @classmethod
    def argtype(cls, s: str) -> int:
        try:
            return cls[s]
        except KeyError:
            try:
                return int(s)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"{s!r} is not a valid {cls.__name__.lower()}")

    def __str__(self):
        return self.name


class Server(_EnumArg):
    rocketpool = 405159462932971535


class Channel(_EnumArg):
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

    analysis = args.analysis(stream, args)
    result = await analysis.run(args.start, args.end)
    await analysis.present(result, client, stream, args)


def get_subclasses(cls: type) -> set[type]:
    classes = [cls]
    i = 0
    while i < len(classes):
        classes.extend(classes[i].__subclasses__())
        i += 1
    return {c for c in classes if not inspect.isabstract(c)}


def parse_args():
    parser = argparse.ArgumentParser(prog='RocketScrape',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    source = parser.add_mutually_exclusive_group(required=True)
    channel_choices = tuple((c.name for c in Channel))
    source.add_argument('-c', '--channels', type=Channel.argtype, action='append',
                        help=f'one or more of {channel_choices} or channel ID(s)')
    server_choices = tuple((s.name for s in Server))
    source.add_argument('--server', type=Server.argtype,
                        help=f'one of {server_choices} or server ID')

    parser.add_argument('-s', '--start', type=datetime.fromisoformat,
                        help=f'start of date range in ISO format')
    parser.add_argument('-e', '--end', type=datetime.fromisoformat,
                        help=f'end of date range in ISO format')
    parser.add_argument('-r', '--max-results', type=int, default=10,
                        help=f'maximum length of analysis output')
    parser.add_argument('-l', '--log-interval', type=int, default=1,
                        help='frequency of progress logs in seconds')

    def add_custom_arg(_parser, _arg):
        if isinstance(_arg, CustomArgument):
            _parser.add_argument(_arg.name, type=_arg.type, help=_arg.help)
        elif isinstance(_arg, CustomOption):
            _parser.add_argument(f'--{_arg.name}', type=_arg.type,
                                default=_arg.default,help=_arg.help, required=True)

    base_cls = MessageAnalysis
    for arg in base_cls.custom_args():
        add_custom_arg(parser, arg)

    subparsers = parser.add_subparsers(title='analysis subcommands')
    for cls in get_subclasses(base_cls):
        subparser = subparsers.add_parser(cls.subcommand())
        subparser.set_defaults(analysis=cls)
        for arg in cls.custom_args():
            add_custom_arg(subparser, arg)

    return parser.parse_args()


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    args = parse_args()
    client = Client(main)
    client.run(os.environ['DISCORD_USER_TOKEN'])
