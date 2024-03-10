import os
import argparse
import logging
import inspect
import pathlib

from datetime import datetime, timezone
from enum import IntEnum
from typing import TypeVar

import discord

from client import Client
from messages import SingleChannelMessageStream, MultiChannelMessageStream, ServerMessageStream
from analysis import MessageAnalysis, CustomArgument, CustomOption


T = TypeVar('T')


class _EnumArg(IntEnum):
    @classmethod
    def argtype(cls, s: str) -> int:
        try:
            return cls[s]
        except KeyError:
            pass  # fallback to regular int
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


def main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    Client(_main, parse_args()).run(os.environ['DISCORD_USER_TOKEN'])


async def _main(client) -> int:
    args = client.args
    if args.start and args.start.tzinfo is None:
        args.start = args.start.replace(tzinfo=timezone.utc)
    if args.end and args.end.tzinfo is None:
        args.end = args.end.replace(tzinfo=timezone.utc)

    common_stream_args = (args.cache_dir, args.refresh_window, args.commit_batch_size)
    if args.server:
        if not (guild := client.get_guild(args.server)):
            logging.error(f'Server {args.server} could not be found')
            return 1
        stream = await ServerMessageStream(guild, *common_stream_args)
    else:
        channels: list[discord.TextChannel | discord.Thread] = []
        for channel_id in args.channels:
            if not (channel := client.get_channel(channel_id)):
                logging.error(f'Channel {channel_id} could not be found')
                return 1
            if isinstance(channel, (discord.TextChannel, discord.Thread)):
                channels.append(channel)

        if len(channels) > 1:
            stream = MultiChannelMessageStream(channels, *common_stream_args)
        else:
            stream = SingleChannelMessageStream(channels[0], *common_stream_args)

    analysis = args.analysis(stream, args)
    result = await analysis.run(args.start, args.end)
    await analysis.display_result(result, client, args.max_results)

    return 0


def get_subclasses(cls: type[T]) -> set[type[T]]:
    classes = [cls]
    i = 0
    while i < len(classes):
        classes.extend(classes[i].__subclasses__())
        i += 1
    return {c for c in classes if not inspect.isabstract(c)}


def parse_args():
    parser = argparse.ArgumentParser(prog='rocketscrape',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    root_dir = pathlib.Path(__file__).parent.parent.resolve()

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
    parser.add_argument('--cache-dir', type=str, default=(root_dir/'cache'),
                        help='directory to store the message cache in')
    parser.add_argument('--refresh-window', type=int, default=1,
                        help='width of window (in hours) in which message data will be refreshed despite being cached')
    parser.add_argument('--commit-batch-size', type=int, default=2500,
                        help='maximum number of new messages that will be committed to disk at once')

    def add_custom_arg(_parser, _arg):
        if type(_arg) is CustomArgument:
            _parser.add_argument(_arg.name, type=_arg.type, help=_arg.help)
        elif type(_arg) is CustomOption:
            _parser.add_argument(f'--{_arg.name}', type=_arg.type,
                                 default=_arg.default, help=_arg.help)

    base_cls = MessageAnalysis
    for arg in base_cls.custom_args():
        add_custom_arg(parser, arg)

    subparsers = parser.add_subparsers(title='analysis subcommands', required=True)
    for cls in get_subclasses(base_cls):
        subparser = subparsers.add_parser(cls.subcommand())
        subparser.set_defaults(analysis=cls)
        for arg in cls.custom_args():
            add_custom_arg(subparser, arg)

    return parser.parse_args()


if __name__ == '__main__':
    main()
