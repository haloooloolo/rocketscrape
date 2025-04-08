import os
import logging
import inspect
import argparse
import discord

from pathlib import Path
from datetime import datetime, timezone
from typing import TypeVar, get_args
from rocketscrape.client import Client
from rocketscrape.utils import Server, Channel
from rocketscrape.messages import (
    ChannelMessageStream,
    MultiChannelMessageStream,
    ServerMessageStream,
    MultiServerMessageStream,
    ChannelType
)
from rocketscrape.analysis import MessageAnalysis


T = TypeVar('T')


def main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    Client(_main, parse_args()).run(os.environ['DISCORD_USER_TOKEN'])


async def _main(client: Client) -> int:
    args = client.args
    if args.start and args.start.tzinfo is None:
        args.start = args.start.replace(tzinfo=timezone.utc)
    if args.end and args.end.tzinfo is None:
        args.end = args.end.replace(tzinfo=timezone.utc)

    common_stream_args = (args.cache_dir, args.refresh_window, args.commit_batch_size)
    if args.server:
        guilds: list[discord.Guild] = []
        for server_id in args.server:
            if not (guild := await client.try_fetch_guild(server_id)):
                logging.error(f'Server {server_id} could not be found')
                return 1

            guilds.append(guild)

        if len(guilds) > 1:
            stream = await MultiServerMessageStream(guilds, args.threads, *common_stream_args)
        else:
            stream = await ServerMessageStream(guilds[0], args.threads, *common_stream_args)
    else:
        channels: list[ChannelType] = []
        for channel_id in args.channel:
            if not (channel := await client.try_fetch_channel(channel_id)):
                logging.error(f'Channel {channel_id} could not be found')
                return 1
            if not isinstance(channel, get_args(ChannelType)):
                logging.error(f'Channel {channel_id} has incompatible type {type(channel)}')
                return 2

            channels.append(channel)

        if len(channels) > 1 or args.threads:
            stream = await MultiChannelMessageStream(channels, args.threads, *common_stream_args)
        else:
            stream = ChannelMessageStream(channels[0], *common_stream_args)

    try:
        analysis = args.analysis(stream, args)
        result = await analysis.run(args.start, args.end)
        print()  # some spacing to make it look nicer
        await result.display(client, args.max_results)
    except Exception as exc:
        logging.exception(str(exc))
        return 1

    return 0


def get_subclasses(cls: type[T]) -> set[type[T]]:
    classes = [cls]
    i = 0
    while i < len(classes):
        classes.extend(classes[i].__subclasses__())
        i += 1
    return {c for c in classes if not inspect.isabstract(c)}


def parse_args():
    parser = argparse.ArgumentParser(
        prog='rocketscrape',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    root_dir = Path(__file__).parent.resolve()

    source = parser.add_mutually_exclusive_group(required=True)
    channel_choices = tuple((c.name for c in Channel))
    source.add_argument('-c', '--channel', type=Channel.argtype, nargs='+',
                        help=f'one or more of {channel_choices} or channel ID(s)')
    server_choices = tuple((s.name for s in Server))
    source.add_argument('-g', '--server', type=Server.argtype, nargs='+',
                        help=f'one or more of {server_choices} or server ID(s)')
    
    parser.add_argument('-t', '--threads', dest='threads', action='store_true',
                        help='include streams for all threads within the specified channels')
    parser.add_argument('-s', '--start', type=datetime.fromisoformat,
                        help='start of date range in ISO format')
    parser.add_argument('-e', '--end', type=datetime.fromisoformat,
                        help='end of date range in ISO format')
    parser.add_argument('-r', '--max-results', type=int, default=10,
                        help='maximum length of analysis output')
    parser.add_argument('-l', '--log-interval', type=int, default=1,
                        help='frequency of progress logs in seconds')
    parser.add_argument('--cache-dir', type=str, default=(root_dir/'.cache'),
                        help='directory to store the message cache in')
    parser.add_argument('--refresh-window', type=int, default=1,
                        help='messages last ')
    parser.add_argument('--commit-batch-size', type=int, default=2500,
                        help='maximum number of new messages that will be committed to disk at once')
    parser.add_argument('--user-filter', type=int, nargs='+', default=None,
                        help='if specified, list of user IDs for which to include data')

    base_cls = MessageAnalysis
    subparsers = parser.add_subparsers(title='analysis subcommands', required=True)
    for cls in get_subclasses(base_cls):
        if subcommand := cls.subcommand():
            subparser = subparsers.add_parser(subcommand, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
            subparser.set_defaults(analysis=cls)
            for custom_arg in cls.custom_args():
                subparser.add_argument(*custom_arg.args, **custom_arg.kwargs)

    return parser.parse_args()


if __name__ == '__main__':
    main()
