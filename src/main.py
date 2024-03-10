import heapq
import argparse
import logging

import matplotlib.pyplot as plt
from enum import Enum, IntEnum

from client import Client
from messages import *
from analysis import *


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


async def plot_contributor_history(stream: MessageStream) -> None:
    analysis = ContributionHistoryAnalysis(args.log_interval)
    x, y = await analysis.run(stream, args.start, args.end)

    for author_id, data in sorted(y.items(), key=lambda a: a[1][-1], reverse=True)[:args.max_results]:
        plt.plot(x, data, label=await client.get_username(author_id))

    plt.ylabel('time (mins)')
    plt.legend()
    plt.title(f'Top {stream} contributors over time')
    plt.show()


async def print_contributors(stream: MessageStream) -> None:
    analysis = TopContributorAnalysis(args.log_interval)
    contributors = await analysis.run(stream, args.start, args.end)
    top_contributors = heapq.nlargest(args.max_results, contributors.items(), key=lambda a: a[1])

    if args.start and args.end:
        range_str = f'from {args.start} to {args.end}'
    elif args.start:
        range_str = f'since {args.start}'
    elif args.end:
        range_str = f'until {args.end}'
    else:
        range_str = '(all time)'

    print()
    print(f'Top {stream} contributors {range_str}')
    for i, (author_id, time) in enumerate(top_contributors):
        time_mins = round(time)
        hours, minutes = time_mins // 60, time_mins % 60
        print(f'{i+1}. {await client.get_username(author_id)}: {hours}h {minutes}m')


async def main() -> None:
    args.start = args.start.replace(tzinfo=timezone.utc) if args.start else None
    args.end = args.end.replace(tzinfo=timezone.utc) if args.end else None

    if args.server:
        stream = await ServerMessageStream(client.get_guild(args.server))
    elif len(args.channels) > 1:
        stream = MultiChannelMessageStream(list(map(client.get_channel, args.channels)))
    else:
        stream = SingleChannelMessageStream(client.get_channel(args.channel[0]))

    await print_contributors(stream)


def get_analysis_types() -> list[type[MessageAnalysis]]:
    classes = MessageAnalysis.__subclasses__
    i = 0
    while i < len(classes):
        classes.extend(classes[i].__subclasses__)
    return [cls.command for cls in classes]


def parse_args():
    parser = argparse.ArgumentParser(prog='RocketScrape')

    analysis_types = {cls.command: cls for cls in get_analysis_types()}
    parser.add_argument('analysis', type=analysis_types.get, choices=analysis_types.keys())

    parser.add_argument('-s', '--start', type=datetime.fromisoformat)
    parser.add_argument('-e', '--end', type=datetime.fromisoformat)
    parser.add_argument('-r', '--max-results', type=int, default=10)
    parser.add_argument('-l', '--log-interval', type=float, default=1.0)

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--channels', type=Channel.argtype, choices=Channel, nargs='+')
    source.add_argument('--server', type=Server.argtype, choices=Server)

    return parser.parse_args()


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    client = Client(main)
    args = parse_args()
    client.run(os.environ['DISCORD_USER_TOKEN'])
