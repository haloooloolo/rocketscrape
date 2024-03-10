import heapq
import argparse
from enum import IntEnum

import matplotlib.pyplot as plt

from messages import *
from analysis import TopContributorAnalysis, HistoricalTopContributorAnalysis


class _EnumArg(IntEnum):
    @classmethod
    def argtype(cls, s: str) -> IntEnum:
        try:
            return cls[s]
        except KeyError:
            raise argparse.ArgumentTypeError(
                f"{s!r} is not a valid {cls.__name__}")

    def __str__(self):
        return self.name


class Server(_EnumArg):
    rocketpool = 405159462932971535


class Channel(_EnumArg):
    general = 704196071881965589
    trading = 405163713063288832
    support = 468923220607762485


async def plot_contributor_history(args):
    start = args.start.replace(tzinfo=timezone.utc) if args.start else None
    end = args.end.replace(tzinfo=timezone.utc) if args.end else None

    stream = ServerMessageStream(client.get_guild(Server.rocketpool))
    analysis = HistoricalTopContributorAnalysis(args.log_interval)
    x, y = await analysis.run(stream, start, end)

    for author, data in sorted(y.items(), key=lambda a: a[1][-1], reverse=True)[:args.max_results]:
        plt.plot(x, data, label=author)

    plt.ylabel('time (mins)')
    plt.legend()
    plt.title(f'Top {stream} contributors over time')
    plt.show()


async def print_contributors(args):
    start = args.start.replace(tzinfo=timezone.utc) if args.start else None
    end = args.end.replace(tzinfo=timezone.utc) if args.end else None

    if args.channel:
        stream = SingleChannelMessageStream(client.get_channel(args.channel))
    elif args.channels:
        stream = MultiChannelMessageStream([client.get_channel(c) for c in args.channels])
    else:
        stream = ServerMessageStream(client.get_guild(Server.rocketpool))

    analysis = TopContributorAnalysis(args.log_interval)
    contributors = await analysis.run(stream, start, end)
    top_contributors = heapq.nlargest(args.max_results, contributors.items(), key=lambda a: a[1])

    if start and end:
        range_str = f'from {start} to {end}'
    elif start:
        range_str = f'since {start}'
    elif end:
        range_str = f'up to {end}'
    else:
        range_str = '(all time)'

    print()
    print(f'Top {stream} contributors {range_str}')
    for i, (author, time) in enumerate(top_contributors):
        time_mins = round(time)
        hours, minutes = time_mins // 60, time_mins % 60
        print(f'{i+1}. {author}: {hours}h {minutes}m')


async def main(args):
    await print_contributors(args)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', type=datetime.fromisoformat)
    parser.add_argument('-e', '--end', type=datetime.fromisoformat)
    parser.add_argument('-r', '--max-results', type=int, default=10)
    parser.add_argument('-l', '--log-interval', type=float, default=1.0)

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--channel', type=Channel.argtype, choices=Channel)
    source.add_argument('--channels', type=Channel.argtype, choices=Channel, nargs='+')
    source.add_argument('--server', type=Server.argtype, choices=Server, default=Server.rocketpool)

    return parser.parse_args()


async def on_ready():
    await main(parse_args())
    await client.close()

if __name__ == '__main__':
    client = discord.Client()
    on_ready = client.event(on_ready)
    client.run(os.environ['DISCORD_USER_TOKEN'])
