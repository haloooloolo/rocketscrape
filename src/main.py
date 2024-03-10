import heapq
import argparse

import discord.errors
import matplotlib.pyplot as plt
from enum import Enum, IntEnum

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


async def get_username(user_id: int):
    try:
        user = client.get_user(user_id) or await client.fetch_user(user_id)
        return user.display_name
    except discord.errors.NotFound:
        return f'<{user_id}>'


async def plot_contributor_history(stream: MessageStream) -> None:
    analysis = HistoricalTopContributorAnalysis(args.log_interval)
    x, y = await analysis.run(stream, args.start, args.end)

    for author_id, data in sorted(y.items(), key=lambda a: a[1][-1], reverse=True)[:args.max_results]:
        plt.plot(x, data, label=await get_username(author_id))

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
        print(f'{i+1}. {await get_username(author_id)}: {hours}h {minutes}m')


async def main():
    args.start = args.start.replace(tzinfo=timezone.utc) if args.start else None
    args.end = args.end.replace(tzinfo=timezone.utc) if args.end else None

    if args.channel:
        stream = SingleChannelMessageStream(client.get_channel(args.channel))
    elif args.channels:
        stream = MultiChannelMessageStream(list(map(client.get_channel, args.channels)))
    else:
        stream = ServerMessageStream(client.get_guild(Server.rocketpool))

    await print_contributors(stream)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', type=datetime.fromisoformat)
    parser.add_argument('-e', '--end', type=datetime.fromisoformat)
    parser.add_argument('-r', '--max-results', type=int, default=10)
    parser.add_argument('-l', '--log-interval', type=float, default=1.0)

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--channel', type=Channel.argtype, choices=Channel)
    source.add_argument('--channels', type=Channel.argtype, choices=Channel, nargs='+')
    source.add_argument('--server', type=Server.argtype, choices=Server)

    return parser.parse_args()


async def on_ready():
    await main()
    await client.close()

if __name__ == '__main__':
    client = discord.Client()
    on_ready = client.event(on_ready)
    args = parse_args()
    client.run(os.environ['DISCORD_USER_TOKEN'])
