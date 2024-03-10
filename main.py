import os
import argparse
from enum import Enum
from datetime import datetime, timezone

import discord
from analysis import TopContributorAnalysis


class Channel(Enum):
    general = 704196071881965589
    trading = 405163713063288832
    support = 468923220607762485

    @classmethod
    def argtype(cls, s: str) -> Enum:
        try:
            return cls[s]
        except KeyError:
            raise argparse.ArgumentTypeError(
                f"{s!r} is not a valid {cls.__name__}")

    def __str__(self):
        return self.name


async def main(args):
    channel = client.get_channel(args.channel.value)
    start = args.start.replace(tzinfo=timezone.utc) if args.start else None
    end = args.end.replace(tzinfo=timezone.utc) if args.end else None

    analysis = TopContributorAnalysis(args.log_interval)
    contributors = await analysis.run(channel, start, end)

    if start and end:
        range_str = f'from {start} to {end}'
    elif start:
        range_str = f'since {start}'
    elif end:
        range_str = f'up to {end}'
    else:
        range_str = '(all time)'

    print()
    print(f'Top # {channel} contributors {range_str}')
    for i, (author, time) in enumerate(list(contributors)[:args.max_results]):
        time_mins = round(time)
        hours, minutes = time_mins // 60, time_mins % 60
        print(f'{i+1}. {author}: {hours}h {minutes}m')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--channel', type=Channel.argtype, choices=Channel, default=Channel.support)
    parser.add_argument('-s', '--start', type=datetime.fromisoformat)
    parser.add_argument('-e', '--end', type=datetime.fromisoformat)
    parser.add_argument('-r', '--max-results', type=int, default=10)
    parser.add_argument('-l', '--log-interval', type=float, default=1)
    return parser.parse_args()


async def on_ready():
    await main(parse_args())
    await client.close()

if __name__ == '__main__':
    client = discord.Client()
    on_ready = client.event(on_ready)
    client.run(os.environ['DISCORD_USER_TOKEN'])
