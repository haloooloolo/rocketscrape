import os
import discord
from enum import Enum
from datetime import datetime, timedelta

BASE_SESSION_TIME_MINS = 5
SESSION_TIMEOUT_MINS = 15
MAX_NUM_CONTRIBUTORS = 10


class Channel(Enum):
    GENERAL = 704196071881965589
    TRADING = 405163713063288832
    SUPPORT = 468923220607762485

def to_minutes(td: timedelta):
    return td / timedelta(minutes=1)

async def get_contributors(channel, start, end):
    open_sessions = {}
    total_time = {}

    async for message in channel.history(limit=None, after=start, before=end, oldest_first=True):
        timestamp = message.created_at
        author = message.author
        print(timestamp)

        session_start, session_end = open_sessions.get(author, (timestamp, timestamp))
        if to_minutes(timestamp - session_end) < SESSION_TIMEOUT_MINS:
            session_end = timestamp
            open_sessions[author] = (session_start, session_end)
        else:
            session_time = to_minutes(session_end - session_start) + BASE_SESSION_TIME_MINS
            total_time[author] = total_time.get(author, 0) + session_time
            del open_sessions[author]

    for author, (session_start, session_end) in open_sessions.items():
        session_time = to_minutes(session_end - session_start) + BASE_SESSION_TIME_MINS
        total_time[author] = total_time.get(author, 0) + session_time

    return sorted(total_time.items(), key=lambda a: a[1], reverse=True)

client = discord.Client()

@client.event
async def on_ready():
    channel = client.get_channel(Channel.SUPPORT.value)
    start, end = None, None # datetime.fromisoformat('2023-01-15'), None
    contributors = await get_contributors(channel, start, end)
    
    if start and end:
        range_str = f'from {start} to {end}'
    elif start:
        range_str = f'since {start}'
    elif end:
        range_str = f'until {end}'
    else:
        range_str = ''

    print(f'Top # {channel} contributors {range_str}')
    for i, (author, time) in enumerate(list(contributors)[:MAX_NUM_CONTRIBUTORS]):
        time_mins = round(time)
        hours, minutes = time_mins // 60, time_mins % 60
        print(f'{i+1}. {author}: {hours}h {minutes}m')


client.run(os.environ['DISCORD_USER_TOKEN'])
