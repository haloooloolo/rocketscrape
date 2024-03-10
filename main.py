import os
import pickle
import discord
from enum import Enum
from datetime import datetime, timedelta, timezone

BASE_SESSION_TIME_MINS = 5
SESSION_TIMEOUT_MINS = 15
MAX_CONTRIBUTORS = 10

CACHE_DIR = 'cache'

class Channel(Enum):
    GENERAL = 704196071881965589
    TRADING = 405163713063288832
    SUPPORT = 468923220607762485

class Message:
    def __init__(self, message: discord.Message) -> None:
        self.time = message.created_at
        self.author = message.author.name
        self.content = message.content
        
    def __repr__(self) -> str:
        return f'Message{{{self.author} @ {self.time}: "{self.content}"}}'

class MessageCache:
    def __init__(self, channel: discord.TextChannel) -> None:
        self.channel = channel
        try:
            self.segments = self.__load()
        except (FileNotFoundError, EOFError):
            self.segments = []

    def __load(self) -> list[list[Message]]:
        path = os.path.join(CACHE_DIR, f'{self.channel.id}.pkl')
        with open(path, 'rb') as file:
            return pickle.load(file)

    def __commit(self, messages, s_low, s_high):
        pre = []
        post = []

        if (s_low is not None) and (s_high is not None):
            for message in self.segments[s_low]:
                # TODO binary search
                if message.time < messages[0].time:
                    pre.append(message)

            for message in self.segments[s_high]:
                # TODO binary search
                if message.time > messages[-1].time:
                    post.append(message)

            new_segment = pre + messages + post
            self.segments = self.segments[:s_low] + [new_segment] + self.segments[s_high+1:]
        elif self.segments:
            raise NotImplementedError
        else:
            self.segments = [messages]

        os.makedirs(CACHE_DIR, exist_ok=True)
        path = os.path.join(CACHE_DIR, f'{self.channel.id}.pkl')
        with open(path, 'wb') as file:
            return pickle.dump(self.segments, file)

    async def get(self, start: datetime, end: datetime):
        messages = []
        s_low, s_high = None, None  # segment range to be used

        def last_timestamp():
            return start if not messages else messages[-1].time

        for i, segment in enumerate(self.segments):
            s_start, s_end = segment[0].time, segment[-1].time
            print(s_start, s_end)

            # segment ahead of requested interval, skip
            if start and start > s_end:
                # TODO binary search
                continue
                
            print(f'Retrieving missing messages between {last_timestamp()} and {s_start}')
            # fill gap between last retrieved message and start of this interval
            async for m in self.channel.history(limit=None, after=last_timestamp(), before=s_start, oldest_first=True):
                message = Message(m)
                if end and message.time > end:
                    self.__commit(messages, s_low, s_high)
                    raise StopIteration

                messages.append(message)
                yield message

            s_low = s_low or i
            s_high = i

            for message in segment:
                if end and message.time > end:
                    self.__commit(messages, s_low, s_high)
                    raise StopIteration

                if (start is None) or message.time >= start:
                    # TODO binary search
                    messages.append(message)
                    yield message

        print(f'Filling tail gap between {last_timestamp()} and {end}')
        # fill gap between last segment end of requested interval
        async for m in self.channel.history(limit=None, after=last_timestamp(), before=end, oldest_first=True):
            message = Message(m)
            messages.append(message)
            yield message

        self.__commit(messages, s_low, s_high)


def to_minutes(td: timedelta):
    return td / timedelta(minutes=1)


async def get_contributors(channel: discord.TextChannel, start: datetime, end: datetime):
    assert (start is None) or (end is None) or (end > start)
    open_sessions = {}
    total_time = {}

    async for message in MessageCache(channel).get(start, end):
        timestamp = message.time
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

    # add remaining open sessions to total
    for author, (session_start, session_end) in open_sessions.items():
        session_time = to_minutes(session_end - session_start) + BASE_SESSION_TIME_MINS
        total_time[author] = total_time.get(author, 0) + session_time

    return sorted(total_time.items(), key=lambda a: a[1], reverse=True)


async def on_ready():
    channel = client.get_channel(Channel.SUPPORT.value)
    start = None  # datetime.fromisoformat('2023-01-01').replace(tzinfo=timezone.utc)
    end = None
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
    for i, (author, time) in enumerate(list(contributors)[:MAX_CONTRIBUTORS]):
        time_mins = round(time)
        hours, minutes = time_mins // 60, time_mins % 60
        print(f'{i+1}. {author}: {hours}h {minutes}m')

if __name__ == '__main__':
    client = discord.Client()
    on_ready = client.event(on_ready)
    client.run(os.environ['DISCORD_USER_TOKEN'])
