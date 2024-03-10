import os
import pickle
import discord

from typing import Optional
from datetime import datetime

CACHE_DIR = 'cache'


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

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]):
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

            # fill gap between last retrieved message and start of this interval
            async for m in self.channel.history(limit=None, after=last_timestamp(), before=s_start, oldest_first=True):
                message = Message(m)
                if end and message.time > end:
                    self.__commit(messages, s_low, s_high)
                    return

                messages.append(message)
                yield message

            s_low = s_low or i
            s_high = i

            for message in segment:
                if end and message.time > end:
                    self.__commit(messages, s_low, s_high)
                    return 

                if (start is None) or (message.time >= start):
                    # TODO binary search
                    messages.append(message)
                    yield message

        # fill gap between last segment end of requested interval
        async for m in self.channel.history(limit=None, after=last_timestamp(), before=end, oldest_first=True):
            message = Message(m)
            messages.append(message)
            yield message

        self.__commit(messages, s_low, s_high)
