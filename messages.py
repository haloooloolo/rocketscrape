import os
import pickle
import discord

from typing import Optional
from datetime import datetime
from dataclasses import dataclass, field

CACHE_DIR = 'cache'


class Message:
    def __init__(self, message: discord.Message) -> None:
        self.time = message.created_at
        self.author = message.author.name
        self.content = message.content

    def __repr__(self) -> str:
        return f'Message{{{self.author} @ {self.time}: "{self.content}"}}'


@dataclass
class Segment:
    start: datetime
    end: datetime
    messages: list[Message] = field(default_factory=list)

    def add(self, message: Message):
        # assume chronological order for now
        self.messages.append(message)
        if self.start is None:
            self.start = message.time
        if self.end is None or self.end < message.time:
            self.end = message.time


class MessageCache:
    def __init__(self, channel: discord.TextChannel) -> None:
        self.channel = channel
        try:
            # TODO linked list
            self.segments = self.__load()
        except (FileNotFoundError, EOFError):
            self.segments = []

    def __load(self) -> list[Segment]:
        path = os.path.join(CACHE_DIR, f'{self.channel.id}.pkl')
        with open(path, 'rb') as file:
            return pickle.load(file)

    def __commit(self, new_segment: Segment):
        pre = []
        post = []
        l, h = None, None

        for i, segment in enumerate(self.segments):
            # TODO binary search
            if segment.start <= new_segment.start <= segment.end:
                pre.extend([m for m in segment.messages if m.time <= new_segment.start])
                new_segment.start = segment.start
                l, h = l or i, i
            if segment.start <= new_segment.end <= segment.end:
                post.extend(([m for m in segment.messages if m.time >= new_segment.end]))
                new_segment.end = segment.end
                l, h = l or i, i

        new_segment.messages = pre + new_segment.messages + post
        if l and h:
            self.segments = self.segments[:l] + [new_segment] + self.segments[h+1:]
        else:
            self.segments = [new_segment]

        os.makedirs(CACHE_DIR, exist_ok=True)
        path = os.path.join(CACHE_DIR, f'{self.channel.id}.pkl')
        with open(path, 'wb') as file:
            return pickle.dump(self.segments, file)

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]):
        new_segment = Segment(start, end)

        def last_timestamp():
            return start if not new_segment.messages else new_segment.messages[-1].time

        for segment in self.segments:
            # segment ahead of requested interval, skip
            if start and start > segment.end:
                continue

            # fill gap between last retrieved message and start of this interval
            async for m in self.channel.history(limit=None, after=last_timestamp(), before=segment.start, oldest_first=True):
                message = Message(m)
                if end and message.time > end:
                    self.__commit(new_segment)
                    return

                new_segment.add(message)
                yield message

            for message in segment.messages:
                if end and message.time > end:
                    self.__commit(new_segment)
                    return 

                if (start is None) or (message.time >= start):
                    # TODO binary search
                    new_segment.add(message)
                    yield message

        # fill gap between last segment end of requested interval
        async for m in self.channel.history(limit=None, after=last_timestamp(), before=end, oldest_first=True):
            message = Message(m)
            new_segment.add(message)
            yield message

        self.__commit(new_segment)
