import os
import pickle
import discord

from typing import Optional
from datetime import datetime
from dataclasses import dataclass

CACHE_DIR = 'cache'


class Message:
    def __init__(self, message: discord.Message) -> None:
        self.time = message.created_at
        self.author = message.author.name
        self.content = message.content

    def __repr__(self) -> str:
        return f'Message{{{self.author} @ {self.time}: "{self.content}"}}'


@dataclass
class _Segment:
    start: datetime
    end: datetime
    messages: list[Message]


class MessageCache:
    def __init__(self, channel: discord.TextChannel) -> None:
        self.channel = channel
        self.uncommitted_messages = []
        try:
            self.segments = self.__load()
        except (FileNotFoundError, EOFError):
            self.segments = []

    def __load(self) -> list[_Segment]:
        path = os.path.join(CACHE_DIR, f'{self.channel.id}.pkl')
        with open(path, 'rb') as file:
            return pickle.load(file)

    def __commit(self, start: Optional[datetime], end: Optional[datetime]):
        pre = []
        post = []
        l, h = None, None

        if not self.uncommitted_messages:
            return

        start = start or self.uncommitted_messages[0].time
        end = end or self.uncommitted_messages[-1].time
        new_segment = _Segment(start, end, self.uncommitted_messages)

        for i, segment in enumerate(self.segments):
            if segment.start <= new_segment.start <= segment.end:
                pre.extend([m for m in segment.messages if m.time < new_segment.messages[0].time])
                new_segment.start = segment.start
                l, h = l or i, i
            if segment.start <= new_segment.end <= segment.end:
                post.extend(([m for m in segment.messages if m.time > new_segment.messages[-1].time]))
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
            self.uncommitted_messages.clear()
            return pickle.dump(self.segments, file)

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]):
        last_timestamp = start

        def process_message(_message):
            nonlocal last_timestamp
            last_timestamp = _message.time

            self.uncommitted_messages.append(_message)
            if len(self.uncommitted_messages) >= 100:
                self.__commit(start, self.uncommitted_messages[-1].time)

        for segment in self.segments:
            # segment ahead of requested interval, skip
            if start and start > segment.end:
                continue

            # fill gap between last retrieved message and start of this interval
            async for m in self.channel.history(limit=None, after=last_timestamp, before=segment.start, oldest_first=True):
                message = Message(m)
                if end and message.time > end:
                    self.__commit(start, end)
                    return

                process_message(message)
                yield message

            for message in segment.messages:
                if end and message.time > end:
                    self.__commit(start, end)
                    return 

                if (start is None) or (message.time >= start):
                    process_message(message)
                    yield message

        # fill gap between last segment end of requested interval
        async for m in self.channel.history(limit=None, after=last_timestamp, before=end, oldest_first=True):
            message = Message(m)
            process_message(message)
            yield message

        self.__commit(start, end)
