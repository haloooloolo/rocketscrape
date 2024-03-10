import os
import copy
import pickle
import discord

from typing import Optional
from datetime import datetime, timezone
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

    def __repr__(self):
        return f'{{{self.start}, {self.end}, [{len(self.messages)}]}}'


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
        l, h, s = None, None, None

        start = start or datetime.fromtimestamp(0).replace(tzinfo=timezone.utc)
        end = end or self.uncommitted_messages[-1].time
        s_front = self.uncommitted_messages[0].time if self.uncommitted_messages else end
        s_back = self.uncommitted_messages[-1].time if self.uncommitted_messages else end

        for i, segment in enumerate(self.segments):
            if end < segment.start:
                s = i
            elif start <= segment.end:  # segments overlap
                pre.extend([m for m in segment.messages if m.time < s_front])
                post.extend(([m for m in segment.messages if m.time > s_back]))
                start = min(start, segment.start)
                end = max(end, segment.end)
                l, h = i if (l is None) else l, i

        new_segment = _Segment(start, end, pre + self.uncommitted_messages + post)
        if (l is not None) and (h is not None):
            self.segments = self.segments[:l] + [new_segment] + self.segments[h+1:]
        elif s is not None:
            self.segments.insert(s, new_segment)
        else:
            self.segments.append(new_segment)

        os.makedirs(CACHE_DIR, exist_ok=True)
        path = os.path.join(CACHE_DIR, f'{self.channel.id}.pkl')
        with open(path, 'wb') as file:
            self.uncommitted_messages.clear()
            return pickle.dump(self.segments, file)

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]):
        last_timestamp = start
        uncached_messages = 0

        def process_message(_message, from_cache=False):
            nonlocal last_timestamp, uncached_messages
            last_timestamp = _message.time
            self.uncommitted_messages.append(_message)

            if not from_cache:
                uncached_messages += 1

            if uncached_messages >= 1000:
                print(f'committing {uncached_messages} new messages to disk...')
                self.__commit(start, self.uncommitted_messages[-1].time)
                uncached_messages = 0

        for segment in copy.copy(self.segments):
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
                    process_message(message, from_cache=True)
                    yield message

        # fill gap between last segment end of requested interval
        async for m in self.channel.history(limit=None, after=last_timestamp, before=end, oldest_first=True):
            message = Message(m)
            process_message(message)
            yield message

        self.__commit(start, end)
