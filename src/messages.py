import os
import copy
import shutil
import pickle
import discord

from abc import ABC, abstractmethod
from typing import Optional, AsyncIterator
from datetime import datetime, timezone
from dataclasses import dataclass

CACHE_DIR = 'cache'


@dataclass
class Message:
    def __init__(self, message: discord.Message) -> None:
        self.id: int = message.id
        self.time: datetime = message.created_at
        self.author: int = message.author.id
        self.content: str = message.content
        self.reactions: dict[str, list[int]] = {}

    async def __load_metadata(self, message: discord.Message) -> None:
        for reaction in message.reactions:
            emoji = str(reaction.emoji)
            users = [member.id async for member in reaction.users()]
            self.reactions[emoji] = users

    def __eq__(self, other) -> bool:
        return self.id == other.id

    @staticmethod
    async def fetch(message: discord.Message) -> 'Message':
        m = Message(message)
        await m.__load_metadata(message)
        return m

    def __repr__(self) -> str:
        return f'Message{{{self.author} @ {self.time}: "{self.content}"}}'

    def __hash__(self):
        return self.id


@dataclass
class _CacheSegment:
    start: datetime
    end: datetime
    messages: list[Message]

    def merge(self, others: list['_CacheSegment']) -> None:
        self.start = min(self.start, others[0].start)
        self.end = max(self.end, others[-1].end)

        other_messages = sum([s.messages for s in others], [])[::-1]
        self_messages = self.messages[::-1]
        self.messages = []

        while self_messages or other_messages:
            if (not other_messages) or (self_messages and (self_messages[-1].time <= other_messages[-1].time)):
                self.messages.append(self_messages.pop())
            else:
                self.messages.append(other_messages.pop())

    def __repr__(self) -> str:
        return f'{{{self.start}, {self.end}, [{len(self.messages)}]}}'


class MessageStream(ABC):
    @abstractmethod
    async def get_history(self, start: Optional[datetime], end: Optional[datetime]) -> AsyncIterator[Message]:
        pass


class SingleChannelMessageStream:
    def __init__(self, channel: discord.TextChannel | discord.Thread) -> None:
        self.channel = channel
        self.uncommitted_messages = []
        try:
            self.segments = self.__load()
        except (FileNotFoundError, EOFError):
            self.segments = []

    def __repr__(self) -> str:
        return '# ' + str(self.channel)

    def __load(self) -> list[_CacheSegment]:
        path = os.path.join(CACHE_DIR, f'{self.channel.id}.pkl')
        with open(path, 'rb') as file:
            return pickle.load(file)

    def __commit(self, start: Optional[datetime], end: Optional[datetime]) -> None:
        if not end and not self.uncommitted_messages:
            return

        start = start or datetime.fromtimestamp(0).replace(tzinfo=timezone.utc)
        end = end or self.uncommitted_messages[-1].time
        low, high, successor = None, None, None

        for segment_nr, segment in enumerate(self.segments):
            if end < segment.start:
                successor = segment_nr
            elif start <= segment.end:  # segments overlap
                low = segment_nr if (low is None) else segment_nr
                high = segment_nr

        new_segment = _CacheSegment(start, end, copy.copy(self.uncommitted_messages))
        self.uncommitted_messages.clear()

        if (low is not None) and (high is not None):
            new_segment.merge(self.segments[low:high + 1])
            self.segments = self.segments[:low] + [new_segment] + self.segments[high + 1:]
        elif successor is not None:
            self.segments.insert(successor, new_segment)
        else:
            self.segments.append(new_segment)

        os.makedirs(CACHE_DIR, exist_ok=True)
        path = os.path.join(CACHE_DIR, f'{self.channel.id}.pkl')
        backup_path = os.path.join(CACHE_DIR, f'{self.channel.id}_backup.pkl')

        if os.path.exists(path):
            shutil.move(path, backup_path)

        with open(path, 'wb') as file:
            pickle.dump(self.segments, file)

        if os.path.exists(backup_path):
            os.remove(backup_path)

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]) -> AsyncIterator[Message]:
        last_timestamp = start

        def process_message(_message: Message, from_cache=False) -> None:
            nonlocal last_timestamp
            last_timestamp = _message.time

            if from_cache:
                return

            self.uncommitted_messages.append(_message)
            if len(self.uncommitted_messages) >= 10_000:
                print(f'committing {len(self.uncommitted_messages)} new messages to disk...')
                self.__commit(start, self.uncommitted_messages[-1].time)

        for segment in copy.copy(self.segments):
            # segment ahead of requested interval, skip
            if start and start > segment.end:
                continue

            # fill gap between last retrieved message and start of this interval
            async for m in self.channel.history(limit=None, after=last_timestamp, before=segment.start, oldest_first=True):
                message = await Message.fetch(m)
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

        try:
            # fill gap between last segment end of requested interval
            async for m in self.channel.history(limit=None, after=last_timestamp, before=end, oldest_first=True):
                message = await Message.fetch(m)
                process_message(message)
                yield message

            self.__commit(start, end)
        except discord.Forbidden:
            return


class MultiChannelMessageStream(MessageStream):
    def __init__(self, channels: list[discord.TextChannel | discord.Thread]) -> None:
        self.channels = [SingleChannelMessageStream(channel) for channel in channels]

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]) -> AsyncIterator[Message]:
        heads = {}

        for channel in self.channels:
            iterator = channel.get_history(start, end)
            if head := await anext(iterator, None):
                heads[iterator] = head

        while heads:
            candidate = None
            for iterator, head in heads.items():
                if (not candidate) or (head.time < heads[candidate].time):
                    candidate = iterator

            yield heads[candidate]
            heads[candidate] = await anext(candidate, None)
            if not heads[candidate]:
                del heads[candidate]

    def __repr__(self) -> str:
        return f'({", ".join([str(c) for c in self.channels])})'


class ServerMessageStream(MultiChannelMessageStream):
    def __init__(self, guild: discord.Guild) -> None:
        channels = [c for c in guild.channels if isinstance(c, discord.TextChannel)]
        threads = list(guild.threads)
        super().__init__(channels + threads)
        self.guild = guild

    def __repr__(self) -> str:
        return str(self.guild)
