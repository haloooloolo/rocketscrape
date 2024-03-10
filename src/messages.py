import os
import copy
import pickle
import asyncio
import logging
import heapq

import discord
import tqdm

from abc import ABC, abstractmethod
from typing import Optional, AsyncIterator, Sequence, Any
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass


@dataclass
class Message:
    def __init__(self, message: discord.Message) -> None:
        self.id: int = message.id
        self.time: datetime = message.created_at
        self.author_id: int = message.author.id
        self.content: str = message.content
        self.reactions: dict[str, set[int]] = {}
        self.__message = message

    async def __async_init(self) -> 'Message':
        async def gather_reactions(_reaction):
            return str(_reaction.emoji), {member.id async for member in _reaction.users()}

        if self.reactions:
            self.reactions.clear()

        reactions = [gather_reactions(r) for r in self.__message.reactions]
        for res in await asyncio.gather(*reactions, return_exceptions=True):
            if isinstance(res, Exception):
                logging.warning(f'Encountered exception while requesting message reaction: {res}')
            else:
                emoji, users = res
                self.reactions[emoji] = users

        return self

    async def refresh(self, channel: discord.TextChannel | discord.Thread) -> None:
        try:
            self.__message = await channel.get_partial_message(self.id).fetch()
        except discord.NotFound:
            logging.warning(f'Failed to refresh message {self.id}, can no longer be found.')
            return

        self.content: str = self.__message.content
        self.reactions.clear()
        await self.__async_init()

    def __await__(self):
        return self.__async_init().__await__()

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        del state['_Message__message']
        return state

    def __setstate__(self, state) -> None:
        self.__dict__.update(state)
        self.__message = None

    def __eq__(self, other) -> bool:
        return self.id == other.id

    def __lt__(self, other) -> bool:
        return self.time < other.time

    def __repr__(self) -> str:
        return f'Message{{{self.author_id} @ {self.time}: "{self.content}"}}'

    def __hash__(self) -> int:
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

            if other_messages and self.messages[-1] == other_messages[-1]:
                # avoid duplicates
                other_messages.pop()

    def __repr__(self) -> str:
        return f'{{{self.start}, {self.end}, [{len(self.messages)}]}}'


class MessageStream(ABC):
    @abstractmethod
    async def get_history(self, start: Optional[datetime], end: Optional[datetime]) -> AsyncIterator[Message]:
        pass


class SingleChannelMessageStream(MessageStream):
    def __init__(self, channel: discord.TextChannel | discord.Thread, cache_dir='cache') -> None:
        self.channel = channel
        self.uncommitted_messages = []
        self.cache_dir = cache_dir
        try:
            self.segments = self.__load()
        except (FileNotFoundError, EOFError):
            self.segments = []

    def __load(self) -> list[_CacheSegment]:
        path = os.path.join(self.cache_dir, f'{self.channel.id}.pkl')
        with open(path, 'rb') as file:
            return pickle.load(file)

    def __commit(self, start: Optional[datetime], end: Optional[datetime]) -> None:
        if not end and not self.uncommitted_messages:
            return

        logging.info(f'Saving {len(self.uncommitted_messages)} new messages from "{self}" to disk...')

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

        os.makedirs(self.cache_dir, exist_ok=True)
        path = os.path.join(self.cache_dir, f'{self.channel.id}.pkl')

        with open(path, 'wb') as file:
            pickle.dump(self.segments, file)

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]) -> AsyncIterator[Message]:
        last_timestamp = start

        async def process_message(_message: Message, from_cache=False) -> None:
            nonlocal last_timestamp
            last_timestamp = _message.time
            now = datetime.now().replace(tzinfo=timezone.utc)

            if not from_cache:
                self.uncommitted_messages.append(_message)
            elif (now - _message.time) < timedelta(days=1):
                await message.refresh(self.channel)
                self.uncommitted_messages.append(_message)

            if len(self.uncommitted_messages) >= 2500:
                self.__commit(start, self.uncommitted_messages[-1].time)

        for segment in copy.copy(self.segments):
            # segment ahead of requested interval, skip
            if start and start > segment.end:
                continue

            # fill gap between last retrieved message and start of this interval
            async for m in self.channel.history(limit=None, after=last_timestamp, before=segment.start, oldest_first=True):
                message = await Message(m)
                await process_message(message)

                if end and message.time > end:
                    self.__commit(start, end)
                    return

                yield message

            for message in segment.messages:
                if end and message.time > end:
                    self.__commit(start, end)
                    return 

                if (start is None) or (message.time >= start):
                    await process_message(message, from_cache=True)
                    yield message

        try:
            # fill gap between last segment end of requested interval
            async for m in self.channel.history(limit=None, after=last_timestamp, before=end, oldest_first=True):
                message = await Message(m)
                await process_message(message)
                yield message

            self.__commit(start, end)
        except discord.Forbidden:
            logging.warning(f'No access to messages in "{self}", ending stream.')
            return

    def __repr__(self) -> str:
        return '# ' + str(self.channel)


class MultiChannelMessageStream(MessageStream):
    def __init__(self, channels: Sequence[discord.TextChannel | discord.Thread]) -> None:
        self.streams = [SingleChannelMessageStream(channel) for channel in channels]

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]) -> AsyncIterator[Message]:
        logging.info('Fetching channel stream heads...')
        heads = []

        for stream in tqdm.tqdm(self.streams):
            iterator = stream.get_history(start, end)
            if head := await anext(iterator, None):
                heads.append((head, iterator))

        heapq.heapify(heads)

        while heads:
            head, iterator = heapq.heappop(heads)
            yield head
            if head := await anext(iterator, None):
                heapq.heappush(heads, (head, iterator))
            else:
                logging.info(f'End of channel stream, {len(heads)} left.')

    def __repr__(self) -> str:
        return f'({", ".join([str(c) for c in self.streams])})'


class ServerMessageStream(MultiChannelMessageStream):
    def __init__(self, guild: discord.Guild) -> None:
        channels = [c for c in guild.channels if isinstance(c, discord.TextChannel)]
        threads = list(guild.threads)
        super().__init__(channels + threads)
        self.__repr = str(guild)

    async def __async_init(self) -> 'ServerMessageStream':
        logging.info('Fetching archived threads...')
        for stream in self.streams:
            if isinstance(stream.channel, discord.TextChannel):
                try:
                    archived_threads = stream.channel.archived_threads(limit=None)
                    self.streams.extend([SingleChannelMessageStream(t) async for t in archived_threads])
                except discord.errors.Forbidden:
                    logging.warning(f'No access to thread list for "{stream}", skipping.')
                    continue

        return self

    def __await__(self):
        return self.__async_init().__await__()

    def __repr__(self) -> str:
        return self.__repr
