import os
import copy
import pickle
import asyncio
import logging
import heapq
import re

import discord
import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from abc import ABC, abstractmethod
from typing import Optional, AsyncIterator, Iterable, Union
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

ChannelType = Union[discord.TextChannel, discord.Thread]


@dataclass
class Message:
    __mention_pattern = re.compile('(?<=<@)[0-9]{18}(?=>)')

    def __init__(self, d_msg: discord.Message) -> None:
        self.id: int = d_msg.id
        self.time: datetime = d_msg.created_at
        self.author_id: int = d_msg.author.id
        self.content: str = d_msg.content
        self.reference: Optional[int] = d_msg.reference.message_id if d_msg.reference else None
        self.attachments: list[str] = [a.content_type for a in d_msg.attachments if a.content_type]
        self.embeds: dict[str, list[str]] = {}
        self._reactions: Optional[dict[str, set[int]]] = None

        for embed in d_msg.embeds:
            if embed.type and embed.url:
                if embed.type not in self.embeds:
                    self.embeds[embed.type] = []
                self.embeds[embed.type].append(embed.url)

    @property
    def reactions(self) -> dict[str, set[int]]:
        return self._reactions or {}

    async def _fetch_reactions(self, d_msg: discord.Message) -> None:
        self._reactions = {}

        async def gather_reactions(_reaction: discord.Reaction) -> tuple[str, set[int]]:
            return str(_reaction.emoji), {member.id async for member in _reaction.users()}

        reactions = [gather_reactions(r) for r in d_msg.reactions]
        for res in await asyncio.gather(*reactions, return_exceptions=True):
            if isinstance(res, BaseException):
                logging.warning(f'Encountered exception while requesting message reaction: {res}')
            else:
                emoji, users = res
                self._reactions[emoji] = users

    async def refresh(self, channel: ChannelType) -> Optional[discord.Message]:
        try:
            d_msg = await channel.get_partial_message(self.id).fetch()
            self.__dict__.update((Message(d_msg)).__dict__)
            return d_msg
        except discord.NotFound:
            logging.warning(f'Failed to refresh message, ID {self.id} no longer exists')
            return None

    @property
    def mentions(self) -> set[int]:
        matches = Message.__mention_pattern.findall(self.content)
        return {int(match) for match in matches}

    def __eq__(self, other) -> bool:
        return self.id == other.id

    def __lt__(self, other) -> bool:
        return self.time < other.time

    def __repr__(self) -> str:
        return f'Message{{{self.author_id} @ {self.time}: "{self.content}"}}'

    def __hash__(self) -> int:
        return hash(self.id)


@dataclass
class _CacheSegment:
    start: datetime
    end: datetime
    messages: dict[int, Message]

    def merge(self, others: list['_CacheSegment']) -> None:
        self.start = min(self.start, others[0].start)
        self.end = max(self.end, others[-1].end)

        other_messages = sum([list(o.messages.values()) for o in others], [])[::-1]
        self_messages = list(self.messages.values())[::-1]
        self.messages = {}

        while self_messages or other_messages:
            if (not self_messages) or (other_messages and (other_messages[-1].time <= self_messages[-1].time)):
                # bias to other_messages so last inserted will be from self_messages in case of duplicate
                message = other_messages.pop()
            else:
                message = self_messages.pop()

            self.messages[message.id] = message

    def __repr__(self) -> str:
        return f'{{{self.start}, {self.end}, [{len(self.messages)}]}}'


class MessageStream(ABC):
    def get_message(self, message_id: Optional[int]) -> Optional[Message]:
        if message_id is None:
            return None
        return self._get_message(message_id)

    @abstractmethod
    def _get_message(self, message_id: int) -> Optional[Message]:
        pass

    @abstractmethod
    def get_history(self, start: Optional[datetime], end: Optional[datetime],
                    include_reactions=True) -> AsyncIterator[Message]:
        pass


class SingleChannelMessageStream(MessageStream):
    def __init__(self,
                 channel: ChannelType,
                 cache_dir: str,
                 refresh_window: int,
                 commit_batch_size: int) -> None:
        self.channel = channel
        self.uncommitted_messages: dict[int, Message] = {}
        self.cache_dir = cache_dir
        self.refresh_window = timedelta(hours=refresh_window)
        self.commit_batch_size = commit_batch_size
        try:
            self.segments = self.__load()
        except (FileNotFoundError, EOFError):
            self.segments = []

    def __load(self) -> list[_CacheSegment]:
        path = os.path.join(self.cache_dir, f'{self.channel.id}.pkl')
        with open(path, 'rb') as file:
            return pickle.load(file)

    def __commit(self, start: Optional[datetime], end: datetime) -> None:
        if self.uncommitted_messages:
            logging.info(f'Saving {len(self.uncommitted_messages)} new messages from "{self}" to disk')

        logging.debug(f'start: {start}, end: {end}')
        logging.debug(str(self.segments))

        start = start or datetime.fromtimestamp(0).replace(tzinfo=timezone.utc)
        low, high, successor = None, None, None

        for segment_nr, segment in enumerate(self.segments):
            if end < segment.start:
                successor = segment_nr
            elif start <= segment.end:  # segments overlap
                low = segment_nr if (low is None) else low
                high = segment_nr

        logging.debug(f'l: {low}, h: {high}, s: {successor}')
        new_segment = _CacheSegment(start, end, self.uncommitted_messages)
        self.uncommitted_messages = {}

        if (low is not None) and (high is not None):
            new_segment.merge(self.segments[low:high + 1])
            self.segments = self.segments[:low] + [new_segment] + self.segments[high + 1:]
        elif successor is not None:
            self.segments.insert(successor, new_segment)
        else:
            self.segments.append(new_segment)

        os.makedirs(self.cache_dir, exist_ok=True)
        path = os.path.join(self.cache_dir, f'{self.channel.id}.pkl')
        logging.debug(str(self.segments))

        with open(path, 'wb') as file:
            pickle.dump(self.segments, file)

    def _get_message(self, message_id: int) -> Optional[Message]:
        if message_id in self.uncommitted_messages:
            return self.uncommitted_messages[message_id]

        for segment in self.segments:
            if message_id in segment.messages:
                return segment.messages[message_id]

        return None

    async def get_history(self, start: Optional[datetime], end: Optional[datetime],
                          include_reactions=True) -> AsyncIterator[Message]:
        last_timestamp = start

        async def handle_message(_message: Union[Message, discord.Message]) -> Message:
            _d_msg: Optional[discord.Message] = None

            if isinstance(_message, discord.Message):
                _d_msg = _message
                _message = Message(_d_msg)
                self.uncommitted_messages[_message.id] = _message
            else:
                now = datetime.now(timezone.utc)
                is_stale = (now - _message.time) <= self.refresh_window
                missing_reactions = (_message._reactions is None) and include_reactions
                if is_stale or missing_reactions:
                    _d_msg = await _message.refresh(self.channel)
                    self.uncommitted_messages[_message.id] = _message

            if _d_msg and include_reactions:
                await _message._fetch_reactions(_d_msg)

            nonlocal last_timestamp
            last_timestamp = _message.time

            if len(self.uncommitted_messages) >= self.commit_batch_size:
                self.__commit(start, _message.time)

            return _message

        for segment in copy.copy(self.segments):
            # segment ahead of requested interval, skip
            if start and start > segment.end:
                continue

            # fill gap between last retrieved message and start of this interval
            async for d_msg in self.channel.history(limit=None, after=last_timestamp,
                                                    before=segment.start, oldest_first=True):
                message = await handle_message(d_msg)
                if end and message.time > end:
                    self.__commit(start, message.time)
                    return

                yield message

            for message in segment.messages.values():
                if end and message.time > end:
                    self.__commit(start, message.time)
                    return

                if (start is None) or (message.time >= start):
                    yield await handle_message(message)

        try:
            # fill gap between last segment end of requested interval
            async for d_msg in self.channel.history(limit=None, after=last_timestamp, before=end, oldest_first=True):
                yield await handle_message(d_msg)

            if last_timestamp is not None:
                if end and end < datetime.now(timezone.utc):
                    self.__commit(start, end)
                else:
                    self.__commit(start, last_timestamp)
        except discord.Forbidden:
            logging.warning(f'No access to messages in "{self}", ending stream')
            return

    def __hash__(self):
        return hash(self.channel)

    def __repr__(self) -> str:
        return str(self.channel)


class MultiChannelMessageStream(MessageStream):
    def __init__(self, channels: Iterable[ChannelType], include_threads, *args) -> None:
        self.streams = {SingleChannelMessageStream(c, *args) for c in channels}
        self.__stream_args = args
        self.__include_threads = include_threads
        suffix = '+' if include_threads else ''
        self.__repr = f'({", ".join([str(s) for s in self.streams])}){suffix}'

    def _get_message(self, message_id: int) -> Optional[Message]:
        for stream in self.streams:
            if message := stream.get_message(message_id):
                return message

        return None

    async def __async_init(self) -> 'MultiChannelMessageStream':
        if not self.__include_threads:
            return self

        logging.info('Fetching archived threads')
        for stream in copy.copy(self.streams):
            assert isinstance(stream.channel, discord.TextChannel)
            try:
                for thread in stream.channel.threads:
                    self.streams.add(SingleChannelMessageStream(thread, *self.__stream_args))
                async for thread in stream.channel.archived_threads(limit=None):
                    self.streams.add(SingleChannelMessageStream(thread, *self.__stream_args))
            except discord.errors.Forbidden:
                logging.warning(f'No access to thread list for "{stream}", skipping')

        return self

    def __await__(self):
        return self.__async_init().__await__()

    async def get_history(self, start: Optional[datetime], end: Optional[datetime],
                          include_reactions=True) -> AsyncIterator[Message]:
        logging.info('Fetching channel stream heads')
        heads = []

        with logging_redirect_tqdm():
            for stream in tqdm.tqdm(self.streams):
                iterator = stream.get_history(start, end, include_reactions)
                if head := await anext(iterator, None):
                    heads.append((head, iterator))

        heapq.heapify(heads)

        while heads:
            head, iterator = heapq.heappop(heads)
            yield head
            if head := await anext(iterator, None):
                heapq.heappush(heads, (head, iterator))
            else:
                logging.info(f'End of channel stream, {len(heads)} left')

    def __repr__(self) -> str:
        return self.__repr


class ServerMessageStream(MultiChannelMessageStream):
    def __init__(self, guild: discord.Guild, include_threads, *args) -> None:
        channels = [c for c in guild.channels if isinstance(c, discord.TextChannel)]
        super().__init__(channels, include_threads, *args)
        self.__repr = str(guild)

    def __repr__(self) -> str:
        return self.__repr
