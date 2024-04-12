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
from typing import Optional, AsyncIterator, Iterable, Union, Any
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

ChannelType = Union[discord.TextChannel, discord.Thread]
UserIDType = int
MessageIDType = int
ChannelIDType = int


@dataclass
class Message:
    __mention_pattern = re.compile('(?<=<@)[0-9]{18}(?=>)')

    def __init__(self, d_msg: discord.Message) -> None:
        self.id: MessageIDType = d_msg.id
        self.channel_id: ChannelIDType = d_msg.channel.id
        self.author_id: UserIDType = d_msg.author.id
        self.created: datetime = d_msg.created_at
        self.last_edited: Optional[datetime] = d_msg.edited_at
        self.content: str = d_msg.content
        self.reference: Optional[MessageIDType] = d_msg.reference.message_id if d_msg.reference else None
        self.attachments: list[str] = [a.content_type for a in d_msg.attachments if a.content_type]
        self.embeds: list[dict] = [dict(embed.to_dict()) for embed in d_msg.embeds]
        self._reactions: Optional[dict[str, set[UserIDType]]] = None

    @property
    def reactions(self) -> dict[str, set[UserIDType]]:
        return self._reactions or {}

    async def _fetch_reactions(self, d_msg: discord.Message) -> None:
        async def gather_reactions(_reaction: discord.Reaction) -> tuple[str, set[UserIDType]]:
            return str(_reaction.emoji), {member.id async for member in _reaction.users()}

        self._reactions = {}
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
    def mentions(self) -> set[UserIDType]:
        matches = self.__mention_pattern.findall(self.content)
        return {UserIDType(match) for match in matches}

    def __eq__(self, other) -> bool:
        return self.id == other.id

    def __lt__(self, other) -> bool:
        return self.created < other.created

    def __repr__(self) -> str:
        return f'Message{{{self.author_id} @ {self.created}: "{self.content}"}}'

    def __hash__(self):
        return hash(self.id)


@dataclass
class _CacheSegment:
    start: datetime
    end: datetime
    messages: dict[MessageIDType, Message]

    def merge(self, others: list['_CacheSegment']) -> '_CacheSegment':
        start = min(self.start, others[0].start)
        end = max(self.end, others[-1].end)
        messages = {}

        self_messages = list(self.messages.values())[::-1]
        other_messages = sum([list(o.messages.values()) for o in others], [])[::-1]

        while self_messages or other_messages:
            if (not self_messages) or (other_messages and (other_messages[-1].created <= self_messages[-1].created)):
                # bias to other_messages so last inserted will be from self_messages in case of duplicate
                message = other_messages.pop()
            else:
                message = self_messages.pop()

            messages[message.id] = message

        return _CacheSegment(start, end, messages)

    def __repr__(self) -> str:
        return f'{{{self.start}, {self.end}, [{len(self.messages)}]}}'


class _Cache:
    LATEST_VERSION = 1

    def __init__(self, cache_dir: str, channel: ChannelType):
        self.version: int = self.LATEST_VERSION
        self.cache_dir: str = cache_dir
        self.channel_id: ChannelIDType = channel.id
        self.__channel_repr: str = str(channel)
        self.segments: list[_CacheSegment] = []
        self.__uncommitted_messages: dict[MessageIDType, Message] = {}

        cache_path = os.path.join(self.cache_dir, f'{self.channel_id}.pkl')
        try:
            with open(cache_path, 'rb') as file:
                cache = pickle.load(file)
            if self == cache:
                self.segments = cache.segments
            else:
                logging.warning(f'''Found mismatched message cache version for
                    "{self.__channel_repr}", dropping data''')
                os.remove(cache_path)
        except (FileNotFoundError, EOFError):
            pass

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, _Cache):
            return False
        return (self.channel_id == other.channel_id) and (self.version == other.version)

    def __getstate__(self) -> dict[str, Any]:
        attributes = {'version', 'channel_id', 'segments'}
        return {k: v for k, v in self.__dict__.items() if k in attributes}

    def __getitem__(self, message_id: MessageIDType) -> Optional[Message]:
        if message_id in self.__uncommitted_messages:
            return self.__uncommitted_messages[message_id]

        for segment in self.segments:
            if message_id in segment.messages:
                return segment.messages[message_id]

        return None

    def add(self, message: Message) -> int:
        self.__uncommitted_messages[message.id] = message
        return len(self.__uncommitted_messages)

    def commit(self, start: Optional[datetime], end: datetime) -> None:
        if self.__uncommitted_messages:
            num_messages = len(self.__uncommitted_messages)
            channel_name = self.__channel_repr
            logging.info(f'Saving {num_messages} new messages from "{channel_name}" to disk')

        logging.debug(f'start: {start}, end: {end}')
        logging.debug(str(self.segments))

        start = start or datetime.fromtimestamp(0, timezone.utc)
        low, high, successor = None, None, None

        for segment_nr, segment in enumerate(self.segments):
            if end < segment.start:  # new segment precedes this one
                successor = segment_nr
            elif start <= segment.end:  # segments overlap
                low = segment_nr if (low is None) else low
                high = segment_nr

        logging.debug(f'l: {low}, h: {high}, s: {successor}')
        new_segment = _CacheSegment(start, end, self.__uncommitted_messages)
        self.__uncommitted_messages = {}

        if (low is not None) and (high is not None):
            new_segment = new_segment.merge(self.segments[low:(high + 1)])
            self.segments = self.segments[:low] + [new_segment] + self.segments[(high + 1):]
        elif successor is not None:
            self.segments.insert(successor, new_segment)
        else:
            self.segments.append(new_segment)

        os.makedirs(self.cache_dir, exist_ok=True)
        path = os.path.join(self.cache_dir, f'{self.channel_id}.pkl')
        logging.debug(str(self.segments))

        with open(path, 'wb') as file:
            pickle.dump(self, file)


class MessageStream(ABC):
    def get_message(self, message_id: Optional[MessageIDType]) -> Optional[Message]:
        if message_id is None:
            return None
        return self._get_message(message_id)

    @abstractmethod
    def _get_message(self, message_id: MessageIDType) -> Optional[Message]:
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
        self.refresh_window = timedelta(hours=refresh_window)
        self.commit_batch_size = commit_batch_size
        self.__cache = _Cache(cache_dir, channel)

    def _get_message(self, message_id: MessageIDType) -> Optional[Message]:
        return self.__cache[message_id]

    async def __fetch_history(self, start: Optional[datetime],
                              end: Optional[datetime]) -> AsyncIterator[discord.Message]:
        # try to avoid actual fetch if possible
        if start:
            if end and end <= start:
                return
            if isinstance(self.channel, discord.Thread) and self.channel.archived:
                if self.channel.archive_timestamp <= start:
                    return

        async for d_msg in self.channel.history(limit=None, after=start, before=end, oldest_first=True):
            yield d_msg

    async def get_history(self, start: Optional[datetime], end: Optional[datetime],
                          include_reactions=True) -> AsyncIterator[Message]:
        last_timestamp = start

        async def handle_message(_message: Union[Message, discord.Message]) -> Message:
            _d_msg: Optional[discord.Message] = None

            if isinstance(_message, discord.Message):
                _d_msg = _message
                _message = Message(_d_msg)
            else:
                is_stale = (datetime.now(timezone.utc) - _message.created) <= self.refresh_window
                missing_reactions = (_message._reactions is None) and include_reactions
                if is_stale or missing_reactions:
                    _d_msg = await _message.refresh(self.channel)

            if _d_msg:
                if include_reactions:
                    await _message._fetch_reactions(_d_msg)
                if self.__cache.add(_message) >= self.commit_batch_size:
                    self.__cache.commit(start, _message.created)

            nonlocal last_timestamp
            last_timestamp = _message.created

            return _message

        for segment in copy.copy(self.__cache.segments):
            # segment ahead of requested interval, skip
            if start and start > segment.end:
                continue

            # fill gap between last retrieved message and start of this interval
            async for d_msg in self.__fetch_history(last_timestamp, segment.start):
                message = await handle_message(d_msg)
                if end and message.created > end:
                    self.__cache.commit(start, message.created)
                    return

                yield message

            for message in segment.messages.values():
                if end and message.created > end:
                    self.__cache.commit(start, message.created)
                    return

                if (start is None) or (message.created >= start):
                    yield await handle_message(message)

        try:
            # fill gap between last segment end of requested interval
            async for d_msg in self.__fetch_history(last_timestamp, end):
                yield await handle_message(d_msg)

            if last_timestamp is not None:
                if end and end < datetime.now(timezone.utc):
                    self.__cache.commit(start, end)
                else:
                    self.__cache.commit(start, last_timestamp)
        except discord.Forbidden:
            logging.warning(f'No access to messages in "{self}", ending stream')
            return

    def __hash__(self):
        return hash(self.channel)

    def __repr__(self) -> str:
        return str(self.channel)


class MultiChannelMessageStream(MessageStream):
    def __init__(self, channels: Iterable[ChannelType], include_threads: bool, *args) -> None:
        self.streams = {SingleChannelMessageStream(c, *args) for c in channels}
        self.__stream_args = args
        self.__include_threads = include_threads
        suffix = '+' if include_threads else ''
        self.__repr = f'({", ".join([str(s) for s in self.streams])}){suffix}'

    def _get_message(self, message_id: MessageIDType) -> Optional[Message]:
        for stream in self.streams:
            if message := stream.get_message(message_id):
                return message

        return None

    async def __async_init(self) -> 'MultiChannelMessageStream':
        if not self.__include_threads:
            return self

        logging.info('Fetching archived threads')
        for stream in copy.copy(self.streams):
            if not isinstance(stream.channel, discord.TextChannel):
                continue
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
    def __init__(self, guild: discord.Guild, include_threads: bool, *args) -> None:
        channels = [c for c in guild.channels if isinstance(c, discord.TextChannel)]
        super().__init__(channels, include_threads, *args)
        self.__repr = str(guild)

    def __repr__(self) -> str:
        return self.__repr
