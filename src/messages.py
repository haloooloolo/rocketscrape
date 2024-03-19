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
from typing import Optional, AsyncIterator, Iterable, Any, Union
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

ChannelType = Union[discord.TextChannel, discord.Thread]


@dataclass
class Message:
    __mention_pattern = re.compile('(?<=<@)[0-9]{18}(?=>)')

    def __init__(self, message: discord.Message) -> None:
        self.id: int = message.id
        self.time: datetime = message.created_at
        self.author_id: int = message.author.id
        self.content: str = message.content
        self.reference: Optional[int] = message.reference.message_id if message.reference else None
        self.attachments: list[str] = [a.content_type for a in message.attachments if a.content_type]
        self.embeds: dict[str, list[str]] = {}
        self.reactions: dict[str, set[int]] = {}
        self.__message: Optional[discord.Message] = message

        for embed in message.embeds:
            if embed.type and embed.url:
                if embed.type not in self.embeds:
                    self.embeds[embed.type] = []
                self.embeds[embed.type].append(embed.url)

    async def __async_init(self) -> 'Message':
        async def gather_reactions(_reaction: discord.Reaction) -> tuple[str, set[int]]:
            return str(_reaction.emoji), {member.id async for member in _reaction.users()}

        assert self.__message is not None
        reactions = [gather_reactions(r) for r in self.__message.reactions]
        for res in await asyncio.gather(*reactions, return_exceptions=True):
            if isinstance(res, BaseException):
                logging.warning(f'Encountered exception while requesting message reaction: {res}')
            else:
                emoji, users = res
                self.reactions[emoji] = users

        return self

    async def refresh(self, channel: ChannelType) -> None:
        try:
            message = await channel.get_partial_message(self.id).fetch()
            self.__setstate__((await Message(message)).__getstate__())
        except discord.NotFound:
            logging.warning(f'Failed to refresh message, ID {self.id} no longer exists')

    def get_mentions(self) -> set[int]:
        matches = Message.__mention_pattern.findall(self.content)
        return {int(match) for match in matches}

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
        self.messages.clear()

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
    def get_history(self, start: Optional[datetime], end: Optional[datetime]) -> AsyncIterator[Message]:
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

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]) -> AsyncIterator[Message]:
        last_timestamp = start

        async def handle_message(_message: Message, from_cache=False) -> None:
            nonlocal last_timestamp
            last_timestamp = _message.time
            now = datetime.now(timezone.utc)

            if not from_cache:
                self.uncommitted_messages[_message.id] = _message
            elif (now - _message.time) <= self.refresh_window:
                await _message.refresh(self.channel)
                self.uncommitted_messages[_message.id] = _message

            if len(self.uncommitted_messages) >= self.commit_batch_size:
                self.__commit(start, _message.time)

        for segment in copy.copy(self.segments):
            # segment ahead of requested interval, skip
            if start and start > segment.end:
                continue

            # fill gap between last retrieved message and start of this interval
            async for d_msg in self.channel.history(limit=None, after=last_timestamp,
                                                    before=segment.start, oldest_first=True):
                message = await Message(d_msg)
                await handle_message(message)

                if end and message.time > end:
                    self.__commit(start, message.time)
                    return

                yield message

            for message in segment.messages.values():
                if end and message.time > end:
                    self.__commit(start, message.time)
                    return 

                if (start is None) or (message.time >= start):
                    await handle_message(message, from_cache=True)
                    yield message

        try:
            # fill gap between last segment end of requested interval
            async for d_msg in self.channel.history(limit=None, after=last_timestamp, before=end, oldest_first=True):
                message = await Message(d_msg)
                await handle_message(message)
                yield message

            if last_timestamp is not None:
                if end and end < datetime.now(timezone.utc):
                    self.__commit(start, end)
                else:
                    self.__commit(start, last_timestamp)
        except discord.Forbidden:
            logging.warning(f'No access to messages in "{self}", ending stream')
            return

    def __repr__(self) -> str:
        return '# ' + str(self.channel)


class MultiChannelMessageStream(MessageStream):
    def __init__(self, channels: Iterable[ChannelType], *args) -> None:
        self.streams = [SingleChannelMessageStream(channel, *args) for channel in channels]

    def _get_message(self, message_id: int) -> Optional[Message]:
        for stream in self.streams:
            if message := stream.get_message(message_id):
                return message

        return None

    async def get_history(self, start: Optional[datetime], end: Optional[datetime]) -> AsyncIterator[Message]:
        logging.info('Fetching channel stream heads')
        heads = []

        with logging_redirect_tqdm():
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
                logging.info(f'End of channel stream, {len(heads)} left')

    def __repr__(self) -> str:
        return f'({", ".join([str(c) for c in self.streams])})'


class ServerMessageStream(MultiChannelMessageStream):
    def __init__(self, guild: discord.Guild, *args) -> None:
        channels = [c for c in guild.channels if isinstance(c, discord.TextChannel)]
        super().__init__(channels + list(guild.threads), *args)
        self.__stream_args = args
        self.__repr = str(guild)

    async def __async_init(self) -> 'ServerMessageStream':
        logging.info('Fetching archived threads')
        for stream in self.streams:
            if isinstance(stream.channel, discord.TextChannel):
                try:
                    async for t in stream.channel.archived_threads(limit=None):
                        self.streams.append(SingleChannelMessageStream(t, *self.__stream_args))
                except discord.errors.Forbidden:
                    logging.warning(f'No access to thread list for "{stream}", skipping')
                    continue

        return self

    def __await__(self):
        return self.__async_init().__await__()

    def __repr__(self) -> str:
        return self.__repr
