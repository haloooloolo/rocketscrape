import asyncio
import logging
import re
import time

import discord
import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from abc import ABC, abstractmethod
from typing import Optional, AsyncIterator, Iterable, Union
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

from rocketscrape.cache import _Cache, _Database

ChannelType = Union[discord.TextChannel, discord.Thread, discord.GroupChannel, discord.DMChannel]
UserIDType = int
MessageIDType = int
ChannelIDType = int

log = logging.getLogger(__name__)

_FETCH_CONCURRENCY = 10
_FETCH_RETRIES = 5
_HISTORY_PAGE_SIZE = 100
_HTTP_REQUESTS_PER_SEC = 7

class _RateLimiter:
    """Async token-bucket-style limiter that paces calls across all callers.

    Each `acquire()` returns once enough time has passed since the last
    acquisition that we stay under `rate_per_sec` overall."""
    def __init__(self, rate_per_sec: float):
        self._interval = 1.0 / rate_per_sec
        self._next_at = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._next_at - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._next_at = max(now, self._next_at) + self._interval


_HTTP_RATE = _RateLimiter(_HTTP_REQUESTS_PER_SEC)


@dataclass
class Message:
    __mention_pattern = re.compile('(?<=<@)[0-9]{18}(?=>)')

    def __init__(self, d_msg: discord.Message) -> None:
        self.id: MessageIDType = d_msg.id
        self.channel_id: ChannelIDType = d_msg.channel.id
        self.author_id: UserIDType = d_msg.author.id
        self.created: datetime = d_msg.created_at
        self.last_edited: Optional[datetime] = d_msg.edited_at
        self.updated: datetime = datetime.now(timezone.utc)
        self.content: str = d_msg.content
        self.reference: Optional[MessageIDType] = (d_msg.reference.message_id if d_msg.reference else None)
        self.attachments: list[str] = [a.url for a in d_msg.attachments]
        self.embeds: list[dict] = [dict(embed.to_dict()) for embed in d_msg.embeds]
        self._reactions: Optional[dict[str, set[UserIDType]]] = None

    @property
    def reactions(self) -> dict[str, set[UserIDType]]:
        return self._reactions or {}

    async def _fetch_reactions(self, d_msg: discord.Message) -> None:
        cached = self._reactions or {}
        new: dict[str, set[UserIDType]] = {}
        # Bind early so a mid-loop exception still leaves _reactions == {}
        # rather than None — keeps the message out of "needs refresh" purgatory
        # on the next run.
        self._reactions = new
        for reaction in d_msg.reactions:
            emoji = str(reaction.emoji)
            prior = cached.get(emoji)
            if prior is not None and reaction.count == len(prior):
                new[emoji] = prior
                continue
            for attempt in range(_FETCH_RETRIES):
                await _HTTP_RATE.acquire()
                try:
                    new[emoji] = {member.id async for member in reaction.users()}
                    break
                except discord.errors.DiscordServerError as exc:
                    delay = 2 ** attempt
                    log.warning(f'Reaction fetch failed ({exc}) — retrying in {delay}s')
                    await asyncio.sleep(delay)
                except discord.errors.HTTPException as exc:
                    log.warning(f'Encountered exception while requesting message reaction: {exc}')
                    break

    def _adopt(self, d_msg: discord.Message) -> None:
        prior_reactions = self._reactions
        self.__dict__.update(Message(d_msg).__dict__)
        self._reactions = prior_reactions

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


class MessageStream(ABC):
    @abstractmethod
    def get_message(self, message_id: MessageIDType) -> Optional[Message]:
        pass

    @abstractmethod
    def get_history(self, start: Optional[datetime], end: Optional[datetime],
                    include_reactions=True, skip_fetch=False) -> AsyncIterator[Message]:
        pass


# Discord rejects snowflakes < 0, so the floor for any `after` we hand to
# channel.history must be at least the Discord epoch (2015-01-01).
_EPOCH = datetime.fromtimestamp(discord.utils.DISCORD_EPOCH / 1000, timezone.utc)


class _FetchExhausted(Exception):
    """Raised by `_iter_history` when retries are exhausted on transient errors,
    so `populate` knows pagination did not actually finish and must commit only
    what was processed (preventing the segment from being extended past the
    real coverage)."""


@dataclass(frozen=True)
class _FetchRange:
    """One contiguous range to paginate through `channel.history`.

    `after` is always a datetime — `_EPOCH` represents "from the channel's
    beginning". `before` is `None` for "to the channel tip"."""
    after: datetime
    before: Optional[datetime]


@dataclass(frozen=True)
class _FetchPlan:
    """Ordered, non-overlapping ranges for `populate` to paginate through.
    An absent plan (None at the call site) means there is no work to do."""
    ranges: list[_FetchRange]


def _merge_ranges(ranges: list[_FetchRange]) -> list[_FetchRange]:
    """Merge overlapping or adjacent ranges, treating `before=None` as +∞."""
    POS_INF = datetime.max.replace(tzinfo=timezone.utc)

    norm = sorted(
        ((r.after, r.before or POS_INF) for r in ranges),
        key=lambda r: r[0],
    )
    merged: list[tuple[datetime, datetime]] = [norm[0]]
    for after, before in norm[1:]:
        last_after, last_before = merged[-1]
        if after <= last_before:
            merged[-1] = (last_after, max(last_before, before))
        else:
            merged.append((after, before))

    return [
        _FetchRange(after=a, before=None if b == POS_INF else b)
        for a, b in merged
    ]


class ChannelMessageStream(MessageStream):
    def __init__(self, channel: ChannelType, database: _Database,
                 refresh_window: int, commit_batch_size: int) -> None:
        self.channel = channel
        self.database = database
        self.refresh_window = timedelta(hours=refresh_window)
        self._cache = _Cache(database, channel, commit_batch_size)

    def get_message(self, message_id: MessageIDType) -> Optional[Message]:
        return self._cache[message_id]

    def needs_fetch(self, start: Optional[datetime], end: Optional[datetime],
                    include_reactions: bool) -> bool:
        return self._fetch_plan(start, end, include_reactions) is not None

    def _fetch_plan(self, start: Optional[datetime], end: Optional[datetime],
                    include_reactions: bool) -> Optional[_FetchPlan]:
        """Returns the pagination plan covering every uncovered or
        needs-refresh sub-range of [start, end], or None if no work is needed."""
        segments = self._cache.segments_in_range(start, end)
        refresh_clusters = self._cache.refresh_clusters(
            start, end, self.refresh_window, include_reactions, _HISTORY_PAGE_SIZE
        )

        raw: list[_FetchRange] = []

        # Gaps between segments inside [start, end]. We track `cursor` as the
        # newest timestamp covered so far. `start=None` (or any pre-Discord
        # date) becomes _EPOCH: discord.py's history(after=epoch) is
        # equivalent to after=None for any real Discord message.
        cursor = max(start, _EPOCH) if start is not None else _EPOCH
        for seg_start, seg_end in segments:
            if seg_start > cursor:
                raw.append(_FetchRange(after=cursor, before=seg_start))
            if seg_end > cursor:
                cursor = seg_end

        # Tail beyond the last segment, if there could be anything new there.
        if self._tail_might_have_new_messages(cursor, end):
            raw.append(_FetchRange(after=cursor, before=end))

        # Refresh clusters: each is a tight range around a contiguous group of
        # stale messages. Bound the range slightly outside the cluster so the
        # boundary messages are actually returned by history(after, before).
        # `_merge_ranges` will coalesce overlapping clusters with the gap/tail
        # ranges automatically.
        delta = timedelta(milliseconds=1)
        for cluster_start, cluster_end in refresh_clusters:
            after = max(cluster_start - delta, _EPOCH)
            raw.append(_FetchRange(after=after, before=cluster_end + delta))

        if not raw:
            return None
        return _FetchPlan(ranges=_merge_ranges(raw))

    def _tail_might_have_new_messages(self, cache_end: Optional[datetime],
                                      end: Optional[datetime]) -> bool:
        """True if there could be messages between cache_end and end that we
        haven't cached. False when we can prove the channel has nothing
        newer than cache_end."""
        if cache_end is None:
            return True  # No segment and no `start` cutoff — must fetch.
        if end is not None and cache_end >= end:
            return False
        if isinstance(self.channel, discord.Thread) and self.channel.archived:
            if self.channel.archive_timestamp <= cache_end:
                return False
        last_id = getattr(self.channel, 'last_message_id', None)
        if last_id is not None and discord.utils.snowflake_time(last_id) <= cache_end:
            return False
        return True

    async def populate(self, start: Optional[datetime], end: Optional[datetime],
                       include_reactions: bool) -> None:
        """Fetch any missing or stale messages in [start, end] into the cache.

        On Forbidden mid-fetch or cancellation, commits whatever we did manage
        to fetch so the cache reflects real coverage (no false-positive segment
        extensions)."""
        plan = self._fetch_plan(start, end, include_reactions)
        if plan is None:
            return

        last_seen: Optional[datetime] = None
        completed = False
        try:
            for fetch_range in plan.ranges:
                async for d_msg in self._iter_history(fetch_range.after, fetch_range.before):
                    await self._absorb(d_msg, start, include_reactions)
                    last_seen = d_msg.created_at
            completed = True
        except discord.Forbidden:
            log.warning(f'No access to messages in "{self}", stopping')
        except _FetchExhausted as exc:
            log.error(f'{exc} — committing partial progress')
            raise
        finally:
            # Always flush whatever's in __uncommitted, regardless of whether
            # we exited via success, Forbidden, _FetchExhausted, Cancel, or an
            # unhandled transport error. This guarantees that messages added
            # to the buffer (with possibly partial reaction state) get
            # persisted, breaking refresh-loop traps.
            if completed:
                self._cache.commit(start, _now_or_min(end, datetime.now(timezone.utc)))
            elif last_seen is not None:
                self._cache.commit(start, last_seen)

    async def _absorb(self, d_msg: discord.Message, start: Optional[datetime],
                      include_reactions: bool) -> None:
        """Update or insert a fetched message in the cache.

        We add the target to the uncommitted buffer *before* fetching reactions
        so a transient failure during reaction fetch doesn't lose the message
        itself — it'll be flushed on the next commit with whatever reaction
        state we managed to populate."""
        cached = self._cache[d_msg.id]
        if cached is not None:
            cached._adopt(d_msg)
            target = cached
        else:
            target = Message(d_msg)
        self._cache.add(target)
        if include_reactions:
            await target._fetch_reactions(d_msg)
        self._cache.commit_maybe(start, target.created)

    async def _iter_history(self, after: Optional[datetime],
                            before: Optional[datetime]) -> AsyncIterator[discord.Message]:
        """Paginate channel.history() through the global rate limiter so the
        IP-wide request rate stays under Discord's user-level cap. Retries
        transient failures and resumes from the last successful message;
        Forbidden propagates to the caller for explicit handling."""
        cursor: Optional[datetime] = after
        for attempt in range(_FETCH_RETRIES):
            try:
                await _HTTP_RATE.acquire()  # before the first page
                yields_in_page = 0
                async for d_msg in self.channel.history(
                        limit=None, after=cursor, before=before, oldest_first=True):
                    cursor = d_msg.created_at
                    yield d_msg
                    yields_in_page += 1
                    if yields_in_page >= _HISTORY_PAGE_SIZE:
                        await _HTTP_RATE.acquire()  # before the next page
                        yields_in_page = 0
                return
            except discord.errors.DiscordServerError as exc:
                delay = 2 ** attempt
                log.warning(f'{exc} — retrying in {delay}s')
                await asyncio.sleep(delay)
            except discord.errors.HTTPException as exc:
                if exc.status != 429:
                    raise
                delay = 60 * (attempt + 1)
                log.warning(f'rate limited ({exc}) — retrying in {delay}s')
                await asyncio.sleep(delay)
        raise _FetchExhausted(f'{self}: giving up after repeated server errors')

    async def get_history(self, start: Optional[datetime], end: Optional[datetime],
                          include_reactions=True, skip_fetch=False) -> AsyncIterator[Message]:
        if not skip_fetch:
            await _fetch_all([self], start, end, include_reactions)
        async for message in _read_all(self.database, [self], start, end):
            yield message

    def __hash__(self):
        return hash(self.channel)

    def __repr__(self) -> str:
        return str(self.channel)


def _now_or_min(a: Optional[datetime], b: datetime) -> datetime:
    """Return whichever of `a`/`b` is earlier, treating None as +infinity."""
    return a if (a is not None and a < b) else b


async def _fetch_all(streams: list[ChannelMessageStream],
                     start: Optional[datetime], end: Optional[datetime],
                     include_reactions: bool) -> None:
    """Phase 1: populate the cache for any channels that need it, in parallel."""
    to_fetch = [s for s in streams if s.needs_fetch(start, end, include_reactions)]

    if not to_fetch:
        log.info(f'All {len(streams)} channels up-to-date, no fetching needed')
        return

    log.info(f'Fetching missing messages for {len(to_fetch)} of {len(streams)} channels')

    sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

    async def fetch_one(stream: ChannelMessageStream) -> None:
        async with sem:
            await stream.populate(start, end, include_reactions)

    database = to_fetch[0].database
    cached_before = database.messages_committed
    started = time.monotonic()
    with tqdm.tqdm(total=len(to_fetch), desc='Fetching channels') as bar, logging_redirect_tqdm():
        tasks = [asyncio.create_task(fetch_one(s)) for s in to_fetch]
        try:
            for coro in asyncio.as_completed(tasks):
                await coro
                bar.update(1)
        except BaseException:
            # Any error in phase 1 — cancellation, _FetchExhausted, transport
            # failure — stops the run. Cancel siblings so they commit partial
            # progress, then re-raise.
            bar.close()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    saved = database.messages_committed - cached_before
    log.info(f'Saved {saved:,} messages in {time.monotonic() - started:.1f}s')


async def _read_all(database: _Database, streams: list[ChannelMessageStream],
                    start: Optional[datetime],
                    end: Optional[datetime]) -> AsyncIterator[Message]:
    """Phase 2: yield all cached messages in [start, end] in chronological order."""
    channel_ids = [s.channel.id for s in streams]
    total = database.count_messages(channel_ids, start, end)
    log.info(f'Processing {total:,} messages')

    started = time.monotonic()
    with tqdm.tqdm(total=total, desc='Processing') as bar, logging_redirect_tqdm():
        for i, message in enumerate(database.iter_messages(channel_ids, start, end)):
            bar.update(1)
            yield message
            # Force a real event-loop iteration periodically so SIGINT can
            # be delivered. Without this, the tight async-generator chain
            # never goes idle long enough for asyncio's signal pipe to drain.
            if i % 1000 == 0:
                await asyncio.sleep(0)
    log.info(f'Processing done in {time.monotonic() - started:.1f}s')


class MultiChannelMessageStream(MessageStream):
    def __init__(self, channels: Iterable[ChannelType], include_threads: bool,
                 database: _Database, *args) -> None:
        self.database = database
        self.streams: set[ChannelMessageStream] = {
            ChannelMessageStream(c, database, *args) for c in channels
        }
        self.__stream_args = (database, *args)
        self.__include_threads = include_threads

        if len(self.streams) == 1:
            base_repr = str(next(iter(self.streams)))
        else:
            base_repr = '(' + ', '.join([str(s) for s in self.streams]) + ')'
        self.__repr = base_repr + ('+🧵' if include_threads else '')

    def get_message(self, message_id: MessageIDType) -> Optional[Message]:
        for stream in self.streams:
            if message := stream.get_message(message_id):
                return message
        return None

    async def _async_init(self) -> 'MultiChannelMessageStream':
        if not self.__include_threads:
            return self

        log.info('Enumerating threads')
        for stream in self.streams.copy():
            if not isinstance(stream.channel, discord.TextChannel):
                continue
            try:
                for thread in stream.channel.threads:
                    self.streams.add(ChannelMessageStream(thread, *self.__stream_args))
                async for thread in stream.channel.archived_threads(limit=None):
                    self.streams.add(ChannelMessageStream(thread, *self.__stream_args))
            except discord.errors.Forbidden:
                log.warning(f'No access to thread list for "{stream}", skipping')
        return self

    def __await__(self):
        return self._async_init().__await__()

    async def get_history(self, start: Optional[datetime], end: Optional[datetime],
                          include_reactions=True, skip_fetch=False) -> AsyncIterator[Message]:
        streams = list(self.streams)
        if not skip_fetch:
            await _fetch_all(streams, start, end, include_reactions)
        async for message in _read_all(self.database, streams, start, end):
            yield message

    def __repr__(self) -> str:
        return self.__repr


class ServerMessageStream(MultiChannelMessageStream):
    def __init__(self, guild: discord.Guild, include_threads: bool,
                 database: _Database, *args) -> None:
        super().__init__([], include_threads, database, *args)
        self.__guild = guild
        self.__stream_args = (database, *args)
        self.__repr = str(guild) + ('+🧵' if include_threads else '')

    async def _async_init(self) -> 'ServerMessageStream':
        log.info(f'Fetching channels for "{self.__guild}"')
        channels = await self.__guild.fetch_channels()
        text_channels = [c for c in channels if isinstance(c, discord.TextChannel)]
        self.streams = {ChannelMessageStream(c, *self.__stream_args) for c in text_channels}
        await super()._async_init()
        return self

    def __repr__(self) -> str:
        return self.__repr


class MultiServerMessageStream(MultiChannelMessageStream):
    def __init__(self, guilds: Iterable[discord.Guild], include_threads: bool,
                 database: _Database, *args) -> None:
        channels = []
        for guild in guilds:
            channels.extend([c for c in guild.channels if isinstance(c, discord.TextChannel)])
        super().__init__(channels, include_threads, database, *args)

        if len(self.streams) == 1:
            base_repr = str(next(iter(guilds)))
        else:
            base_repr = '(' + ', '.join([str(g) for g in guilds]) + ')'
        self.__repr = base_repr + ('+🧵' if include_threads else '')

    def __repr__(self) -> str:
        return self.__repr
