"""DuckDB-backed message cache with shared connection."""
from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Iterable, Iterator, TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from rocketscrape.messages import Message, MessageIDType, ChannelIDType, ChannelType


log = logging.getLogger(__name__)

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id              BIGINT PRIMARY KEY,
    channel_id      BIGINT NOT NULL,
    author_id       BIGINT NOT NULL,
    created_ts      TIMESTAMP NOT NULL,
    last_edited_ts  TIMESTAMP,
    updated_ts      TIMESTAMP NOT NULL,
    reference_id    BIGINT,
    content         TEXT NOT NULL,
    attachments     JSON NOT NULL,
    embeds          JSON NOT NULL,
    reactions       JSON
);
CREATE INDEX IF NOT EXISTS msgs_chan_ts ON messages(channel_id, created_ts);

CREATE TABLE IF NOT EXISTS segments (
    channel_id  BIGINT NOT NULL,
    start_ts    TIMESTAMP NOT NULL,
    end_ts      TIMESTAMP NOT NULL,
    PRIMARY KEY (channel_id, start_ts)
);

CREATE TABLE IF NOT EXISTS _meta (version INTEGER NOT NULL);
"""

_SELECT_COLS = (
    "id, channel_id, author_id, created_ts, last_edited_ts, "
    "updated_ts, reference_id, content, attachments, embeds, reactions"
)


def _open_db(cache_dir: str) -> duckdb.DuckDBPyConnection:
    """Open the DuckDB cache, recreating it if its schema version doesn't match."""
    path = os.path.join(cache_dir, 'cache.duckdb')
    conn = duckdb.connect(path)
    conn.execute("CREATE TABLE IF NOT EXISTS _meta (version INTEGER NOT NULL)")
    row = conn.execute("SELECT version FROM _meta").fetchone()

    if row is None:
        conn.execute(_SCHEMA)
        conn.execute("INSERT INTO _meta VALUES (?)", [SCHEMA_VERSION])
    elif row[0] != SCHEMA_VERSION:
        log.warning(
            f'Cache schema version {row[0]} != {SCHEMA_VERSION}, discarding {path}'
        )
        conn.close()
        os.remove(path)
        conn = duckdb.connect(path)
        conn.execute(_SCHEMA)
        conn.execute("INSERT INTO _meta VALUES (?)", [SCHEMA_VERSION])

    return conn


def _to_db_ts(dt: Optional[datetime]) -> Optional[datetime]:
    """DuckDB TIMESTAMP is naive — strip tzinfo (datetimes are assumed UTC)."""
    return dt.replace(tzinfo=None) if dt is not None else None


def _from_db_ts(dt: Optional[datetime]) -> Optional[datetime]:
    """Reattach UTC tzinfo when reading naive TIMESTAMP back."""
    return dt.replace(tzinfo=timezone.utc) if dt is not None else None


def _ts_range(start: Optional[datetime], end: Optional[datetime]) -> tuple[datetime, datetime]:
    """Default an open-ended timestamp range to [epoch, now] in DuckDB-naive form."""
    return (
        (start or datetime.fromtimestamp(0, timezone.utc)).replace(tzinfo=None),
        (end or datetime.now(timezone.utc)).replace(tzinfo=None),
    )


def _row_to_message(row: tuple) -> Message:
    from rocketscrape.messages import Message
    msg = Message.__new__(Message)
    (msg.id, msg.channel_id, msg.author_id, created, last_edited,
     updated, msg.reference, content, attachments, embeds, reactions) = row
    msg.created = _from_db_ts(created)
    msg.last_edited = _from_db_ts(last_edited)
    msg.updated = _from_db_ts(updated)
    msg.content = content
    msg.attachments = json.loads(attachments)
    msg.embeds = json.loads(embeds)
    msg._reactions = (
        {emoji: set(uids) for emoji, uids in json.loads(reactions).items()}
        if reactions is not None else None
    )
    return msg


def _message_to_row(msg: Message, channel_id: int) -> tuple:
    return (
        msg.id, channel_id, msg.author_id,
        _to_db_ts(msg.created), _to_db_ts(msg.last_edited), _to_db_ts(msg.updated),
        msg.reference, msg.content,
        json.dumps(msg.attachments), json.dumps(msg.embeds),
        json.dumps({e: list(u) for e, u in msg._reactions.items()})
            if msg._reactions is not None else None,
    )


class _Database:
    """Shared DuckDB connection and bulk reader for the run."""
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.conn = _open_db(cache_dir)
        self.messages_committed = 0  # cumulative count for end-of-run reporting

    def count_messages(self, channel_ids: Iterable[int],
                       start: Optional[datetime], end: Optional[datetime]) -> int:
        ids = list(channel_ids)
        if not ids:
            return 0
        placeholders = ",".join(["?"] * len(ids))
        db_start, db_end = _ts_range(start, end)
        return self.conn.execute(
            f"SELECT COUNT(*) FROM messages WHERE channel_id IN ({placeholders}) "
            "AND created_ts BETWEEN ? AND ?",
            [*ids, db_start, db_end],
        ).fetchone()[0]

    def iter_messages(self, channel_ids: Iterable[int],
                      start: Optional[datetime], end: Optional[datetime]) -> Iterator[Message]:
        ids = list(channel_ids)
        if not ids:
            return
        placeholders = ",".join(["?"] * len(ids))
        db_start, db_end = _ts_range(start, end)
        cursor = self.conn.execute(
            f"SELECT {_SELECT_COLS} FROM messages WHERE channel_id IN ({placeholders}) "
            "AND created_ts BETWEEN ? AND ? ORDER BY created_ts",
            [*ids, db_start, db_end],
        )
        while (row := cursor.fetchone()) is not None:
            yield _row_to_message(row)


class _Cache:
    """Per-channel slice of the shared cache: holds the channel id, an
    in-memory write buffer, and per-channel queries against the shared
    DuckDB connection."""
    def __init__(self, database: _Database, channel: ChannelType, max_commit_size: int):
        self.__database = database
        self.channel_id: ChannelIDType = channel.id
        self.__channel_repr = str(channel)
        self.max_commit_size = max_commit_size
        self.__uncommitted: dict[MessageIDType, Message] = {}

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        return self.__database.conn

    def segments_in_range(self, start: Optional[datetime],
                          end: Optional[datetime]) -> list[tuple[datetime, datetime]]:
        """All segments overlapping [start, end] as (start_ts, end_ts) pairs,
        ordered by start_ts. Both columns are NOT NULL in the schema."""
        db_start, db_end = _ts_range(start, end)
        rows = self.conn.execute(
            "SELECT start_ts, end_ts FROM segments WHERE channel_id = ? "
            "AND NOT (end_ts < ? OR start_ts > ?) ORDER BY start_ts",
            [self.channel_id, db_start, db_end],
        ).fetchall()
        return [(s.replace(tzinfo=timezone.utc), e.replace(tzinfo=timezone.utc))
                for s, e in rows]

    def oldest_needing_refresh(self, start: Optional[datetime], end: Optional[datetime],
                               refresh_window: timedelta,
                               include_reactions: bool) -> Optional[datetime]:
        """Oldest created_ts of a cached message in [start, end] that needs refresh.

        A message needs refresh if it was cached within `refresh_window` of its
        last known change (suggesting more edits/reactions may have arrived since)
        or — when reactions are required — if its reactions were never cached."""
        db_start, db_end = _ts_range(start, end)
        window_ms = int(refresh_window.total_seconds() * 1000)
        is_stale = (
            f"DATE_DIFF('millisecond', COALESCE(last_edited_ts, created_ts), updated_ts) "
            f"<= {window_ms}"
        )
        predicates = [is_stale]
        if include_reactions:
            predicates.append("reactions IS NULL")
        where = " OR ".join(f"({p})" for p in predicates)
        row = self.conn.execute(
            f"SELECT MIN(created_ts) FROM messages WHERE channel_id = ? "
            f"AND created_ts BETWEEN ? AND ? AND ({where})",
            [self.channel_id, db_start, db_end],
        ).fetchone()
        return _from_db_ts(row[0]) if row and row[0] is not None else None

    def __getitem__(self, message_id: MessageIDType) -> Optional[Message]:
        if (msg := self.__uncommitted.get(message_id)) is not None:
            return msg
        row = self.conn.execute(
            f"SELECT {_SELECT_COLS} FROM messages WHERE id = ? AND channel_id = ?",
            [message_id, self.channel_id],
        ).fetchone()
        return _row_to_message(row) if row else None

    def __len__(self) -> int:
        return self.conn.execute(
            "SELECT count(*) FROM messages WHERE channel_id = ?",
            [self.channel_id],
        ).fetchone()[0]

    def add(self, message: Message) -> None:
        self.__uncommitted[message.id] = message

    def commit_maybe(self, start: Optional[datetime], end: datetime) -> bool:
        if len(self.__uncommitted) < self.max_commit_size:
            return False
        self.commit(start, end)
        return True

    def commit(self, start: Optional[datetime], end: datetime) -> None:
        n = len(self.__uncommitted)
        if n:
            log.info(f'Saving {n} messages from "{self.__channel_repr}" to disk')

        start = start or datetime.fromtimestamp(0, timezone.utc)
        rows = [_message_to_row(m, self.channel_id) for m in self.__uncommitted.values()]

        self.conn.execute("BEGIN TRANSACTION")
        try:
            if rows:
                self.conn.executemany(
                    "INSERT OR REPLACE INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
            self.__merge_segment(start, end, had_new_messages=bool(rows))
            self.conn.execute("COMMIT")
            self.__database.messages_committed += len(rows)
            self.__uncommitted = {}  # clear only after successful commit
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    def __merge_segment(self, start: datetime, end: datetime, had_new_messages: bool) -> None:
        """Replace any segments overlapping [start, end] with their merged union."""
        db_start, db_end = _to_db_ts(start), _to_db_ts(end)
        overlaps = self.conn.execute(
            "SELECT start_ts, end_ts FROM segments WHERE channel_id = ? "
            "AND NOT (end_ts < ? OR start_ts > ?)",
            [self.channel_id, db_start, db_end],
        ).fetchall()

        if not overlaps and not had_new_messages:
            return  # empty new segment with no overlap, skip

        merged_start = min([db_start] + [s for s, _ in overlaps])
        merged_end = max([db_end] + [e for _, e in overlaps])

        self.conn.execute(
            "DELETE FROM segments WHERE channel_id = ? AND NOT (end_ts < ? OR start_ts > ?)",
            [self.channel_id, db_start, db_end],
        )
        self.conn.execute(
            "INSERT INTO segments VALUES (?, ?, ?)",
            [self.channel_id, merged_start, merged_end],
        )
