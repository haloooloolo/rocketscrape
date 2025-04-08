import logging
import heapq
import re
import json

import discord

import matplotlib.pyplot as plt
import numpy as np

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Any, Generic, TypeVar, Union, Callable, Awaitable, cast
from datetime import datetime, timedelta, date
from tabulate import tabulate

from rocketscrape.client import Client
from rocketscrape.utils import (
    Server,
    Role,
    sanitize_str
)
from rocketscrape.messages import (
    MessageStream,
    Message,
    UserIDType,
    ChannelIDType
)

T = TypeVar('T')


@dataclass(frozen=True)
class CustomArgument(ABC):
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def __hash__(self):
        return hash(self.args)


class CustomPositionalArgument(CustomArgument):
    def __init__(self, _name: str, _type: type[Any], _help: Optional[str] = None):
        args = _name,
        kwargs = dict(type=_type, help=_help)
        super().__init__(args, kwargs)


class CustomFlag(CustomArgument):
    def __init__(self, _name: str, _help: Optional[str] = None):
        args = f'--{_name}',
        kwargs = dict(action='store_true', help=_help)
        super().__init__(args, kwargs)


class CustomOption(CustomArgument):
    def __init__(self, _name: str, _type: type[Any], _default: Any, _help: Optional[str] = None):
        args = f'--{_name}',
        kwargs = dict(type=_type, default=_default, help=_help)
        super().__init__(args, kwargs)


class CustomList(CustomArgument):
    def __init__(self, _name: str, _type: type[Any], _help: Optional[str] = None):
        args = f'--{_name}',
        kwargs = dict(nargs='+', type=_type, help=_help)
        super().__init__(args, kwargs)


@dataclass(frozen=True)
class Result(Generic[T]):
    start: Optional[datetime]
    end: Optional[datetime]
    data: T
    __display: Callable[['Result[T]', Client, int], Awaitable[None]]

    async def display(self, client: Client, max_results: int):
        await self.__display(self, client, max_results)


class MessageAnalysis(ABC, Generic[T]):
    def __init__(self, stream: MessageStream, args):
        self.stream = stream
        self.log_interval = timedelta(seconds=args.log_interval)
        self.user_filter = set(args.user_filter) if args.user_filter else None

    async def run(self, start: Optional[datetime], end: Optional[datetime]) -> Result[T]:
        assert (start is None) or (end is None) or (end > start)
        last_ts = datetime.now()
        self._prepare()

        async for message in self.stream.get_history(start, end, self._require_reactions):
            ts = datetime.now()
            if (ts - last_ts) >= self.log_interval:
                logging.info(f'Message stream reached {message.created}')
                last_ts = ts

            if self.user_filter and (message.author_id not in self.user_filter):
                continue

            self._on_message(message)

        return Result(start, end, self._finalize(), self._display_result)

    @property
    @abstractmethod
    def _require_reactions(self) -> bool:
        pass

    @abstractmethod
    def _prepare(self) -> None:
        pass

    @abstractmethod
    def _on_message(self, message: Message) -> None:
        pass

    @abstractmethod
    def _finalize(self) -> T:
        pass

    @staticmethod
    def _get_date_range_str(start: Optional[datetime], end: Optional[datetime]) -> str:
        if start and end:
            range_str = f'from {start} to {end}'
        elif start:
            range_str = f'since {start}'
        elif end:
            range_str = f'up to {end}'
        else:
            range_str = '(all time)'

        return range_str

    @abstractmethod
    async def _display_result(self, result: Result[T], client: Client, max_results: int) -> None:
        pass

    @staticmethod
    @abstractmethod
    def subcommand() -> str:
        pass

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return set()


class CountBasedMessageAnalysis(MessageAnalysis[dict[UserIDType, int]]):
    def _prepare(self) -> None:
        self.count: dict[UserIDType, int] = {}

    @abstractmethod
    def _on_message(self, message: Message) -> None:
        pass

    def _finalize(self) -> dict[UserIDType, int]:
        return self.count

    @abstractmethod
    def _title(self) -> str:
        pass

    async def _display_result(self, result: Result[dict[UserIDType, int]], client: Client, max_results: int) -> None:
        range_str = self._get_date_range_str(result.start, result.end)
        top_users = heapq.nlargest(max_results, result.data.items(), key=lambda a: a[1])

        print(f'{self._title()} {range_str}')
        for i, (user_id, count) in enumerate(top_users):
            print(f'{i + 1}. {await client.try_fetch_username(user_id)}: {count}')


S = TypeVar('S', bound=MessageAnalysis)


class HistoryBasedMessageAnalysis(MessageAnalysis[tuple[list[datetime], list[T]]], Generic[S, T]):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self._base_analysis = self._base_analysis_class()(stream, args)
        self.interval = timedelta(days=1)

    @classmethod
    @abstractmethod
    def _base_analysis_class(cls) -> type[S]:
        pass

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return cls._base_analysis_class().custom_args()

    @property
    def _require_reactions(self) -> bool:
        return self._base_analysis._require_reactions

    def _prepare(self) -> None:
        self._base_analysis._prepare()
        self.next_date: Optional[datetime] = None
        self.last_ts: Optional[datetime] = None
        self.x: list[datetime] = []
        self.y: list[T] = []

    def __add_data_point(self, dt: datetime) -> None:
        self.x.append(dt)
        self.y.append(self._get_data())

    @abstractmethod
    def _get_data(self) -> T:
        pass

    def _on_message(self, message: Message) -> None:
        self._base_analysis._on_message(message)
        self.last_ts = message.created

        if self.next_date is None:
            self.next_date = message.created
        elif message.created < self.next_date:
            return

        self.__add_data_point(message.created)
        self.next_date += self.interval

    def _finalize(self) -> tuple[list[datetime], list[T]]:
        self._base_analysis._finalize()
        if self.last_ts:
            self.__add_data_point(self.last_ts)

        return self.x, self.y

    @abstractmethod
    async def _display_result(
            self,
            result: Result[tuple[list[datetime], list[T]]],
            client: Client,
            max_results: int
    ) -> None:
        pass


class TopContributorAnalysis(MessageAnalysis[dict[UserIDType, float]]):
    def __init__(self, stream, args):
        super().__init__(stream, args)
        self.base_session_time: float = args.base_session_time
        self.session_timeout: float = args.session_timeout

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return MessageAnalysis.custom_args() | {
            CustomOption('base-session-time', float, 3.0,
                         'added time (in minutes) to account for activity immediately before and after a session'),
            CustomOption('session-timeout', float, 15.0,
                         'maximum amount of time (in minutes) between two messages in the same session'),
        }

    @property
    def _require_reactions(self) -> bool:
        return False

    @staticmethod
    def __to_minutes(td: timedelta) -> float:
        return td / timedelta(minutes=1)

    def _prepare(self) -> None:
        self.open_sessions: dict[UserIDType, tuple[datetime, datetime]] = {}
        self.total_time: dict[UserIDType, float] = {}

    def _get_session_time(self, start: datetime, end: datetime) -> float:
        return self.__to_minutes(end - start) + self.base_session_time

    def _close_session(self, author_id: UserIDType, start: datetime, end: datetime) -> None:
        session_time = self._get_session_time(start, end)
        self.total_time[author_id] = self.total_time.get(author_id, 0.0) + session_time

    def _on_message(self, message: Message) -> None:
        timestamp = message.created
        author_id = message.author_id

        session_start, session_end = self.open_sessions.get(author_id, (timestamp, timestamp))
        if self.__to_minutes(timestamp - session_end) < self.session_timeout:
            # extend session to current timestamp
            self.open_sessions[author_id] = (session_start, timestamp)
        else:
            self._close_session(author_id, session_start, session_end)
            del self.open_sessions[author_id]

    def _finalize(self) -> dict[UserIDType, float]:
        # end remaining sessions
        for author_id, (session_start, session_end) in self.open_sessions.items():
            self._close_session(author_id, session_start, session_end)

        return self.total_time

    async def _display_result(self, result: Result[dict[UserIDType, float]], client: Client, max_results: int) -> None:
        range_str = self._get_date_range_str(result.start, result.end)
        top_contributors = heapq.nlargest(max_results, result.data.items(), key=lambda a: a[1])

        print(f'Top {self.stream} contributors {range_str}')
        for i, (user_id, time) in enumerate(top_contributors):
            time_mins = round(time)
            hours, minutes = time_mins // 60, time_mins % 60
            print(f'{i + 1}. {await client.try_fetch_username(user_id)}: {hours}h {minutes}m')

    @staticmethod
    def subcommand() -> str:
        return 'contributors'


class ContributorHistoryAnalysis(HistoryBasedMessageAnalysis[TopContributorAnalysis, dict[UserIDType, float]]):
    @classmethod
    def _base_analysis_class(cls) -> type[TopContributorAnalysis]:
        return TopContributorAnalysis

    def _get_data(self) -> dict[UserIDType, float]:
        return self._base_analysis.total_time.copy()

    async def _display_result(self, result: Result[tuple[list[datetime], list[dict[UserIDType, float]]]],
                              client: Client, max_results: int) -> None:
        x, y = result.data

        times_by_user: dict[UserIDType, list[float]] = {}
        for i, snapshot in enumerate(y):
            for author, time_min in snapshot.items():
                if author not in times_by_user:
                    times_by_user[author] = [0.0] * i
                times_by_user[author].append(time_min)

        for user_id, data in sorted(times_by_user.items(), key=lambda a: a[1][-1], reverse=True)[:max_results]:
            username = await client.try_fetch_username(user_id)
            plt.plot(np.array(x), np.array(data), label=sanitize_str(username))

        plt.xticks(rotation=30, ha='right')
        plt.ylabel('time (mins)')
        plt.legend()
        plt.title(sanitize_str(f'Top {self.stream} contributors over time'))
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'contributor-history'


class MessageCountAnalysis(CountBasedMessageAnalysis):
    def _on_message(self, message: Message) -> None:
        self.count[message.author_id] = self.count.get(message.author_id, 0) + 1

    @property
    def _require_reactions(self) -> bool:
        return False

    def _title(self) -> str:
        return f'Top {self.stream} contributors by message count'

    @staticmethod
    def subcommand() -> str:
        return 'message-count'


class SelfKekAnalysis(CountBasedMessageAnalysis):
    @property
    def _require_reactions(self) -> bool:
        return True

    def _on_message(self, message: Message) -> None:
        for emoji_name, users in message.reactions.items():
            if ('kek' in emoji_name) and (message.author_id in users):
                self.count[message.author_id] = self.count.get(message.author_id, 0) + 1

    def _title(self) -> str:
        return f'Top {self.stream} self kek offenders'

    @staticmethod
    def subcommand() -> str:
        return 'self-kek'


class MissingPersonAnalysis(TopContributorAnalysis):
    def __init__(self, stream, args):
        super().__init__(stream, args)
        self.inactivity_threshold = timedelta(days=args.inactivity_threshold)

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return TopContributorAnalysis.custom_args() | {
            CustomOption('inactivity_threshold', int, 90,
                         'number of days without activity required to be considered inactive'),
        }

    @property
    def _require_reactions(self) -> bool:
        return False

    def _prepare(self) -> None:
        super()._prepare()
        self.last_seen: dict[UserIDType, datetime] = {}
        self.last_ts: Optional[datetime] = None

    def _on_message(self, message: Message) -> None:
        super()._on_message(message)
        self.last_ts = message.created
        self.last_seen[message.author_id] = message.created

    def _finalize(self) -> dict[UserIDType, float]:
        total_time = super()._finalize()
        for author_id, ts in self.last_seen.items():
            last_ts = cast(datetime, self.last_ts)
            if (last_ts - ts) < self.inactivity_threshold:
                del total_time[author_id]

        return total_time

    async def _display_result(self, result: Result[dict[UserIDType, float]], client: Client, max_results: int) -> None:
        top_contributors = heapq.nlargest(max_results, result.data.items(), key=lambda a: a[1])

        print(f'Top {self.stream} contributors with no recent activity ')
        for i, (author_id, time) in enumerate(top_contributors):
            time_mins = round(time)
            hours, minutes = time_mins // 60, time_mins % 60
            print(f'{i + 1}. {await client.try_fetch_username(author_id)}: {hours}h {minutes}m')

    @staticmethod
    def subcommand() -> str:
        return 'missing-persons'


class ReactionsGivenAnalysis(CountBasedMessageAnalysis):
    @property
    def _require_reactions(self) -> bool:
        return True

    def _on_message(self, message: Message) -> None:
        for emoji_name, users in message.reactions.items():
            for user_id in users:
                self.count[user_id] = self.count.get(user_id, 0) + 1

    def _title(self) -> str:
        return f'{self.stream} members with most reactions given'

    @staticmethod
    def subcommand() -> str:
        return 'total-reactions-given'


class ReactionsReceivedAnalysis(CountBasedMessageAnalysis):
    @property
    def _require_reactions(self) -> bool:
        return True

    def _on_message(self, message: Message) -> None:
        for emoji_name, users in message.reactions.items():
            self.count[message.author_id] = self.count.get(message.author_id, 0) + len(users)

    def _title(self) -> str:
        return f'{self.stream} members with most reactions received'

    @staticmethod
    def subcommand() -> str:
        return 'total-reactions-received'


class ThankYouCountAnalysis(CountBasedMessageAnalysis):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.__pattern = re.compile('\b(ty)|(thank(?:s| (?:yo)?u)?)|(thx)\b')

    @property
    def _require_reactions(self) -> bool:
        return False

    def _on_message(self, message: Message) -> None:
        content = message.content.lower()
        if not self.__pattern.search(content):
            return

        mentions: set[UserIDType] = message.mentions
        if message.reference:
            if replied_to := self.stream.get_message(message.reference):
                mentions.add(replied_to.author_id)

        for user_id in mentions:
            self.count[user_id] = self.count.get(user_id, 0) + 1

    def _title(self) -> str:
        return f'{self.stream} members thanked most often'

    @staticmethod
    def subcommand() -> str:
        return 'thank-count'


class KronScoreAnalysis(CountBasedMessageAnalysis):
    @property
    def _require_reactions(self) -> bool:
        return False

    def _on_message(self, message: Message) -> None:
        if message.author_id != 118923557265735680:
            return

        if "thanks for reporting" not in message.content.lower():
            return

        for user_id in message.mentions:
            self.count[user_id] = self.count.get(user_id, 0) + 1

    def _title(self) -> str:
        return f'{self.stream} Kron score'

    @staticmethod
    def subcommand() -> str:
        return 'kron-score'


class ReactionReceivedAnalysis(CountBasedMessageAnalysis):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.emoji = args.react

    @property
    def _require_reactions(self) -> bool:
        return True

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return CountBasedMessageAnalysis.custom_args() | {
            CustomPositionalArgument('react', str, 'emoji to count received reactions for'),
        }

    def _on_message(self, message: Message) -> None:
        if self.emoji in message.reactions:
            num_reactions = len(message.reactions[self.emoji])
            self.count[message.author_id] = self.count.get(message.author_id, 0) + num_reactions

    def _title(self) -> str:
        return f'{self.stream} members by {self.emoji} received'

    @staticmethod
    def subcommand() -> str:
        return 'reaction-received-count'


class ReactionGivenAnalysis(CountBasedMessageAnalysis):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.emoji = args.react

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return CountBasedMessageAnalysis.custom_args() | {
            CustomPositionalArgument('react', str, 'emoji to count given reactions for'),
        }

    @property
    def _require_reactions(self) -> bool:
        return True

    def _on_message(self, message: Message) -> None:
        for user in message.reactions.get(self.emoji, []):
            self.count[user] = self.count.get(user, 0) + 1

    def _title(self) -> str:
        return f'{self.stream} members by {self.emoji} given'

    @staticmethod
    def subcommand() -> str:
        return 'reaction-given-count'


class ActivityTimeAnalyis(MessageAnalysis[dict[str, list[int]]]):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.user_id: UserIDType = args.user
        self.num_buckets: int = args.num_buckets
        assert (24 * 60 % self.num_buckets) == 0, \
            'bucket count doesn\'t cleanly divide into minutes'
        self.key_format: str = args.key_format

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return MessageAnalysis.custom_args() | {
            CustomPositionalArgument('user', UserIDType),
            CustomOption('num-buckets', int, 24),
            CustomOption('key-format', str, '',
                         'split messages based on time, supports $y, $q, $m, $d (e.g. Q$q $y)'),
        }

    @property
    def _require_reactions(self) -> bool:
        return False

    def _prepare(self) -> None:
        self.buckets: dict[str, list[int]] = {}

    def _get_key(self, timestamp: datetime) -> str:
        return self.key_format \
            .replace('$y', str(timestamp.year)) \
            .replace('$q', str(int((timestamp.month + 2) / 3))) \
            .replace('$m', str(timestamp.month)) \
            .replace('$d', str(timestamp.day))

    def _on_message(self, message: Message) -> None:
        if message.author_id != self.user_id:
            return

        timestamp = message.created.astimezone()
        key = self._get_key(timestamp)
        if key not in self.buckets:
            self.buckets[key] = [0] * self.num_buckets

        bucket = int((60 * timestamp.hour + timestamp.minute) / (24 * 60 / self.num_buckets))
        self.buckets[key][bucket] += 1

    def _finalize(self) -> dict[str, list[int]]:
        return self.buckets

    async def _display_result(self, result: Result[dict[str, list[int]]], client: Client, max_results: int) -> None:
        x = np.arange(self.num_buckets)

        labels = []
        bucket_width = int(24 * 60 / self.num_buckets)
        for i in x:
            start = i * bucket_width
            end = start + bucket_width - 1
            start_fmt = f'{(start // 60):02}:{(start % 60):02}'
            end_fmt = f'{(end // 60):02}:{(end % 60):02}'
            labels.append(f'{start_fmt} - {end_fmt}')

        full_width = 0.75
        bar_width = full_width / len(result.data)
        offset = (bar_width - full_width) / 2
        for key, buckets in result.data.items():
            plt.bar(x + offset, buckets, bar_width, label=key)
            offset += bar_width

        plt.xticks(x, labels, rotation=90)
        plt.xlim(-1, self.num_buckets)
        plt.ylabel('message count')

        if self.key_format:
            plt.legend(loc='upper left')

        username = await client.try_fetch_username(self.user_id)
        title = f'{username} message activity in {self.stream} by local time'
        plt.title(sanitize_str(title))

        plt.subplots_adjust(bottom=0.25)
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'message-time-histogram'


class WordCountAnalysis(CountBasedMessageAnalysis):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.word: str = args.word
        self.ignore_case = args.ignore_case
        self.message_based = args.message_based
        self.total_count = {}

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return MessageAnalysis.custom_args() | {
            CustomPositionalArgument('word', str),
            CustomFlag('ignore-case', 'make word match case-insensitive'),
            CustomFlag('message-based', 'only count the first occurence in each message'),
        }

    @property
    def _require_reactions(self) -> bool:
        return False

    def _title(self) -> str:
        return f"Number of \"{self.word}\" occurrences in {self.stream}"

    def _on_message(self, message: Message) -> None:
        word, content = self.word, message.content
        if self.ignore_case:
            word, content = word.lower(), content.lower()

        if word in content:
            author_id = message.author_id
            self.count[author_id] = self.count.get(author_id, 0) + 1

    @staticmethod
    def subcommand() -> str:
        return 'word-count'


class WordCountHistoryAnalysis(HistoryBasedMessageAnalysis[WordCountAnalysis, dict[UserIDType, int]]):
    @classmethod
    def _base_analysis_class(cls) -> type[WordCountAnalysis]:
        return WordCountAnalysis

    def _get_data(self) -> dict[UserIDType, int]:
        return self._base_analysis.count

    async def _display_result(
            self,
            result: Result[tuple[list[datetime], list[dict[UserIDType, int]]]],
            client: Client,
            max_results: int
    ) -> None:
        x, y = result.data
        plt.plot(np.array(x), np.array(y))
        plt.xticks(rotation=30, ha='right')
        title = f'Occurences of "{self._base_analysis.word}" in {self.stream} over time'
        plt.title(sanitize_str(title))
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'word-count-history'


class SupportBountyAnalysis(MessageAnalysis[dict[tuple[int, int], dict[UserIDType, float]]]):
    class __SupportBountyHelper(TopContributorAnalysis):
        def _prepare(self) -> None:
            super()._prepare()
            self.time_by_month: dict[tuple[int, int], dict[UserIDType, float]] = {}

        def _close_session(self, author_id: UserIDType, start: datetime, end: datetime) -> None:
            session_time = self._get_session_time(start, end)
            month = (end.month, end.year)
            if month not in self.time_by_month:
                self.time_by_month[month] = {}
            self.time_by_month[month][author_id] = self.time_by_month[month].get(author_id, 0) + session_time

        @staticmethod
        def subcommand() -> str:
            return ''

    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.min_monthly_activity = args.min_monthly_activity
        self.__helper = self.__SupportBountyHelper(stream, args)

    @property
    def _require_reactions(self) -> bool:
        return False

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return cls.__SupportBountyHelper.custom_args() | {
            CustomOption('min-monthly-activity', int, 60,
                         'minimum required activity per month in minutes'),
        }

    def _prepare(self) -> None:
        self.__helper._prepare()

    def _on_message(self, message: Message) -> None:
        self.__helper._on_message(message)

    def _finalize(self) -> dict[tuple[int, int], dict[UserIDType, float]]:
        self.__helper._finalize()
        return self.__helper.time_by_month

    async def _display_result(self, result: Result[dict[tuple[int, int], dict[UserIDType, float]]],
                              client: Client, max_results: int) -> None:
        exclusion_list: set[UserIDType] = set()

        role_id, server_id = Role.rpcore.value
        if core_team_role := await client.try_fetch_role(role_id, server_id):
            team_members = {member.id for member in await core_team_role.fetch_members()}
            exclusion_list.update(team_members)
        else:
            logging.warning(f'Could not fetch Rocket Pool team members (role id {role_id})')

        def user_eligible(_user) -> bool:
            if _user is None:
                return True
            return not (_user.bot or (_user.id in exclusion_list))

        time_by_user: dict[UserIDType, dict[tuple[int, int], float]] = {}

        for month, users in result.data.items():
            for user_id, time in users.items():
                if user_id not in time_by_user:
                    time_by_user[user_id] = {}
                time_by_user[user_id][month] = time

        months: list[tuple[int, int]] = list(result.data.keys())
        headers = ['user', 'total'] + [f'{m:02}/{y}' for (m, y) in months]
        contributors: list[tuple[str, list[float]]] = []

        for user_id, monthly_data in time_by_user.items():
            total_time = sum(monthly_data.values())
            monthly_times = [monthly_data.get(m, 0.0) for m in months]
            if total_time < self.min_monthly_activity * len(months):
                continue

            user = await client.try_fetch_user(user_id)
            if not user_eligible(user):
                continue

            username = await client.try_fetch_username(user or user_id)
            if (total_time < 300 * len(months)) and any((t < self.min_monthly_activity for t in monthly_times)):
                username = '* ' + username

            row = (username, [total_time] + monthly_times)
            contributors.append(row)

        def fmt_h_m(_time: float) -> str:
            time_mins = round(_time)
            hours, minutes = time_mins // 60, time_mins % 60
            return f'{hours}h {minutes:02}m'

        table = []
        for row in heapq.nlargest(max_results, contributors, key=lambda a: a[1]):
            name, times = row
            table.append([name] + [fmt_h_m(t) for t in times])

        if table:
            print(tabulate(table, headers=headers, colalign=('left',), stralign='right'))
        else:
            print("No users are eligible for the bounty.")

    @staticmethod
    def subcommand() -> str:
        return 'support-bounty'


class ThreadListAnalysis(MessageAnalysis[set[ChannelIDType]]):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.user_id: UserIDType = args.user

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return MessageAnalysis.custom_args() | {
            CustomPositionalArgument('user', UserIDType),
        }

    @property
    def _require_reactions(self) -> bool:
        return False

    def _prepare(self) -> None:
        self.channel_ids: set[ChannelIDType] = set()

    def _on_message(self, message: Message) -> None:
        if message.author_id == self.user_id:
            self.channel_ids.add(message.channel_id)

    def _finalize(self) -> set[ChannelIDType]:
        return self.channel_ids

    async def _display_result(self, result: Result[set[ChannelIDType]], client: Client, max_results: int) -> None:
        server_id = Server.rocketpool.value
        for channel_id in result.data:
            channel = await client.try_fetch_channel(channel_id)
            if isinstance(channel, discord.Thread):
                print(f'https://discord.com/channels/{server_id}/{channel_id}')

    @staticmethod
    def subcommand() -> str:
        return 'thread-list'


class TimeToThresholdAnalysis(MessageAnalysis[dict[UserIDType, list[float]]]):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.__analysis = TopContributorAnalysis(stream, args)
        self.threshold = args.time_threshold

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return TopContributorAnalysis.custom_args() | {
            CustomOption('time-threshold', int, 50_000),
        }

    @property
    def _require_reactions(self) -> bool:
        return False

    def _prepare(self) -> None:
        self.__analysis._prepare()
        self.y: dict[UserIDType, list[float]] = {}
        self.next_dates: dict[UserIDType, datetime] = {}

    def _on_message(self, message: Message) -> None:
        self.__analysis._on_message(message)
        author_id = message.author_id

        if author_id not in self.next_dates:
            self.next_dates[author_id] = message.created
        elif message.created < self.next_dates[author_id]:
            return

        time_min = self.__analysis.total_time.get(author_id, 0.0)

        if author_id not in self.y:
            self.y[author_id] = [0.0]
        if self.y[author_id][-1] < self.threshold:
            self.y[author_id].append(min(time_min, self.threshold))

        self.next_dates[author_id] += timedelta(days=1)

    def _finalize(self) -> dict[UserIDType, list[float]]:
        self.__analysis._finalize()
        return self.y

    async def _display_result(self, result: Result[dict[UserIDType, list[float]]],
                              client: Client, max_results: int) -> None:
        y = {u: d for (u, d) in result.data.items() if d[-1] >= self.threshold}

        for user_id, data in sorted(y.items(), key=lambda a: len(a[1]))[:max_results]:
            username = await client.try_fetch_username(user_id)
            plt.plot(np.arange(len(data)), np.array(data), label=sanitize_str(username))

        plt.xticks(rotation=30, ha='right')
        plt.xlabel('days')
        plt.ylabel('time (mins)')
        plt.legend()
        title = f'Fastest {self.stream} contributors to reach {self.threshold:,} minutes'
        plt.title(sanitize_str(title))
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'days-to-threshold'


class DailyMessageHistoryAnalysis(MessageAnalysis[dict[date, int]]):
    @property
    def _require_reactions(self) -> bool:
        return False

    def _prepare(self) -> None:
        self.tally: dict[date, int] = {}

    def _on_message(self, message: Message) -> None:
        msg_date = message.created.date()
        if self.tally and msg_date not in self.tally:
            last_date = list(self.tally.keys())[-1]
            while last_date != msg_date:
                last_date += timedelta(days=1)
                self.tally[last_date] = 0

        self.tally[msg_date] = self.tally.get(msg_date, 0) + 1

    def _finalize(self) -> dict[date, int]:
        return self.tally

    async def _display_result(self, result: Result[dict[date, int]],
                              client: Client, max_results: int) -> None:
        x = list(result.data.keys())
        y = list(result.data.values())
        plt.plot(np.array(x), np.array(y))
        plt.ylim(bottom=0)
        plt.ylabel('message count')
        title = f'Daily {self.stream} message count'
        plt.title(sanitize_str(title))
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'daily-messages'


class MessageStreakAnalyis(MessageAnalysis[dict[UserIDType, tuple[int, int]]]):
    @property
    def _require_reactions(self) -> bool:
        return False

    def _prepare(self) -> None:
        self.current_streak: dict[UserIDType, tuple[date, int]] = {}
        self.max_streak: dict[UserIDType, int] = {}

    def _on_message(self, message: Message) -> None:
        msg_date = message.created.date()
        msg_author = message.author_id

        if msg_author not in self.current_streak:
            self.current_streak[msg_author] = msg_date, 1
            self.max_streak[msg_author] = 1
            return

        last_date, streak = self.current_streak[msg_author]
        if (msg_date - last_date) == timedelta(days=1):
            self.current_streak[msg_author] = msg_date, streak + 1
            if streak >= self.max_streak[msg_author]:
                self.max_streak[msg_author] = streak + 1
        elif (msg_date - last_date) > timedelta(days=1):
            self.current_streak[msg_author] = msg_date, 1

    def _finalize(self) -> dict[UserIDType, tuple[int, int]]:
        combined_dict = {}
        yesterday = date.today() - timedelta(days=1)
        for user in self.current_streak:
            last_msg_date, current_streak = self.current_streak[user]
            if last_msg_date >= yesterday:
                combined_dict[user] = current_streak, self.max_streak[user]
            else:
                combined_dict[user] = 0, self.max_streak[user]
        return combined_dict

    async def _display_result(self, result: Result[dict[UserIDType, tuple[int, int]]], client: Client, max_results: int) -> None:
        range_str = self._get_date_range_str(result.start, result.end)
        top_streaks_cur = heapq.nlargest(max_results, result.data.items(), key=lambda a: a[1][0])
        top_streaks_max = heapq.nlargest(max_results, result.data.items(), key=lambda a: a[1][1])

        print(f'Longest active {self.stream} message streaks {range_str}')
        for i, (user_id, (streak, _)) in enumerate(top_streaks_cur):
          print(f'{i + 1}. {await client.try_fetch_username(user_id)}: {streak}')

        print('')

        print(f'Longest {self.stream} message streaks {range_str}')
        for i, (user_id, (_, streak)) in enumerate(top_streaks_max):
          print(f'{i + 1}. {await client.try_fetch_username(user_id)}: {streak}')

    @staticmethod
    def subcommand() -> str:
        return 'message-streaks'

class ActiveDaysAnalyis(MessageAnalysis[dict[UserIDType, tuple[int, int]]]):
    @property
    def _require_reactions(self) -> bool:
        return False

    def _prepare(self) -> None:
        self.days: dict[UserIDType, set[date]] = {}

    def _on_message(self, message: Message) -> None:
        msg_date = message.created.date()
        msg_author = message.author_id

        if msg_author not in self.days:
            self.days[msg_author] = set()

        self.days[msg_author].add(msg_date)

    def _finalize(self) -> dict[UserIDType, int]:
        return {u: len(d) for u,d in self.days.items()}

    async def _display_result(self, result: Result[dict[UserIDType, int]], client: Client, max_results: int) -> None:
        range_str = self._get_date_range_str(result.start, result.end)
        top_streaks_cur = heapq.nlargest(max_results, result.data.items(), key=lambda a: a[1])

        print(f'Most active days in {self.stream} {range_str}')
        for i, (user_id, active_days) in enumerate(top_streaks_cur):
            print(f'{i + 1}. {await client.try_fetch_username(user_id)}: {active_days}')

    @staticmethod
    def subcommand() -> str:
        return 'active-days'


class DailyUniqueUserHistoryAnalysis(MessageAnalysis[dict[date, int]]):
    @property
    def _require_reactions(self) -> bool:
        return False

    def _prepare(self) -> None:
        self.tally: dict[date, set[UserIDType]] = {}

    def _on_message(self, message: Message) -> None:
        msg_date = message.created.date()
        if msg_date not in self.tally:
            if self.tally:
                last_date = list(self.tally.keys())[-1]
                while last_date != msg_date:
                    last_date += timedelta(days=1)
                    self.tally[last_date] = set()
            self.tally[msg_date] = set()

        self.tally[msg_date].add(message.author_id)

    def _finalize(self) -> dict[date, set[UserIDType]]:
        return self.tally

    async def _display_result(self, result: Result[dict[date, set[UserIDType]]],
                              client: Client, max_results: int) -> None:
        x = list(result.data.keys())
        y = [len(users) for users in result.data.values()]
        plt.plot(np.array(x), np.array(y))
        plt.ylim(bottom=0)
        plt.ylabel('user count')
        title = f'Daily unique user count for {self.stream}'
        plt.title(sanitize_str(title))
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'daily-unique-users'


class UniqueUserHistoryAnalysis(
    HistoryBasedMessageAnalysis['UniqueUserHistoryAnalysis.__UniqueUserCountAnalysis', int]):
    class __UniqueUserCountAnalysis(MessageAnalysis):
        @property
        def _require_reactions(self) -> bool:
            return False

        def _prepare(self) -> None:
            self.users: set[UserIDType] = set()

        def _on_message(self, message: Message) -> None:
            self.users.add(message.author_id)

        def _finalize(self) -> int:
            return len(self.users)

        async def _display_result(self, result: Result[tuple[int, int]], client: Client, max_results: int) -> None:
            start, end = result.start, result.end
            print(f'Unique user count for {self.stream} {self._get_date_range_str(start, end)}')
            print(result.data)

        @staticmethod
        def subcommand() -> str:
            return ''

    @classmethod
    def _base_analysis_class(cls) -> type[__UniqueUserCountAnalysis]:
        return cls.__UniqueUserCountAnalysis

    def _get_data(self) -> int:
        return len(self._base_analysis.users)

    async def _display_result(self, result: Result[tuple[list[datetime], list[int]]],
                              client: Client, max_results: int) -> None:
        x, y = result.data
        plt.plot(np.array(x), np.array(y))
        plt.xticks(rotation=30, ha='right')
        title = f'Number of unique {self.stream} participants over time'
        plt.title(sanitize_str(title))
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'unique-user-history'


class WickPenaltyHistoryAnalysis(
    HistoryBasedMessageAnalysis['WickPenaltyHistoryAnalysis.__WickPenaltyCountAnalysis', tuple[int, int]]):
    class __WickPenaltyCountAnalysis(MessageAnalysis):
        @property
        def _require_reactions(self) -> bool:
            return False

        def _prepare(self) -> None:
            self.bans = 0
            self.timeouts = 0

        def _on_message(self, message: Message) -> None:
            if message.author_id != 536991182035746816:
                return

            if not message.embeds:
                return

            description = message.embeds[0].get('description', '')
            if 'banned' in description:
                self.bans += 1
            elif any((kw in description for kw in ('timed out', 'silenced'))):
                self.timeouts += 1

        def _finalize(self) -> tuple[int, int]:
            return self.bans, self.timeouts

        async def _display_result(self, result: Result[tuple[int, int]], client: Client, max_results: int) -> None:
            start, end = result.start, result.end
            bans, timeouts = result.data

            print(f'Wick penalty count for {self.stream} {self._get_date_range_str(start, end)}')
            print(f'Total bans: {bans}')
            print(f'Total timeouts: {timeouts}')

        @staticmethod
        def subcommand() -> str:
            return ''

    @classmethod
    def _base_analysis_class(cls) -> type[__WickPenaltyCountAnalysis]:
        return cls.__WickPenaltyCountAnalysis

    def _get_data(self) -> tuple[int, int]:
        return self._base_analysis.bans, self._base_analysis.timeouts

    async def _display_result(self, result: Result[tuple[list[datetime], list[tuple[int, int]]]],
                              client: Client, max_results: int) -> None:
        x, y = result.data
        x_arr, y_arr = np.array(x), np.array(y)

        plt.plot(x_arr, y_arr[:, 0], label='Ban', color='firebrick')
        plt.plot(x_arr, y_arr[:, 1], label='Timeout', color='orange')
        plt.xticks(rotation=30, ha='right')
        title = f'Cumulative Wick Penalty Count ({self.stream})'
        plt.title(sanitize_str(title))
        plt.legend()
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'wick-penalties'


class IMCContributionAnalysis(MessageAnalysis):
    imc_members = {
        109422960682496000,  # valdorff
        474028048551772160,  # yokem55
        995528889860370442,  # orangesamus
        707707212184944702,  # philcares
        354099029434695681,  # drworm.eth
        343180040747614209,  # jasper.the.friendly.ghost
        806275470140244019,  # peteris
        764676584832761878,  # waqwaqattack
        360474629988548608,  # haloooloolo
    }

    @property
    def _require_reactions(self) -> bool:
        return False

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return TopContributorAnalysis.custom_args() | MessageCountAnalysis.custom_args()

    def __init__(self, stream, args):
        super().__init__(stream, args)
        self.__time = TopContributorAnalysis(stream, args)
        self.__count = MessageCountAnalysis(stream, args)

    def _prepare(self) -> None:
        self.__time._prepare()
        self.__count._prepare()

    def _on_message(self, message: Message) -> None:
        self.__time._on_message(message)
        self.__count._on_message(message)

    def _finalize(self) -> dict[UserIDType, tuple[float, int]]:
        time_data = self.__time._finalize()
        count_data = self.__count._finalize()
        return {uid: (time_data[uid], count_data[uid]) for uid in time_data.keys()}

    async def _display_result(self, result: Result[dict[UserIDType, tuple[float, int]]], client: Client,
                              max_results: int) -> None:
        data = result.data
        top_contributors: set[UserIDType] = set(
            heapq.nlargest(max_results, result.data.keys(), key=lambda a: data[a][0]))
        contributors: list[UserIDType] = list(self.imc_members.union(top_contributors))
        contributors.sort(key=lambda a: data.get(a, (0.0, 0))[0], reverse=True)

        table: list[tuple[str, int, int]] = []

        for user_id in contributors:
            username = await client.try_fetch_username(user_id)
            contrib_time, msg_count = data.get(user_id, (0.0, 0))
            table.append((username, round(contrib_time), msg_count))

        start, end = result.start, result.end
        print(f'IMC contributions for {self.stream} {self._get_date_range_str(start, end)}')
        print(tabulate(table, headers=('user', 'time (mins)', 'msg count')))

    @staticmethod
    def subcommand() -> str:
        return 'imc-contributions'


class JSONDump(MessageAnalysis[list['JSONDump.JSONMessageType']]):
    JSONFieldType = Optional[Union[int, str, list]]
    JSONMessageType = dict[str, JSONFieldType]

    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.file_path = args.file_path
        if not self.file_path:
            self.file_path = sanitize_str(str(stream).strip()).lower()
            self.file_path = self.file_path.replace(' ', '-')
            self.file_path += '.json'
        self.include_reactions = args.include_reactions
        self.include_usernames = args.include_usernames

    @classmethod
    def custom_args(cls) -> set[CustomArgument]:
        return TopContributorAnalysis.custom_args() | {
            CustomOption('file-path', str, None),
            CustomFlag('include-reactions'),
            CustomFlag('include-usernames'),
        }

    @property
    def _require_reactions(self) -> bool:
        return self.include_reactions

    def _prepare(self) -> None:
        self.data: list[JSONDump.JSONMessageType] = []

    def _on_message(self, message: Message) -> None:
        msg_data = {k: str(v) if isinstance(v, datetime) else v for (k, v) in message.__dict__.items()}
        self.data.append(msg_data)

    def _finalize(self) -> list[JSONMessageType]:
        return self.data

    async def _display_result(self, result: Result[list[JSONMessageType]], client: Client,
                              max_results: int) -> None:
        async def fetch_all_usernames() -> None:
            logging.info('Fetching usernames')
            usernames: dict[UserIDType, Optional[str]] = {}

            for msg in result.data:
                user_id = cast(int, msg['author_id'])
                username: Optional[str] = None

                if user_id in usernames:
                    username = usernames[user_id]
                else:
                    if user := await client.try_fetch_user(user_id):
                        username = user.display_name
                    usernames[user_id] = username

                msg['author_username'] = username

        if self.include_usernames:
            await fetch_all_usernames()

        logging.info(f'Saving {self.file_path}')
        with open(self.file_path, 'w') as f:
            json.dump(result.data, f, indent=4)

    @staticmethod
    def subcommand() -> str:
        return 'json-dump'


class RocketFuelRaffle(CountBasedMessageAnalysis):
    WAQ = 764676584832761878
    WAQ_EMOTE = '<:waq:1022673797943406603>'
    
    def _on_message(self, message: Message) -> None:
        author = message.author_id
        is_waq = (author == self.WAQ) and (message.reference is None)
        is_waqqed = (self.WAQ in message.reactions.get(self.WAQ_EMOTE, {}))
        if is_waq or is_waqqed:
            self.count[author] = self.count.get(author, 0) + 1
    
    @property
    def _require_reactions(self) -> bool:
        return True

    def _title(self) -> str:
        return 'Rocket Fuel Raffle Tickets'

    @staticmethod
    def subcommand() -> str:
        return 'rf-raffle'
