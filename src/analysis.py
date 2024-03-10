import time
import logging
import heapq

from abc import ABC, abstractmethod
from typing import Optional, Any
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

from client import Client
from messages import MessageStream, Message


class MessageAnalysis(ABC):
    def __init__(self, log_interval=1):
        self.log_interval = log_interval

    async def run(self, stream: MessageStream, start: Optional[datetime], end: Optional[datetime]) -> Any:
        assert (start is None) or (end is None) or (end > start)
        last_ts = time.time()
        self._prepare()

        async for message in stream.get_history(start, end):
            ts = time.time()
            if (ts - last_ts) >= self.log_interval:
                logging.info(f'Message stream reached {message.time}.')
                last_ts = ts

            self._on_message(message)

        return self._finalize()

    @abstractmethod
    def _prepare(self) -> None:
        pass

    @abstractmethod
    def _on_message(self, message: Message) -> None:
        pass

    @abstractmethod
    def _finalize(self) -> Any:
        pass

    @staticmethod
    def _get_date_range_str(start: Optional[datetime], end: [datetime]) -> str:
        if start and end:
            range_str = f'from {start} to {end}'
        elif start:
            range_str = f'since {start}'
        elif end:
            range_str = f'until {end}'
        else:
            range_str = '(all time)'

        return range_str

    @abstractmethod
    async def present(self, result: Any, client: Client, stream: MessageStream, args) -> None:
        pass

    @staticmethod
    @abstractmethod
    def subcommand() -> str:
        pass


class TopContributorAnalysis(MessageAnalysis):
    def __init__(self, log_interval=1, base_session_time=5, session_timeout=15):
        super().__init__(log_interval)
        self.base_session_time = base_session_time
        self.session_timeout = session_timeout
        
    @staticmethod
    def __to_minutes(td: timedelta) -> float:
        return td / timedelta(minutes=1)
    
    def _prepare(self) -> None:
        self.open_sessions = {}
        self.total_time = {}

    def _on_message(self, message: Message) -> None:
        timestamp = message.time
        author_id = message.author_id

        session_start, session_end = self.open_sessions.get(author_id, (timestamp, timestamp))
        if self.__to_minutes(timestamp - session_end) < self.session_timeout:
            # extend session to current timestamp
            self.open_sessions[author_id] = (session_start, timestamp)
        else:
            session_time = self.__to_minutes(session_end - session_start) + self.base_session_time
            self.total_time[author_id] = self.total_time.get(author_id, 0) + session_time
            del self.open_sessions[author_id]
    
    def _finalize(self) -> dict[int, int]:
        # end remaining sessions
        for author_id, (session_start, session_end) in self.open_sessions.items():
            session_time = self.__to_minutes(session_end - session_start) + self.base_session_time
            self.total_time[author_id] = self.total_time.get(author_id, 0) + session_time

        return self.total_time

    async def present(self, result: dict[int, int], client: Client, stream: MessageStream, args) -> None:
        range_str = self._get_date_range_str(args.start, args.end)
        top_contributors = heapq.nlargest(args.max_results, result.items(), key=lambda a: a[1])

        print()
        print(f'Top {stream} contributors {range_str}')
        for i, (author_id, time) in enumerate(top_contributors):
            time_mins = round(time)
            hours, minutes = time_mins // 60, time_mins % 60
            print(f'{i+1}. {await client.get_username(author_id)}: {hours}h {minutes}m')

    @staticmethod
    def subcommand() -> str:
        return 'contributors'


class ContributionHistoryAnalysis(TopContributorAnalysis):
    def _prepare(self) -> None:
        super()._prepare()
        self.x = []
        self.y = {}
        self.next_date = None
        self.last_ts = None

    def __add_snapshot(self, date: datetime) -> None:
        for author, time_min in self.total_time.items():
            if author not in self.y:
                self.y[author] = [0] * len(self.x)
            self.y[author].append(time_min)

        self.x.append(date)

    def _on_message(self, message: Message) -> None:
        super()._on_message(message)
        self.last_ts = message.time

        if self.next_date is None:
            self.next_date = message.time
        elif message.time < self.next_date:
            return

        self.__add_snapshot(self.next_date)
        self.next_date += timedelta(days=28)

    def _finalize(self) -> tuple[list[datetime], dict[int, list[int]]]:
        super()._finalize()
        self.__add_snapshot(self.last_ts)
        return self.x, self.y

    async def present(self, result: tuple[list[datetime], dict[int, list[int]]], client: Client, stream: MessageStream, args) -> None:
        x, y = result
        for author_id, data in sorted(y.items(), key=lambda a: a[1][-1], reverse=True)[:args.max_results]:
            plt.plot(x, data, label=await client.get_username(author_id))

        plt.ylabel('time (mins)')
        plt.legend()
        plt.title(f'Top {stream} contributors over time'.encode('ascii', 'ignore').decode('ascii'))
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'contributor-history'


class MessageCountAnalysis(MessageAnalysis):
    def _prepare(self) -> None:
        self.count = {}

    def _on_message(self, message: Message) -> None:
        self.count[message.author_id] = self.count.get(message.author_id, 0) + 1

    def _finalize(self) -> dict[int, int]:
        return self.count

    async def present(self, result: dict[int, int], client: Client, stream: MessageStream, args) -> None:
        range_str = self._get_date_range_str(args.start, args.end)
        top_contributors = heapq.nlargest(args.max_results, result.items(), key=lambda a: a[1])

        print()
        print(f'Top {stream} contributors {range_str}')
        for i, (author_id, count) in enumerate(top_contributors):
            print(f'{i+1}. {await client.get_username(author_id)}: {count}')

    @classmethod
    def subcommand(cls) -> str:
        return 'message-count'


class SelfKekAnalysis(MessageAnalysis):
    def _prepare(self) -> None:
        self.count = {}

    def _on_message(self, message: Message) -> None:
        for emoji_name, users in message.reactions.items():
            if ('kek' in emoji_name) and (message.author_id in users):
                self.count[message.author_id] = self.count.get(message.author_id, 0) + 1

    def _finalize(self) -> dict[int, int]:
        return self.count

    async def present(self, result: dict[int, int], client: Client, stream: MessageStream, args) -> None:
        range_str = self._get_date_range_str(args.start, args.end)
        top_offenders = heapq.nlargest(args.max_results, result.items(), key=lambda a: a[1])

        print()
        print(f'Top {stream} self-kek offenders {range_str} ')
        for i, (author_id, count) in enumerate(top_offenders):
            print(f'{i+1}. {await client.get_username(author_id)}: {count}')

    @staticmethod
    def subcommand() -> str:
        return 'self-kek'


class MissingPersonAnalysis(TopContributorAnalysis):
    def __init__(self, log_interval=1, base_session_time=5,
                 session_timeout=15, inactivity_threshold=timedelta(days=90)):
        super().__init__(log_interval, base_session_time, session_timeout)
        self.inactivity_threshold = inactivity_threshold

    def _prepare(self) -> None:
        super()._prepare()
        self.last_seen = {}
        self.last_ts = None

    def _on_message(self, message: Message) -> None:
        super()._on_message(message)
        self.last_ts = message.time
        self.last_seen[message.author_id] = message.time

    def _finalize(self) -> dict[int, int]:
        total_time = super()._finalize()
        for author_id, ts in self.last_seen:
            if (self.last_ts - ts) < self.inactivity_threshold:
                del total_time[author_id]

        return total_time

    async def present(self, result: dict[int, int], client: Client, stream: MessageStream, args) -> None:
        top_contributors = heapq.nlargest(args.max_results, result.items(), key=lambda a: a[1])

        print()
        print(f'Top {stream} contributors without recent activity ')
        for i, (author_id, time) in enumerate(top_contributors):
            time_mins = round(time)
            hours, minutes = time_mins // 60, time_mins % 60
            print(f'{i+1}. {await client.get_username(author_id)}: {hours}h {minutes}m')

    @staticmethod
    def subcommand() -> str:
        return 'missing-persons'
