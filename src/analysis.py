import time
from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime, timedelta

from messages import MessageStream, Message


class MessageAnalysis(ABC):
    def __init__(self, log_interval=1):
        self.log_interval = log_interval

    async def run(self, stream: MessageStream, start: Optional[datetime], end: Optional[datetime]):
        assert (start is None) or (end is None) or (end > start)
        last_ts = time.time()
        self._prepare()

        async for message in stream.get_history(start, end):
            ts = time.time()
            if (ts - last_ts) >= self.log_interval:
                print(message.time)
                last_ts = ts

            self._on_message(message)

        return self._finalize()

    @abstractmethod
    def _prepare(self):
        pass

    @abstractmethod
    def _on_message(self, message: Message):
        pass

    @abstractmethod
    def _finalize(self):
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
            session_end = timestamp
            self.open_sessions[author_id] = (session_start, session_end)
        else:
            session_time = self.__to_minutes(session_end - session_start) + self.base_session_time
            self.total_time[author_id] = self.total_time.get(author_id, 0) + session_time
            del self.open_sessions[author_id]
    
    def _finalize(self) -> dict[int, int]:
        # add remaining open sessions to total
        for author_id, (session_start, session_end) in self.open_sessions.items():
            session_time = self.__to_minutes(session_end - session_start) + self.base_session_time
            self.total_time[author_id] = self.total_time.get(author_id, 0) + session_time

        return self.total_time


class HistoricalTopContributorAnalysis(TopContributorAnalysis):
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

    def _finalize(self) -> tuple[list[datetime], dict[str, list[int]]]:
        super()._finalize()
        self.__add_snapshot(self.last_ts)
        return self.x, self.y


class MessageCountAnalysis(MessageAnalysis):
    def _prepare(self):
        self.count = {}

    def _on_message(self, message: Message):
        self.count[message.author_id] = self.count.get(message.author_id, 0) + 1

    def _finalize(self) -> dict[int, int]:
        return self.count


class SelfKekAnalysis(MessageAnalysis):
    def _prepare(self) -> None:
        self.count = {}

    def _on_message(self, message: Message) -> None:
        for emoji_name, users in message.reactions.items():
            if ('kek' in emoji_name) and (message.author_id in users):
                self.count[message.author_id] = self.count.get(message.author_id, 0) + 1

    def _finalize(self) -> dict[int, int]:
        return self.count
