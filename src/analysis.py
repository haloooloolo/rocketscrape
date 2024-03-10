import time
from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime, timedelta

import discord
from messages import MessageCache, Message


class MessageAnalysis(ABC):
    def __init__(self, log_interval=1):
        self.log_interval = log_interval

    async def run(self, channel: discord.TextChannel, start: Optional[datetime], end: Optional[datetime]):
        assert (start is None) or (end is None) or (end > start)
        last_ts = time.time()
        self._prepare()

        async for message in MessageCache(channel).get_history(start, end):
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
    def __to_minutes(td: timedelta):
        return td / timedelta(minutes=1)
    
    def _prepare(self) -> None:
        self.open_sessions = {}
        self.total_time = {}

    def _on_message(self, message: Message) -> None:
        timestamp = message.time
        author = message.author

        session_start, session_end = self.open_sessions.get(author, (timestamp, timestamp))
        if self.__to_minutes(timestamp - session_end) < self.session_timeout:
            session_end = timestamp
            self.open_sessions[author] = (session_start, session_end)
        else:
            session_time = self.__to_minutes(session_end - session_start) + self.base_session_time
            self.total_time[author] = self.total_time.get(author, 0) + session_time
            del self.open_sessions[author]
    
    def _finalize(self) -> dict[str, int]:
        # add remaining open sessions to total
        for author, (session_start, session_end) in self.open_sessions.items():
            session_time = self.__to_minutes(session_end - session_start) + self.base_session_time
            self.total_time[author] = self.total_time.get(author, 0) + session_time

        return self.total_time


class HistoricalTopContributorAnalysis(TopContributorAnalysis):
    def _prepare(self) -> None:
        super()._prepare()
        self.x = []
        self.y = {}
        self.next_date = None
        self.last_ts = None

    def __add_datapoints(self, date) -> None:
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

        self.__add_datapoints(self.next_date)
        self.next_date += timedelta(days=28)

    def _finalize(self) -> tuple[list[datetime], dict[str, list[int]]]:
        super()._finalize()
        self.__add_datapoints(self.last_ts)
        return self.x, self.y
