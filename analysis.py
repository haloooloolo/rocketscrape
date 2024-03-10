import time
from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime, timedelta

import discord
from messages import MessageCache


class MessageAnalysis(ABC):
    @abstractmethod
    async def analyze(self, channel: discord.TextChannel, start: Optional[datetime], end: Optional[datetime]):
        pass
    
    
class TopContributorAnalysis(MessageAnalysis):
    def __init__(self, base_session_time=5, session_timeout=15):
        self.base_session_time = base_session_time
        self.session_timeout = session_timeout
        
    @staticmethod
    def __to_minutes(td: timedelta):
        return td / timedelta(minutes=1)
    
    async def analyze(self, channel: discord.TextChannel, start: Optional[datetime], end: Optional[datetime]):
        assert (start is None) or (end is None) or (end > start)
        open_sessions = {}
        total_time = {}
        last_ts = time.time()
    
        async for message in MessageCache(channel).get_history(start, end):
            timestamp = message.time
            author = message.author

            ts = time.time()
            if (ts - last_ts) >= 1:
                print(timestamp)
                last_ts = ts

            session_start, session_end = open_sessions.get(author, (timestamp, timestamp))
            if self.__to_minutes(timestamp - session_end) < self.session_timeout:
                session_end = timestamp
                open_sessions[author] = (session_start, session_end)
            else:
                session_time = self.__to_minutes(session_end - session_start) + self.base_session_time
                total_time[author] = total_time.get(author, 0) + session_time
                del open_sessions[author]
    
        # add remaining open sessions to total
        for author, (session_start, session_end) in open_sessions.items():
            session_time = self.__to_minutes(session_end - session_start) + self.base_session_time
            total_time[author] = total_time.get(author, 0) + session_time
    
        return sorted(total_time.items(), key=lambda a: a[1], reverse=True)
