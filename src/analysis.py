import logging
import heapq
import re
from dataclasses import dataclass

from abc import ABC, abstractmethod
from typing import Optional, Any
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

from client import Client
from messages import MessageStream, Message


@dataclass
class CustomArgument:
    name: str
    type: type[Any]
    help: Optional[str] = None


@dataclass
class CustomOption:
    name: str
    type: type[Any]
    default: Any
    help: Optional[str] = None


class MessageAnalysis(ABC):
    def __init__(self, stream: MessageStream, args):
        self.log_interval = timedelta(seconds=args.log_interval)
        self.stream = stream

    async def run(self, start: Optional[datetime], end: Optional[datetime]) -> Any:
        assert (start is None) or (end is None) or (end > start)
        last_ts = datetime.now()
        self._prepare()

        async for message in self.stream.get_history(start, end):
            ts = datetime.now()
            if (ts - last_ts) >= self.log_interval:
                logging.info(f'Message stream reached {message.time}')
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
            range_str = f'up to {end}'
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

    @classmethod
    def custom_args(cls) -> tuple[CustomArgument | CustomOption, ...]:
        return ()


class CountBasedMessageAnalysis(MessageAnalysis):
    def _prepare(self) -> None:
        self.count = {}

    @abstractmethod
    def _on_message(self, message: Message) -> None:
        pass

    def _finalize(self) -> dict[int, int]:
        return self.count

    def _title(self, stream_name: str, range_str: str) -> str:
        pass

    async def present(self, result: dict[int, int], client: Client, stream: MessageStream, args) -> None:
        range_str = self._get_date_range_str(args.start, args.end)
        top_users = heapq.nlargest(args.max_results, result.items(), key=lambda a: a[1])

        print()
        print(self._title(str(stream), range_str))
        for i, (user_id, count) in enumerate(top_users):
            print(f'{i+1}. {await client.get_username(user_id)}: {count}')


class TopContributorAnalysis(MessageAnalysis):
    def __init__(self, stream, args):
        super().__init__(stream, args)
        self.base_session_time = args.base_session_time
        self.session_timeout = args.session_timeout

    @classmethod
    def custom_args(cls) -> tuple[CustomArgument | CustomOption, ...]:
        return MessageAnalysis.custom_args() + (
            CustomOption('base-session-time', int, 5),
            CustomOption('session-timeout', int, 15)
        )
        
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
        for i, (user_id, time) in enumerate(top_contributors):
            time_mins = round(time)
            hours, minutes = time_mins // 60, time_mins % 60
            print(f'{i+1}. {await client.get_username(user_id)}: {hours}h {minutes}m')

    @staticmethod
    def subcommand() -> str:
        return 'contributors'


class ContributionHistoryAnalysis(TopContributorAnalysis):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.interval = timedelta(days=args.snapshot_interval)

    @classmethod
    def custom_args(cls) -> tuple[CustomArgument | CustomOption, ...]:
        return TopContributorAnalysis.custom_args() + (
            CustomOption('snapshot-interval', int, 28, 'time between data snapshots in days'),
        )

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
        self.next_date += self.interval

    def _finalize(self) -> tuple[list[datetime], dict[int, list[int]]]:
        super()._finalize()
        self.__add_snapshot(self.last_ts)
        return self.x, self.y

    async def present(self,
                      result: tuple[list[datetime], dict[int, list[int]]],
                      client: Client,
                      stream: MessageStream,
                      args
                      ) -> None:
        x, y = result
        for user_id, data in sorted(y.items(), key=lambda a: a[1][-1], reverse=True)[:args.max_results]:
            plt.plot(x, data, label=(await client.get_username(user_id)).encode('ascii', 'ignore').decode('ascii'))

        plt.ylabel('time (mins)')
        plt.legend()
        plt.title(f'Top {stream} contributors over time'.encode('ascii', 'ignore').decode('ascii'))
        plt.show()

    @staticmethod
    def subcommand() -> str:
        return 'contributor-history'


class MessageCountAnalysis(CountBasedMessageAnalysis):
    def _on_message(self, message: Message) -> None:
        self.count[message.author_id] = self.count.get(message.author_id, 0) + 1

    def _title(self, stream_name: str, range_str: str) -> str:
        return f'Top {stream_name} contributors by message count {range_str}'

    @staticmethod
    def subcommand() -> str:
        return 'message-count'


class SelfKekAnalysis(CountBasedMessageAnalysis):
    def _on_message(self, message: Message) -> None:
        for emoji_name, users in message.reactions.items():
            if ('kek' in emoji_name) and (message.author_id in users):
                self.count[message.author_id] = self.count.get(message.author_id, 0) + 1

    def _title(self, stream_name: str, range_str: str) -> str:
        return f'Top {stream_name} self kek offenders {range_str}'

    @staticmethod
    def subcommand() -> str:
        return 'self-kek'


class MissingPersonAnalysis(TopContributorAnalysis):
    def __init__(self, stream, args):
        super().__init__(stream, args)
        self.inactivity_threshold = args.inactivity_threshold

    @classmethod
    def custom_args(cls) -> tuple[CustomArgument | CustomOption, ...]:
        return TopContributorAnalysis.custom_args() + (
            CustomOption('inactivity_threshold', int, 90),
        )

    def _prepare(self) -> None:
        super()._prepare()
        self.last_seen: dict[int, datetime] = {}
        self.last_ts: Optional[datetime] = None

    def _on_message(self, message: Message) -> None:
        super()._on_message(message)
        self.last_ts = message.time
        self.last_seen[message.author_id] = message.time

    def _finalize(self) -> dict[int, int]:
        total_time = super()._finalize()
        for author_id, ts in self.last_seen.items():
            if (self.last_ts - ts) < self.inactivity_threshold:
                del total_time[author_id]

        return total_time

    async def present(self, result: dict[int, int], client: Client, stream: MessageStream, args) -> None:
        top_contributors = heapq.nlargest(args.max_results, result.items(), key=lambda a: a[1])

        print()
        print(f'Top {stream} contributors with no recent activity ')
        for i, (author_id, time) in enumerate(top_contributors):
            time_mins = round(time)
            hours, minutes = time_mins // 60, time_mins % 60
            print(f'{i+1}. {await client.get_username(author_id)}: {hours}h {minutes}m')

    @staticmethod
    def subcommand() -> str:
        return 'missing-persons'


class ReactionsGivenAnalysis(CountBasedMessageAnalysis):
    def _on_message(self, message: Message) -> None:
        for emoji_name, users in message.reactions.items():
            for user_id in users:
                self.count[user_id] = self.count.get(user_id, 0) + 1

    def _title(self, stream_name: str, range_str: str) -> str:
        return f'{stream_name} members with most reactions given {range_str}'

    @staticmethod
    def subcommand() -> str:
        return 'total-reactions-given'


class ReactionsReceivedAnalysis(CountBasedMessageAnalysis):
    def _on_message(self, message: Message) -> None:
        for emoji_name, users in message.reactions.items():
            self.count[message.author_id] = self.count.get(message.author_id, 0) + len(users)

    def _title(self, stream_name: str, range_str: str) -> str:
        return f'{stream_name} members with most reactions received {range_str}'

    @staticmethod
    def subcommand() -> str:
        return 'total-reactions-received'


class ThankYouCountAnalysis(CountBasedMessageAnalysis):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.__pattern = re.compile('((^| |\n)(ty)( |$|\n|.|!))|(thank(s| you)?)|(thx)')

    def _on_message(self, message: Message) -> None:
        content = message.content.lower()
        if not self.__pattern.search(content):
            return

        mentions = message.get_mentions()
        if replied_to := self.stream.get_message(message.reference):
            mentions.add(replied_to.author_id)

        for user_id in mentions:
            self.count[user_id] = self.count.get(user_id, 0) + 1

    def _title(self, stream_name: str, range_str: str) -> str:
        return f'{stream_name} users thanked most often {range_str}'

    @staticmethod
    def subcommand() -> str:
        return 'thank-count'


class ReactionReceivedAnalysis(CountBasedMessageAnalysis):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.emoji = args.react

    @classmethod
    def custom_args(cls) -> tuple[CustomArgument | CustomOption, ...]:
        return CountBasedMessageAnalysis.custom_args() + (
            CustomArgument('react', str, 'emoji to count received reactions for'),
        )

    def _on_message(self, message: Message) -> None:
        if self.emoji in message.reactions:
            num_reactions = len(message.reactions[self.emoji])
            self.count[message.author_id] = self.count.get(message.author_id, 0) + num_reactions

    def _title(self, stream_name: str, range_str: str) -> str:
        return f'{stream_name} members by {self.emoji} given {range_str}'

    @staticmethod
    def subcommand() -> str:
        return 'reaction-received-count'


class ReactionGivenAnalysis(CountBasedMessageAnalysis):
    def __init__(self, stream: MessageStream, args):
        super().__init__(stream, args)
        self.emoji = args.react

    @classmethod
    def custom_args(cls) -> tuple[CustomArgument | CustomOption, ...]:
        return CountBasedMessageAnalysis.custom_args() + (
            CustomArgument('react', str, 'emoji to count given reactions for'),
        )

    def _on_message(self, message: Message) -> None:
        for user in message.reactions.get(self.emoji, []):
            self.count[user] = self.count.get(user, 0) + 1

    def _title(self, stream_name: str, range_str: str) -> str:
        return f'{stream_name} members by {self.emoji} given {range_str}'

    @staticmethod
    def subcommand() -> str:
        return 'reaction-given-count'
