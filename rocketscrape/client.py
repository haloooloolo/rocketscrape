import discord

import asyncio
import logging
import signal
from typing import Any, Callable, Awaitable, Optional, Union
from threading import Lock
from asyncio.exceptions import CancelledError

DiscordChannel = Union[discord.abc.GuildChannel, discord.abc.PrivateChannel, discord.Thread]
DiscordUser = Union[discord.User, discord.ClientUser]

log = logging.getLogger(__name__)


class Client(discord.Client):
    def __init__(self, func: Callable[['Client'], Awaitable], args: Any):
        super().__init__()
        self.__func = func
        self.__lock = Lock()
        self.args = args

    def run(self, token: str, **kwargs) -> None:
        kwargs.setdefault('log_handler', None)
        try:
            super().run(token, **kwargs)
        except CancelledError:
            pass

    async def on_ready(self) -> None:
        if not self.__lock.acquire(blocking=False):
            log.info('Back online')
            return

        log.info(f'Logged in as {self.user}')
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()

        def on_sigint():
            log.warning('Interrupted, finishing in-flight commit...')
            if task and not task.done():
                task.cancel()
            loop.remove_signal_handler(signal.SIGINT)  # next ^C uses default handler

        try:
            loop.add_signal_handler(signal.SIGINT, on_sigint)
        except NotImplementedError:
            pass  # not supported on Windows

        try:
            await self.__func(self)
            await self.close()
        finally:
            try:
                loop.remove_signal_handler(signal.SIGINT)
            except (NotImplementedError, ValueError):
                pass
            self.__lock.release()

    async def try_fetch_user(self, user_id: int) -> Optional[DiscordUser]:
        try:
            return super().get_user(user_id) or (await super().fetch_user(user_id))
        except (discord.errors.NotFound, discord.errors.Forbidden):
            return None

    async def try_fetch_role(self, role_id: int, guild_id: int) -> Optional[discord.Role]:
        if not (guild := await self.try_fetch_guild(guild_id)):
            return None
        return guild.get_role(role_id)

    async def try_fetch_guild(self, guild_id: int) -> Optional[discord.Guild]:
        try:
            return super().get_guild(guild_id) or (await super().fetch_guild(guild_id))
        except (discord.errors.NotFound, discord.errors.Forbidden):
            return None

    async def try_fetch_channel(self, channel_id: int) -> Optional[DiscordChannel]:
        try:
            return super().get_channel(channel_id) or (await super().fetch_channel(channel_id))
        except (discord.errors.NotFound, discord.errors.Forbidden):
            return None

    async def try_fetch_username(self, user: Union[int, DiscordUser]) -> str:
        if isinstance(user, int):
            return self.__get_username(user, await self.try_fetch_user(user))
        else:
            return self.__get_username(user.id, user)

    @staticmethod
    def __get_username(user_id: int, user: Optional[DiscordUser]):
        if user is None:
            return f'<{user_id}>'
        return user.display_name
