import discord

import logging
from typing import Any, Callable, Awaitable, Optional, Union
from threading import Lock
from asyncio.exceptions import CancelledError

DiscordChannel = Union[discord.abc.GuildChannel, discord.abc.PrivateChannel, discord.Thread]
DiscordUser = Union[discord.User, discord.ClientUser]


class Client(discord.Client):
    def __init__(self, func: Callable[['Client'], Awaitable], args: Any):
        super().__init__()
        self.__func = func
        self.__lock = Lock()
        self.args = args

    def run(self, token: str, **kwargs) -> None:
        try:
            super().run(token, **kwargs)
        except CancelledError:
            pass

    async def on_ready(self) -> None:
        if not self.__lock.acquire(blocking=False):
            logging.info(f'Back online')
            return

        logging.info(f'Logged in as {self.user}')
        try:
            await self.__func(self)
            await self.close()
        finally:
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
