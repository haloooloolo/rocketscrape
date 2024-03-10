import discord

import logging
from typing import Any, Callable, Awaitable, Optional

from discord.utils import MISSING


class Client(discord.Client):
    def __init__(self, func: Callable[['Client'], Awaitable], args: Any):
        super().__init__()
        self.__func = func
        self.args = args

    async def on_ready(self) -> None:
        logging.info(f'Logged in as {self.user}')
        await self.__func(self)
        await self.close()

    async def get_username(self, user_id: int) -> str:
        try:
            user = self.get_user(user_id) or await self.fetch_user(user_id)
            return user.display_name
        except discord.errors.NotFound:
            return f'<{user_id}>'
