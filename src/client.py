import discord

from typing import Callable, Awaitable


class Client(discord.Client):
    def __init__(self, main: Callable[[], Awaitable]):
        self.__main = main
    
    async def on_ready(self):
        await self.__main()
        await self.close()

    async def get_username(self, user_id: int) -> str:
        try:
            user = self.get_user(user_id) or await self.fetch_user(user_id)
            return user.display_name
        except discord.errors.NotFound:
            return f'<{user_id}>'
