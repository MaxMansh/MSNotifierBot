from aiogram import Bot
from aiogram.enums import ParseMode
from typing import List
import asyncio
import logging

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, bot: Bot, chat_id: str, message_limit: int = 4096):
        self.bot = bot
        self.chat_id = chat_id
        self.message_limit = message_limit

    async def send(self, header: str, messages: List[str]) -> bool:
        if not messages:
            return False

        try:
            full_message = f"{header}\n\n" + "\n\n".join(messages)
            if len(full_message) <= self.message_limit:
                await self._send_message(full_message)
            else:
                await self._send_batched(header, messages)
            return True
        except Exception as e:
            logger.error(f"Failed to send notification: {str(e)}")
            return False

    async def _send_batched(self, header: str, messages: List[str]) -> None:
        current_message = header + "\n\n"

        for msg in messages:
            if len(current_message) + len(msg) + 2 > self.message_limit:
                await self._send_message(current_message.strip())
                current_message = ""
                await asyncio.sleep(1)

            current_message += msg + "\n\n"

        if current_message.strip():
            await self._send_message(current_message.strip())

    async def _send_message(self, text: str) -> None:
        await self.bot.send_message(
            chat_id=self.chat_id,
            text=text,
            parse_mode=ParseMode.HTML
        )