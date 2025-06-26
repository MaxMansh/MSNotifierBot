import asyncio
from aiogram import Bot, html
from aiogram.enums import ParseMode
from typing import List
from datetime import datetime
from utils.logger import AppLogger
from core.entities.product import Product

logger = AppLogger().get_logger(__name__)

class ExpirationAlert:
    def create_expiration_header(group_path: str) -> str:
        return f"⏳ {html.bold('УВЕДОМЛЕНИЯ ПО СРОКАМ ГОДНОСТИ')} ({group_path})"

    def create_expired_alert(product: Product) -> str:
        return (
            f"🚨 {html.bold('ПРОСРОЧЕННЫЙ ТОВАР')}\n"
            f"▸ Товар: {product.name}\n"
            f"▸ Срок истёк: {product.expiration_date.strftime('%d.%m.%Y')}"
        )

    def create_near_expired_alert(product: Product, days_left: int) -> str:
        emoji = "🔴" if days_left <= 3 else "🟡"
        return (
            f"{emoji} {html.bold('ТОВАР С ИСТЕКАЮЩИМ СРОКОМ')}\n"
            f"▸ Товар: {product.name}\n"
            f"▸ Срок: {product.expiration_date.strftime('%d.%m.%Y')}\n"
            f"▸ Осталось дней: {days_left}"
        )

class StockAlert:
    def create_stock_header(group_path: str) -> str:
        return f"📊 {html.bold('УВЕДОМЛЕНИЯ ПО ОСТАТКАМ')} ({group_path})"

    def create_min_balance_alert(product: Product) -> str:
        return (
            f"⚠️ {html.bold(f'Товар: {product.name} достиг минимума!')}\n"
            f"▸ Остаток: {int(product.stock)} (минимум: {int(product.min_balance)})\n"
            f"▸ {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )

    def create_zero_stock_alert(product: Product) -> str:
        return (
            f"🛑️ {html.bold(f'Товар: {product.name} закончился!')}\n"
            f"▸ Остаток: {int(product.stock)} (минимум: {int(product.min_balance)})\n"
            f"▸ {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )

class TelegramNotifier:
    def __init__(self, bot: Bot, chat_id: str, message_limit: int = 4096):
        self.bot = bot
        self.chat_id = chat_id
        self.message_limit = message_limit

    async def send(self, header: str, messages: List[str]) -> bool:
        if not messages:
            logger.info("Нет сообщений для отправки.")
            return False

        try:
            full_message = f"{header}\n\n" + "\n\n".join(messages)
            if len(full_message) <= self.message_limit:
                await self._send_message(full_message)
            else:
                logger.info("Сообщение превышает лимит длины, используется пакетная отправка.")
                await self._send_batched(header, messages)
            return True
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления: {str(e)}")
            return False

    async def _send_batched(self, header: str, messages: List[str]) -> None:
        logger.info("Начало пакетной отправки сообщений.")
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
        logger.info("Отправка сообщения в Telegram.")
        await self.bot.send_message(
            chat_id=self.chat_id,
            text=text,
            parse_mode=ParseMode.HTML
        )