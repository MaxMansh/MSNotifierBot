import re
import aiohttp
import pandas as pd
from aiogram import Router, F
from aiogram.types import Message, Document
from core.services.api.moysklad import MoyskladAPI
from io import BytesIO
from typing import Optional, List
from utils.logger import AppLogger
from aiogram.filters import Command
from utils.cacher import CacheManager

logger = AppLogger().get_logger(__name__)
router = Router()


@router.message(Command("start"))
async def handle_start(message: Message):
    """Обработка команды /start"""
    # Логируем запуск бота
    logger.info(f"Пользователь {message.from_user.username} (ID: {message.from_user.id}) запустил бота.")

    welcome_text = (
        "👋 Добро пожаловать!\n\n"
        "Отправьте мне номер телефона в любом формате.\n"
        "Или пришлите Excel-файл с номерами в колонке 'Наименование'"
    )
    await message.answer(welcome_text)


def extract_phone(text: str) -> Optional[str]:
    """Извлекает номер телефона из текста"""
    if not text:
        return None
    clean_phone = re.sub(r'\D', '', text)
    return clean_phone if clean_phone else None


async def process_excel(file: BytesIO) -> List[str]:
    """Парсит номера из Excel файла"""
    try:
        df = pd.read_excel(file)
        if 'Наименование' not in df.columns:
            logger.warning("Столбец 'Наименование' не найден в файле.")
            return []

        phones = []
        for phone in df['Наименование'].astype(str):
            if cleaned := extract_phone(phone):
                phones.append(cleaned)
        logger.info(f"Извлечено {len(phones)} номеров из файла.")
        return phones
    except Exception as e:
        logger.error(f"Ошибка обработки Excel: {str(e)}", exc_info=True)
        return []


@router.message(F.text)
async def handle_text(
        message: Message,
        api: MoyskladAPI,
        phone_cache: CacheManager
):
    """Обработка текстовых сообщений с проверкой кэша"""
    logger.info(
        f"Пользователь {message.from_user.username} (ID: {message.from_user.id}) "
        f"отправил сообщение: {message.text}"
    )

    phone = extract_phone(message.text)
    if not phone:
        logger.warning("Номер телефона не найден в сообщении.")
        return

    # Проверяем наличие в кэше
    if phone_cache.has_counterparty(phone):
        logger.info(f"Контрагент {phone} уже существует в системе.")
        await message.answer(f"ℹ️ Номер {phone} уже есть в системе")
        return

    try:
        async with aiohttp.ClientSession() as session:
            if await api.create_counterparty(session, phone):
                phone_cache.add_counterparty(phone, "individual")
                logger.info(f"Контрагент {phone} успешно добавлен.")
                await message.answer(f"✅ Номер {phone} добавлен!")
            else:
                logger.warning(f"Ошибка при добавлении номера {phone}.")
                await message.answer("❌ Ошибка при создании контрагента")
    except Exception as e:
        logger.error(f"Ошибка обработки номера {phone}: {str(e)}", exc_info=True)
        await message.answer("⚠️ Внутренняя ошибка сервера")


@router.message(F.document)
async def handle_document(
        message: Message,
        api: MoyskladAPI,
        phone_cache: CacheManager
):
    """Обработка загруженных Excel-файлов с проверкой кэша"""
    logger.info(
        f"Пользователь {message.from_user.username} (ID: {message.from_user.id}) "
        f"загрузил файл: {message.document.file_name}"
    )

    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        logger.warning(f"Неверный формат файла: {message.document.file_name}")
        await message.answer("❌ Пожалуйста, загрузите файл Excel (.xlsx)")
        return

    try:
        file = await message.bot.download(message.document)
        phones = await process_excel(file)

        if not phones:
            logger.info("Файл не содержит номеров для обработки.")
            await message.answer("🔍 Не найдено номеров в файле")
            return

        logger.info(f"Начало обработки {len(phones)} номеров из файла.")

        success_count = 0
        skipped_count = 0
        async with aiohttp.ClientSession() as session:
            for phone in phones:
                if phone_cache.has_counterparty(phone):
                    skipped_count += 1
                    continue

                try:
                    if await api.create_counterparty(session, phone):
                        phone_cache.add_counterparty(phone, "individual")
                        success_count += 1
                except Exception as e:
                    logger.error(f"Ошибка обработки номера {phone}: {str(e)}", exc_info=True)

        result_msg = (
            f"📊 Итоги обработки файла:\n"
            f"• Всего номеров: {len(phones)}\n"
            f"• Добавлено новых: {success_count}\n"
            f"• Уже существовало: {skipped_count}\n"
            f"• Примеры: {phones[0]}...{phones[-1] if len(phones) > 1 else ''}"
        )

        logger.info(f"Итог обработки файла: {result_msg}")
        await message.answer(result_msg)

    except Exception as e:
        logger.error(f"Ошибка обработки файла: {str(e)}", exc_info=True)
        await message.answer("⚠️ Ошибка обработки файла")