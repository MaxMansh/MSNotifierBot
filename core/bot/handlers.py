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
            return []

        phones = []
        for phone in df['Наименование'].astype(str):
            if cleaned := extract_phone(phone):
                phones.append(cleaned)
        return phones
    except Exception as e:
        logger.error(f"Ошибка обработки Excel: {str(e)}")
        return []


@router.message(F.document)
async def handle_document(
        message: Message,
        api: MoyskladAPI,
        phone_cache: CacheManager
):
    """Обработка загруженных Excel-файлов с проверкой кэша"""
    logger.info(f"Получен документ: {message.document.file_name}")

    # Проверка формата файла
    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        await message.answer("❌ Пожалуйста, загрузите файл Excel (.xlsx)")
        return

    try:
        # Скачивание и парсинг файла
        file = await message.bot.download(message.document)
        phones = await process_excel(file)

        if not phones:
            await message.answer("🔍 Не найдено номеров в файле")
            return

        logger.info(f"Начало обработки {len(phones)} номеров из файла")

        # Добавление контрагентов
        success_count = 0
        skipped_count = 0
        async with aiohttp.ClientSession() as session:
            for phone in phones:
                # Проверка наличия в кэше
                if phone_cache.has_counterparty(phone):
                    logger.debug(f"Пропуск существующего номера: {phone}")
                    skipped_count += 1
                    continue

                try:
                    if await api.create_counterparty(session, phone):
                        phone_cache.add_counterparty(phone, phone)
                        success_count += 1
                        logger.debug(f"Добавлен новый номер: {phone}")
                except Exception as e:
                    logger.error(f"Ошибка обработки номера {phone}: {str(e)}")

        # Формирование отчета
        result_msg = (
            f"📊 Итоги обработки файла:\n"
            f"• Всего номеров: {len(phones)}\n"
            f"• Добавлено новых: {success_count}\n"
            f"• Уже существовало: {skipped_count}\n"
            f"• Примеры: {phones[0]}...{phones[-1] if len(phones) > 1 else ''}"
        )

        logger.info(result_msg)
        await message.answer(result_msg)

    except Exception as e:
        logger.error(f"Ошибка обработки файла: {str(e)}", exc_info=True)
        await message.answer("⚠️ Ошибка обработки файла")


@router.message(F.text)
async def handle_text(
        message: Message,
        api: MoyskladAPI,
        phone_cache: CacheManager
):
    """Обработка текстовых сообщений с проверкой кэша"""
    phone = extract_phone(message.text)
    if not phone:
        return

    # Проверка наличия в кэше
    if phone_cache.has_counterparty(phone):
        logger.debug(f"Номер {phone} уже существует")
        await message.answer(f"ℹ️ Номер {phone} уже есть в системе")
        return

    try:
        async with aiohttp.ClientSession() as session:
            if await api.create_counterparty(session, phone):
                phone_cache.add_counterparty(phone, phone)
                await message.answer(f"✅ Номер {phone} добавлен!")
            else:
                await message.answer("❌ Ошибка при создании контрагента")
    except Exception as e:
        logger.error(f"Ошибка обработки номера {phone}: {str(e)}")
        await message.answer("⚠️ Внутренняя ошибка сервера")