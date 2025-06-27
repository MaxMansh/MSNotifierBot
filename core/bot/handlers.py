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
    """Извлекает номер телефона из текста с подробным логированием"""
    logger.debug(f"Начало обработки текста для извлечения номера: '{text}'")

    if not text:
        logger.warning("Получен пустой текст для извлечения номера")
        return None

    try:
        clean_phone = re.sub(r'\D', '', text)
        if not clean_phone:
            logger.warning(f"Не удалось извлечь номер из текста: '{text}'")
            return None

        logger.debug(f"Успешно извлечен номер: '{clean_phone}' из текста: '{text}'")
        return clean_phone
    except Exception as e:
        logger.error(f"Ошибка при извлечении номера из '{text}': {str(e)}")
        return None


async def process_excel(file: BytesIO) -> List[str]:
    """Парсит номера из Excel файла с детальным логированием"""
    logger.info("Начало обработки Excel файла")

    try:
        # Чтение файла
        logger.debug("Попытка чтения Excel файла")
        df = pd.read_excel(file)

        # Проверка наличия нужной колонки
        if 'Наименование' not in df.columns:
            logger.error("В файле отсутствует колонка 'Наименование'")
            return []

        phones = []
        logger.debug(f"Начало обработки {len(df)} строк из файла")

        for idx, phone in enumerate(df['Наименование'].astype(str)):
            try:
                cleaned = extract_phone(phone)
                if cleaned:
                    phones.append(cleaned)
                    logger.debug(f"Обработана строка {idx + 1}: номер {cleaned}")
                else:
                    logger.warning(f"Пропущена строка {idx + 1}: '{phone}' - не удалось извлечь номер")
            except Exception as e:
                logger.error(f"Ошибка обработки строки {idx + 1} ('{phone}'): {str(e)}")

        logger.info(f"Успешно обработан Excel файл. Найдено номеров: {len(phones)}")
        return phones

    except Exception as e:
        logger.error(f"Критическая ошибка при обработке Excel файла: {str(e)}")
        return []


@router.message(F.document)
async def handle_document(message: Message, api: MoyskladAPI):
    """Обработка загруженных Excel-файлов с полным логированием"""
    logger.info(f"Получен документ: {message.document.file_name}")

    # Проверка формата файла
    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        error_msg = f"Неверный формат файла: {message.document.file_name}"
        logger.error(error_msg)
        await message.answer("❌ Пожалуйста, загрузите файл Excel (.xlsx)")
        return

    try:
        # Скачивание файла
        logger.debug("Начало загрузки файла с серверов Telegram")
        file = await message.bot.download(message.document)
        logger.info("Файл успешно загружен")

        # Парсинг файла
        logger.debug("Начало парсинга файла")
        phones = await process_excel(file)

        if not phones:
            logger.warning("В файле не найдено ни одного валидного номера")
            await message.answer("🔍 Не найдено номеров в файле")
            return

        logger.info(f"Начало обработки {len(phones)} номеров из файла")

        # Добавление контрагентов
        success_count = 0
        async with aiohttp.ClientSession() as session:
            for idx, phone in enumerate(phones):
                try:
                    logger.debug(f"Обработка номера {idx + 1}/{len(phones)}: {phone}")
                    if await api.create_counterparty(session, phone):
                        success_count += 1
                        logger.debug(f"Успешно добавлен номер: {phone}")
                    else:
                        logger.warning(f"Не удалось добавить номер: {phone}")
                except Exception as e:
                    logger.error(f"Ошибка при обработке номера {phone}: {str(e)}")

        result_msg = (
            f"✅ Обработано: {success_count}/{len(phones)} номеров\n"
            f"Пример: {phones[0]}...{phones[-1] if len(phones) > 1 else ''}"
        )

        logger.info(f"Итоги обработки файла: {result_msg}")
        await message.answer(result_msg)

    except Exception as e:
        error_msg = f"Критическая ошибка обработки файла: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await message.answer("⚠️ Ошибка обработки файла")


@router.message(F.text)
async def handle_text(message: Message, api: MoyskladAPI):
    """Обработка текстовых сообщений с фильтрацией только номеров"""
    logger.info(f"Получено сообщение от {message.from_user.id}: '{message.text}'")

    # Пропускаем команды (например /start)
    if message.text.startswith('/'):
        return

    # Извлечение номера
    phone = extract_phone(message.text)
    if not phone:
        logger.debug(f"Сообщение не содержит номер: '{message.text}'")
        return  # Просто игнорируем, не отвечаем

    try:
        logger.debug(f"Обработка номера: {phone}")
        async with aiohttp.ClientSession() as session:
            if await api.create_counterparty(session, phone):
                logger.info(f"Добавлен номер: {phone}")
                await message.answer(f"✅ Номер {phone} добавлен!")
            else:
                logger.error(f"Ошибка API для номера: {phone}")
                await message.answer("❌ Ошибка при создании контрагента")

    except Exception as e:
        logger.error(f"Ошибка обработки номера {phone}: {str(e)}", exc_info=True)
        await message.answer("⚠️ Внутренняя ошибка сервера")