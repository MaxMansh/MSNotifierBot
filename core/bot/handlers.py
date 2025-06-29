import aiohttp
import asyncio
import pandas as pd
import re
from aiogram import Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from core.services.api.moysklad import MoyskladAPI
from utils.cacher import CacheManager
from config.config import Settings
from utils.logger import AppLogger
from io import BytesIO
from typing import List, Optional, Tuple

router = Router()
logger = AppLogger().get_logger(__name__)

# Константы
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
BATCH_SIZE = 20  # Размер пакета для обработки


# Клавиатуры
def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает основную клавиатуру с командами"""
    buttons = [
        [KeyboardButton(text="📱 Добавить номер"), KeyboardButton(text="📊 Загрузить файл")],
        [KeyboardButton(text="🔄 Статус"), KeyboardButton(text="ℹ️ Помощь")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_back_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой Назад"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🔙 Назад")]],
        resize_keyboard=True
    )


def extract_phone(text: str) -> Optional[str]:
    """Извлекает и нормализует номер телефона (белорусский/российский)"""
    if not text or not any(char.isdigit() for char in text):
        return None

    clean_phone = re.sub(r'\D', '', text)

    # Белорусские номера (+375/80)
    if clean_phone.startswith('375') and len(clean_phone) == 12:
        return clean_phone[3:5] + clean_phone[5:]
    elif clean_phone.startswith('80') and len(clean_phone) == 11:
        return clean_phone[2:4] + clean_phone[4:]

    # Российские номера (+7/8)
    elif clean_phone.startswith('7') and len(clean_phone) == 11:
        return clean_phone[1:4] + clean_phone[4:]
    elif clean_phone.startswith('8') and len(clean_phone) == 11:
        return clean_phone[1:4] + clean_phone[4:]

    # Уже нормализованные номера
    elif len(clean_phone) in (9, 10):
        return clean_phone

    return None


async def process_excel(file: BytesIO) -> List[str]:
    """Парсит номера телефонов из Excel файла"""
    try:
        df = pd.read_excel(file, usecols=['Наименование'])
        phones = [extract_phone(str(phone)) for phone in df['Наименование']]
        return list(set(filter(None, phones)))
    except Exception as e:
        logger.error(f"Ошибка обработки Excel: {str(e)}", exc_info=True)
        return []


async def process_batch(session: aiohttp.ClientSession,
                        batch: List[str],
                        api: MoyskladAPI,
                        phone_cache: CacheManager) -> Tuple[int, int, List[str]]:
    """Обрабатывает пакет номеров"""
    success, skipped, errors = 0, 0, []

    for phone in batch:
        if phone_cache.has_counterparty(phone):
            skipped += 1
            continue

        try:
            if await api.create_counterparty(session, phone):
                phone_cache.add_counterparty(phone, "individual")
                success += 1
            else:
                errors.append(phone)
        except Exception as e:
            logger.error(f"Ошибка обработки {phone}: {str(e)}")
            errors.append(phone)

    return success, skipped, errors


@router.message(Command("start"))
async def handle_start(message: Message):
    """Обработка команды /start"""
    logger.info(f"Пользователь {message.from_user.id} начал взаимодействие с ботом.")
    logger.info("Отправлено приветственное сообщение.")
    welcome_text = (
        "👋 Добро пожаловать в <b>Многофункциональный бот для МойСклад</b>!\n\n"
        "Отправьте номер телефона в любом формате."
    )
    await message.answer(welcome_text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(Command("help"))
@router.message(F.text.lower() == "ℹ️ помощь")
async def handle_help(message: Message):
    """Показывает справку"""
    logger.info(f"Пользователь {message.from_user.id} запросил справку.")
    logger.info("Отправлено сообщение с инструкциями.")
    help_text = (
        "📚 <b>Справка по боту</b>\n\n"
        "• Отправьте номер телефона или Excel-файл\n"
        "• Номера добавляются как физ. лица\n"
        "• Автопроверка дубликатов\n\n"
        "📌 Команды:\n"
        "/start - перезапуск бота\n"
        "/status - проверка API\n"
        "/help - эта справка"
    )
    await message.answer(help_text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(Command("status"))
@router.message(F.text.lower() == "🔄 статус")
async def check_status(message: Message, api: MoyskladAPI):
    """Проверяет соединение с API"""
    logger.info(f"Пользователь {message.from_user.id} запросил статус API.")
    try:
        async with aiohttp.ClientSession() as session:
            if await api.check_connection(session):
                logger.info("API МойСклад доступен.")
                await message.answer("🟢 API МойСклад доступен", reply_markup=get_main_keyboard())
            else:
                logger.error("Ошибка подключения к API МойСклад.")
                await message.answer("🔴 Ошибка подключения к API", reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка при проверке статуса API: {e}")
        await message.answer("⚠️ Ошибка проверки статуса", reply_markup=get_main_keyboard())


@router.message(F.text.lower() == "📱 добавить номер")
async def handle_add_number(message: Message):
    """Запрос номера телефона"""
    logger.info(f"Пользователь {message.from_user.id} запросил добавление номера.")
    await message.answer("Отправьте номер телефона в любом формате:", reply_markup=get_back_keyboard())


@router.message(F.text.lower() == "📊 загрузить файл")
async def handle_upload_file(message: Message):
    """Запрос Excel-файла"""
    logger.info(f"Пользователь {message.from_user.id} запросил загрузку файла.")
    await message.answer(
        "Пришлите Excel-файл с номерами в колонке 'Наименование':",
        reply_markup=get_back_keyboard()
    )


@router.message(F.text.lower() == "🔙 назад")
async def handle_back(message: Message):
    """Возврат в главное меню"""
    logger.info(f"Пользователь {message.from_user.id} вернулся в главное меню.")
    await handle_start(message)


@router.message(F.text)
async def handle_text(message: Message, api: MoyskladAPI, phone_cache: CacheManager, config: Settings):
    """Обработка текста с номером телефона"""
    logger.info(f"Пользователь {message.from_user.id} отправил текстовое сообщение: {message.text}")
    if not (phone := extract_phone(message.text)):
        logger.warning(f"Не удалось распознать номер в сообщении: {message.text}")
        await message.answer("🔍 Не удалось распознать номер", reply_markup=get_main_keyboard())
        return

    logger.info(f"Извлечен номер телефона: {phone}")

    if phone_cache.has_counterparty(phone):
        logger.info(f"Номер {phone} уже существует в системе.")
        await message.answer(f"ℹ️ Номер {phone} уже в системе", reply_markup=get_main_keyboard())
        return

    for attempt in range(1, config.MAX_ATTEMPTS + 1):
        try:
            async with aiohttp.ClientSession() as session:
                if await api.create_counterparty(session, phone):
                    logger.info(f"Номер {phone} успешно добавлен в систему.")
                    phone_cache.add_counterparty(phone, "individual")
                    await message.answer(f"✅ Номер {phone} добавлен!", reply_markup=get_main_keyboard())
                    return
        except Exception as e:
            logger.error(f"Ошибка при добавлении номера {phone} (попытка {attempt}): {e}")
            if attempt < config.MAX_ATTEMPTS:
                await asyncio.sleep(config.RETRY_DELAY * attempt)

    logger.error(f"Не удалось добавить номер {phone} после {config.MAX_ATTEMPTS} попыток.")
    await message.answer(f"❌ Не удалось добавить номер {phone}", reply_markup=get_main_keyboard())


@router.message(F.document)
async def handle_document(message: Message, api: MoyskladAPI, phone_cache: CacheManager, config: Settings):
    """Обработка Excel-файла"""
    logger.info(f"Пользователь {message.from_user.id} отправил файл: {message.document.file_name}")
    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        logger.warning(f"Неподдерживаемый формат файла: {message.document.file_name}")
        await message.answer("❌ Нужен файл Excel (.xlsx)", reply_markup=get_main_keyboard())
        return

    if message.document.file_size > MAX_FILE_SIZE:
        logger.warning(f"Файл слишком большой: {message.document.file_size} байт.")
        await message.answer("❌ Файл слишком большой (макс. 10МБ)", reply_markup=get_main_keyboard())
        return

    try:
        file = await message.bot.download(message.document)
        phones = await process_excel(file)

        if not phones:
            logger.warning("Номера не найдены в файле.")
            await message.answer("🔍 Номера не найдены", reply_markup=get_main_keyboard())
            return

        total = len(phones)
        logger.info(f"Из файла извлечено {total} номеров.")
        msg = await message.answer(f"🔍 Найдено {total} номеров. Обработка...", reply_markup=get_back_keyboard())

        success, skipped, failed = 0, 0, []
        async with aiohttp.ClientSession() as session:
            for i in range(0, total, BATCH_SIZE):
                batch = phones[i:i + BATCH_SIZE]
                s, sk, f = await process_batch(session, batch, api, phone_cache)
                success += s
                skipped += sk
                failed.extend(f)

                if i % 100 == 0:
                    logger.info(f"Обработано {i + len(batch)}/{total} номеров.")
                    await msg.edit_text(f"⏳ Обработано {i + len(batch)}/{total}...")

        result = (
            f"📊 Итоги:\n"
            f"• Добавлено: {success}\n"
            f"• Дубликатов: {skipped}\n"
            f"• Ошибок: {len(failed)}"
        )
        logger.info(f"Итоги обработки файла: {result}")
        await message.answer(result, reply_markup=get_main_keyboard())

    except Exception as e:
        logger.error(f"Ошибка обработки файла: {e}")
        await message.answer("❌ Ошибка обработки файла", reply_markup=get_main_keyboard())
