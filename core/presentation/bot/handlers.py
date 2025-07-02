import aiohttp
import asyncio
import pandas as pd
import re
from aiogram import Router, F, types
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from core.api.moysklad import MoyskladAPI
from core.infrastructure import CacheManager
from core.config import Settings
from core.infrastructure import AppLogger
from io import BytesIO
from typing import List, Optional, Tuple
from datetime import datetime

router = Router()
logger = AppLogger().get_logger(__name__)


def log_user_activity(user: types.User, action: str, details: str = ""):
    """Логирует действия пользователя с подробной информацией"""
    user_info = (
        f"Пользователь: ID={user.id}, "
        f"Имя={user.full_name}, "
        f"Никнейм=@{user.username}" if user.username else "Без никнейма"
    )
    log_message = (
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        f"{user_info} | "
        f"Действие: {action}"
    )
    if details:
        log_message += f" | Детали: {details}"
    logger.info(log_message)


# Исправленная мидлварь для проверки доступа
@router.message.middleware()
async def check_access_middleware(handler, event: types.Message, data: dict):
    config: Settings = data.get("config")
    if not config:
        logger.error("Конфигурация не найдена в данных.")
        return False

    user_id = event.from_user.id
    if user_id not in config.ALLOWED_USER_IDS:
        log_user_activity(event.from_user, "Попытка неавторизованного доступа")
        await event.answer("🚫 Доступ запрещен.")
        return  # Прекращаем дальнейшую обработку

    log_user_activity(event.from_user, "Авторизованный доступ")
    return await handler(event, data)


# Регистрация мидлвари
router.message.middleware(check_access_middleware)

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


# Функции
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


# Хэндлеры
@router.message(Command("start"))
async def handle_start(message: Message):
    """Обработка команды /start"""
    log_user_activity(message.from_user, "Начал взаимодействие с ботом", "Команда /start")
    welcome_text = (
        "👋 Добро пожаловать в <b>Многофункциональный бот для МойСклад</b>!\n\n"
        "Отправьте номер телефона в любом формате."
    )
    await message.answer(welcome_text, parse_mode="HTML", reply_markup=get_main_keyboard())
    log_user_activity(message.from_user, "Получил приветственное сообщение")


@router.message(Command("help"))
@router.message(F.text.lower() == "ℹ️ помощь")
async def handle_help(message: Message):
    """Показывает справку"""
    log_user_activity(message.from_user, "Запросил справку", "Команда /help или кнопка Помощь")
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
    log_user_activity(message.from_user, "Получил сообщение со справкой")


@router.message(Command("status"))
@router.message(F.text.lower() == "🔄 статус")
async def check_status(message: Message, api: MoyskladAPI):
    """Проверяет соединение с API"""
    log_user_activity(message.from_user, "Проверка статуса API", "Команда /status или кнопка Статус")
    try:
        async with aiohttp.ClientSession() as session:
            if await api.check_connection(session):
                log_user_activity(message.from_user, "Проверка API", "API доступен")
                await message.answer("🟢 API МойСклад доступен", reply_markup=get_main_keyboard())
            else:
                log_user_activity(message.from_user, "Проверка API", "Ошибка подключения")
                await message.answer("🔴 Ошибка подключения к API", reply_markup=get_main_keyboard())
    except Exception as e:
        log_user_activity(message.from_user, "Ошибка проверки API", f"Ошибка: {str(e)}")
        await message.answer("⚠️ Ошибка проверки статуса", reply_markup=get_main_keyboard())


@router.message(F.text.lower() == "📱 добавить номер")
async def handle_add_number(message: Message):
    """Запрос номера телефона"""
    log_user_activity(message.from_user, "Нажал кнопку 'Добавить номер'")
    await message.answer("Отправьте номер телефона в любом формате:", reply_markup=get_back_keyboard())


@router.message(F.text.lower() == "📊 загрузить файл")
async def handle_upload_file(message: Message):
    """Запрос Excel-файла"""
    log_user_activity(message.from_user, "Нажал кнопку 'Загрузить файл'")
    await message.answer(
        "Пришлите Excel-файл с номерами в колонке 'Наименование':",
        reply_markup=get_back_keyboard()
    )


@router.message(F.text.lower() == "🔙 назад")
async def handle_back(message: Message):
    """Возврат в главное меню"""
    log_user_activity(message.from_user, "Нажал кнопку 'Назад'")
    await handle_start(message)


@router.message(F.text)
async def handle_text(message: Message, api: MoyskladAPI, phone_cache: CacheManager, config: Settings):
    """Обработка текста с номером телефона"""
    log_user_activity(message.from_user, "Отправил текстовое сообщение", f"Текст: {message.text}")

    if not (phone := extract_phone(message.text)):
        log_user_activity(message.from_user, "Не удалось распознать номер", f"В сообщении: {message.text}")
        await message.answer("🔍 Не удалось распознать номер", reply_markup=get_main_keyboard())
        return

    log_user_activity(message.from_user, "Попытка добавить номер", f"Номер: {phone}")

    if phone_cache.has_counterparty(phone):
        log_user_activity(message.from_user, "Номер уже существует", f"Номер: {phone}")
        await message.answer(f"ℹ️ Номер {phone} уже в системе", reply_markup=get_main_keyboard())
        return

    for attempt in range(1, config.MAX_ATTEMPTS + 1):
        try:
            async with aiohttp.ClientSession() as session:
                if await api.create_counterparty(session, phone):
                    log_user_activity(message.from_user, "Успешно добавил номер", f"Номер: {phone}")
                    phone_cache.add_counterparty(phone, "individual")
                    await message.answer(f"✅ Номер {phone} добавлен!", reply_markup=get_main_keyboard())
                    return
        except Exception as e:
            log_user_activity(message.from_user, "Ошибка добавления номера",
                              f"Номер: {phone}, попытка {attempt}, ошибка: {str(e)}")
            if attempt < config.MAX_ATTEMPTS:
                await asyncio.sleep(config.RETRY_DELAY * attempt)

    log_user_activity(message.from_user, "Не удалось добавить номер",
                      f"Номер: {phone} после {config.MAX_ATTEMPTS} попыток")
    await message.answer(f"❌ Не удалось добавить номер {phone}", reply_markup=get_main_keyboard())


@router.message(F.document)
async def handle_document(message: Message, api: MoyskladAPI, phone_cache: CacheManager, config: Settings):
    """Обработка Excel-файла"""
    log_user_activity(message.from_user, "Попытка загрузить файл",
                      f"Имя файла: {message.document.file_name}, размер: {message.document.file_size} байт")

    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        log_user_activity(message.from_user, "Неподдерживаемый формат файла", message.document.file_name)
        await message.answer("❌ Нужен файл Excel (.xlsx)", reply_markup=get_main_keyboard())
        return

    if message.document.file_size > MAX_FILE_SIZE:
        log_user_activity(message.from_user, "Файл слишком большой",
                          f"Размер: {message.document.file_size} байт")
        await message.answer("❌ Файл слишком большой (макс. 10МБ)", reply_markup=get_main_keyboard())
        return

    try:
        file = await message.bot.download(message.document)
        phones = await process_excel(file)

        if not phones:
            log_user_activity(message.from_user, "Номера не найдены в файле", message.document.file_name)
            await message.answer("🔍 Номера не найдены", reply_markup=get_main_keyboard())
            return

        total = len(phones)
        log_user_activity(message.from_user, "Начал обработку файла",
                          f"Файл: {message.document.file_name}, номеров: {total}")
        msg = await message.answer(f"🔍 Найдено {total} номеров. Обработка...", reply_markup=get_back_keyboard())

        success, skipped, failed = 0, 0, []
        async with aiohttp.ClientSession() as session:
            for i in range(0, total, BATCH_SIZE):
                batch = phones[i:i + BATCH_SIZE]
                s, sk, f = await process_batch(session, batch, api, phone_cache)
                success += s
                skipped += sk
                failed.extend(f)

                current_processed = i + len(batch)
                if current_processed % BATCH_SIZE == 0 or current_processed == total:
                    try:
                        await msg.edit_text(f"⏳ Обработано {current_processed}/{total}...")
                        log_user_activity(message.from_user, "Обработка файла",
                                          f"Прогресс: {current_processed}/{total}")
                    except Exception as e:
                        logger.warning(f"Не удалось отредактировать сообщение: {e}")
                        msg = await message.answer(f"⏳ Обработано {current_processed}/{total}...")

        result = (
            f"📊 Итоги:\n"
            f"• Добавлено: {success}\n"
            f"• Дубликатов: {skipped}\n"
            f"• Ошибок: {len(failed)}"
        )
        log_user_activity(message.from_user, "Завершил обработку файла",
                          f"Файл: {message.document.file_name}, результат: {result}")
        await message.answer(result, reply_markup=get_main_keyboard())

    except Exception as e:
        log_user_activity(message.from_user, "Ошибка обработки файла",
                          f"Файл: {message.document.file_name}, ошибка: {str(e)}")
        await message.answer("❌ Ошибка обработки файла", reply_markup=get_main_keyboard())