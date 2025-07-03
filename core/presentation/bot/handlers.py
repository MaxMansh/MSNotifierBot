import aiohttp
import asyncio
import pandas as pd
import re
from aiogram import Router, F, types
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from core.api.moysklad import MoyskladAPI
from core.infrastructure import CacheManager
from core.config import Settings
from core.infrastructure import AppLogger
from io import BytesIO
from typing import List, Optional, Tuple
from .admin_panel import router as admin_router, get_admin_keyboard

router = Router()
router.include_router(admin_router)
logger = AppLogger().get_logger(__name__)

# Константы
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
BATCH_SIZE = 20  # Размер пакета для обработки


class CounterpartyCreationStates(StatesGroup):
    """Состояния для режима создания контрагента."""
    ACTIVE = State()  # Режим "Создание контрагента" активен


@router.message.middleware()
async def check_access_middleware(handler, event: types.Message, data: dict):
    """Мидлварь для проверки доступа."""
    config: Settings = data.get("config")
    if not config:
        logger.error("Конфигурация не найдена в данных.")
        return False

    user_id = event.from_user.id
    if user_id not in config.ALLOWED_USER_IDS:
        AppLogger().log_user_activity(event.from_user, "Попытка неавторизованного доступа")
        await event.answer("🚫 Доступ запрещен.")
        return

    return await handler(event, data)


router.message.middleware(check_access_middleware)


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает основную клавиатуру для пользователя."""
    buttons = [
        [KeyboardButton(text="➕ Создание контрагента"), KeyboardButton(text="🔄 Статус")],
        [KeyboardButton(text="ℹ️ Помощь"), KeyboardButton(text="🔐 Админ-панель")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_back_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой Назад."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🔙 Назад")]],
        resize_keyboard=True
    )


def get_counterparty_mode_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для режима создания контрагента."""
    buttons = [
        [KeyboardButton(text="🔙 Назад")],
        [KeyboardButton(text="ℹ️ Помощь")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def extract_phone(text: str) -> Optional[str]:
    """Извлекает и нормализует номер телефона (белорусский/российский)."""
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
    """Парсит номера телефонов из Excel файла."""
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
    """Обрабатывает пакет номеров."""
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
    """Обработка команды /start."""
    AppLogger().log_user_activity(message.from_user, "Начал взаимодействие с ботом", "Команда /start")
    welcome_text = (
        "👋 Добро пожаловать в <b>Многофункциональный бот для МойСклад</b>!\n\n"
        "Вы можете:\n"
        "• Создавать контрагентов по номерам телефонов\n"
        "• Загружать Excel-файлы с номерами\n"
        "• Проверять статус системы\n\n"
        "Используйте кнопки ниже для навигации"
    )
    await message.answer(welcome_text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(F.text == "🔐 Админ-панель")
async def handle_admin_panel(message: Message, config: Settings):
    """Обработка входа в админ-панель."""
    AppLogger().log_user_activity(message.from_user, "Попытка входа в админ-панель")

    if message.from_user.id not in config.ADMIN_USER_IDS:
        AppLogger().log_user_activity(message.from_user, "Отказано в доступе к админ-панели")
        await message.answer("🚫 У вас нет доступа к админ-панели.")
        return

    AppLogger().log_user_activity(message.from_user, "Успешный вход в админ-панель")
    await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())


@router.message(Command("help"))
@router.message(F.text == "ℹ️ Помощь")
async def handle_help(message: Message):
    """Показывает справку."""
    AppLogger().log_user_activity(message.from_user, "Запросил справку")
    help_text = (
        "📚 <b>Справка по боту</b>\n\n"
        "Основные функции:\n"
        "• <b>Создание контрагента</b> - добавляет новый контрагент по номеру телефона\n"
        "• <b>Загрузка Excel</b> - массовое добавление из файла (колонка 'Наименование')\n\n"
        "📌 Команды:\n"
        "/start - перезапуск бота\n"
        "/help - эта справка\n\n"
        "Для админов доступна админ-панель с дополнительными функциями"
    )
    await message.answer(help_text, parse_mode="HTML", reply_markup=get_main_keyboard())


@router.message(Command("status"))
@router.message(F.text == "🔄 Статус")
async def check_status(message: Message, api: MoyskladAPI):
    """Проверяет соединение с API."""
    AppLogger().log_user_activity(message.from_user, "Проверка статуса API")
    try:
        async with aiohttp.ClientSession() as session:
            if await api.check_connection(session):
                AppLogger().log_user_activity(message.from_user, "Проверка API", "API доступен")
                await message.answer("🟢 API МойСклад доступен", reply_markup=get_main_keyboard())
            else:
                AppLogger().log_user_activity(message.from_user, "Проверка API", "Ошибка подключения")
                await message.answer("🔴 Ошибка подключения к API", reply_markup=get_main_keyboard())
    except Exception as e:
        AppLogger().log_user_activity(message.from_user, "Ошибка проверки API", f"Ошибка: {str(e)}")
        await message.answer("⚠️ Ошибка проверки статуса", reply_markup=get_main_keyboard())


@router.message(F.text == "🔙 Назад")
async def handle_back(message: Message, state: FSMContext):
    """Возврат в главное меню."""
    current_state = await state.get_state()
    if current_state == CounterpartyCreationStates.ACTIVE:
        await state.clear()
        AppLogger().log_user_activity(message.from_user, "Выход из режима создания контрагента")
        await message.answer("❌ Режим создания контрагента отменен.", reply_markup=get_main_keyboard())
    else:
        AppLogger().log_user_activity(message.from_user, "Возврат в главное меню")
        await handle_start(message)


@router.message(F.text == "➕ Создание контрагента")
async def handle_counterparty_creation_start(message: Message, state: FSMContext):
    """Активация режима создания контрагента."""
    await state.set_state(CounterpartyCreationStates.ACTIVE)
    AppLogger().log_user_activity(message.from_user, "Активация режима создания контрагента")
    await message.answer(
        "📝 <b>Режим создания контрагента активирован</b>\n\n"
        "Теперь вы можете:\n"
        "• Отправлять номера телефонов для добавления\n"
        "• Загружать Excel-файлы с номерами\n\n"
        "Для выхода из режима нажмите <b>Назад</b>",
        parse_mode="HTML",
        reply_markup=get_counterparty_mode_keyboard()
    )


@router.message(F.text, CounterpartyCreationStates.ACTIVE)
async def handle_phone_number(message: Message, api: MoyskladAPI, phone_cache: CacheManager, config: Settings):
    """Обработка номеров телефонов в режиме создания контрагента."""
    AppLogger().log_user_activity(message.from_user, "Отправил текстовое сообщение", f"Текст: {message.text}")

    if not (phone := extract_phone(message.text)):
        AppLogger().log_user_activity(message.from_user, "Не удалось распознать номер", f"В сообщении: {message.text}")
        await message.answer("🔍 Не удалось распознать номер. Отправьте номер телефона в любом формате.",
                           reply_markup=get_counterparty_mode_keyboard())
        return

    AppLogger().log_user_activity(message.from_user, "Попытка добавить номер", f"Номер: {phone}")

    if phone_cache.has_counterparty(phone):
        AppLogger().log_user_activity(message.from_user, "Номер уже существует", f"Номер: {phone}")
        await message.answer(f"ℹ️ Номер {phone} уже в системе",
                           reply_markup=get_counterparty_mode_keyboard())
        return

    for attempt in range(1, config.MAX_ATTEMPTS + 1):
        try:
            async with aiohttp.ClientSession() as session:
                if await api.create_counterparty(session, phone):
                    AppLogger().log_user_activity(message.from_user, "Успешно добавил номер", f"Номер: {phone}")
                    phone_cache.add_counterparty(phone, "individual")
                    await message.answer(
                        f"✅ Номер {phone} успешно добавлен!",
                        reply_markup=get_counterparty_mode_keyboard()
                    )
                    return
        except Exception as e:
            AppLogger().log_user_activity(
                message.from_user,
                "Ошибка добавления номера",
                f"Номер: {phone}, попытка {attempt}, ошибка: {str(e)}"
            )
            if attempt < config.MAX_ATTEMPTS:
                await asyncio.sleep(config.RETRY_DELAY * attempt)

    AppLogger().log_user_activity(
        message.from_user,
        "Не удалось добавить номер",
        f"Номер: {phone} после {config.MAX_ATTEMPTS} попыток"
    )
    await message.answer(
        f"❌ Не удалось добавить номер {phone}",
        reply_markup=get_counterparty_mode_keyboard()
    )


@router.message(F.document, CounterpartyCreationStates.ACTIVE)
async def handle_excel_file(message: Message, api: MoyskladAPI, phone_cache: CacheManager, config: Settings):
    """Обработка Excel-файла с номерами в режиме создания контрагента."""
    AppLogger().log_user_activity(
        message.from_user,
        "Попытка загрузить файл",
        f"Имя файла: {message.document.file_name}, размер: {message.document.file_size} байт"
    )

    if not message.document.file_name.endswith(('.xlsx', '.xls')):
        AppLogger().log_user_activity(
            message.from_user,
            "Неподдерживаемый формат файла",
            message.document.file_name
        )
        await message.answer(
            "❌ Нужен файл Excel (.xlsx или .xls)",
            reply_markup=get_counterparty_mode_keyboard()
        )
        return

    if message.document.file_size > MAX_FILE_SIZE:
        AppLogger().log_user_activity(
            message.from_user,
            "Файл слишком большой",
            f"Размер: {message.document.file_size} байт"
        )
        await message.answer(
            "❌ Файл слишком большой (макс. 10МБ)",
            reply_markup=get_counterparty_mode_keyboard()
        )
        return

    try:
        file = await message.bot.download(message.document)
        phones = await process_excel(file)

        if not phones:
            AppLogger().log_user_activity(
                message.from_user,
                "Номера не найдены в файле",
                message.document.file_name
            )
            await message.answer(
                "🔍 В файле не найдены номера телефонов",
                reply_markup=get_counterparty_mode_keyboard()
            )
            return

        total = len(phones)
        AppLogger().log_user_activity(
            message.from_user,
            "Начал обработку файла",
            f"Файл: {message.document.file_name}, номеров: {total}"
        )
        msg = await message.answer(
            f"🔍 Найдено {total} номеров. Обработка...",
            reply_markup=get_counterparty_mode_keyboard()
        )

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
                    except Exception as e:
                        logger.warning(f"Не удалось отредактировать сообщение: {e}")
                        msg = await message.answer(f"⏳ Обработано {current_processed}/{total}...")

        result = (
            f"📊 <b>Итоги обработки файла:</b>\n\n"
            f"• Добавлено: <b>{success}</b>\n"
            f"• Пропущено (дубликаты): <b>{skipped}</b>\n"
            f"• Ошибок: <b>{len(failed)}</b>"
        )
        AppLogger().log_user_activity(
            message.from_user,
            "Завершил обработку файла",
            f"Файл: {message.document.file_name}, результат: {result}"
        )
        await message.answer(
            result,
            parse_mode="HTML",
            reply_markup=get_counterparty_mode_keyboard()
        )

    except Exception as e:
        AppLogger().log_user_activity(
            message.from_user,
            "Ошибка обработки файла",
            f"Файл: {message.document.file_name}, ошибка: {str(e)}"
        )
        await message.answer(
            "❌ Ошибка обработки файла",
            reply_markup=get_counterparty_mode_keyboard()
        )