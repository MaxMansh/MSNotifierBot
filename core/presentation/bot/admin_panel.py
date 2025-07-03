import os
from datetime import datetime, timedelta
from collections import defaultdict
from aiogram import Router, F, types
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from core.api.moysklad import MoyskladAPI
from core.infrastructure import CacheManager
from core.config import Settings, PathManager
from core.infrastructure import AppLogger

router = Router()
logger = AppLogger().get_logger(__name__)

# Статистика бота
bot_stats = defaultdict(int)
active_users = set()


class AccessControlStates(StatesGroup):
    """Состояния для управления доступом"""
    CHOOSE_ACTION = State()  # Выбор действия в меню управления доступом
    ADD_USER = State()  # Добавление пользователя
    REMOVE_USER = State()  # Удаление пользователя
    ADD_ADMIN = State()  # Добавление админа
    REMOVE_ADMIN = State()  # Удаление админа
    VIEW_LOG_FILE = State()  # Состояние для просмотра лог-файлов


class CacheControlStates(StatesGroup):
    """Состояния для управления кэшем"""
    CHOOSE_ACTION = State()  # Выбор действия в меню управления кэшем


def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """Создает клавиатуру для админ-панели."""
    buttons = [
        [KeyboardButton(text="📁 Логи"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="🗑️ Управление кэшем"), KeyboardButton(text="⏰ Следующая проверка")],
        [KeyboardButton(text="ℹ️ Информация о боте"), KeyboardButton(text="🔑 Права доступа")],
        [KeyboardButton(text="🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_access_control_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для управления доступом."""
    buttons = [
        [KeyboardButton(text="➕ Добавить пользователя"), KeyboardButton(text="➖ Удалить пользователя")],
        [KeyboardButton(text="➕ Добавить админа"), KeyboardButton(text="➖ Удалить админа")],
        [KeyboardButton(text="🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_cache_control_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура для управления кэшем."""
    buttons = [
        [KeyboardButton(text="🔄 Обновить контрагентов"), KeyboardButton(text="🗑️ Очистить кэш")],
        [KeyboardButton(text="🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


@router.message(F.text == "📁 Логи")
async def handle_logs(message: Message, state: FSMContext, paths: PathManager):
    """Обработка запроса логов. Показывает список лог-файлов с кнопками."""
    AppLogger().log_user_activity(message.from_user, "Запросил список лог-файлов")
    try:
        log_files = sorted(os.listdir(paths.logs_dir), reverse=True)
        if not log_files:
            await message.answer("🟢 Лог-файлы отсутствуют.")
            return

        # Создаем клавиатуру с кнопками для выбора файла
        buttons = []
        for file_name in log_files[:10]:  # Ограничиваем 10 файлами
            buttons.append([KeyboardButton(text=file_name)])

        buttons.append([KeyboardButton(text="🔙 Назад")])  # Кнопка "Назад"
        keyboard = ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

        await message.answer("📂 Выберите лог-файл:", reply_markup=keyboard)

        # Сохраняем список лог-файлов в состояние
        await state.update_data(log_files=log_files)
        await state.set_state(AccessControlStates.VIEW_LOG_FILE)

    except Exception as e:
        AppLogger().log_user_activity(message.from_user, "Ошибка при получении списка логов", f"Ошибка: {str(e)}")
        await message.answer("❌ Ошибка при получении списка логов.")


@router.message(AccessControlStates.VIEW_LOG_FILE)
async def handle_view_log_file(message: Message, state: FSMContext, paths: PathManager):
    """Обработка выбора лог-файла из списка кнопок."""
    try:
        if message.text == "🔙 Назад":
            await state.clear()
            await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())
            return

        data = await state.get_data()
        log_files = data.get("log_files", [])

        if message.text not in log_files:
            AppLogger().log_user_activity(message.from_user, "Неверный выбор лог-файла", f"Имя файла: {message.text}")
            await message.answer("❌ Указанный файл не найден.")
            return

        log_path = paths.logs_dir / message.text
        if not log_path.exists():
            AppLogger().log_user_activity(message.from_user, "Файл не найден", f"Имя файла: {message.text}")
            await message.answer("❌ Файл не найден.")
            return

        await message.answer_document(
            document=types.FSInputFile(log_path),
            caption=f"📄 Вот ваш лог-файл: {message.text}"
        )
        AppLogger().log_user_activity(message.from_user, "Успешно отправлен лог-файл", f"Имя файла: {message.text}")

        # После отправки файла автоматически возвращаемся в админ-панель
        await state.clear()
        await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())

    except Exception as e:
        AppLogger().log_user_activity(message.from_user, "Ошибка при отправке лог-файла", f"Ошибка: {str(e)}")
        await message.answer("❌ Ошибка при отправке файла логов.")
        await state.clear()
        await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())


@router.message(F.text == "📊 Статистика")
async def handle_stats(message: Message):
    """Обработка запроса статистики."""
    AppLogger().log_user_activity(message.from_user, "Запросил статистику")
    stats_text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"• Запросов на добавление: {bot_stats['add_number']}\n"
        f"• Успешных операций: {bot_stats['success']}\n"
        f"• Ошибок: {bot_stats['failed']}\n"
        f"• Активных пользователей: {len(active_users)}"
    )
    await message.answer(stats_text, parse_mode="HTML")


@router.message(F.text == "🗑️ Управление кэшем")
async def handle_cache_control_menu(message: Message, state: FSMContext):
    """Обработка входа в меню управления кэшем."""
    AppLogger().log_user_activity(message.from_user, "Открыл меню управления кэшем")
    await state.set_state(CacheControlStates.CHOOSE_ACTION)
    await message.answer(
        "Управление кэшем:",
        reply_markup=get_cache_control_keyboard()
    )


@router.message(CacheControlStates.CHOOSE_ACTION)
async def handle_cache_action(message: Message, state: FSMContext, phone_cache: CacheManager, api: MoyskladAPI):
    """Обработка выбора действия в управлении кэшем."""
    AppLogger().log_user_activity(message.from_user, f"Выбрал действие в кэше: {message.text}")

    if message.text == "🔄 Обновить контрагентов":
        await message.answer("🔄 Обновление списка контрагентов...")
        try:
            success = await api.initialize_counterparties_cache(phone_cache)
            if success:
                await message.answer("✅ Список контрагентов успешно обновлен!")
            else:
                await message.answer("❌ Не удалось обновить список контрагентов")
        except Exception as e:
            await message.answer(f"❌ Ошибка при обновлении: {str(e)}")

    elif message.text == "🗑️ Очистить кэш":
        await message.answer("🔄 Очистка кэша...")
        try:
            phone_cache.clear_cache()
            await message.answer("✅ Кэш успешно очищен!")
        except Exception as e:
            await message.answer(f"❌ Ошибка при очистке кэша: {str(e)}")

    elif message.text == "🔙 Назад":
        await state.clear()
        await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())

    await state.clear()


@router.message(F.text == "⏰ Следующая проверка")
async def handle_next_check(message: Message, config: Settings):
    """Обработка запроса времени следующей проверки."""
    AppLogger().log_user_activity(message.from_user, "Запросил время проверки")
    next_check = datetime.now() + timedelta(minutes=config.CHECK_INTERVAL_MINUTES)
    await message.answer(f"⏳ Следующая проверка в {next_check.strftime('%H:%M:%S')}.")


@router.message(F.text == "ℹ️ Информация о боте")
async def handle_bot_info(message: Message, config: Settings):
    """Обработка запроса информации о боте."""
    AppLogger().log_user_activity(message.from_user, "Запросил информацию о боте")
    bot_info = (
        "🤖 <b>Информация о боте</b>\n\n"
        f"• Версия: 1.0.0\n"
        f"• Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"• Активных пользователей: {len(active_users)}\n"
        f"• Статус API: {'🟢 Работает' if config.MS_TOKEN else '🔴 Не работает'}"
    )
    await message.answer(bot_info, parse_mode="HTML")


@router.message(F.text == "🔑 Права доступа")
async def handle_access_control(message: Message, state: FSMContext):
    """Обработка входа в меню управления доступом."""
    AppLogger().log_user_activity(message.from_user, "Открыл управление доступом")
    await state.set_state(AccessControlStates.CHOOSE_ACTION)
    await message.answer(
        "Выберите действие:",
        reply_markup=get_access_control_keyboard()
    )


@router.message(AccessControlStates.CHOOSE_ACTION)
async def handle_access_action(message: Message, state: FSMContext):
    """Обработка выбора действия в управлении доступом."""
    AppLogger().log_user_activity(message.from_user, f"Выбрал действие: {message.text}")
    match message.text:
        case "➕ Добавить пользователя":
            await state.set_state(AccessControlStates.ADD_USER)
            await message.answer("Введите ID пользователя:")
        case "➖ Удалить пользователя":
            await state.set_state(AccessControlStates.REMOVE_USER)
            await message.answer("Введите ID пользователя:")
        case "➕ Добавить админа":
            await state.set_state(AccessControlStates.ADD_ADMIN)
            await message.answer("Введите ID админа:")
        case "➖ Удалить админа":
            await state.set_state(AccessControlStates.REMOVE_ADMIN)
            await message.answer("Введите ID админа:")
        case "🔙 Назад":
            await state.clear()
            await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())


@router.message(F.text, AccessControlStates.ADD_USER)
async def handle_add_user(message: Message, state: FSMContext, config: Settings):
    """Обработка добавления пользователя."""
    await _handle_access_change(message, state, config, "user", "add")


@router.message(F.text, AccessControlStates.REMOVE_USER)
async def handle_remove_user(message: Message, state: FSMContext, config: Settings):
    """Обработка удаления пользователя."""
    await _handle_access_change(message, state, config, "user", "remove")


@router.message(F.text, AccessControlStates.ADD_ADMIN)
async def handle_add_admin(message: Message, state: FSMContext, config: Settings):
    """Обработка добавления админа."""
    await _handle_access_change(message, state, config, "admin", "add")


@router.message(F.text, AccessControlStates.REMOVE_ADMIN)
async def handle_remove_admin(message: Message, state: FSMContext, config: Settings):
    """Обработка удаления админа."""
    await _handle_access_change(message, state, config, "admin", "remove")


async def _handle_access_change(message: Message, state: FSMContext, config: Settings, user_type: str, action: str):
    """Общая функция для изменения прав доступа."""
    try:
        user_id = int(message.text)
        AppLogger().log_user_activity(message.from_user, f"Изменяет права для ID {user_id}")

        if user_type == "user":
            ids_list = config.ALLOWED_USER_IDS
            role_name = "Пользователь"
        else:
            ids_list = config.ADMIN_USER_IDS
            role_name = "Админ"

        if action == "add":
            if user_id not in ids_list:
                ids_list.append(user_id)
                await message.answer(f"✅ {role_name} {user_id} добавлен.")
            else:
                await message.answer(f"ℹ️ {role_name} {user_id} уже есть в списке.")
        else:
            if user_id in ids_list:
                ids_list.remove(user_id)
                await message.answer(f"✅ {role_name} {user_id} удален.")
            else:
                await message.answer(f"ℹ️ {role_name} {user_id} отсутствует в списке.")

    except ValueError:
        AppLogger().log_user_activity(message.from_user, "Некорректный ID пользователя", f"ID: {message.text}")
        await message.answer("❌ Некорректный ID. Введите числовое значение.")

    await state.clear()