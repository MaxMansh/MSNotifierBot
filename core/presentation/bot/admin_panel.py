import os
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv
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
    CHOOSE_ACTION = State()
    ADD_USER = State()
    REMOVE_USER = State()
    ADD_ADMIN = State()
    REMOVE_ADMIN = State()
    VIEW_LOG_FILE = State()


class CacheControlStates(StatesGroup):
    """Состояния для управления кэшем"""
    CHOOSE_ACTION = State()


def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура админ-панели"""
    buttons = [
        [KeyboardButton(text="📁 Логи"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="🗑️ Управление кэшем"), KeyboardButton(text="⏰ Следующая проверка")],
        [KeyboardButton(text="ℹ️ Информация о боте"), KeyboardButton(text="🔑 Права доступа")],
        [KeyboardButton(text="🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_access_control_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура управления доступом"""
    buttons = [
        [KeyboardButton(text="➕ Добавить пользователя"), KeyboardButton(text="➖ Удалить пользователя")],
        [KeyboardButton(text="➕ Добавить админа"), KeyboardButton(text="➖ Удалить админа")],
        [KeyboardButton(text="🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_cache_control_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура управления кэшем"""
    buttons = [
        [KeyboardButton(text="🔄 Обновить контрагентов"), KeyboardButton(text="🗑️ Очистить кэш")],
        [KeyboardButton(text="🔙 Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_log_files_keyboard(log_files: list) -> ReplyKeyboardMarkup:
    """Клавиатура для выбора лог-файлов"""
    buttons = [[KeyboardButton(text=file_name)] for file_name in log_files[:10]]
    buttons.append([KeyboardButton(text="🔙 Назад")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


class EnvManager:
    """Класс для работы с .env файлом"""

    def __init__(self):
        self.env_path = Path('.env')
        load_dotenv(self.env_path)

    def update_env_file(self, variable_name: str, ids: list):
        """Обновляет переменную в .env файле"""
        try:
            os.environ[variable_name] = ','.join(map(str, ids))

            lines = []
            if self.env_path.exists():
                with open(self.env_path, 'r') as f:
                    lines = f.readlines()

            new_lines = []
            var_found = False
            for line in lines:
                if line.startswith(f"{variable_name}="):
                    new_lines.append(f"{variable_name}={os.environ[variable_name]}\n")
                    var_found = True
                else:
                    new_lines.append(line)

            if not var_found:
                new_lines.append(f"{variable_name}={os.environ[variable_name]}\n")

            with open(self.env_path, 'w') as f:
                f.writelines(new_lines)

        except Exception as e:
            logger.error(f"Ошибка обновления .env файла: {str(e)}")
            raise


env_manager = EnvManager()


@router.message(F.text == "📁 Логи")
async def handle_logs(message: Message, state: FSMContext, paths: PathManager):
    """Обработка запроса логов"""
    AppLogger().log_user_activity(message.from_user, "Запросил список лог-файлов")
    try:
        log_files = sorted(os.listdir(paths.logs_dir), reverse=True)
        if not log_files:
            await message.answer("🟢 Лог-файлы отсутствуют.")
            return

        await message.answer(
            "📂 Выберите лог-файл:",
            reply_markup=get_log_files_keyboard(log_files)
        )
        await state.update_data(log_files=log_files)
        await state.set_state(AccessControlStates.VIEW_LOG_FILE)

    except Exception as e:
        AppLogger().log_user_activity(message.from_user, "Ошибка при получении списка логов", f"Ошибка: {str(e)}")
        await message.answer("❌ Ошибка при получении списка логов.")


@router.message(AccessControlStates.VIEW_LOG_FILE)
async def handle_view_log_file(message: Message, state: FSMContext, paths: PathManager):
    """Обработка выбора лог-файла"""
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
        await message.answer(
            "📂 Выберите лог-файл:",
            reply_markup=get_log_files_keyboard(log_files)
        )

    except Exception as e:
        AppLogger().log_user_activity(message.from_user, "Ошибка при отправке лог-файла", f"Ошибка: {str(e)}")
        await state.clear()
        await message.answer("❌ Ошибка при отправке файла логов.", reply_markup=get_admin_keyboard())


@router.message(F.text == "📊 Статистика")
async def handle_stats(message: Message):
    """Обработка запроса статистики"""
    AppLogger().log_user_activity(message.from_user, "Запросил статистику")
    stats_text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"• Запросов на добавление: {bot_stats['add_number']}\n"
        f"• Успешных операций: {bot_stats['success']}\n"
        f"• Ошибок: {bot_stats['failed']}\n"
        f"• Активных пользователей: {len(active_users)}"
    )
    await message.answer(stats_text, parse_mode="HTML", reply_markup=get_admin_keyboard())


@router.message(F.text == "ℹ️ Информация о боте")
async def handle_bot_info(message: Message, config: Settings):
    """Обработка запроса информации о боте"""
    AppLogger().log_user_activity(message.from_user, "Запросил информацию о боте")
    bot_info = (
        "🤖 <b>Информация о боте</b>\n\n"
        f"• Версия: 1.0.0\n"
        f"• Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"• Активных пользователей: {len(active_users)}\n"
        f"• Статус API: {'🟢 Работает' if config.MS_TOKEN else '🔴 Не работает'}"
    )
    await message.answer(bot_info, parse_mode="HTML", reply_markup=get_admin_keyboard())


@router.message(F.text == "⏰ Следующая проверка")
async def handle_next_check(message: Message, config: Settings):
    """Обработка запроса времени следующей проверки"""
    AppLogger().log_user_activity(message.from_user, "Запросил время проверки")
    next_check = datetime.now() + timedelta(minutes=config.CHECK_INTERVAL_MINUTES)
    await message.answer(f"⏳ Следующая проверка в {next_check.strftime('%H:%M:%S')}", reply_markup=get_admin_keyboard())


@router.message(F.text == "🔑 Права доступа")
async def handle_access_control(message: Message, state: FSMContext, config: Settings):
    """Обработка входа в меню управления доступом"""
    if message.from_user.id not in config.ADMIN_USER_IDS:
        AppLogger().log_user_activity(message.from_user, "Попытка входа без прав")
        await message.answer("🚫 У вас нет доступа к этой функции")
        return

    AppLogger().log_user_activity(message.from_user, "Открыл управление доступом")
    await state.set_state(AccessControlStates.CHOOSE_ACTION)
    await message.answer(
        "Выберите действие:",
        reply_markup=get_access_control_keyboard()
    )


@router.message(AccessControlStates.CHOOSE_ACTION)
async def handle_access_action(message: Message, state: FSMContext):
    """Обработка выбора действия в управлении доступом"""
    action_map = {
        "➕ Добавить пользователя": AccessControlStates.ADD_USER,
        "➖ Удалить пользователя": AccessControlStates.REMOVE_USER,
        "➕ Добавить админа": AccessControlStates.ADD_ADMIN,
        "➖ Удалить админа": AccessControlStates.REMOVE_ADMIN,
        "🔙 Назад": None
    }

    if message.text not in action_map:
        await message.answer("❌ Неизвестная команда")
        return

    if message.text == "🔙 Назад":
        await state.clear()
        await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())
        return

    await state.set_state(action_map[message.text])
    await message.answer(f"Введите ID {message.text[2:].lower()}:")


async def _handle_access_change(message: Message, state: FSMContext, config: Settings, user_type: str, action: str):
    """Общая функция для изменения прав доступа"""
    try:
        user_id = int(message.text)
        AppLogger().log_user_activity(message.from_user, f"Изменяет права для ID {user_id}")

        if user_type == "user":
            ids_list = config.ALLOWED_USER_IDS
            role_name = "Пользователь"
            env_var = "ALLOWED_USER_IDS"
        else:
            ids_list = config.ADMIN_USER_IDS
            role_name = "Админ"
            env_var = "ADMIN_USER_IDS"

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

        env_manager.update_env_file(env_var, ids_list)
        await message.answer("Выберите действие:", reply_markup=get_access_control_keyboard())

    except ValueError:
        await message.answer("❌ Некорректный ID. Введите числовое значение.")
        await message.answer("Выберите действие:", reply_markup=get_access_control_keyboard())

    await state.clear()


@router.message(F.text, AccessControlStates.ADD_USER)
async def handle_add_user(message: Message, state: FSMContext, config: Settings):
    """Обработка добавления пользователя"""
    await _handle_access_change(message, state, config, "user", "add")


@router.message(F.text, AccessControlStates.REMOVE_USER)
async def handle_remove_user(message: Message, state: FSMContext, config: Settings):
    """Обработка удаления пользователя"""
    await _handle_access_change(message, state, config, "user", "remove")


@router.message(F.text, AccessControlStates.ADD_ADMIN)
async def handle_add_admin(message: Message, state: FSMContext, config: Settings):
    """Обработка добавления админа"""
    await _handle_access_change(message, state, config, "admin", "add")


@router.message(F.text, AccessControlStates.REMOVE_ADMIN)
async def handle_remove_admin(message: Message, state: FSMContext, config: Settings):
    """Обработка удаления админа"""
    await _handle_access_change(message, state, config, "admin", "remove")


@router.message(F.text == "🗑️ Управление кэшем")
async def handle_cache_control_menu(message: Message, state: FSMContext):
    """Обработка входа в меню управления кэшем"""
    AppLogger().log_user_activity(message.from_user, "Открыл меню управления кэшем")
    await state.set_state(CacheControlStates.CHOOSE_ACTION)
    await message.answer(
        "Управление кэшем:",
        reply_markup=get_cache_control_keyboard()
    )


@router.message(CacheControlStates.CHOOSE_ACTION)
async def handle_cache_action(message: Message, state: FSMContext, phone_cache: CacheManager, api: MoyskladAPI):
    """Обработка выбора действия в управлении кэшем"""
    try:
        if message.text == "🔄 Обновить контрагентов":
            msg = await message.answer("🔄 Обновление списка контрагентов... (это может занять несколько минут)")
            try:
                success = await api.initialize_counterparties_cache(phone_cache)
                if success:
                    await msg.edit_text("✅ Список контрагентов успешно обновлен!")
                else:
                    await msg.edit_text("❌ Не удалось обновить список контрагентов")
            except Exception as e:
                await msg.edit_text(f"❌ Ошибка при обновлении: {str(e)}")

            await message.answer("Управление кэшем:", reply_markup=get_cache_control_keyboard())

        elif message.text == "🗑️ Очистить кэш":
            msg = await message.answer("🔄 Очистка кэша...")
            try:
                phone_cache.clear_cache()
                await msg.edit_text("✅ Кэш успешно очищен!")
            except Exception as e:
                await msg.edit_text(f"❌ Ошибка при очистке кэша: {str(e)}")

            await message.answer("Управление кэшем:", reply_markup=get_cache_control_keyboard())

        elif message.text == "🔙 Назад":
            await state.clear()
            await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())

    except Exception as e:
        logger.error(f"Ошибка в обработчике кэша: {str(e)}")
        await state.clear()
        await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())