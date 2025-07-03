import os
from aiogram import Router, F, types
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from core.api.moysklad import MoyskladAPI
from core.infrastructure import CacheManager
from core.config import PathManager
from core.infrastructure import AppLogger

router = Router()
logger = AppLogger().get_logger(__name__)

class AccessControlStates(StatesGroup):
    """Состояния для просмотра логов"""
    VIEW_LOG_FILE = State()

class CacheControlStates(StatesGroup):
    """Состояния для управления кэшем"""
    CHOOSE_ACTION = State()

def get_admin_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура админ-панели"""
    buttons = [
        [KeyboardButton(text="📁 Логи"), KeyboardButton(text="🗑️ Управление кэшем")],
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

@router.message(F.text == "🔐 Админ-панель")
async def show_admin_panel(message: Message):
    """Показ админ-панели"""
    await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())

@router.message(F.text == "📁 Логи")
async def handle_logs(message: Message, state: FSMContext, paths: PathManager):
    """Обработка запроса логов"""
    try:
        log_files = sorted(os.listdir(paths.logs_dir), reverse=True)
        if not log_files:
            await message.answer("🟢 Лог-файлы отсутствуют.")
            return

        await message.answer(
            "📂 Выберите лог-файл:",
            reply_markup=get_log_files_keyboard(log_files)
        )
        await state.set_state(AccessControlStates.VIEW_LOG_FILE)
        await state.update_data(log_files=log_files)

    except Exception as e:
        logger.error(f"Ошибка при получении логов: {str(e)}")
        await message.answer("❌ Ошибка при получении списка логов.")

@router.message(AccessControlStates.VIEW_LOG_FILE)
async def handle_log_selection(message: Message, state: FSMContext, paths: PathManager):
    """Обработка выбора лог-файла"""
    if message.text == "🔙 Назад":
        await state.clear()
        await show_admin_panel(message)
        return

    data = await state.get_data()
    log_files = data.get("log_files", [])

    if message.text not in log_files:
        await message.answer("❌ Указанный файл не найден.")
        return

    log_path = paths.logs_dir / message.text
    if not log_path.exists():
        await message.answer("❌ Файл не найден.")
        return

    try:
        await message.answer_document(
            document=types.FSInputFile(log_path),
            caption=f"📄 Вот ваш лог-файл: {message.text}"
        )
    except Exception as e:
        logger.error(f"Ошибка отправки лог-файла: {str(e)}")
        await message.answer("❌ Ошибка при отправке файла логов.")

@router.message(F.text == "🗑️ Управление кэшем")
async def handle_cache_control_menu(message: Message, state: FSMContext):
    """Обработка входа в меню управления кэшем"""
    await state.set_state(CacheControlStates.CHOOSE_ACTION)
    await message.answer(
        "Управление кэшем:",
        reply_markup=get_cache_control_keyboard()
    )

@router.message(CacheControlStates.CHOOSE_ACTION)
async def handle_cache_action(message: Message, state: FSMContext, phone_cache: CacheManager, api: MoyskladAPI):
    """Обработка выбора действия в управлении кэшем"""
    if message.text == "🔙 Назад":
        await state.clear()
        await show_admin_panel(message)
        return

    try:
        if message.text == "🔄 Обновить контрагентов":
            msg = await message.answer("🔄 Обновление списка контрагентов...")
            try:
                success = await api.initialize_counterparties_cache(phone_cache)
                await msg.edit_text("✅ Список контрагентов успешно обновлен!" if success
                                   else "❌ Не удалось обновить список контрагентов")
            except Exception as e:
                await msg.edit_text(f"❌ Ошибка при обновлении: {str(e)}")

        elif message.text == "🗑️ Очистить кэш":
            msg = await message.answer("🔄 Очистка кэша...")
            try:
                phone_cache.clear_cache()
                await msg.edit_text("✅ Кэш успешно очищен!")
            except Exception as e:
                await msg.edit_text(f"❌ Ошибка при очистке кэша: {str(e)}")

        await message.answer("Управление кэшем:", reply_markup=get_cache_control_keyboard())

    except Exception as e:
        logger.error(f"Ошибка в обработчике кэша: {str(e)}")
        await state.clear()
        await message.answer("🔐 Админ-панель", reply_markup=get_admin_keyboard())
