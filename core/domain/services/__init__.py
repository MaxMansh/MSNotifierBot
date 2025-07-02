from core.domain.services.notify import TelegramNotifier
from core.domain.scheduler import CheckerScheduler
from core.domain.entities import Product
from core.presentation.bot import phone_router  # Добавляем новый роутер
from core.domain.services.notify import TelegramNotifier
from core.domain.scheduler import CheckerScheduler

__all__ = [
    'Product',
    'TelegramNotifier',
    'CheckerScheduler',
    'phone_router'  # Добавляем в экспорт
]
