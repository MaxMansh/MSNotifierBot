from .notification import TelegramNotifier
from .scheduler import CheckerScheduler
from .entities.product import Product
from .bot import phone_router  # Добавляем новый роутер
from .notification import TelegramNotifier
from .scheduler import CheckerScheduler

__all__ = [
    'Product',
    'TelegramNotifier',
    'CheckerScheduler',
    'phone_router'  # Добавляем в экспорт
]
