import asyncio
import signal
import ssl
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from core.services.api.moysklad import MoyskladAPI
from core.services.checkers import StockChecker, ExpirationChecker
from core.notification import TelegramNotifier
from core.scheduler import CheckerScheduler
from utils.cacher import CacheManager
from config.config import PathManager, Settings
from utils.logger import AppLogger


async def shutdown(scheduler, bot, logger):
    logger.info("Завершение работы...")
    await scheduler.stop()
    await bot.session.close()
    logger.info("Ресурсы освобождены")


async def main():
    # Инициализация конфигурации
    config = Settings()
    paths = PathManager(base_dir="data")

    # Инициализация логгера
    app_logger = AppLogger(logs_dir=paths.logs_dir, days_to_keep=config.DAYS_TO_KEEP)
    global logger
    logger = app_logger.get_logger("main")

    logger.info("Приложение запущено")

    try:
        logger.info("Инициализация бота...")
        # Добавляем таймаут для инициализации бота
        try:
            bot = Bot(
                token=config.BOT_TOKEN,
                default=DefaultBotProperties(parse_mode=ParseMode.HTML),
                timeout=30  # 30 секунд таймаут
            )
            logger.info("Бот успешно инициализирован")
        except Exception as e:
            logger.error(f"Ошибка инициализации бота: {str(e)}")
            raise

        logger.debug("Создание SSL контекста...")
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        logger.debug("Инициализация notifier...")
        notifier = TelegramNotifier(bot, config.CHAT_ID, config.TG_MESSAGE_LIMIT)

        logger.debug("Инициализация API клиента...")
        api = MoyskladAPI(config.MS_TOKEN)

        logger.debug("Инициализация кэшей...")
        stock_cache = CacheManager(paths.stocks_cache, config.CACHE_RESET_DAYS)
        exp_cache = CacheManager(paths.expiration_cache, config.CACHE_RESET_DAYS)

        logger.debug("Создание проверяющих...")
        checkers = [
            StockChecker(notifier, stock_cache),
            ExpirationChecker(notifier, exp_cache, alert_days=7)
        ]

        logger.debug("Создание планировщика...")
        scheduler = CheckerScheduler(
            api=api,
            checkers=checkers,
            check_interval=config.CHECK_INTERVAL_MINUTES,
            logger=logger
        )

        # Создаем задачу для обработки завершения работы
        shutdown_task = None

        def handle_signal():
            nonlocal shutdown_task
            if shutdown_task is None:
                shutdown_task = asyncio.create_task(shutdown(scheduler, bot, logger))

        # Настройка обработчиков сигналов только для Unix-систем
        if hasattr(signal, 'SIGINT'):
            try:
                loop = asyncio.get_running_loop()
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(sig, handle_signal)
            except NotImplementedError:
                # Windows не поддерживает add_signal_handler
                pass

        logger.debug("=== ЗАПУСК ОСНОВНОГО ЦИКЛА ===")
        try:
            await scheduler.run()
        except asyncio.CancelledError:
            # Обработка отмены задачи (например, при нажатии Ctrl+C)
            await shutdown(scheduler, bot, logger)

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
    finally:
        if 'bot' in locals():
            logger.debug("Закрытие сессии бота...")
            await bot.session.close()
        logger.info("Работа бота завершена")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass