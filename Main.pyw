import asyncio
import signal
import sys
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from core.api.moysklad import MoyskladAPI
from core.domain.services.checkers import StockChecker, ExpirationChecker
from core.domain.services.notify import TelegramNotifier
from core.domain.scheduler import CheckerScheduler
from core.infrastructure import CacheManager
from core.config import PathManager, Settings
from core.infrastructure import AppLogger
from core.presentation.bot.handlers import router as phone_router


class BotApplication:
    def __init__(self):
        self.scheduler_task = None
        self.dp = None
        self.bot = None
        self.scheduler = None
        self.logger = None
        self.config = None
        self.paths = None
        self.phone_cache = None

    async def shutdown(self, sig=None):
        """Корректное завершение работы"""
        if self.logger:
            self.logger.info(f"Начало остановки по сигналу {sig.name if sig else 'вручную'}")

        tasks = []
        if self.scheduler_task:
            self.scheduler_task.cancel()
            tasks.append(self.scheduler_task)

        if self.dp:
            tasks.append(self.dp.stop_polling())

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if self.bot:
            await self.bot.session.close()

        if self.logger:
            self.logger.info("Бот полностью остановлен")

    async def setup(self):
        """Инициализация компонентов"""
        # Настройка для Windows
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        # Инициализация конфигурации
        self.config = Settings()
        self.paths = PathManager(base_dir="data")

        # Инициализация логгера
        app_logger = AppLogger(
            logs_dir=self.paths.logs_dir,
            days_to_keep=self.config.DAYS_TO_KEEP
        )
        self.logger = app_logger.get_logger("main")
        self.logger.info("Инициализация приложения")

        # Инициализация бота
        self.bot = Bot(
            token=self.config.BOT_TOKEN,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
            timeout=30
        )
        self.logger.info("Бот инициализирован")

        # Инициализация API
        api = MoyskladAPI(self.config.MS_TOKEN)
        self.logger.info("API клиент создан")

        # Инициализация сервисов
        notifier = TelegramNotifier(
            self.bot,
            self.config.CHAT_ID,
            self.config.TG_MESSAGE_LIMIT
        )

        stock_cache = CacheManager(
            self.paths.stocks_cache,
            self.config.CACHE_RESET_DAYS
        )
        exp_cache = CacheManager(
            self.paths.expiration_cache,
            self.config.CACHE_RESET_DAYS
        )

        checkers = [
            StockChecker(notifier, stock_cache),
            ExpirationChecker(notifier, exp_cache, alert_days=7)
        ]

        self.scheduler = CheckerScheduler(
            api=api,
            checkers=checkers,
            check_interval=self.config.CHECK_INTERVAL_MINUTES,
            logger=self.logger
        )

        # Инициализация кэша телефонов
        self.phone_cache = CacheManager(
            self.paths.phone_cache,
            self.config.CACHE_RESET_DAYS
        )

        self.phone_cache._save_cache()

        # Загрузка контрагентов через API
        if not await api.initialize_counterparties_cache(self.phone_cache):
            self.logger.warning("Не удалось загрузить контрагентов при старте")

        # Настройка диспетчера
        self.dp = Dispatcher()
        self.dp["api"] = api
        self.dp["phone_cache"] = self.phone_cache
        self.dp["config"] = self.config
        self.dp.include_router(phone_router)

        # Настройка обработчиков сигналов
        if hasattr(signal, 'SIGINT'):
            try:
                loop = asyncio.get_running_loop()
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(
                        sig,
                        lambda s=sig: asyncio.create_task(self.shutdown(s)))
            except NotImplementedError:
                self.logger.warning("Обработчики сигналов не поддерживаются")

    async def run(self):
        """Основной цикл работы"""
        try:
            await self.setup()

            self.logger.info("Запуск основных задач")
            self.scheduler_task = asyncio.create_task(self.scheduler.run())

            await self.dp.start_polling(self.bot)
        except asyncio.CancelledError:
            self.logger.info("Получен сигнал отмены")
        except Exception as e:
            self.logger.critical(f"Критическая ошибка: {str(e)}", exc_info=True)
        finally:
            await self.shutdown()


if __name__ == "__main__":
    app = BotApplication()

    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        asyncio.run(app.run())
    except KeyboardInterrupt:
        if app.logger:
            app.logger.info("Приложение остановлено вручную")
        else:
            print("Приложение остановлено вручную")
    except Exception as e:
        if app.logger:
            app.logger.critical(f"Фатальная ошибка: {str(e)}", exc_info=True)
        else:
            print(f"Фатальная ошибка до инициализации логгера: {str(e)}")