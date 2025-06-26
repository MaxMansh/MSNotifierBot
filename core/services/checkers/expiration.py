from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict
from core.entities.product import Product
from core.services.checkers.base import BaseChecker
from core.notification import ExpirationAlert
from utils.logger import AppLogger

logger = AppLogger().get_logger(__name__)

class ExpirationChecker(BaseChecker):
    """
    Класс для проверки сроков годности товаров.
    Отправляет уведомления о просроченных или истекающих сроках.
    """

    def __init__(self, notifier, cache_manager, alert_days: int = 7):
        """
                :param notifier: Объект для отправки уведомлений.
                :param cache_manager: Объект для управления кэшем.
                :param alert_days: Количество дней для предупреждения об истекающем сроке.
                """
        super().__init__(notifier, cache_manager)
        self.alert_days = alert_days

    async def process(self, products: List[Product]) -> None:
        """
        Проверяет сроки годности товаров и отправляет уведомления.
        :param products: Список товаров для проверки.
        """
        cache = self.cache_manager.load()
        logger.info("Идёт обработка товаров...")
        alerts_by_group = defaultdict(list)
        expired_count = 0
        processed_count = 0
        alerted_count = 0
        near_expired_count = 0

        for product in filter(lambda p: p.needs_expiration_check, products):
            processed_count += 1
            alert, is_expired = self._check_product(product, cache)
            if alert:
                alerts_by_group[product.group_path].append(alert)
                alerted_count += 1
                if is_expired:
                    expired_count += 1
                else:
                    near_expired_count += 1

        if alerts_by_group:
            logger.info(f"Найдено товаров для уведомления: {alerted_count}")
            for group_path, alerts in alerts_by_group.items():
                header = ExpirationAlert.create_expiration_header(group_path)
                await self.notifier.send(header, alerts)

        self.cache_manager.save(cache)
        logger.info(f"Проверка сроков годности завершена."
                    f"\nТоваров: {processed_count}"
                    f"\nУведомлений: {len(alerts_by_group)}"
                    f"\nПросроченных: {expired_count} "
                    f"\nС истекающим сроком: {near_expired_count}"
        )

    def _check_product(self, product: Product, cache: Dict) -> (Optional[str], bool):
        """
               Проверяет срок годности товара и возвращает уведомление и статус просроченности.
               :param product: Товар для проверки.
               :param cache: Кэш данных по товарам.
               :return: Кортеж (уведомление, является ли товар просроченным).
               """
        days_left = (product.expiration_date - datetime.now()).days
        cached_data = cache.get(product.id, {})

        logger.debug(f"Товар {product.name} (ID: {product.id}): дней осталось={days_left}")

        # Преобразуем expiration_date в строку для JSON
        expiration_date_str = product.expiration_date.strftime('%Y-%m-%d')

        # Проверяем, изменилась ли дата истечения срока
        if cached_data.get('expiration_date') != expiration_date_str:
            # Если дата изменилась, сбрасываем кэш для этого товара
            cached_data = {'expiration_date': expiration_date_str}
            cache[product.id] = cached_data

        # Проверяем, нужно ли отправить уведомление
        if days_left < 0 and not cached_data.get('was_expired', False):
            cache[product.id] = {**cached_data, 'was_expired': True}
            return ExpirationAlert.create_expired_alert(product), True

        is_near_expired = 0 <= days_left <= self.alert_days
        if is_near_expired:
            if days_left >= 7 and not cached_data.get('was_notified_7_days', False):
                cache[product.id] = {**cached_data, 'was_notified_7_days': True}
                return ExpirationAlert.create_near_expired_alert(product, days_left), False
            if days_left <= 3 and not cached_data.get('was_notified_3_days', False):
                cache[product.id] = {**cached_data, 'was_notified_3_days': True}
                return ExpirationAlert.create_near_expired_alert(product, days_left), False

        return None, False

