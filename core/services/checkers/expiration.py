from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict
from core.entities.product import Product
from core.services.checkers.base import BaseChecker
from aiogram import html
import logging

logger = logging.getLogger(__name__)

class ExpirationChecker(BaseChecker):
    def __init__(self, notifier, cache_manager, alert_days: int = 7):
        super().__init__(notifier, cache_manager)
        self.alert_days = alert_days

    async def process(self, products: List[Product]) -> None:
        cache = self.cache_manager.load()
        alerts_by_group = defaultdict(list)
        expired_count = 0
        near_expired_count = 0

        for product in filter(lambda p: p.needs_expiration_check, products):
            alert, is_expired = self._check_product(product, cache)
            if alert:
                alerts_by_group[product.group_path].append(alert)
                if is_expired:
                    expired_count += 1
                else:
                    near_expired_count += 1

        if alerts_by_group:
            for group_path, alerts in alerts_by_group.items():
                header = f"⏳ {html.bold('УВЕДОМЛЕНИЯ ПО СРОКАМ ГОДНОСТИ')} ({group_path})"
                await self.notifier.send(header, alerts)

        self.cache_manager.save(cache)
        logger.info(f"Expiration check completed. Expired: {expired_count}, Near expired: {near_expired_count}")

    def _check_product(self, product: Product, cache: Dict) -> (Optional[str], bool):
        days_left = (product.expiration_date - datetime.now()).days
        cached_data = cache.get(product.id, {})

        if days_left < 0 and not cached_data.get('was_expired', False):
            cache[product.id] = {'was_expired': True}
            return self._create_expired_alert(product), True

        if 0 <= days_left <= self.alert_days and not cached_data.get('was_near_expired', False):
            cache[product.id] = {'was_near_expired': True}
            return self._create_near_expired_alert(product, days_left), False

        return None, False

    def _create_expired_alert(self, product: Product) -> str:
        return (
            f"🚨 {html.bold('ПРОСРОЧЕННЫЙ ТОВАР')}\n"
            f"▸ Товар: {product.name}\n"
            f"▸ Срок истёк: {product.expiration_date.strftime('%d.%m.%Y')}"
        )

    def _create_near_expired_alert(self, product: Product, days_left: int) -> str:
        emoji = "🔴" if days_left <= 3 else "🟡"
        return (
            f"{emoji} {html.bold('ТОВАР С ИСТЕКАЮЩИМ СРОКОМ')}\n"
            f"▸ Товар: {product.name}\n"
            f"▸ Срок: {product.expiration_date.strftime('%d.%m.%Y')}\n"
            f"▸ Осталось дней: {days_left}"
        )