from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict
from core.entities.product import Product
from core.services.checkers.base import BaseChecker
from aiogram import html
import logging

logger = logging.getLogger(__name__)

class StockChecker(BaseChecker):
    async def process(self, products: List[Product]) -> None:
        cache = self.cache_manager.load()
        alerts_by_group = defaultdict(list)
        processed_count = 0
        alerted_count = 0

        for product in filter(lambda p: p.needs_stock_check, products):
            alert = self._check_product(product, cache)
            if alert:
                alerts_by_group[product.group_path].append(alert)
                alerted_count += 1
            processed_count += 1

        if alerts_by_group:
            logger.info(f"Найдено товаров для уведомления: {alerted_count}")
            for group_path, alerts in alerts_by_group.items():
                header = f"📊 {html.bold('УВЕДОМЛЕНИЯ ПО ОСТАТКАМ')} ({group_path})"
                await self.notifier.send(header, alerts)

        self.cache_manager.save(cache)
        logger.info(f"Проверка остатков завершена. Товаров: {processed_count}, Уведомлений: {alerted_count}")

    def _check_product(self, product: Product, cache: Dict) -> Optional[str]:
        cached_data = cache.get(product.id, {})
        current_stock = product.stock
        min_balance = product.min_balance
        alert = None

        if current_stock <= min_balance and not cached_data.get('below_min_reported', False):
            alert = self._create_min_balance_alert(product)
            cache[product.id] = {
                'below_min_reported': True,
                'zero_reported': False,
                'last_stock': current_stock
            }
        else:
            # Обновляем кэш, но не отправляем уведомление
            cache[product.id] = {
                'below_min_reported': cached_data.get('below_min_reported', False),
                'zero_reported': cached_data.get('zero_reported', False),
                'last_stock': current_stock
            }

        return alert

    def _create_min_balance_alert(self, product: Product) -> str:
        return (
            f"⚠️ {html.bold(f'Товар: {product.name} достиг минимума!')}\n"
            f"▸ Остаток: {int(product.stock)} (минимум: {int(product.min_balance)})\n"
            f"▸ {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )

    def _create_zero_stock_alert(self, product: Product) -> str:
        return (
            f"🛑️ {html.bold(f'Товар: {product.name} закончился!')}\n"
            f"▸ Остаток: {int(product.stock)} (минимум: {int(product.min_balance)})\n"
            f"▸ {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        )