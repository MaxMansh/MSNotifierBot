from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict
from core.entities.product import Product
from core.services.checkers.base import BaseChecker
from aiogram import html
from utils.logger import AppLogger

logger = AppLogger().get_logger(__name__)

class StockChecker(BaseChecker):
    async def process(self, products: List[Product]) -> None:
        cache = self.cache_manager.load()
        alerts_by_group = defaultdict(list)
        processed_count = 0
        alerted_count = 0
        stats = {'zero_stock': 0, 'below_min': 0}  # Добавляем stats

        for product in filter(lambda p: p.needs_stock_check, products):
            alert = self._check_product(product, cache, stats)  # Передаем stats
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
        logger.info(
            f"Проверка остатков завершена. "
            f"\nТоваров: {processed_count} "
            f"\nУведомлений: {alerted_count} "
            f"\nНулевых остатков: {stats['zero_stock']} "
            f"\nНиже минимума: {stats['below_min']}"
        )

    def _check_product(self, product: Product, cache: Dict, stats: dict) -> Optional[str]:
        cached_data = cache.get(product.id, {})
        current_stock = product.stock
        min_balance = product.min_balance
        alert = None

        is_zero = current_stock <= 0
        is_below_min = current_stock <= min_balance
        was_below_min = cached_data.get('below_min_reported', False)
        last_stock = cached_data.get('last_stock', None)

        logger.debug(
            f"Проверка товара {product.name} (ID: {product.id}): "
            f"остаток={current_stock}, минимум={min_balance}, "
            f"был ниже мин={was_below_min}, последний остаток={last_stock}"
        )

        # Логика для нулевого остатка
        if is_zero:
            stats['zero_stock'] += 1
            if not cached_data.get('zero_reported', False):
                alert = self._create_zero_stock_alert(product)
                logger.info(f"Обнаружен нулевой остаток: {product.name} (ID: {product.id})")
        # Логика для остатка ниже минимума
        elif is_below_min:
            stats['below_min'] += 1

            # Если ранее не было уведомления или остаток поднялся выше минимума и снова упал
            if not was_below_min or (last_stock is not None and last_stock > min_balance):
                alert = self._create_min_balance_alert(product)
                logger.info(f"Обнаружен остаток ниже минимума: {product.name} (ID: {product.id})")

        # Обновляем кэш
        cache[product.id] = {
            'below_min_reported': is_below_min,
            'zero_reported': is_zero,
            'last_stock': current_stock,
            'last_check': datetime.now().isoformat()
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