"""
第一步：从已存储的原始数据中提取字段
只提取你指定的9种数据类型：
1. http_account
2. account_update(ORDER)
3. account_update(FUNDING_FEE)
4. order_update(_02_开仓(全部成交))
5. order_update(_03_设置止损)
6. order_update(_04_设置止盈)
7. order_update(_06_触发止损(全部成交))
8. order_update(_08_触发止盈(全部成交))
9. order_update(_10_主动平仓(全部成交))
"""
import logging
import asyncio
from typing import Dict, Any, List, Optional

from ..scheduler import get_scheduler

logger = logging.getLogger(__name__)


class Step1Extract:
    """第一步：字段提取"""

    # 你指定的6种订单事件类型（带全部成交的4种 + 2种设置）
    VALID_ORDER_EVENTS = {
        '_02_开仓(全部成交)',
        '_03_设置止损',
        '_04_设置止盈',
        '_06_触发止损(全部成交)',
        '_08_触发止盈(全部成交)',
        '_10_主动平仓(全部成交)'
    }

    def process_one(self, stored_item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        处理单条数据 - 供 manager 直接调用
        manager 不管 step1 有没有启动，直接调这个方法喂数据
        """
        exchange = stored_item.get('exchange', '').lower()
        data_type = stored_item.get('data_type', '')

        if exchange != 'binance':
            return []

        results = []

        # 1. http_account
        if data_type == 'http_account':
            result = self._extract_http(stored_item)
            if result:
                results.append(result)

        # 2. account_update
        elif data_type == 'account_update':
            result = self._extract_account(stored_item)
            if result:
                results.append(result)

        # 3. order_update
        elif data_type == 'order_update':
            results = self._extract_orders(stored_item)

        # 提取完成后，交给调度器（不管调度器有没有启动）
        if results:
            try:
                scheduler = get_scheduler()
                # 创建异步任务，不阻塞当前调用
                asyncio.create_task(scheduler.process_extracted(results))
                logger.debug(f"✅ 已将 {len(results)} 条提取结果交给调度器")
            except Exception as e:
                logger.error(f"❌ 交给调度器失败: {e}")

        return results

    # ------------------------------------------------------------------
    # http_account 提取
    # ------------------------------------------------------------------
    def _extract_http(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取 http_account 数据，返回单个字典"""
        data = item.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "http_account",
            "event_type": "http_account"
        }

        # 从 assets 中找 USDT
        assets = data.get('assets', [])
        for asset in assets:
            if asset.get('asset') == 'USDT':
                if asset.get('marginBalance') is not None:
                    result["账户资产额"] = asset['marginBalance']
                result["资产币种"] = "USDT"
                break

        # positions 数据（可能没有）
        positions = data.get('positions', [])
        if positions:
            pos = positions[0]
            if pos.get('initialMargin') is not None:
                result["标记价保证金"] = pos['initialMargin']
            if pos.get('notional') is not None:
                result["标记价仓位价值"] = pos['notional']
            if pos.get('unrealizedProfit') is not None:
                result["标记价浮盈"] = pos['unrealizedProfit']

        return result

    # ------------------------------------------------------------------
    # account_update 提取
    # ------------------------------------------------------------------
    def _extract_account(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取 account_update 数据，返回单个字典"""
        data = item.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "account_update"
        }

        m_type = data.get('a', {}).get('m', '')

        # ORDER 类型
        if m_type == 'ORDER':
            p_data = data.get('a', {}).get('P', [])
            if p_data:
                result["event_type"] = "account_order"
                if p_data[0].get('mt') is not None:
                    result["保证金模式"] = p_data[0]['mt']
                if p_data[0].get('ma') is not None:
                    result["保证金币种"] = p_data[0]['ma']
                return result

        # FUNDING_FEE 类型
        elif m_type == 'FUNDING_FEE':
            b_data = data.get('a', {}).get('B', [])
            if b_data:
                result["event_type"] = "account_funding"
                if b_data[0].get('bc') is not None:
                    result["本次资金费"] = b_data[0]['bc']
                if data.get('T') is not None:
                    result["本次资金费结算时间"] = data['T']
                return result

        return None

    # ------------------------------------------------------------------
    # order_update 提取（多条）
    # ------------------------------------------------------------------
    def _extract_orders(self, item: Dict) -> List[Dict[str, Any]]:
        """
        提取 order_update 数据，返回列表
        遍历 classified 中所有事件，只提取你指定的6种订单事件
        """
        classified = item.get('classified', {})
        if not classified:
            return []

        results = []
        for event_key, event_list in classified.items():
            # event_key 格式如 "TAKEUSDT_02_开仓(全部成交)"
            # 提取后面的部分，例如 "02_开仓(全部成交)"
            parts = event_key.split('_', 1)
            if len(parts) < 2:
                continue
            type_part = parts[1]  # 例如 "02_开仓(全部成交)"

            # 只提取你指定的6种订单事件
            if type_part not in self.VALID_ORDER_EVENTS:
                continue

            # 遍历该类型下的所有事件
            for event in event_list:
                data = event.get('data', {})
                o_data = data.get('o', {})

                result = {
                    "交易所": "binance",
                    "data_type": "order_update",
                    "event_type": event_key  # 保留完整key，包含合约名
                }

                # 根据事件类型提取字段
                if '开仓' in type_part:  # _02_开仓(全部成交)
                    if o_data.get('s') is not None:
                        result["开仓合约名"] = o_data['s']
                    if o_data.get('ps') is not None:
                        result["开仓方向"] = o_data['ps']
                    if o_data.get('ot') is not None:
                        result["开仓执行方式"] = o_data['ot']
                    if o_data.get('ap') is not None:
                        result["开仓价"] = o_data['ap']
                    if o_data.get('z') is not None:
                        result["持仓币数"] = o_data['z']
                    if o_data.get('n') is not None:
                        result["开仓手续费"] = o_data['n']
                    if o_data.get('N') is not None:
                        result["开仓手续费币种"] = o_data['N']
                    if o_data.get('T') is not None:
                        result["开仓时间"] = o_data['T']

                elif '设置止损' in type_part:  # _03_设置止损
                    if o_data.get('wt') is not None:
                        result["止损触发方式"] = o_data['wt']
                    if o_data.get('sp') is not None:
                        result["止损触发价"] = o_data['sp']

                elif '设置止盈' in type_part:  # _04_设置止盈
                    if o_data.get('wt') is not None:
                        result["止盈触发方式"] = o_data['wt']
                    if o_data.get('sp') is not None:
                        result["止盈触发价"] = o_data['sp']

                elif any(x in type_part for x in ['触发止损', '触发止盈', '主动平仓']):
                    # _06_触发止损(全部成交)、_08_触发止盈(全部成交)、_10_主动平仓(全部成交)
                    if o_data.get('ot') is not None:
                        result["平仓执行方式"] = o_data['ot']
                    if o_data.get('ap') is not None:
                        result["平仓价"] = o_data['ap']
                    if o_data.get('n') is not None:
                        result["平仓手续费"] = o_data['n']
                    if o_data.get('N') is not None:
                        result["平仓手续费币种"] = o_data['N']
                    if o_data.get('rp') is not None:
                        result["平仓收益"] = o_data['rp']
                    if o_data.get('T') is not None:
                        result["平仓时间"] = o_data['T']

                results.append(result)

        return results