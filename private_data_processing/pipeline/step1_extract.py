"""
第一步：从已存储的原始数据中提取字段
"""
import logging
import asyncio
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class Step1Extract:
    """第一步：字段提取"""

    VALID_ORDER_EVENTS = {
        '_02_开仓(全部成交)',
        '_03_设置止损',
        '_04_设置止盈',
        '_06_触发止损(全部成交)',
        '_08_触发止盈(全部成交)',
        '_10_主动平仓(全部成交)'
    }

    def __init__(self, output_queue: asyncio.Queue):
        self.output_queue = output_queue
        logger.info("✅【Step1】字段提取器已创建")

    async def receive(self, stored_item: Dict[str, Any]):
        """
        接收Manager塞进来的原始数据
        """
        # ===== 调试日志1：确认receive被调用 =====
        logger.info(f"🎯【Step1】receive被调用！数据类型: {stored_item.get('data_type')}, 交易所: {stored_item.get('exchange')}")
        
        try:
            results = self.process(stored_item)
            
            # ===== 调试日志2：确认提取结果数量 =====
            logger.info(f"🔍【Step1】提取完成，结果数: {len(results)}")
            
            if results:
                for i, result in enumerate(results):
                    await self.output_queue.put(result)
                    # ===== 调试日志3：确认每条结果推入队列 =====
                    logger.info(f"📤【Step1】第{i+1}条结果已推入队列: {result.get('event_type')}, 队列大小: {self.output_queue.qsize()}")
            else:
                logger.warning(f"⚠️【Step1】提取结果为空！数据类型: {stored_item.get('data_type')}")
                    
        except Exception as e:
            logger.error(f"❌【Step1】处理失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def process(self, stored_item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        处理单条数据 - 纯提取逻辑
        """
        exchange = stored_item.get('exchange', '').lower()
        data_type = stored_item.get('data_type', '')

        # ===== 调试日志4：确认进入process =====
        logger.info(f"🔍【Step1】process开始: exchange={exchange}, data_type={data_type}")

        if exchange != 'binance':
            logger.warning(f"⚠️【Step1】非币安数据，跳过: {exchange}")
            return []

        results = []

        # 1. http_account
        if data_type == 'http_account':
            logger.info(f"🔍【Step1】处理http_account...")
            result = self._extract_http(stored_item)
            if result:
                results.append(result)
                logger.info(f"✅【Step1】http_account提取成功")
            else:
                logger.warning(f"⚠️【Step1】http_account提取为空")

        # 2. account_update
        elif data_type == 'account_update':
            logger.info(f"🔍【Step1】处理account_update...")
            result = self._extract_account(stored_item)
            if result:
                results.append(result)
                logger.info(f"✅【Step1】account_update提取成功: {result.get('event_type')}")
            else:
                logger.warning(f"⚠️【Step1】account_update提取为空")

        # 3. order_update
        elif data_type == 'order_update':
            logger.info(f"🔍【Step1】处理order_update...")
            results = self._extract_orders(stored_item)
            logger.info(f"✅【Step1】order_update提取完成，共{len(results)}条")

        else:
            logger.warning(f"⚠️【Step1】未知数据类型，不处理: {data_type}")

        return results

    def _extract_http(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取 http_account 数据"""
        data = item.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "http_account",
            "event_type": "http_account"
        }

        assets = data.get('assets', [])
        for asset in assets:
            if asset.get('asset') == 'USDT':
                if asset.get('marginBalance') is not None:
                    result["账户资产额"] = asset['marginBalance']
                result["资产币种"] = "USDT"
                break

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

    def _extract_account(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取 account_update 数据"""
        data = item.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "account_update"
        }

        m_type = data.get('a', {}).get('m', '')

        if m_type == 'ORDER':
            p_data = data.get('a', {}).get('P', [])
            if p_data:
                result["event_type"] = "account_order"
                if p_data[0].get('mt') is not None:
                    result["保证金模式"] = p_data[0]['mt']
                if p_data[0].get('ma') is not None:
                    result["保证金币种"] = p_data[0]['ma']
                return result

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

    def _extract_orders(self, item: Dict) -> List[Dict[str, Any]]:
        """提取 order_update 数据"""
        classified = item.get('classified', {})
        if not classified:
            logger.warning(f"⚠️【Step1】order_update无classified数据")
            return []

        results = []
        for event_key, event_list in classified.items():
            parts = event_key.split('_', 1)
            if len(parts) < 2:
                continue
            type_part = parts[1]

            if type_part not in self.VALID_ORDER_EVENTS:
                continue

            for event in event_list:
                data = event.get('data', {})
                o_data = data.get('o', {})

                result = {
                    "交易所": "binance",
                    "data_type": "order_update",
                    "event_type": event_key
                }

                if '开仓' in type_part:
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

                elif '设置止损' in type_part:
                    if o_data.get('wt') is not None:
                        result["止损触发方式"] = o_data['wt']
                    if o_data.get('sp') is not None:
                        result["止损触发价"] = o_data['sp']

                elif '设置止盈' in type_part:
                    if o_data.get('wt') is not None:
                        result["止盈触发方式"] = o_data['wt']
                    if o_data.get('sp') is not None:
                        result["止盈触发价"] = o_data['sp']

                elif any(x in type_part for x in ['触发止损', '触发止盈', '主动平仓']):
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
