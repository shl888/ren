"""
第一步：从已存储的原始数据中提取字段
"""
import logging
import asyncio
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class Step1Extract:
    """第一步：字段提取"""

    # ===== 订单数据白名单（只提取这些分类）=====
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
        logger.info(f"📋【Step1】有效订单事件: {self.VALID_ORDER_EVENTS}")

    async def receive(self, full_storage_item: Dict[str, Any]):
        """
        接收Manager塞进来的完整存储区数据
        格式: {'full_storage': {...}}
        用key精确路由到对应的处理函数
        """
        logger.info(f"🎯【Step1】收到完整存储区数据")
        
        try:
            # 获取完整存储区
            full_storage = full_storage_item.get('full_storage', {})
            if not full_storage:
                logger.warning(f"⚠️【Step1】收到空存储区")
                return
                
            logger.info(f"📦【Step1】存储区包含 {len(full_storage)} 个数据项")
            
            # 遍历存储区中的所有数据，用key精确路由
            all_results = []
            for key, data_item in full_storage.items():
                
                # ===== 币安订单数据 =====
                if key == 'binance_order_update':
                    logger.info(f"🔍【Step1】处理币安订单数据")
                    
                    pseudo_item = {
                        'exchange': 'binance',
                        'data_type': 'order_update',
                        'classified': data_item.get('classified', {})
                    }
                    
                    results = self._extract_orders(pseudo_item)
                    if results:
                        all_results.extend(results)
                        logger.info(f"✅【Step1】从 {key} 提取了 {len(results)} 条订单结果")
                
                # ===== 币安HTTP账户数据 =====
                elif key == 'binance_http_account':
                    logger.info(f"🔍【Step1】处理币安HTTP账户数据")
                    
                    pseudo_item = {
                        'exchange': 'binance',
                        'data_type': 'http_account',
                        'data': data_item.get('data', {})
                    }
                    
                    result = self._extract_http(pseudo_item)
                    if result:
                        all_results.append(result)
                        logger.info(f"✅【Step1】从 {key} 提取了HTTP账户数据")
                
                # ===== 币安WebSocket账户更新 =====
                elif key == 'binance_account_update':
                    logger.info(f"🔍【Step1】处理币安WebSocket账户更新")
                    
                    pseudo_item = {
                        'exchange': 'binance',
                        'data_type': 'account_update',
                        'data': data_item.get('data', {})
                    }
                    
                    result = self._extract_account(pseudo_item)
                    if result:
                        all_results.append(result)
                        logger.info(f"✅【Step1】从 {key} 提取了账户更新数据")
                
                # ===== 其他key：暂不处理 =====
                else:
                    logger.debug(f"⏭️【Step1】跳过未处理的key: {key}")
            
            # 将所有结果推入队列
            if all_results:
                for i, result in enumerate(all_results):
                    await self.output_queue.put(result)
                    logger.info(f"📤【Step1】第{i+1}条结果已推入队列: {result.get('event_type', 'unknown')}，队列大小: {self.output_queue.qsize()}")
            else:
                logger.warning(f"⚠️【Step1】完整存储区未提取到任何结果")
                    
        except Exception as e:
            logger.error(f"❌【Step1】处理失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _extract_http(self, item: Dict) -> Optional[Dict[str, Any]]:
        """
        提取 http_account 数据
        提取字段：
        - 账户资产额
        - 资产币种
        - 标记价保证金
        - 标记价仓位价值
        - 标记价浮盈
        """
        data = item.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "http_account",
            "event_type": "http_account"
        }

        # 提取资产信息
        assets = data.get('assets', [])
        for asset in assets:
            if asset.get('asset') == 'USDT':
                if asset.get('marginBalance') is not None:
                    result["账户资产额"] = asset['marginBalance']
                result["资产币种"] = "USDT"
                break

        # 提取持仓信息
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
        """
        提取 account_update 数据
        两种事件类型：
        1. ORDER事件 - 提取保证金模式、保证金币种
        2. FUNDING_FEE事件 - 提取本次资金费、结算时间
        """
        data = item.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "account_update"
        }

        m_type = data.get('a', {}).get('m', '')

        # ORDER事件：保证金模式更新
        if m_type == 'ORDER':
            p_data = data.get('a', {}).get('P', [])
            if p_data:
                result["event_type"] = "account_order"
                if p_data[0].get('mt') is not None:
                    result["保证金模式"] = p_data[0]['mt']
                if p_data[0].get('ma') is not None:
                    result["保证金币种"] = p_data[0]['ma']
                return result

        # FUNDING_FEE事件：资金费结算
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
        """
        提取 order_update 数据
        只提取 VALID_ORDER_EVENTS 中定义的事件：
        - _02_开仓(全部成交)
        - _03_设置止损
        - _04_设置止盈  
        - _06_触发止损(全部成交)
        - _08_触发止盈(全部成交)
        - _10_主动平仓(全部成交)
        """
        classified = item.get('classified', {})
        if not classified:
            logger.warning(f"⚠️【Step1】order_update无classified数据")
            return []

        logger.info(f"🔍【Step1】classified keys: {list(classified.keys())}")

        results = []
        for event_key, event_list in classified.items():
            # 提取事件类型（去掉合约名前缀）
            parts = event_key.split('_', 1)
            if len(parts) < 2:
                continue
            
            # 添加下划线前缀，匹配 VALID_ORDER_EVENTS 的格式
            event_type = f"_{parts[1]}"  # 变成 "_02_开仓(全部成交)"
            
            # 检查是否在白名单中
            if event_type not in self.VALID_ORDER_EVENTS:
                logger.debug(f"⏭️【Step1】跳过非白名单事件: {event_type}")
                continue

            logger.info(f"✅【Step1】处理白名单事件: {event_type}")

            for event in event_list:
                data = event.get('data', {})
                o_data = data.get('o', {})

                result = {
                    "交易所": "binance",
                    "data_type": "order_update",  # 加上data_type字段，保持格式一致
                    "event_type": event_key
                }

                # 开仓事件
                if '开仓' in parts[1]:
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

                # 设置止损
                elif '设置止损' in parts[1]:
                    if o_data.get('wt') is not None:
                        result["止损触发方式"] = o_data['wt']
                    if o_data.get('sp') is not None:
                        result["止损触发价"] = o_data['sp']

                # 设置止盈
                elif '设置止盈' in parts[1]:
                    if o_data.get('wt') is not None:
                        result["止盈触发方式"] = o_data['wt']
                    if o_data.get('sp') is not None:
                        result["止盈触发价"] = o_data['sp']

                # 平仓事件（触发止损、触发止盈、主动平仓）
                elif any(x in parts[1] for x in ['触发止损', '触发止盈', '主动平仓']):
                    if o_data.get('ot') is not None:
                        result["平仓执行方式"] = o_data['ot']
                    if o_data.get('ap') is not None:
                        result["平仓价"] = o_data['ap']
                    if o_data.get('n') is not None:
                        result["平仓手续费"] = o_data['n']
                    if o_data.get('N') is not None:
                        result["平仓手续费币种"] = o_data['N']
#                    if o_data.get('rp') is not None:
#                        result["平仓收益"] = o_data['rp']
                    if o_data.get('T') is not None:
                        result["平仓时间"] = o_data['T']

                results.append(result)

        logger.info(f"📊【Step1】订单提取完成，共 {len(results)} 条")
        return results