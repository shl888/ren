"""
私人数据处理器 - 最简版本
只接收、存储、查看私人数据
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

from .binance_classifier import classify_binance_order, is_closing_event
from .okx_classifier import classify_okx_order


class PrivateDataProcessor:
    """私人数据处理器（单例模式）"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.memory_store = {'private_data': {}}
            self._initialized = True
            logger.info("✅ [私人数据处理] 模块已初始化")
    
    async def _delayed_delete(self, keys: List[str], symbol: str):
        """5分钟后删除该symbol所有当前存在的key（仅币安使用）"""
        try:
            await asyncio.sleep(300)
            
            if 'binance_order_update' not in self.memory_store['private_data']:
                return
                
            classified = self.memory_store['private_data']['binance_order_update'].get('classified', {})
            current_keys = [k for k in classified.keys() if k.startswith(f"{symbol}_")]
            
            for k in current_keys:
                del classified[k]
            
            if current_keys:
                logger.info(f"🧹 [币安订单] 延迟清理完成: {symbol} 已删除 {len(current_keys)}类")
                
        except Exception as e:
            logger.error(f"❌ [币安订单] 延迟清理失败: {e}")
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据
        格式：{'exchange': 'binance', 'data_type': 'account_update', 'data': {...}, 'timestamp': '...'}
        """
        try:
            exchange = private_data.get('exchange', 'unknown')
            raw_data = private_data.get('data', {})
            source = private_data.get('source', '')
            
            # ========== 币安订单更新处理 ==========
            if exchange == 'binance' and raw_data.get('e') == 'ORDER_TRADE_UPDATE':
                
                o = raw_data['o']
                
                # 过滤市价单未成交中间状态
                if o.get('o') == 'MARKET' and o.get('ot') == 'MARKET' and o.get('X') == 'NEW' and o.get('l') == '0' and o.get('z') == '0':
                    logger.debug(f"⏭️ [币安订单] 过滤市价单未成交中间状态: {o.get('i')}")
                    return
                
                # 分类
                category = classify_binance_order(private_data)
                logger.debug(f"🔍 [币安订单] 分类结果: {category}")
                
                symbol = raw_data['o']['s']
                classified_key = f"{symbol}_{category}"
                
                # 初始化存储
                if 'binance_order_update' not in self.memory_store['private_data']:
                    self.memory_store['private_data']['binance_order_update'] = {
                        'exchange': 'binance',
                        'data_type': 'order_update',
                        'classified': {}
                    }
                
                classified = self.memory_store['private_data']['binance_order_update']['classified']
                
                # 取消止损/止盈的立即清除
                if category == '11_取消止损':
                    stop_loss_key = f"{symbol}_03_设置止损"
                    if stop_loss_key in classified:
                        del classified[stop_loss_key]
                        logger.info(f"🗑️ [币安订单] {symbol} 取消止损，已删除设置止损记录")
                    return
                
                if category == '12_取消止盈':
                    take_profit_key = f"{symbol}_04_设置止盈"
                    if take_profit_key in classified:
                        del classified[take_profit_key]
                        logger.info(f"🗑️ [币安订单] {symbol} 取消止盈，已删除设置止盈记录")
                    return
                
                # 过期事件不保存
                if category in ['13_止损过期(被触发)', '14_止损过期(被取消)', 
                                '15_止盈过期(被触发)', '16_止盈过期(被取消)']:
                    logger.debug(f"⏭️ [币安订单] 过期事件不缓存: {category}")
                    return
                
                # 按分类存储
                if classified_key not in classified:
                    classified[classified_key] = []
                
                # 止盈止损的设置事件只保留最新一条
                if category in ['03_设置止损', '04_设置止盈']:
                    classified[classified_key] = []
                    logger.debug(f"🔄 [币安订单] {symbol} {category} 已清空旧记录")
                
                # 去重追加
                order_id = raw_data['o'].get('i')
                if order_id:
                    existing = False
                    for item in classified[classified_key]:
                        if item['data']['o'].get('i') == order_id:
                            existing = True
                            logger.debug(f"🔄 [币安订单] 跳过重复订单: {order_id}")
                            break
                    
                    if not existing:
                        classified[classified_key].append({
                            'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                            'received_at': private_data.get('received_at', datetime.now().isoformat()),
                            'data': raw_data
                        })
                else:
                    classified[classified_key].append({
                        'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                        'received_at': private_data.get('received_at', datetime.now().isoformat()),
                        'data': raw_data
                    })
                
                # 平仓处理：延迟5分钟清理
                if is_closing_event(category):
                    keys_to_delayed_delete = [k for k in classified.keys() if k.startswith(f"{symbol}_")]
                    asyncio.create_task(self._delayed_delete(keys_to_delayed_delete, symbol))
                    logger.info(f"⏰ [币安订单] 平仓标记: {symbol} 将在5分钟后清理")
                
                return
            
            # ========== OKX订单更新处理（纯净版：只分类存储，不清理）==========
            if exchange == 'okx' and private_data.get('data_type') == 'order_update':
                
                # 确保数据结构正确
                if 'data' not in raw_data or 'data' not in raw_data['data']:
                    logger.debug(f"⏭️ [OKX订单] 数据格式不正确")
                    return
                
                # 分类
                category = classify_okx_order(private_data)
                logger.debug(f"🔍 [OKX订单] 分类结果: {category}")
                
                # 获取交易对
                d = raw_data['data']['data'][0]
                symbol = d.get('instId', '').replace('-SWAP', '').replace('-USDT', '')
                if not symbol:
                    symbol = d.get('instId', 'unknown')
                
                classified_key = f"{symbol}_{category}"
                
                # 初始化存储
                if 'okx_order_update' not in self.memory_store['private_data']:
                    self.memory_store['private_data']['okx_order_update'] = {
                        'exchange': 'okx',
                        'data_type': 'order_update',
                        'classified': {}
                    }
                
                classified = self.memory_store['private_data']['okx_order_update']['classified']
                
                # 按分类存储（不过滤任何分类）
                if classified_key not in classified:
                    classified[classified_key] = []
                
                # 去重追加
                order_id = d.get('ordId')
                if order_id:
                    existing = False
                    for item in classified[classified_key]:
                        item_data = item['data']['data']['data'][0]
                        if item_data.get('ordId') == order_id:
                            existing = True
                            logger.debug(f"🔄 [OKX订单] 跳过重复订单: {order_id}")
                            break
                    
                    if not existing:
                        classified[classified_key].append({
                            'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                            'received_at': private_data.get('received_at', datetime.now().isoformat()),
                            'data': raw_data
                        })
                        logger.debug(f"📦 [OKX订单] {symbol} {category} 已追加")
                else:
                    classified[classified_key].append({
                        'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                        'received_at': private_data.get('received_at', datetime.now().isoformat()),
                        'data': raw_data
                    })
                    logger.debug(f"📦 [OKX订单] {symbol} {category} 已追加")
                
                # 没有清理逻辑，只有分类存储
                return
            
            # ========== 其他数据类型 ==========
            if source == 'http_fetcher':
                final_data_type = private_data.get('data_type', 'unknown')
            else:
                event_type = raw_data.get('e', 'unknown')
                
                if exchange == 'binance':
                    if event_type == 'TRADE_LITE':
                        logger.debug(f"📨 [私人数据处理] 过滤掉 TRADE_LITE 事件")
                        return
                    
                    binance_mapping = {
                        'ACCOUNT_UPDATE': 'account_update',
                        'ACCOUNT_CONFIG_UPDATE': 'account_config_update',
                        'MARGIN_CALL': 'risk_event',
                        'listenKeyExpired': 'system_event',
                        'balanceUpdate': 'balance_update',
                        'outboundAccountPosition': 'account_update',
                        'executionReport': 'order_update'
                    }
                    
                    if event_type in binance_mapping:
                        final_data_type = binance_mapping[event_type]
                    else:
                        final_data_type = event_type.lower()
                else:
                    final_data_type = private_data.get('data_type', 'unknown')
            
            # 存储其他数据
            storage_key = f"{exchange}_{final_data_type}"
            self.memory_store['private_data'][storage_key] = {
                'exchange': exchange,
                'data_type': final_data_type,
                'data': raw_data,
                'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                'received_at': private_data.get('received_at', datetime.now().isoformat())
            }
            
            logger.debug(f"✅ [私人数据处理] 已保存: {storage_key}")
            
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 接收数据失败: {e}")
    
    async def get_all_data(self) -> Dict[str, Any]:
        """获取所有私人数据概览"""
        try:
            formatted_data = {}
            for key, data in self.memory_store['private_data'].items():
                formatted_data[key] = {
                    "exchange": data.get('exchange'),
                    "data_type": data.get('data_type'),
                    "received_at": data.get('received_at'),
                    "timestamp": data.get('timestamp'),
                    "data_keys": list(data.get('data', {}).keys()) if isinstance(data.get('data'), dict) else type(data.get('data')).__name__
                }
            
            return {
                "timestamp": datetime.now().isoformat(),
                "total_count": len(self.memory_store['private_data']),
                "private_data": formatted_data,
                "note": "私人数据处理模块 - 只存储最新一份数据"
            }
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 获取所有数据失败: {e}")
            return {"timestamp": datetime.now().isoformat(), "error": str(e), "private_data": {}}
    
    async def get_data_by_exchange(self, exchange: str) -> Dict[str, Any]:
        """按交易所获取私人数据"""
        try:
            exchange_data = {}
            for key, data in self.memory_store['private_data'].items():
                if key.startswith(f"{exchange.lower()}_"):
                    if key in ['binance_order_update', 'okx_order_update']:
                        classified = data.get('classified', {})
                        summary = {}
                        for k, v in classified.items():
                            summary[k] = len(v)
                        
                        exchange_data[key] = {
                            "exchange": data.get('exchange'),
                            "data_type": data.get('data_type'),
                            "timestamp": data.get('timestamp'),
                            "received_at": data.get('received_at'),
                            "summary": summary,
                            "note": "各类别事件数量统计，详情请查询具体data_type"
                        }
                    else:
                        exchange_data[key] = {
                            "exchange": data.get('exchange'),
                            "data_type": data.get('data_type'),
                            "timestamp": data.get('timestamp'),
                            "received_at": data.get('received_at'),
                            "data": data.get('data')
                        }
            
            return {
                "exchange": exchange,
                "timestamp": datetime.now().isoformat(),
                "count": len(exchange_data),
                "data": exchange_data,
                "note": f"{exchange}私人数据"
            }
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 按交易所获取数据失败: {e}")
            return {"exchange": exchange, "timestamp": datetime.now().isoformat(), "error": str(e), "data": {}}
    
    async def get_data_detail(self, exchange: str, data_type: str) -> Dict[str, Any]:
        """获取特定私人数据详情"""
        try:
            key = f"{exchange.lower()}_{data_type.lower()}"
            
            if key in ['binance_order_update', 'okx_order_update']:
                if key in self.memory_store['private_data']:
                    return self.memory_store['private_data'][key]
                else:
                    return {
                        "exchange": exchange,
                        "data_type": data_type,
                        "classified": {},
                        "note": "暂无订单数据"
                    }
            
            if key in self.memory_store['private_data']:
                data = self.memory_store['private_data'][key]
                return {
                    "key": key,
                    "exchange": exchange,
                    "data_type": data_type,
                    "timestamp": data.get('timestamp'),
                    "received_at": data.get('received_at'),
                    "data": data.get('data'),
                    "note": "最新一份数据，新数据会覆盖旧数据"
                }
            else:
                return {
                    "error": f"未找到数据: {key}",
                    "available_keys": list(self.memory_store['private_data'].keys()),
                    "timestamp": datetime.now().isoformat()
                }
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 获取数据详情失败: {e}")
            return {"error": str(e), "exchange": exchange, "data_type": data_type, "timestamp": datetime.now().isoformat()}


# 全局单例实例
_global_processor = PrivateDataProcessor()

def get_processor():
    """获取处理器单例"""
    return _global_processor

async def receive_private_data(private_data):
    """供连接池调用的函数接口"""
    return await _global_processor.receive_private_data(private_data)