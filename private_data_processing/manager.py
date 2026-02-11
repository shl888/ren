"""
私人数据处理器 - 增加币安订单分类缓存
只修改 receive_private_data 方法，其他完全不变
"""
import logging
from datetime import datetime
from typing import Dict, Any

from .classifier import classify_binance_order
from .cache_manager import save_order_event, clear_symbol_cache

logger = logging.getLogger(__name__)

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
            # 仿制大脑的存储结构
            self.memory_store = {'private_data': {}}
            self._initialized = True
            logger.info("✅ [私人数据处理] 模块已初始化")
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据（仿制大脑接口）
        格式：{'exchange': 'binance', 'data_type': 'account_update', 'data': {...}, 'timestamp': '...'}
        """
        try:
            exchange = private_data.get('exchange', 'unknown')
            raw_data = private_data.get('data', {})
            source = private_data.get('source', '')
            
            # 🔴【新增】判断是否是币安订单更新
            is_binance_order = (
                exchange == 'binance' 
                and raw_data.get('e') == 'ORDER_TRADE_UPDATE'
            )
            
            # 🔴【新增】币安订单分类缓存流程
            if is_binance_order:
                # 1. 分类
                category = classify_binance_order(private_data)
                
                # 2. 提取合约名
                try:
                    symbol = raw_data['o']['s']
                except (KeyError, TypeError):
                    logger.error("❌ 币安订单数据缺少 o.s 字段")
                    symbol = 'unknown'
                
                # 3. 保存到分类文件（追加）
                save_order_event(symbol, category, private_data)
                
                # 4. 如果是平仓类事件，清理该合约所有缓存
                if category in ['08_主动平仓', '06_触发止损', '07_触发止盈']:
                    clear_symbol_cache(symbol)
                
                # 5. 仍然存入 memory_store（保持API兼容）
                storage_key = f"{exchange}_order_update"
                self.memory_store['private_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': 'order_update',
                    'data': raw_data,
                    'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                    'received_at': datetime.now().isoformat()
                }
                
                logger.debug(f"📨 [币安订单] {symbol} {category}")
                return  # 直接返回，不走下面的通用流程
            
            # ---------- 原有代码，一字不改 ----------
            # 🔴 判断数据来源：HTTP获取器 vs WebSocket
            if source == 'http_fetcher':
                # HTTP获取器的数据：直接使用传入的 data_type
                final_data_type = private_data.get('data_type', 'unknown')
                logger.debug(f"📨 [私人数据处理] HTTP数据: {exchange}.{final_data_type}")
                
            else:
                # WebSocket数据：原有逻辑，通过 'e' 字段映射
                event_type = raw_data.get('e', 'unknown')
                
                if exchange == 'binance':
                    # 🚫 1. 过滤掉 TRADE_LITE 事件
                    if event_type == 'TRADE_LITE':
                        logger.debug(f"📨 [私人数据处理] 过滤掉 TRADE_LITE 事件: {raw_data.get('i')}")
                        return
                    
                    # 🗺️ 2. 币安事件类型映射
                    binance_mapping = {
                        'ACCOUNT_UPDATE': 'account_update',
                        'ORDER_TRADE_UPDATE': 'order_update',
                        'ACCOUNT_CONFIG_UPDATE': 'account_config_update',
                        'MARGIN_CALL': 'risk_event',
                        'listenKeyExpired': 'system_event',
                        'balanceUpdate': 'balance_update',
                        'outboundAccountPosition': 'account_update',
                        'executionReport': 'order_update'
                    }
                    
                    if event_type in binance_mapping:
                        final_data_type = binance_mapping[event_type]
                        logger.debug(f"📨 [私人数据处理] 币安事件映射: {event_type} -> {final_data_type}")
                    else:
                        final_data_type = event_type.lower()
                        
                else:
                    # 其他交易所（如OKX）保持原有的data_type
                    final_data_type = private_data.get('data_type', 'unknown')
            
            # 存储数据到 memory_store
            storage_key = f"{exchange}_{final_data_type}"
            
            self.memory_store['private_data'][storage_key] = {
                'exchange': exchange,
                'data_type': final_data_type,
                'data': raw_data,
                'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                'received_at': datetime.now().isoformat()
            }
            
            logger.debug(f"✅ [私人数据处理] 已保存: {storage_key}")
            
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 接收数据失败: {e}")
    
    # ---------- 以下所有方法一字不改，完全保留 ----------
    async def get_all_data(self) -> Dict[str, Any]:
        """获取所有私人数据概览（仿制大脑接口）"""
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
            return {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "private_data": {}
            }
    
    async def get_data_by_exchange(self, exchange: str) -> Dict[str, Any]:
        """按交易所获取私人数据（仿制大脑接口）"""
        try:
            exchange_data = {}
            for key, data in self.memory_store['private_data'].items():
                if key.startswith(f"{exchange.lower()}_"):
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
                "note": f"{exchange}私人数据（最新一份）"
            }
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 按交易所获取数据失败: {e}")
            return {
                "exchange": exchange,
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "data": {}
            }
    
    async def get_data_detail(self, exchange: str, data_type: str) -> Dict[str, Any]:
        """获取特定私人数据详情（仿制大脑接口）"""
        try:
            key = f"{exchange.lower()}_{data_type.lower()}"
            
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
            return {
                "error": str(e),
                "exchange": exchange,
                "data_type": data_type,
                "timestamp": datetime.now().isoformat()
            }

# 全局单例实例
_global_processor = PrivateDataProcessor()

def get_processor():
    """获取处理器单例"""
    return _global_processor

async def receive_private_data(private_data):
    """供连接池调用的函数接口"""
    return await _global_processor.receive_private_data(private_data)