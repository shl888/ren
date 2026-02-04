"""
私人数据处理器 - 最简版本
只接收、存储、查看私人数据
"""
import logging
from datetime import datetime
from typing import Dict, Any

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
            # 🔴 仿制大脑的存储结构
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
            source = private_data.get('source', '')  # 获取来源标识
            
            # 🔴 【关键修复】判断数据来源：HTTP获取器 vs WebSocket
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
                        return  # 直接返回，不存储
                    
                    # 🗺️ 2. 币安事件类型映射
                    binance_mapping = {
                        'ACCOUNT_UPDATE': 'account_update',
                        'ORDER_TRADE_UPDATE': 'order_update',  # 关键映射：ORDER_TRADE_UPDATE -> order_update
                        'ACCOUNT_CONFIG_UPDATE': 'account_config_update',  # 不再未知
                        'MARGIN_CALL': 'risk_event',
                        'listenKeyExpired': 'system_event',
                        'balanceUpdate': 'balance_update',
                        'outboundAccountPosition': 'account_update',
                        'executionReport': 'order_update'
                    }
                    
                    # 使用映射后的data_type
                    if event_type in binance_mapping:
                        final_data_type = binance_mapping[event_type]
                        logger.debug(f"📨 [私人数据处理] 币安事件映射: {event_type} -> {final_data_type}")
                    else:
                        # 对于未映射的事件，使用原生事件名的小写
                        final_data_type = event_type.lower()
                        
                else:
                    # 其他交易所（如OKX）保持原有的data_type
                    final_data_type = private_data.get('data_type', 'unknown')
            
            # 🔴 【新增】记录完整信息便于调试
            logger.debug(f"📨 [私人数据处理] 收到{exchange}.{final_data_type}数据")
            
            # 存储数据
            storage_key = f"{exchange}_{final_data_type}"
            
            self.memory_store['private_data'][storage_key] = {
                'exchange': exchange,
                'data_type': final_data_type,
                'data': raw_data,
                'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                'received_at': datetime.now().isoformat()
            }
            
            logger.debug(f"✅ [私人数据处理] 已保存: {storage_key}")  # 需要时，可改为info方便观察
            
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 接收数据失败: {e}")
    
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
                        "data": data.get('data')  # 直接返回原始数据
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
                    "data": data.get('data'),  # 直接返回原始数据
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
    """
    供连接池调用的函数接口
    使用全局单例
    """
    return await _global_processor.receive_private_data(private_data)
    