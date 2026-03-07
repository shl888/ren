"""
数据完成部门 - 数据接收器（问题修复版）
"""
from datetime import datetime
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class DataCompletionReceiver:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            # 内存存储（按数据源分类）
            self.memory_store = {
                'market_data': {},      # 公开市场数据
                'user_data': {},        # 私人用户数据
            }
            
            # 时间戳跟踪
            self.last_market_time: Optional[datetime] = None
            self.last_account_time: Optional[datetime] = None
            
            self._initialized = True
            logger.info("✅ 数据完成接收器初始化完成")
    
    # ==================== 接收数据 ====================
    
    async def receive_private_data(self, private_data: Dict[str, Any]):
        """
        接收私人数据
        """
        try:
            exchange = private_data.get('exchange', 'unknown')
            data_type = private_data.get('data_type', 'unknown')
            
            logger.info(f"📨 收到 {exchange}.{data_type} 数据")
            
            now = datetime.now()
            
            # ===== 根据data_type分类存储 =====
            if data_type in ['user_summary', 'account_update', 'order_update', 'position_update']:
                # 用户数据
                storage_key = f"{exchange}_user"
                self.memory_store['user_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': 'user_summary',
                    'data': private_data.get('data', {}),
                    'timestamp': private_data.get('timestamp', now.isoformat()),
                    'received_at': now.isoformat(),
                    'source': 'private_processor'
                }
                self.last_account_time = now
                logger.info(f"✅ 用户数据 {exchange} 已保存")
                
            elif data_type == 'contract_info':
                # 参考数据（暂时存到 user_data，但加上类型标记）
                storage_key = f"{exchange}_{data_type}"
                self.memory_store['user_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': 'contract_info',
                    'data': private_data.get('data', {}),
                    'timestamp': private_data.get('timestamp', now.isoformat()),
                    'received_at': now.isoformat(),
                    'source': 'reference_task'
                }
                self.last_account_time = now
                logger.info(f"✅ 参考数据 {exchange}.{data_type} 已保存")
                
            else:
                # 未知类型 - 可能是行情数据误入，直接丢弃并警告
                logger.warning(f"⚠️ 收到未知数据类型 {data_type}，可能不是私人数据，已丢弃")
                    
        except Exception as e:
            logger.error(f"❌ 接收私人数据失败: {e}", exc_info=True)
    
    async def receive_market_data(self, processed_data):
        """
        接收市场数据
        """
        try:
            now = datetime.now()
            
            if isinstance(processed_data, list):
                # 批量行情数据
                stored_count = 0
                for item in processed_data:
                    symbol = item.get('symbol')
                    if not symbol:
                        continue
                    
                    # 存储市场数据
                    self.memory_store['market_data'][symbol] = item
                    stored_count += 1
                
                self.last_market_time = now
                logger.info(f"✅ 批量存储市场数据，共{stored_count}条，当前总计{len(self.memory_store['market_data'])}个交易对")
                
            elif isinstance(processed_data, dict):
                # 单个行情数据
                symbol = processed_data.get('symbol')
                if symbol:
                    self.memory_store['market_data'][symbol] = processed_data
                    self.last_market_time = now
                    logger.info(f"✅ 存储单个市场数据 {symbol}")
                else:
                    logger.warning(f"⚠️ 收到无symbol的行情数据: {str(processed_data)[:200]}")
            
            else:
                logger.warning(f"⚠️ 收到非列表/非字典类型市场数据: {type(processed_data)}")
            
        except Exception as e:
            logger.error(f"❌ 接收市场数据错误: {e}", exc_info=True)
    
    async def receive_data(self, data: Dict[str, Any]):
        """
        统一接收接口 - 自动判断调用哪个
        """
        try:
            # 判断是市场数据还是私人数据
            if isinstance(data, list):
                # 列表肯定是行情数据
                await self.receive_market_data(data)
                
            elif isinstance(data, dict):
                # 字典类型需要判断
                if 'symbol' in data:
                    # 有symbol字段，是单个行情数据
                    await self.receive_market_data(data)
                elif data.get('data_type') in ['user_summary', 'account_update', 'order_update', 'position_update', 'contract_info']:
                    # 明确是私人数据
                    await self.receive_private_data(data)
                elif data.get('exchange') and 'data' in data:
                    # 有exchange和data字段，可能是私人数据
                    await self.receive_private_data(data)
                else:
                    # 实在判断不出的，打印警告并尝试按行情处理
                    logger.warning(f"⚠️ 无法判断数据类型，尝试按行情处理: {str(data)[:200]}")
                    await self.receive_market_data(data)
            else:
                logger.error(f"❌ 未知数据类型: {type(data)}")
                
        except Exception as e:
            logger.error(f"❌ 接收数据失败: {e}", exc_info=True)
    
    # ==================== 数据查询接口 ====================
    
    def get_data_summary(self):
        """获取数据大纲"""
        sources = []
        
        # 1. 行情数据源
        if self.memory_store['market_data']:
            sources.append({
                "name": "public_market",
                "description": "公开市场数据（实时行情、费率差）",
                "item_count": len(self.memory_store['market_data']),
                "endpoint": "/api/completion/data/public_market",
                "last_update": self.last_market_time.isoformat() if self.last_market_time else None
            })
        
        # 2. 私人数据源
        if self.memory_store['user_data']:
            # 只统计真正的用户数据（排除参考数据）
            user_count = 0
            for key, data in self.memory_store['user_data'].items():
                if data.get('data_type') in ['user_summary', 'account_update', 'order_update', 'position_update']:
                    user_count += 1
            
            if user_count > 0:
                sources.append({
                    "name": "private_user",
                    "description": "私人用户数据（欧易+币安）",
                    "user_count": user_count,
                    "endpoint": "/api/completion/data/private_user",
                    "last_update": self.last_account_time.isoformat() if self.last_account_time else None
                })
        
        return {
            "timestamp": datetime.now().isoformat(),
            "source_count": len(sources),
            "sources": sources,
            "note": f"共{len(sources)}个数据来源，点击endpoint查看详情"
        }
    
    def get_public_market_data(self):
        """获取行情数据详情"""
        return {
            "source": "public_market",
            "description": "公开市场数据（实时行情、费率差）",
            "timestamp": self.last_market_time.isoformat() if self.last_market_time else datetime.now().isoformat(),
            "count": len(self.memory_store['market_data']),
            "data": self.memory_store['market_data']
        }
    
    def get_private_user_data(self):
        """获取私人数据详情"""
        # 只提取真正的用户数据（排除参考数据）
        user_data = {}
        for key, data in self.memory_store['user_data'].items():
            data_type = data.get('data_type')
            exchange = data.get('exchange')
            
            # 只包含用户数据，不包含参考数据
            if data_type in ['user_summary', 'account_update', 'order_update', 'position_update'] and exchange:
                user_data[exchange] = data.get('data', {})
        
        return {
            "source": "private_user",
            "description": "私人用户数据（币安 + 欧易）",
            "timestamp": self.last_account_time.isoformat() if self.last_account_time else datetime.now().isoformat(),
            "count": len(user_data),
            "data": user_data
        }
    
    def get_store_status(self):
        """获取存储状态（调试用）"""
        # 统计不同类型的数据
        user_summary_count = 0
        contract_info_count = 0
        
        for key, data in self.memory_store['user_data'].items():
            if data.get('data_type') == 'user_summary':
                user_summary_count += 1
            elif data.get('data_type') == 'contract_info':
                contract_info_count += 1
        
        return {
            "market_data_count": len(self.memory_store['market_data']),
            "user_data_total": len(self.memory_store['user_data']),
            "user_summary_count": user_summary_count,
            "contract_info_count": contract_info_count,
            "last_market_time": self.last_market_time.isoformat() if self.last_market_time else None,
            "last_account_time": self.last_account_time.isoformat() if self.last_account_time else None,
            "exchanges": list(set([d.get('exchange') for d in self.memory_store['user_data'].values() if d.get('exchange')]))
        }


# 全局单例
_global_receiver = DataCompletionReceiver()


def get_receiver():
    return _global_receiver


async def receive_data(data):
    await _global_receiver.receive_data(data)