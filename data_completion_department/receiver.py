"""
数据完成部门 - 数据接收器
完全按照大脑模块的设计重写
"""
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
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
            # 内存存储 - 分库存储，各找各妈
            self.memory_store = {
                'market_data': {},      # 行情数据专用库，key=symbol
                'user_data': {},        # 私人数据专用库，key=交易所_user
            }
            
            # 时间戳跟踪
            self.last_market_time = None
            self.last_account_time = None
            self.last_market_count = 0
            
            self._initialized = True
            logger.info("✅ 数据完成接收器初始化完成")
    
    # ==================== 两个独立入口 ====================
    
    async def receive_private_data(self, private_data: Dict[str, Any]):
        """
        接收私人数据 - 调用方保证这是私人数据
        完全照抄大脑模块的 receive_private_data
        """
        try:
            exchange = private_data.get('exchange', 'unknown')
            data_type = private_data.get('data_type', 'unknown')
            
            logger.info(f"📨 收到私人数据: {exchange}.{data_type}")
            
            now = datetime.now()
            
            # ===== 只存业务数据，不存外层包装 =====
            # 统一用 {exchange}_user 作为 key，避免覆盖
            storage_key = f"{exchange}_user"
            
            self.memory_store['user_data'][storage_key] = {
                'exchange': exchange,
                'data_type': data_type,
                'data': private_data.get('data', {}),          # ← 只存业务数据
                'timestamp': private_data.get('timestamp', now.isoformat()),
                'received_at': now.isoformat()
            }
            
            self.last_account_time = now
            logger.info(f"✅ 私人数据已存储: {exchange}")
            
        except Exception as e:
            logger.error(f"❌ 接收私人数据失败: {e}", exc_info=True)
    
    async def receive_market_data(self, market_data: Union[List, Dict]):
        """
        接收市场数据 - 调用方保证这是市场数据
        完全照抄大脑模块的 receive_market_data
        """
        try:
            # 大脑模块直接处理，不判断
            if isinstance(market_data, list):
                self.last_market_count = len(market_data)
                await self._store_market_data(market_data)
            else:
                logger.warning(f"⚠️ 收到非列表市场数据: {type(market_data)}")
                self.last_market_count = 0
            
            self.last_market_time = datetime.now()
            
        except Exception as e:
            logger.error(f"❌ 接收市场数据失败: {e}", exc_info=True)
    
    async def _store_market_data(self, data_list: List):
        """存储市场数据列表"""
        try:
            if not data_list:
                return 0
                
            stored_count = 0
            for item in data_list:
                symbol = item.get('symbol')
                if not symbol:
                    continue
                
                # 创建简化版市场数据（和大脑模块一致）
                simplified_data = self._create_simplified_market_data(item)
                self.memory_store['market_data'][symbol] = simplified_data
                stored_count += 1
            
            logger.info(f"✅ 市场数据已存储: {stored_count} 条")
            return stored_count
            
        except Exception as e:
            logger.error(f"❌ 存储市场数据失败: {e}")
            return 0
    
    def _create_simplified_market_data(self, raw_data: Dict) -> Dict:
        """创建简化格式的市场数据 - 照抄大脑模块"""
        try:
            metadata = raw_data.get('metadata', {})
            
            return {
                'symbol': raw_data.get('symbol'),
                'trade_price_diff': raw_data.get('trade_price_diff'),
                'trade_price_diff_percent': raw_data.get('trade_price_diff_percent'),
                'rate_diff': raw_data.get('rate_diff'),
                'okx_trade_price': raw_data.get('okx_trade_price'),
                'okx_mark_price': raw_data.get('okx_mark_price'),
                'okx_price_to_mark_diff': raw_data.get('okx_price_to_mark_diff'),
                'okx_price_to_mark_diff_percent': raw_data.get('okx_price_to_mark_diff_percent'),
                'okx_funding_rate': raw_data.get('okx_funding_rate'),
                'okx_period_seconds': raw_data.get('okx_period_seconds'),
                'okx_countdown_seconds': raw_data.get('okx_countdown_seconds'),
                'okx_last_settlement': raw_data.get('okx_last_settlement'),
                'okx_current_settlement': raw_data.get('okx_current_settlement'),
                'okx_next_settlement': raw_data.get('okx_next_settlement'),
                'binance_trade_price': raw_data.get('binance_trade_price'),
                'binance_mark_price': raw_data.get('binance_mark_price'),
                'binance_price_to_mark_diff': raw_data.get('binance_price_to_mark_diff'),
                'binance_price_to_mark_diff_percent': raw_data.get('binance_price_to_mark_diff_percent'),
                'binance_funding_rate': raw_data.get('binance_funding_rate'),
                'binance_period_seconds': raw_data.get('binance_period_seconds'),
                'binance_countdown_seconds': raw_data.get('binance_countdown_seconds'),
                'binance_last_settlement': raw_data.get('binance_last_settlement'),
                'binance_current_settlement': raw_data.get('binance_current_settlement'),
                'binance_next_settlement': raw_data.get('binance_next_settlement'),
                'calculated_at': metadata.get('calculated_at', datetime.now().isoformat()),
                'source': metadata.get('source', 'step5_cross_calc')
            }
        except Exception as e:
            logger.error(f"❌ 创建简化市场数据失败: {e}")
            return {'symbol': raw_data.get('symbol', 'unknown')}
    
    # ==================== 查询接口 ====================
    
    def get_data_summary(self):
        """获取数据大纲"""
        sources = []
        
        if self.memory_store['market_data']:
            sources.append({
                "name": "public_market",
                "description": "公开市场数据（实时行情、费率差）",
                "item_count": len(self.memory_store['market_data']),
                "endpoint": "/api/completion/data/public_market",
                "last_update": self.last_market_time.isoformat() if self.last_market_time else None
            })
        
        if self.memory_store['user_data']:
            sources.append({
                "name": "private_user",
                "description": "私人用户数据（欧易+币安）",
                "user_count": len(self.memory_store['user_data']),
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
        """获取私人数据详情 - 按交易所合并"""
        user_data = {}
        for key, data in self.memory_store['user_data'].items():
            exchange = data.get('exchange')
            if exchange in ['binance', 'okx']:
                user_data[exchange] = data.get('data', {})
        
        return {
            "source": "private_user",
            "description": "私人用户数据（币安 + 欧易）",
            "timestamp": self.last_account_time.isoformat() if self.last_account_time else datetime.now().isoformat(),
            "count": len(user_data),
            "data": user_data
        }


# ==================== 全局单例 ====================

_global_receiver = DataCompletionReceiver()


def get_receiver():
    return _global_receiver


# 对外只暴露这两个独立入口
async def receive_private_data(data):
    await _global_receiver.receive_private_data(data)


async def receive_market_data(data):
    await _global_receiver.receive_market_data(data)