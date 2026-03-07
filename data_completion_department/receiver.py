"""
数据完成部门 - 数据接收器（完全照抄大脑模块 + 修复三个问题）
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
            # 内存存储（和大脑模块完全一致）
            self.memory_store = {
                'market_data': {},      # 公开市场数据
                'user_data': {},        # 私人用户数据
            }
            
            # 时间戳跟踪（和大脑模块一致）
            self.last_market_time = None
            self.last_account_time = None
            self.last_market_count = 0
            
            self._initialized = True
            logger.info("✅ 数据完成接收器初始化完成")
    
    # ==================== 接收数据（和大脑模块一模一样） ====================
    
    async def receive_private_data(self, private_data: Dict[str, Any]):
        """
        接收私人数据 - 完全照抄大脑模块的 receive_private_data
        """
        try:
            exchange = private_data.get('exchange', 'unknown')
            data_type = private_data.get('data_type', 'unknown')
            
            # 🛠️ 修复3：只接受合法的交易所名称
            valid_exchanges = ['binance', 'okx', 'unknown']
            if exchange not in valid_exchanges:
                logger.warning(f"⚠️ 非交易所数据被送到私人接收器: exchange={exchange}，丢弃")
                logger.debug(f"被丢弃的数据: {str(private_data)[:200]}")
                return
            
            logger.info(f"📨 收到 {exchange}.{data_type} 数据")
            
            now = datetime.now()
            
            # ===== 根据data_type分类存储（和大脑模块完全一致） =====
            if data_type == 'contract_info':
                # 参考数据（大脑模块存到 reference_data，我这里简化，先存到 user_data）
                storage_key = f"{exchange}_{data_type}"
                self.memory_store['user_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': data_type,
                    'data': private_data.get('data', {}),
                    'timestamp': private_data.get('timestamp', now.isoformat()),
                    'received_at': now.isoformat(),
                    'source': 'reference_task'
                }
                self.last_account_time = now
                logger.info(f"✅ 参考数据 {exchange}.{data_type} 已保存")
                
            elif data_type == 'user_summary':
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
                
            else:
                # 🛠️ 修复2：未知类型，暂存，但要更新时间戳
                logger.warning(f"⚠️ 未知数据类型 {data_type}，暂存到 user_data")
                storage_key = f"{exchange}_{data_type}"
                self.memory_store['user_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': data_type,
                    'data': private_data.get('data', {}),
                    'timestamp': private_data.get('timestamp', now.isoformat()),
                    'received_at': now.isoformat()
                }
                self.last_account_time = now  # ← 修复2：加上这一行
                logger.info(f"✅ 未知类型数据 {exchange}.{data_type} 已暂存")
                    
        except Exception as e:
            logger.error(f"❌ 接收私人数据失败: {e}", exc_info=True)
    
    async def receive_market_data(self, processed_data: Union[List, Dict]):
        """
        接收市场数据 - 完全照抄大脑模块的 receive_market_data
        """
        try:
            # 🛠️ 修复1扩展：处理可能被包装的行情数据
            actual_data = processed_data
            
            # 如果传入的是带 total_contracts 的包装，提取真正的数据列表
            if isinstance(processed_data, dict) and 'total_contracts' in processed_data:
                actual_data = processed_data.get('data', [])
                logger.info(f"📦 从包装中提取行情数据列表，共 {len(actual_data) if isinstance(actual_data, list) else 'unknown'} 条")
            
            if isinstance(actual_data, list):
                self.last_market_count = len(actual_data)
                
                # 存储市场数据
                stored_count = await self._store_market_data(actual_data)
                logger.info(f"✅ 市场数据存储完成，实际存储 {stored_count} 条")
                
            else:
                logger.warning(f"⚠️ 收到非列表类型市场数据: {type(actual_data)}")
                self.last_market_count = 0
            
            self.last_market_time = datetime.now()
            
        except Exception as e:
            logger.error(f"❌ 接收市场数据错误: {e}", exc_info=True)
    
    async def _store_market_data(self, data: List) -> int:
        """存储市场数据 - 照抄大脑模块的 _store_market_data_simplified"""
        try:
            if not data:
                return 0
                
            stored_count = 0
            if isinstance(data, list):
                for item in data:
                    symbol = item.get('symbol', 'unknown')
                    if not symbol or symbol == 'unknown':
                        continue
                    
                    # 创建简化版市场数据（和大脑模块一致）
                    simplified_data = self._create_simplified_market_data(item)
                    self.memory_store['market_data'][symbol] = simplified_data
                    stored_count += 1
                
                logger.info(f"✅ 批量存储市场数据，共{stored_count}条")
                return stored_count
                
            return 0
                
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
            return {'symbol': raw_data.get('symbol', 'unknown'), 'error': str(e)}
    
    # ==================== 对外接口（保持和之前一样） ====================
    
    async def receive_data(self, data: Union[List, Dict]):
        """
        统一接收接口 - 🛠️ 修复1：加强判断逻辑
        """
        try:
            # 🛠️ 修复1：更精确的市场数据判断
            if isinstance(data, list):
                # 明确是列表，一定是市场数据
                logger.info(f"📊 检测到列表类型数据，长度 {len(data)}，作为市场数据处理")
                await self.receive_market_data(data)
                
            elif isinstance(data, dict):
                # 字典类型，需要进一步判断
                if 'total_contracts' in data:
                    # 是带 total_contracts 的行情数据包装
                    logger.info("📊 检测到 total_contracts 字段，作为市场数据包装处理")
                    await self.receive_market_data(data)
                    
                elif 'symbol' in data:
                    # 单个行情数据（可能是单个合约）
                    logger.info(f"📊 检测到单个行情数据: {data.get('symbol')}")
                    await self.receive_market_data([data])
                    
                else:
                    # 没有行情数据特征，当作私人数据
                    logger.info("🔐 检测到字典类型，无行情特征，作为私人数据处理")
                    await self.receive_private_data(data)
            else:
                logger.warning(f"⚠️ 未知数据类型: {type(data)}，无法处理")
                
        except Exception as e:
            logger.error(f"❌ receive_data 统一入口失败: {e}", exc_info=True)
    
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
        """获取私人数据详情"""
        # 按交易所重新组织（和大脑模块一样）
        user_data = {}
        for key, data in self.memory_store['user_data'].items():
            exchange = data.get('exchange')
            # 🛠️ 修复3辅助：只取合法的交易所数据，过滤掉可能的脏数据
            if exchange and exchange in ['binance', 'okx']:
                user_data[exchange] = data.get('data', {})
            else:
                logger.debug(f"过滤掉非交易所数据: {key} -> {exchange}")
        
        return {
            "source": "private_user",
            "description": "私人用户数据（币安 + 欧易）",
            "timestamp": self.last_account_time.isoformat() if self.last_account_time else datetime.now().isoformat(),
            "count": len(user_data),  # 🛠️ 修复3：只计算实际交易所数量
            "data": user_data
        }


# 全局单例
_global_receiver = DataCompletionReceiver()


def get_receiver():
    return _global_receiver


async def receive_data(data):
    await _global_receiver.receive_data(data)