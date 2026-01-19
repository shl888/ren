# smart_brain/data_receiver.py
"""
数据接收器 - 专门处理来自pipeline的数据
"""

import logging
from datetime import datetime
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


class DataReceiver:
    """接收并处理来自pipeline的数据"""
    
    def __init__(self):
        # 数据接收统计
        self.last_market_time = None      # 最后收到成品数据的时间
        self.last_market_count = 0        # 最后一次收到的合约数量
        
        self.last_account_time = None     # 最后收到账户私人数据的时间
        self.last_trade_time = None       # 最后收到交易私人数据的时间
        
        # 回调函数注册表（未来扩展用）
        self.market_data_callbacks = []
        self.private_data_callbacks = []
    
    def register_market_callback(self, callback):
        """注册市场数据回调"""
        self.market_data_callbacks.append(callback)
        
    def register_private_callback(self, callback):
        """注册私人数据回调"""
        self.private_data_callbacks.append(callback)
    
    async def receive_market_data(self, processed_data: List[Dict]):
        """接收成品数据"""
        try:
            # 现在processed_data应该是一个列表（包含所有合约数据）
            if isinstance(processed_data, list):
                # 正确：记录列表长度作为合约数量
                self.last_market_count = len(processed_data)
                
                # 可选：记录调试信息
                if logger.isEnabledFor(logging.DEBUG):
                    if processed_data and len(processed_data) > 0:
                        symbol = processed_data[0].get('symbol', 'unknown')
                        logger.debug(f"收到批量数据: {self.last_market_count}条, 第一个合约: {symbol}")
            else:
                # 如果不是列表，记录警告
                logger.warning(f"⚠️ 收到非列表类型市场数据: {type(processed_data)}")
                self.last_market_count = 1  # 备用逻辑
            
            # 更新最后接收时间
            self.last_market_time = datetime.now()
            
            # 分发给所有注册的回调
            for callback in self.market_data_callbacks:
                try:
                    await callback(processed_data)
                except Exception as e:
                    logger.error(f"市场数据回调执行失败: {e}")
                    
        except Exception as e:
            logger.error(f"接收数据错误: {e}")
    
    async def receive_private_data(self, private_data: Dict[str, Any]):
        """接收私人数据"""
        try:
            data_type = private_data.get('data_type', 'unknown')
            exchange = private_data.get('exchange', 'unknown')
            
            # 更新对应类型数据的最后接收时间
            now = datetime.now()
            
            # 匹配PipelineManager的数据类型
            if data_type in ['account_update', 'account']:
                self.last_account_time = now
                logger.info(f"💰 收到账户私人数据: {exchange}")
            elif data_type in ['order_update', 'trade']:
                self.last_trade_time = now
                logger.info(f"📝 收到交易私人数据: {exchange}")
            else:
                # 如果没有明确类型，默认认为是账户数据
                self.last_account_time = now
                logger.info(f"📨 收到未知类型私人数据: {exchange}.{data_type}")
            
            # 分发给所有注册的回调
            for callback in self.private_data_callbacks:
                try:
                    await callback(private_data)
                except Exception as e:
                    logger.error(f"私人数据回调执行失败: {e}")
                    
        except Exception as e:
            logger.error(f"接收私人数据错误: {e}")
    
    def get_status_info(self) -> Dict:
        """获取接收器状态信息"""
        return {
            'last_market_time': self.last_market_time,
            'last_market_count': self.last_market_count,
            'last_account_time': self.last_account_time,
            'last_trade_time': self.last_trade_time
        }
        