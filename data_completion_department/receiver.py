"""
数据完成部门 - 数据接收器
只接收、存储数据，不做复杂处理
"""
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


class DataCompletionReceiver:
    """数据完成部门接收器（单例模式）"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            # 内存存储，只保留最新数据
            self.memory_store = {
                'okx_data': None,           # 欧易成品数据
                'binance_semi_data': None,  # 币安半成品数据
                'market_data': None,         # 行情数据
                'all_received': []           # 接收记录（只保留最近10条）
            }
            self._initialized = True
            logger.info("✅【数据完成部门】接收器已初始化")
    
    async def receive_data(self, data: Dict[str, Any]):
        """
        统一数据接收接口
        不管谁送，送什么，先收下再说
        """
        try:
            # 记录接收时间
            received_at = datetime.now().isoformat()
            
            # 判断数据类型（根据数据内容简单判断）
            data_type = self._identify_data_type(data)
            
            # 根据类型存储
            if data_type == 'okx_complete':
                self.memory_store['okx_data'] = {
                    'data': data,
                    'received_at': received_at
                }
                logger.info(f"✅ 收到欧易成品数据")
                
            elif data_type == 'binance_semi':
                self.memory_store['binance_semi_data'] = {
                    'data': data,
                    'received_at': received_at
                }
                logger.info(f"✅ 收到币安半成品数据")
                
            elif data_type == 'market_data':
                self.memory_store['market_data'] = {
                    'data': data,
                    'received_at': received_at
                }
                logger.info(f"✅ 收到行情数据，包含 {len(data.get('markets', {}))} 个币种")
                
            else:
                # 未知类型也存起来，方便调试
                logger.info(f"📥 收到未知类型数据: {list(data.keys())}")
                self.memory_store['all_received'].append({
                    'type': 'unknown',
                    'data': data,
                    'received_at': received_at
                })
                # 只保留最近10条
                if len(self.memory_store['all_received']) > 10:
                    self.memory_store['all_received'].pop(0)
            
            # 简单打印当前存储状态
            self._log_status()
            
        except Exception as e:
            logger.error(f"❌ 接收数据失败: {e}")
    
    def _identify_data_type(self, data: Dict) -> str:
        """简单识别数据类型"""
        # 如果有markets字段，可能是行情数据
        if 'markets' in data:
            return 'market_data'
        
        # 如果有交易所字段
        exchange = data.get('exchange') or data.get('交易所')
        if exchange == 'okx':
            return 'okx_complete'
        elif exchange == 'binance':
            return 'binance_semi'
        
        # 如果都没有，再看看data字段
        if 'data' in data:
            inner_data = data['data']
            if isinstance(inner_data, dict):
                if 'okx' in inner_data:
                    return 'okx_complete'
                if 'binance' in inner_data:
                    return 'binance_semi'
        
        return 'unknown'
    
    def _log_status(self):
        """打印当前存储状态"""
        status = []
        if self.memory_store['okx_data']:
            status.append("欧易✅")
        if self.memory_store['binance_semi_data']:
            status.append("币安半成品✅")
        if self.memory_store['market_data']:
            status.append("行情✅")
        
        logger.info(f"📊 当前状态: {' | '.join(status) if status else '空'}")

    async def get_status(self) -> Dict[str, Any]:
        """获取当前状态（用于调试）"""
        return {
            "timestamp": datetime.now().isoformat(),
            "okx_data": self.memory_store['okx_data'] is not None,
            "binance_semi_data": self.memory_store['binance_semi_data'] is not None,
            "market_data": self.memory_store['market_data'] is not None,
            "recent_received": self.memory_store['all_received'][-5:]  # 最近5条
        }


# 全局单例实例
_global_receiver = DataCompletionReceiver()

def get_receiver():
    """获取接收器单例"""
    return _global_receiver

async def receive_data(data):
    """供外部调用的函数接口"""
    return await _global_receiver.receive_data(data)