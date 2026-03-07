"""
数据完成部门 - 数据接收器
只接收、存储数据
"""
from datetime import datetime
from typing import Dict, Any


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
                'private_data': None,      # 私人数据（欧易成品/币安半成品）
                'market_data': None,       # 行情数据
                'all_received': []         # 接收记录
            }
            self._initialized = True
    
    async def receive_data(self, data: Dict[str, Any]):
        """
        统一数据接收接口
        根据数据特征判断存哪
        """
        try:
            received_at = datetime.now().isoformat()
            
            # ===== 判断数据类型 =====
            
            # 1. 行情数据特征：exchange=public, data_type=market_data
            if data.get('exchange') == 'public' and data.get('data_type') == 'market_data':
                self.memory_store['market_data'] = {
                    'data': data,
                    'received_at': received_at
                }
                data_type = 'market_data'
            
            # 2. 私人数据特征：有交易所字段（okx/binance）
            elif data.get('exchange') in ['okx', 'binance']:
                self.memory_store['private_data'] = {
                    'data': data,
                    'received_at': received_at
                }
                data_type = 'private_data'
            
            # 3. 未知数据
            else:
                data_type = 'unknown'
                # 也可以存到 private_data 作为后备
                self.memory_store['private_data'] = {
                    'data': data,
                    'received_at': received_at
                }
            
            # 记录最近10条
            self.memory_store['all_received'].append({
                'type': data_type,
                'received_at': received_at
            })
            if len(self.memory_store['all_received']) > 10:
                self.memory_store['all_received'].pop(0)
            
        except Exception:
            pass

    async def get_status(self) -> Dict[str, Any]:
        """获取当前状态（用于路由查看）"""
        return {
            "timestamp": datetime.now().isoformat(),
            "private_data": self.memory_store['private_data'],
            "market_data": self.memory_store['market_data'],
            "recent_received": self.memory_store['all_received'][-5:]
        }


# 全局单例实例
_global_receiver = DataCompletionReceiver()

def get_receiver():
    return _global_receiver

async def receive_data(data):
    return await _global_receiver.receive_data(data)