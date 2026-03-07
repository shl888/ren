"""
数据完成部门 - 数据接收器
只接收、存储数据，不做任何判断
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
                'private_data': None,      # 私人数据（okx+binance）
                'market_data': None,       # 行情数据
                'all_received': []         # 接收记录
            }
            self._initialized = True
    
    async def receive_data(self, data: Dict[str, Any]):
        """
        统一数据接收接口
        什么都不判断，只管存
        """
        try:
            received_at = datetime.now().isoformat()
            
            # 根据数据内容简单判断存哪（但也不严格）
            if 'total_contracts' in data:
                self.memory_store['market_data'] = {
                    'data': data,
                    'received_at': received_at
                }
            else:
                self.memory_store['private_data'] = {
                    'data': data,
                    'received_at': received_at
                }
            
            # 记录最近10条
            self.memory_store['all_received'].append({
                'type': 'unknown',
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