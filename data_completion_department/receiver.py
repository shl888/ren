"""
数据完成部门 - 数据接收器
只接收、存储数据
"""
from datetime import datetime
from typing import Dict, Any


class DataCompletionReceiver:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.memory_store = {
                'private_data': None,
                'market_data': None,
                'all_received': []
            }
            self._initialized = True
    
    async def receive_data(self, data: Dict[str, Any]):
        """接收数据"""
        try:
            received_at = datetime.now().isoformat()
            
            # 行情数据
            if data.get('exchange') == 'public' and data.get('data_type') == 'market_data':
                self.memory_store['market_data'] = {
                    'data': data,
                    'received_at': received_at
                }
                data_type = 'market_data'
            
            # 私人数据
            else:
                self.memory_store['private_data'] = {
                    'data': data,
                    'received_at': received_at
                }
                data_type = 'private_data'
            
            # 记录
            self.memory_store['all_received'].append({
                'type': data_type,
                'received_at': received_at
            })
            if len(self.memory_store['all_received']) > 10:
                self.memory_store['all_received'].pop(0)
            
        except Exception:
            pass

    async def get_status(self) -> Dict[str, Any]:
        """获取状态"""
        return {
            "timestamp": datetime.now().isoformat(),
            "private_data": self.memory_store['private_data'],
            "market_data": self.memory_store['market_data'],
            "recent_received": self.memory_store['all_received'][-5:]
        }


_global_receiver = DataCompletionReceiver()

def get_receiver():
    return _global_receiver

async def receive_data(data):
    return await _global_receiver.receive_data(data)