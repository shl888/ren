"""
数据完成部门 - 数据接收器
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
                'private_data': None,  # 一条数据包含okx+binance
                'market_data': None,   # 行情数据
            }
            self._initialized = True
    
    async def receive_data(self, data: Dict[str, Any]):
        """接收数据，什么都不判断，只管存"""
        try:
            received_at = datetime.now().isoformat()
            
            # 看数据里有没有total_contracts，有就是行情，没有就是私人
            if 'total_contracts' in str(data):
                self.memory_store['market_data'] = {
                    'data': data,
                    'received_at': received_at
                }
            else:
                self.memory_store['private_data'] = {
                    'data': data,
                    'received_at': received_at
                }
        except Exception:
            pass


_global_receiver = DataCompletionReceiver()

def get_receiver():
    return _global_receiver

async def receive_data(data):
    return await _global_receiver.receive_data(data)