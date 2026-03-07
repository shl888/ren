"""
数据完成部门
接收、存储、提供数据查询接口
"""
from .receiver import receive_private_data, receive_market_data, get_receiver

__all__ = [
    'receive_private_data',
    'receive_market_data', 
    'get_receiver'
]

__version__ = '1.0.0'