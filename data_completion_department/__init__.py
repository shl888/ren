"""
数据完成部门
接收、存储、提供数据查询接口
"""
from .receiver import receive_private_data, receive_market_data, get_receiver
from .data_completion import setup_data_completion_routes

__all__ = [
    'receive_private_data',    # ← 改这里
    'receive_market_data',      # ← 改这里
    'get_receiver',
    'setup_data_completion_routes'
]

__version__ = '1.0.0'