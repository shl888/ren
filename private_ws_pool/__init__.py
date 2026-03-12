"""
私人WebSocket连接池模块
"""
from .pool_manager import PrivateWebSocketPool
from .connection import (
    PrivateWebSocketConnection,
    BinancePrivateConnection,
    OKXPrivateConnection
)

# 🔴 【修改点】删除RawDataCache导入
# from .raw_data_cache import RawDataCache  # 删除这行

__version__ = '1.0.0'
__all__ = [
    'PrivateWebSocketPool',
    # 🔴 【修改点】删除RawDataCache导出
    # 'RawDataCache',  # 删除这行
    'PrivateWebSocketConnection',
    'BinancePrivateConnection',
    'OKXPrivateConnection'
]