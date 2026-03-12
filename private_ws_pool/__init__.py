"""
私人WebSocket连接池模块 - 双连接热备版
"""
from .pool_manager import PrivateWebSocketPool
from .connection import (
    PrivateWebSocketConnection,
    BinancePrivateConnection,
    OKXPrivateConnection
)

__version__ = '2.0.0'  # 升级版本号
__all__ = [
    'PrivateWebSocketPool',
    'PrivateWebSocketConnection',
    'BinancePrivateConnection',
    'OKXPrivateConnection'
]