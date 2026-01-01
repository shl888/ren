# websocket_pool/__init__.py
"""
WebSocket连接池模块 - 生产环境配置
"""

# 核心组件（供内部使用）
from .pool_manager import WebSocketPoolManager
from .exchange_pool import ExchangeWebSocketPool
from .connection import WebSocketConnection
from .monitor import ConnectionMonitor
from .config import EXCHANGE_CONFIGS, SUBSCRIPTION_TYPES, SYMBOL_FILTERS
from .static_symbols import STATIC_SYMBOLS

# ✅ 新增：管理员接口（供大脑核心直接调用）
from .admin import WebSocketAdmin

__all__ = [
    'WebSocketAdmin',           # ✅ 大脑只用这个
    'WebSocketPoolManager',     # 保留供其他模块使用
    'ExchangeWebSocketPool', 
    'WebSocketConnection',
    'ConnectionMonitor',
    'EXCHANGE_CONFIGS',
    'SUBSCRIPTION_TYPES',
    'SYMBOL_FILTERS',
    'STATIC_SYMBOLS',
]
