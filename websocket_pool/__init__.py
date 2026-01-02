"""
WebSocket连接池模块 - 工业级稳定版本
"""

# 核心组件
from .admin import WebSocketAdmin
from .pools.global_pool import GlobalPoolManager
from .pools.exchange_pool import ExchangePool
from .core.connection import Connection
from .core.worker import DataWorker, BackupWorker
from .core.monitor import MonitorCenter

# 配置
from .config import EXCHANGE_CONFIGS, SUBSCRIPTION_TYPES, SYMBOL_FILTERS
from .static_symbols import STATIC_SYMBOLS

__all__ = [
    'WebSocketAdmin',           # ✅ 大脑只用这个
    'GlobalPoolManager',        # 全局管理器
    'ExchangePool',            # 交易所连接池
    'Connection',              # 物理连接
    'DataWorker',              # 数据工作者
    'BackupWorker',            # 备份工作者
    'MonitorCenter',           # 监控中心
    'EXCHANGE_CONFIGS',        # 配置
    'SUBSCRIPTION_TYPES',      # 订阅类型
    'SYMBOL_FILTERS',         # 合约筛选
    'STATIC_SYMBOLS',         # 静态合约
]