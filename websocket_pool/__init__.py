"""
WebSocket连接池模块 - 工业级稳定版本
"""

# 核心组件
from websocket_pool.admin import WebSocketAdmin
from websocket_pool.pools.global_pool import GlobalPoolManager
from websocket_pool.pools.exchange_pool import ExchangePool
from websocket_pool.core.connection import Connection
from websocket_pool.core.data_worker import DataWorker
from websocket_pool.core.backup_worker import BackupWorker
from websocket_pool.core.monitor import MonitorCenter

# 配置
from websocket_pool.config import EXCHANGE_CONFIGS, SUBSCRIPTION_TYPES, SYMBOL_FILTERS

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
]