"""
WebSocket连接池核心模块
工业级稳定版本 - 简化版
"""
__version__ = "1.0.0-stable-simple"

# 暴露核心组件
from websocket_pool.core.connection import Connection
from websocket_pool.core.worker_base import BaseWorker
from websocket_pool.core.data_worker import DataWorker
from websocket_pool.core.backup_worker import BackupWorker
from websocket_pool.core.monitor import MonitorCenter

__all__ = [
    'Connection',
    'BaseWorker', 
    'DataWorker', 
    'BackupWorker',
    'MonitorCenter'
]