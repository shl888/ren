"""
WebSocket连接池核心模块
工业级稳定版本 - 简化版
"""
__version__ = "1.0.0-stable-simple"

# 暴露核心组件
from .connection import Connection
from .worker_base import BaseWorker
from .data_worker import DataWorker
from .backup_worker import BackupWorker
from .monitor import MonitorCenter

__all__ = [
    'Connection',
    'BaseWorker', 
    'DataWorker', 
    'BackupWorker',
    'MonitorCenter'
]