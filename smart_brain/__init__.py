# smart_brain/__init__.py
"""
智能大脑模块 - 交易系统的核心控制器
"""

__version__ = "1.0.0"
__all__ = ['SmartBrain', 'DataReceiver', 'StatusMonitor']

from .core import SmartBrain
from .data_receiver import DataReceiver
from .status_monitor import StatusMonitor
