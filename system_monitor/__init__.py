"""
系统状态监控模块
按需采集系统数据，不常驻运行
"""
from .collector import SystemMonitor
from .api import setup_monitor_routes

__version__ = "1.0.0"
__all__ = ['SystemMonitor', 'setup_monitor_routes']