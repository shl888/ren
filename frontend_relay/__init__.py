# frontend_relay/__init__.py
"""
前端中继模块 - 连接大脑与前端
"""

from .qd_server import FrontendRelayServer
from .stats_handler import StatsHandler  # 🆕 新增

__version__ = "1.0.0"
__all__ = ['FrontendRelayServer', 'StatsHandler']  # 🆕 新增 StatsHandler