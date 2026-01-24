"""
智能大脑模块 - 重构版
"""

from .core import SmartBrain
from .data_manager import DataManager
from .command_router import CommandRouter

__all__ = [
    'SmartBrain',
    'DataManager',
    'CommandRouter', 
]
__version__ = '2.0.0'