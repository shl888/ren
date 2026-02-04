"""
私人数据处理模块
简化版：只接收、存储、查看私人数据
"""
from .manager import (
    PrivateDataProcessor,
    get_processor,
    receive_private_data
)

__version__ = '1.0.0'
__all__ = [
    'PrivateDataProcessor',
    'get_processor',
    'receive_private_data'
]