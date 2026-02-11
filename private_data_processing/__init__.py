"""
私人数据处理模块
简化版：只接收、存储、查看私人数据
"""
from .manager import (
    PrivateDataProcessor,
    get_processor,
    receive_private_data
)
from .binance_classifier import classify_binance_order, is_closing_event

__version__ = '1.1.0'
__all__ = [
    'PrivateDataProcessor',
    'get_processor',
    'receive_private_data',
    'classify_binance_order',
    'is_closing_event'
]