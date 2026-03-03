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
from .okx_classifier import classify_okx_order, is_closing_event as is_okx_closing  # 新增

__version__ = '1.2.0'  # 版本号提升
__all__ = [
    'PrivateDataProcessor',
    'get_processor',
    'receive_private_data',
    'classify_binance_order',
    'is_closing_event',
    'classify_okx_order',   # 新增
    'is_okx_closing'        # 新增
]