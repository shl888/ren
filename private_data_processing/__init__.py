"""
私人数据处理模块初始化文件
导出接口保持不变，完全兼容
"""
from .manager import (
    PrivateDataProcessor,
    get_processor,
    receive_private_data
)

# 新增导出（可选，不导出也不影响核心功能）
from .classifier import classify_binance_order
from .cache_manager import save_order_event, clear_symbol_cache

__version__ = '1.1.0'  # 版本号升级
__all__ = [
    'PrivateDataProcessor',
    'get_processor',
    'receive_private_data'
]