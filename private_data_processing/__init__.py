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
from .okx_classifier import classify_okx_order, is_closing_event as is_okx_closing

# ===== 导入调度器，触发自动启动 =====
from . import scheduler

# ===== 关键：调度器启动后，把 step1 实例给 manager =====
from .scheduler import get_scheduler
from .manager import get_processor

# 获取调度器实例（这会触发自动启动）
sched = get_scheduler()

# 把调度器创建的 step1 给 manager
proc = get_processor()
if sched.step1:  # 如果调度器已经创建了 step1
    proc.set_step1(sched.step1)
    logger.info("🔗【初始化】manager 已获取 step1 实例")
else:
    logger.warning("⚠️【初始化】step1 尚未创建，稍后重试？")

__version__ = '1.2.0'
__all__ = [
    'PrivateDataProcessor',
    'get_processor',
    'receive_private_data',
    'classify_binance_order',
    'is_closing_event',
    'classify_okx_order',
    'is_okx_closing'
]