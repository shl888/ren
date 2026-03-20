"""
修复区 - 模块入口
==================================================
【文件职责】
这个文件是修复区的根目录入口，负责导出所有修复类，
方便外部模块（如调度器）导入。

【导出内容】
- BinanceRepairArea    - 币安修复区入口（统一接收数据和标签）
- BinanceSemiRepair    - 币安半成品修复类
- BinanceMissingRepair - 币安持仓缺失修复类
- OkxMissingRepair     - 欧意持仓缺失修复类
- order_dict
- STANDARD_FIELD_ORDER

【使用示例】
    from data_completion_department.repair import (
        BinanceRepairArea,
        BinanceSemiRepair,
        BinanceMissingRepair,
        OkxMissingRepair,
        order_dict,
        STANDARD_FIELD_ORDER
    )
    
    # 创建币安修复区入口
    binance_repair = BinanceRepairArea(scheduler)
==================================================
"""

from .binance import BinanceRepairArea, BinanceSemiRepair, BinanceMissingRepair
from .okx import OkxMissingRepair
from .utils import order_dict, STANDARD_FIELD_ORDER

__all__ = [
    'BinanceRepairArea',      # 币安修复区入口（新增！）
    'BinanceSemiRepair',      # 币安半成品修复
    'BinanceMissingRepair',   # 币安持仓缺失修复
    'OkxMissingRepair',        # 欧意持仓缺失修复
    'order_dict',
    'STANDARD_FIELD_ORDER'
]