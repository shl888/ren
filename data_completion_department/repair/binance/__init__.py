"""
币安修复区 - 模块入口
==================================================
【文件职责】
统一接收门外标签和门外数据，分发给两个修复文件：
1. semi_repair.py     - 处理"币安半成品"和"币安已平仓"
2. missing_repair.py  - 处理"币安持仓缺失"和"币安已平仓"

【调用关系】
接收存储区 (receiver.py) 
    ↓ handle_store_snapshot
币安修复区入口 (这个文件)
    ├─→ semi_repair.update_snapshot()
    └─→ missing_repair.update_snapshot()

调度器 (scheduler.py)
    ↓ handle_info
币安修复区入口 (这个文件)
    ├─→ "币安半成品" → semi_repair.handle_info()
    ├─→ "币安持仓缺失" → missing_repair.handle_info()
    └─→ "币安已平仓" → 同时发给两个文件

【数据共享】
- 两个修复文件共用同一份门外存储区快照
- 门外标签根据类型分发给对应的修复文件
- "币安已平仓"标签同时发给两个文件（用于关闭修复流程）

【导出内容】
- BinanceRepairArea    - 币安修复区入口类
- BinanceSemiRepair    - 币安半成品修复类
- BinanceMissingRepair - 币安持仓缺失修复类
==================================================
"""

import logging
import asyncio  # ✅ [蚂蚁基因修复] 导入asyncio用于并行执行
from typing import Dict
from .semi_repair import BinanceSemiRepair
from .missing_repair import BinanceMissingRepair

# 导入常量
from ...constants import (
    INFO_BINANCE_SEMI,
    INFO_BINANCE_MISSING,
    INFO_BINANCE_CLOSED
)

logger = logging.getLogger(__name__)


class BinanceRepairArea:
    """
    币安修复区
    ==================================================
    负责：
        1. 统一接收门外存储区快照（semi_repair和missing_repair共用）
        2. 统一接收门外标签，并根据标签分发给对应的修复文件
        3. "币安已平仓"标签同时分发给两个文件
    
    门外数据规则：
        - 永远只有1份存储区快照（覆盖更新）
        - 两个修复文件通过 update_snapshot() 获取
    
    门外标签规则：
        - 永远只有1个标签（覆盖更新）
        - 根据标签类型分发给对应的修复文件
    ==================================================
    """
    
    def __init__(self, scheduler):
        """
        初始化币安修复区
        
        :param scheduler: 调度器实例，用于推送修复结果
        """
        self.scheduler = scheduler
        
        # ===== 创建两个修复实例 =====
        self.semi_repair = BinanceSemiRepair(scheduler)
        self.missing_repair = BinanceMissingRepair(scheduler)
        
        # ===== 门外存储区快照（两个修复文件共用）=====
        self.latest_snapshot = None
        
        logger.info("✅ 【币安修复区】初始化完成")
    
    async def handle_store_snapshot(self, snapshot: Dict):
        """
        接收receiver推送的存储区快照
        ==================================================
        两个修复文件共用这一份数据，通过 update_snapshot() 分发
        
        :param snapshot: 完整的存储区快照
            {
                'market_data': {...},  # 行情数据（币安需要）
                'user_data': {...},     # 私人数据
                'timestamp': '...'
            }
        ==================================================
        """
        if not snapshot:
            logger.warning("⚠️ 【币安修复区】收到空快照")
            return
            
        self.latest_snapshot = snapshot
        
        # ✅ [蚂蚁基因修复] 改为并行执行，两个修复文件同时更新快照
        await asyncio.gather(
            self.semi_repair.update_snapshot(snapshot),
            self.missing_repair.update_snapshot(snapshot),
            return_exceptions=True  # 一个失败不影响另一个
        )
        
        logger.debug(f"📦 币安修复区收到存储区快照，时间戳: {snapshot.get('timestamp')}")
    
    async def handle_info(self, info: str):
        """
        接收调度器推送的信息标签
        ==================================================
        根据标签类型分发给对应的修复文件：
            - "币安半成品" → 发给 semi_repair
            - "币安持仓缺失" → 发给 missing_repair
            - "币安已平仓" → 同时发给两个文件（关闭修复流程）
        
        :param info: 信息标签
        ==================================================
        """
        if not info:
            logger.warning("⚠️【币安修复区】 收到空标签")
            return
            
        logger.debug(f"📨 币安修复区收到标签: {info}")
        
        # 使用常量判断
        if info == INFO_BINANCE_SEMI:
            await self.semi_repair.handle_info(info)
            
        elif info == INFO_BINANCE_MISSING:
            await self.missing_repair.handle_info(info)
            
        elif info == INFO_BINANCE_CLOSED:
            # ✅ [蚂蚁基因修复] 改为并行执行，同时通知两个文件关闭修复流程
            await asyncio.gather(
                self.semi_repair.handle_info(info),
                self.missing_repair.handle_info(info),
                return_exceptions=True  # 一个失败不影响另一个
            )
            
        else:
            logger.warning(f"⚠️ 币安修复区收到未知信息标签: {info}")


# 导出方便外部导入
__all__ = [
    'BinanceRepairArea',      # 币安修复区入口
    'BinanceSemiRepair',      # 币安半成品修复
    'BinanceMissingRepair'    # 币安持仓缺失修复
]