"""
检测区：接收receiver推送的存储区快照
==================================================
【文件职责】
这个文件负责检测数据状态，根据数据字段的不同组合打上对应的标签，
并将检测结果推送给调度器。

【数据来源】
通过 handle_store_snapshot 接收 receiver 推送的存储区快照

【推送目的地】
调度器 (scheduler.py)

【检测规则 - 完全按照你的设计文档】

欧意数据检测：
    a. 标记价保证金为空 → 推送两条：
        - 数据标签：{'tag': '空仓', 'data': {...}}
        - 信息标签：{'info': '欧易空仓'}
    
    b. 标记价保证金有值 AND 开仓合约名空 → 推送一条：
        - 信息标签：{'info': '欧意持仓缺失'}
    
    c. 开仓合约名有值 AND 平仓时间空 → 推送一条：
        - 数据标签：{'tag': '持仓完整', 'data': {...}}
    
    d. 开仓合约名有值 AND 平仓时间有值 → 推送一条：
        - 数据标签：{'tag': '平仓完整', 'data': {...}}

币安数据检测：
    a. 标记价保证金为空 → 推送两条：
        - 数据标签：{'tag': '空仓', 'data': {...}}
        - 信息标签：{'info': '币安空仓'}
    
    b. 标记价保证金有值 AND 开仓合约名有值 → 推送一条：
        - 信息标签：{'info': '币安半成品'}
    
    c. 标记价保证金有值 AND 开仓合约名空 → 推送一条：
        - 信息标签：{'info': '币安持仓缺失'}

【重要规则】
- 数据标签必须带数据，信息标签绝对不能带数据
- 空仓时同时推送数据标签和信息标签（两条独立消息）
==================================================
"""

from datetime import datetime
from typing import Dict, Any, Optional, List
import logging
import asyncio

# 导入常量（使用正确的常量名）
from .constants import (
    TAG_CLOSED_COMPLETE,  # 原 TAG_CLOSED 改为平仓完整
    TAG_EMPTY,
    TAG_COMPLETE,
    INFO_BINANCE_SEMI,
    INFO_BINANCE_MISSING,
    INFO_BINANCE_EMPTY,   # 原 INFO_BINANCE_CLOSED 改为币安空仓
    INFO_OKX_MISSING,
    INFO_OKX_EMPTY,        # 原 INFO_OKX_CLOSED 改为欧易空仓
    FIELD_CLOSE_TIME,
    FIELD_OPEN_CONTRACT,
    FIELD_MARK_MARGIN,
)

logger = logging.getLogger(__name__)


class DataDetector:
    """
    数据检测器
    ==================================================
    这个类负责：
        1. 接收receiver推送的存储区快照
        2. 分别检测欧意数据和币安数据
        3. 根据检测结果打标签
        4. 将标签和数据推送给调度器
    
    检测逻辑完全按照你的设计文档，不增加任何额外判断。
    ==================================================
    """
    
    def __init__(self, scheduler):
        """
        初始化检测区
        
        :param scheduler: 调度器实例，用于推送检测结果
        """
        self.scheduler = scheduler
        logger.info("✅ 【检测区】初始化完成")
    
    async def handle_store_snapshot(self, snapshot: Dict[str, Any]):
        """
        接收receiver推送的存储区快照
        ==================================================
        这是检测区的唯一入口，receiver每次更新数据都会调用此方法。
        
        处理流程：
            1. 从快照中提取用户数据
            2. 检测欧意数据（如果存在）
            3. 检测币安数据（如果存在）
        
        :param snapshot: 存储区快照，格式：
            {
                'market_data': {...},
                'user_data': {
                    'okx_user': {'data': {...}},
                    'binance_user': {'data': {...}}
                },
                'timestamp': '...'
            }
        ==================================================
        """
        if not snapshot:
            logger.warning("⚠️ 【检测区】收到空快照")
            return
        
        logger.debug("【检测区】收到存储区快照，开始检测")
        
        # 提取用户数据
        user_data = snapshot.get('user_data', {})
        
        # ===== 检测欧意数据 =====
        if 'okx_user' in user_data:
            okx_item = user_data['okx_user']
            okx_data = okx_item.get('data', {})
            if okx_data:
                await self._detect_okx(okx_data)
            else:
                logger.debug("【检测区】欧意数据为空")
        
        # ===== 检测币安数据 =====
        if 'binance_user' in user_data:
            binance_item = user_data['binance_user']
            binance_data = binance_item.get('data', {})
            if binance_data:
                await self._detect_binance(binance_data)
            else:
                logger.debug("【检测区】币安数据为空")
    
    async def _detect_okx(self, data: Dict[str, Any]):
        """
        检测欧意数据
        ==================================================
        完全按照你的设计文档的4种情况：
            a. 空仓（标记价保证金为空）
            b. 持仓缺失（标记价保证金有值 AND 开仓合约名空）
            c. 持仓完整（开仓合约名有值 AND 平仓时间空）
            d. 平仓完整（开仓合约名有值 AND 平仓时间有值）
        
        每种情况推送对应的标签（可能1条或2条）
        
        :param data: 欧意业务数据，字段全是中文
        ==================================================
        """
        
        # ----- 情况a：空仓（标记价保证金为空）-----
        if data.get(FIELD_MARK_MARGIN) is None:
            logger.info(f"🔍 【检测区】欧意检测到空仓: {data.get('交易所')}")
            
            # 同时推送两条独立消息
            await asyncio.gather(
                self.scheduler.handle({  # 数据标签
                    'tag': TAG_EMPTY,
                    'data': data.copy()
                }),
                self.scheduler.handle({  # 信息标签
                    'info': INFO_OKX_EMPTY  # 使用常量
                })
            )
            return
        
        # ----- 情况b：持仓缺失（标记价保证金有值 AND 开仓合约名空）-----
        if data.get(FIELD_MARK_MARGIN) is not None and data.get(FIELD_OPEN_CONTRACT) is None:
            logger.info(f"🔍 【检测区】欧意检测到持仓缺失: {data.get('交易所')}")
            
            await self.scheduler.handle({
                'info': INFO_OKX_MISSING
            })
            return
        
        # ----- 情况c：持仓完整（开仓合约名有值 AND 平仓时间空）-----
        if data.get(FIELD_OPEN_CONTRACT) and data.get(FIELD_CLOSE_TIME) is None:
            logger.info(f"🔍【检测区】 欧意检测到持仓完整: {data.get('交易所')} - {data.get(FIELD_OPEN_CONTRACT)}")
            
            await self.scheduler.handle({
                'tag': TAG_COMPLETE,
                'data': data.copy()
            })
            return
        
        # ----- 情况d：平仓完整（开仓合约名有值 AND 平仓时间有值）-----
        if data.get(FIELD_OPEN_CONTRACT) and data.get(FIELD_CLOSE_TIME):
            logger.info(f"🔍【检测区】 欧意检测到平仓完整: {data.get('交易所')} - {data.get(FIELD_OPEN_CONTRACT)}")
            
            await self.scheduler.handle({
                'tag': TAG_CLOSED_COMPLETE,  # 使用正确的常量
                'data': data.copy()
            })
            return
        
        # 其他情况（理论上不会发生）
        logger.debug(f"【检测区】欧意数据无匹配标签: {data.get('交易所')}")
    
    async def _detect_binance(self, data: Dict[str, Any]):
        """
        检测币安数据
        ==================================================
        完全按照你的设计文档的3种情况：
            a. 空仓（标记价保证金为空）
            b. 半成品（标记价保证金有值 AND 开仓合约名有值）
            c. 持仓缺失（标记价保证金有值 AND 开仓合约名空）
        
        每种情况推送对应的标签（可能1条或2条）
        
        :param data: 币安业务数据，字段全是中文
        ==================================================
        """
        
        # ----- 情况a：空仓（标记价保证金为空）-----
        if data.get(FIELD_MARK_MARGIN) is None:
            logger.info(f"🔍 【检测区】币安检测到空仓: {data.get('交易所')}")
            
            # 同时推送两条独立消息
            await asyncio.gather(
                self.scheduler.handle({  # 数据标签
                    'tag': TAG_EMPTY,
                    'data': data.copy()
                }),
                self.scheduler.handle({  # 信息标签
                    'info': INFO_BINANCE_EMPTY  # 使用常量
                })
            )
            return
        
        # ----- 情况b：半成品（标记价保证金有值 AND 开仓合约名有值）-----
        if data.get(FIELD_MARK_MARGIN) is not None and data.get(FIELD_OPEN_CONTRACT):
            logger.info(f"🔍 【检测区】币安检测到半成品: {data.get('交易所')} - {data.get(FIELD_OPEN_CONTRACT)}")
            
            await self.scheduler.handle({
                'info': INFO_BINANCE_SEMI
            })
            return
        
        # ----- 情况c：持仓缺失（标记价保证金有值 AND 开仓合约名空）-----
        if data.get(FIELD_MARK_MARGIN) is not None and data.get(FIELD_OPEN_CONTRACT) is None:
            logger.info(f"🔍【检测区】 币安检测到持仓缺失: {data.get('交易所')}")
            
            await self.scheduler.handle({
                'info': INFO_BINANCE_MISSING
            })
            return
        
        # 其他情况（理论上不会发生）
        logger.debug(f"【检测区】币安数据无匹配标签: {data.get('交易所')}")