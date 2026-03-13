"""
调度区：接收检测区的推送，根据标签分发
==================================================
【文件职责】
这个文件是整个模块的交通枢纽，负责：
1. 接收检测区推送的标签和数据
2. 根据标签类型分发给不同的目的地

【转发规则 - 完全按照你的设计文档】

数据标签（带数据）：
    - 空仓 → 只推大脑（去除标签）
    - 持仓完整 → 推大脑（去除标签） + 推数据库（保留标签）
    - 已平仓 → 推大脑（去除标签） + 推数据库（保留标签）

信息标签（纯标签）：
    - 币安半成品、币安持仓缺失、币安已平仓 → 推币安修复区入口
    - 欧意持仓缺失、欧意已平仓 → 推欧易修复文件

【重要规则】
- 推大脑的数据必须去除标签，只保留原始数据
- 推数据库的数据必须保留标签，因为数据库需要根据标签决定如何存储
- 信息标签绝对不能带数据，只推标签本身

【修正记录】
- 2026-03-13：修正推送到大脑的数据类型为 'user_summary'（原'position'在data_manager中无对应处理）
==================================================
"""

import logging
from typing import Dict, Any
import asyncio
from datetime import datetime

# 导入常量
from .constants import (
    TAG_CLOSED,
    TAG_EMPTY,
    TAG_COMPLETE,
    INFO_BINANCE_SEMI,
    INFO_BINANCE_MISSING,
    INFO_BINANCE_CLOSED,
    INFO_OKX_MISSING,
    INFO_OKX_CLOSED,
)

logger = logging.getLogger(__name__)


class Scheduler:
    """
    调度器
    ==================================================
    这个类负责：
        1. 接收检测区的推送（handle方法）
        2. 根据消息类型（数据标签/信息标签）进行分发
        3. 维护数据库和修复区的引用（通过set方法注入）

    注意：调度器不关心数据的具体内容，只根据标签转发。
    ==================================================
    """

    def __init__(self, brain):
        """
        初始化调度器

        :param brain: 大脑模块实例（data_manager），用于推送数据
        """
        self.brain = brain

        # ===== 下游模块引用（通过set方法注入）=====
        self.database = None           # 数据库存储区
        self.repair_binance = None     # 币安修复区入口
        self.repair_okx = None         # 欧易修复文件

        logger.info("✅ 调度区初始化完成")

    # ==================== 设置下游模块 ====================

    def set_database(self, database):
        """
        设置数据库存储区
        :param database: Database实例，必须有 handle_data(tag, data) 方法
        """
        self.database = database
        logger.info("✅ 调度区已连接数据库")

    def set_repair_binance(self, repair):
        """
        设置币安修复区
        :param repair: BinanceRepairArea实例，必须有 handle_info(info) 方法
        """
        self.repair_binance = repair
        logger.info("✅ 调度区已连接币安修复区")

    def set_repair_okx(self, repair):
        """
        设置欧易修复区
        :param repair: OkxMissingRepair实例，必须有 handle_info(info) 方法
        """
        self.repair_okx = repair
        logger.info("✅ 调度区已连接欧易修复区")

    # ==================== 核心处理方法 ====================

    async def handle(self, message: Dict[str, Any]):
        """
        处理检测区推送的消息
        ==================================================
        这是调度器的唯一入口，检测区调用此方法推送结果。

        消息格式有两种：
            1. 数据标签：{'tag': '已平仓', 'data': {...}}
            2. 信息标签：{'info': '币安半成品'}

        处理流程：
            - 根据消息类型分别调用 _handle_tag 或 _handle_info
            - 异常捕获，确保单个消息处理失败不影响后续

        :param message: 检测区推送的消息
        ==================================================
        """
        try:
            # ===== 情况1：收到数据标签 =====
            if 'tag' in message:
                await self._handle_tag(message)

            # ===== 情况2：收到信息标签 =====
            elif 'info' in message:
                await self._handle_info(message)

            else:
                logger.warning(f"⚠️ 收到未知格式消息: {message}")

        except Exception as e:
            logger.error(f"❌ 调度处理失败: {e}", exc_info=True)

    async def _handle_tag(self, message: Dict[str, Any]):
        """
        处理数据标签
        ==================================================
        数据标签格式：{'tag': '已平仓', 'data': {...}}

        转发规则：
            - 空仓 → 只推大脑（去除标签）
            - 持仓完整 → 推大脑（去除标签） + 推数据库（保留标签）
            - 已平仓 → 推大脑（去除标签） + 推数据库（保留标签）

        :param message: 包含tag和data的消息
        ==================================================
        """
        tag = message['tag']
        data = message.get('data', {})

        # 提取交易所信息（用于日志）
        exchange = data.get('交易所', 'unknown')

        logger.info(f"📨 调度收到数据标签: {tag} - {exchange}")

        # ===== 根据标签处理 =====
        if tag in [TAG_CLOSED, TAG_COMPLETE]:
            # 已平仓 或 持仓完整：推大脑 + 推数据库

            # 1. 推大脑（不带标签）
            await self._push_to_brain(exchange, data)

            # 2. 推数据库（保留标签）
            if self.database:
                try:
                    # 数据库的 handle_data 方法接收 tag 和 data
                    await self.database.handle_data(tag, data)
                except Exception as e:
                    logger.error(f"❌ 推送到数据库失败: {e}")
            else:
                logger.warning("⚠️ 数据库未就绪，无法保存数据")

        elif tag == TAG_EMPTY:
            # 空仓：只推大脑（不带标签）
            await self._push_to_brain(exchange, data)

        else:
            logger.warning(f"⚠️ 未知数据标签: {tag}")

    async def _handle_info(self, message: Dict[str, Any]):
        """
        处理信息标签
        ==================================================
        信息标签格式：{'info': '币安半成品'}

        转发规则：
            - 币安相关 → 推币安修复区
            - 欧意相关 → 推欧易修复区

        :param message: 包含info的消息
        ==================================================
        """
        info = message['info']
        logger.info(f"📨 调度收到信息标签: {info}")

        # ===== 币安相关信息标签 =====
        if info in [INFO_BINANCE_SEMI, INFO_BINANCE_MISSING, INFO_BINANCE_CLOSED]:
            if self.repair_binance:
                try:
                    # 币安修复区入口的 handle_info 方法
                    await self.repair_binance.handle_info(info)
                except Exception as e:
                    logger.error(f"❌ 推送到币安修复区失败: {e}")
            else:
                logger.warning(f"⚠️ 币安修复区未就绪，无法处理: {info}")

        # ===== 欧意相关信息标签 =====
        elif info in [INFO_OKX_MISSING, INFO_OKX_CLOSED]:
            if self.repair_okx:
                try:
                    # 欧易修复文件的 handle_info 方法
                    await self.repair_okx.handle_info(info)
                except Exception as e:
                    logger.error(f"❌ 推送到欧易修复区失败: {e}")
            else:
                logger.warning(f"⚠️ 欧易修复区未就绪，无法处理: {info}")

        else:
            logger.warning(f"⚠️ 未知信息标签: {info}")

    # ==================== 推送大脑 ====================

    async def _push_to_brain(self, exchange: str, data: Dict[str, Any]):
        """
        推送到大脑模块（不带标签）
        ==================================================
        大脑模块的接口是 receive_private_data，需要特定格式。

        【修正】数据类型改为 'user_summary'，与 data_manager 的接口匹配

        构造格式：
            {
                'exchange': exchange,           # 交易所
                'data_type': 'user_summary',   # 修正：与data_manager匹配
                'data': data,                    # 原始数据（已去除标签）
                'timestamp': ...                # 时间戳
            }

        :param exchange: 交易所名称
        :param data: 原始数据（已去除标签）
        ==================================================
        """
        try:
            if not self.brain:
                logger.warning("⚠️ 大脑模块未就绪，无法推送")
                return

            # 获取时间戳（优先用数据里的，没有就用当前时间）
            timestamp = data.get('开仓时间') or data.get('平仓时间') or datetime.now().isoformat()

            # 构造大脑期望的格式
            brain_message = {
                'exchange': exchange,
                'data_type': 'user_summary',  # 【修正】原为'position'，改为'user_summary'与data_manager匹配
                'data': data,                   # 原始数据，不带标签
                'timestamp': timestamp
            }

            # 调用大脑的接收方法
            await self.brain.receive_private_data(brain_message)
            logger.debug(f"✅ 已推送到大脑: {exchange}")

        except Exception as e:
            logger.error(f"❌ 推送到大脑失败: {e}")
