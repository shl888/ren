"""
币安半成品修复
==================================================
【文件职责】
这个文件是币安半成品修复的核心，负责处理两种信息标签：
1. "币安半成品" - 启动修复流程（循环运行）
2. "币安已平仓" - 停止修复流程

【重要提醒】
这个文件会被币安修复区入口调用：
- update_snapshot() - 接收存储区快照
- handle_info() - 接收信息标签

【什么是半成品？】
币安数据天生缺失最新价和标记价，需要从行情数据中补充。
半成品数据的特征：
    - 开仓合约名有值
    - 平仓时间为空
    - 但最新价、标记价等字段为空

【数据来源】
1. 门外存储区快照（通过 update_snapshot 获取）
   - market_data：获取币安最新价和标记价
   - user_data：获取最新的币安数据（用于第4步融合）
2. 本文件缓存：保存修复过程中的中间数据

【修复流程 - 共4步】（完全按照你的设计文档）
第1步：获取缓存数据（从门外存储区或直接用缓存）
第2步：从行情数据提取最新价和标记价
第3步：计算6个字段（根据多空方向）- 【严格按照原始公式，独立计算】
第4步：融合修复并推送
==================================================
"""

import os
import logging
import asyncio
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime

# 导入常量
from ...constants import (
    TAG_COMPLETE,
    INFO_BINANCE_SEMI,
    INFO_BINANCE_CLOSED,
    EXCHANGE_BINANCE,
    FIELD_EXCHANGE,
    FIELD_OPEN_CONTRACT,
    FIELD_OPEN_PRICE,
    FIELD_OPEN_DIRECTION,
    FIELD_POSITION_SIZE,
    FIELD_POSITION_CONTRACTS,
    FIELD_CONTRACT_VALUE,
    FIELD_LEVERAGE,
    FIELD_OPEN_POSITION_VALUE,
    FIELD_OPEN_MARGIN,
    FIELD_MARK_PRICE,
    FIELD_LATEST_PRICE,
    FIELD_MARK_PNL_PERCENT,
    FIELD_LATEST_PNL_PERCENT,
    FIELD_LATEST_MARGIN,
    FIELD_LATEST_POSITION_VALUE,
    FIELD_LATEST_PNL,
    FIELD_LATEST_PNL_PERCENT_OF_MARGIN,
    # 资金费字段（虽然半成品修复不用，但保持导入完整）
    FIELD_FUNDING_THIS,
    FIELD_FUNDING_TOTAL,
    FIELD_FUNDING_COUNT,
    FIELD_FUNDING_TIME,
)

logger = logging.getLogger(__name__)


class BinanceSemiRepair:
    """
    币安半成品修复类
    ==================================================
    这个类负责：
        1. 接收门外标签（半成品/已平仓）- 通过 handle_info()
        2. 接收门外存储区快照（最新数据）- 通过 update_snapshot()
        3. 根据标签启动/停止修复循环
        4. 执行4步修复流程

    门外标签规则：
        - 永远只有1个标签（覆盖更新）
        - 半成品 = 开（启动循环）
        - 已平仓 = 关（停止循环）

    门外数据规则：
        - 永远只有1份存储区快照（覆盖更新）
        - 包含 market_data 和 user_data

    修复循环规则：
        - 每秒执行一次
        - 每次执行前检查门外标签
        - 如果门外标签变了，自己停止
        - 如果修复过程出错，等待5秒后重试
    ==================================================
    """

    def __init__(self, scheduler):
        """
        初始化修复区

        :param scheduler: 调度器实例，用于推送修复结果
        """
        self.scheduler = scheduler

        # ===== 门外标签状态（不是缓存，只是记录）=====
        self.current_info = None      # 当前是什么标签

        # ===== 门外存储区快照（从BinanceRepairArea分发）=====
        self.latest_snapshot = None    # 最新的存储区快照

        # ===== 修复循环控制 =====
        self.is_running = False       # 修复流程是否在运行
        self.repair_task = None       # 修复任务

        # ===== 本文件数据缓存（用于修复计算）=====
        # 只存1条数据，覆盖更新
        self.cache = None              # 类型: Dict or None

        logger.info("✅【币安修复区】【半成品修复】 初始化完成")

    # ==================== 对外入口 ====================

    async def update_snapshot(self, snapshot: Dict):
        """
        接收门外存储区快照（从BinanceRepairArea分发）
        ==================================================
        存储区快照格式：
            {
                'market_data': {
                    'BTCUSDT': {
                        'binance_trade_price': '69457.10',
                        'binance_mark_price': '69466.68',
                        ...
                    },
                    ...
                },
                'user_data': {
                    'binance_user': {
                        'exchange': 'binance',
                        'data': {...}  # 币安业务数据
                    }
                },
                'timestamp': '...'
            }
        ==================================================

        :param snapshot: 完整的存储区快照
        """
        if not snapshot:
            logger.warning("⚠️ 【币安修复区】【半成品修复】收到空快照")
            return

        self.latest_snapshot = snapshot
        logger.debug(f"📦【币安修复区】【半成品修复】 收到存储区快照，时间戳: {snapshot.get('timestamp')}")

    async def handle_info(self, info: str):
        """
        接收调度器推送的信息标签
        ==================================================
        可能收到两种标签：
            - "币安半成品" - 启动修复循环
            - "币安已平仓" - 停止修复循环

        门外标签规则：
            - 永远只有1个标签（覆盖更新）
            - 新标签到来直接覆盖旧标签
        ==================================================

        :param info: 信息标签
        """
        if not info:
            logger.warning("⚠️【币安修复区】【半成品修复】 收到空标签")
            return

        old_info = self.current_info
        self.current_info = info

        logger.info(f"📨【币安修复区】【半成品修复】 门外标签更新: {old_info} → {info}")

        if info == INFO_BINANCE_SEMI:
            await self._start_repair()
        elif info == INFO_BINANCE_CLOSED:
            await self._stop_repair()
        else:
            logger.warning(f"⚠️ 【币安修复区】【半成品修复】收到未知标签: {info}")

    # ==================== 修复循环控制 ====================

    async def _start_repair(self):
        """启动修复流程（循环运行）"""
        if self.is_running:
            logger.debug("【币安修复区】【半成品修复】修复流程已在运行中")
            return

        self.is_running = True
        self.repair_task = asyncio.create_task(self._repair_loop())
        logger.info("🚀【币安修复区】【半成品修复】 修复流程已启动（循环运行）")

    async def _stop_repair(self):
        """停止修复流程"""
        if not self.is_running:
            return

        self.is_running = False
        if self.repair_task:
            self.repair_task.cancel()
            try:
                await self.repair_task
            except asyncio.CancelledError:
                pass
            self.repair_task = None
        logger.info("🛑 【币安修复区】【半成品修复】修复流程已停止")

    async def _repair_loop(self):
        """
        修复循环
        ==================================================
        只要门外标签是"币安半成品"，就一直运行

        循环频率：每秒执行一次
        安全机制：
            - 每次执行前检查门外标签
            - 如果标签变了，自己停止
            - 如果修复过程出错，等待5秒后重试
        ==================================================
        """
        logger.info("🔄【币安修复区】【半成品修复】 修复循环开始")

        while self.is_running:
            try:
                if self.current_info != INFO_BINANCE_SEMI:
                    logger.info("【币安修复区】【半成品修复】门外标签已不是币安半成品，停止修复循环")
                    await self._stop_repair()
                    break

                await self._repair_once()
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("【币安修复区】【半成品修复】修复循环被取消")
                break
            except Exception as e:
                logger.error(f"❌ 【币安修复区】【半成品修复】修复循环出错: {e}", exc_info=True)
                await asyncio.sleep(5)

        logger.info("🔄【币安修复区】【半成品修复】 修复循环结束")

    async def _repair_once(self):
        """
        执行一次修复流程
        ==================================================
        完全按照你的4步设计文档：
            第1步：获取缓存数据（从门外存储区或直接用缓存）
            第2步：从行情数据提取最新价和标记价
            第3步：计算6个字段（根据多空方向）- 【严格按照原始公式，不复用任何中间结果】
            第4步：融合修复并推送
        ==================================================
        """
        logger.debug("【币安修复区】【半成品修复】执行一次修复")

        # 检查门外是否有存储区数据
        if not self.latest_snapshot:
            logger.warning("⚠️【币安修复区】【半成品修复】 门外还没有存储区数据，等待下次循环")
            return

        if not await self._step1_get_cache():
            logger.error("❌【币安修复区】【半成品修复】 第1步失败：无法获取缓存数据，本次修复终止")
            return

        if not await self._step2_get_prices():
            logger.error("❌【币安修复区】【半成品修复】 第2步失败：无法获取行情数据，本次修复终止")
            return

        await self._step3_calc_fields()
        await self._step4_merge_and_push()

        logger.debug("✅【币安修复区】【半成品修复】一次修复执行完成")

    # ==================== 4步修复流程 ====================

    async def _step1_get_cache(self) -> bool:
        """
        第1步：获取缓存数据
        ==================================================
        规则：
            - 如果已经有缓存，直接用
            - 如果没有缓存，从门外存储区读取1条币安数据

        数据来源：
            从门外存储区快照的 user_data 中获取最新的币安数据

        注意：
            只取1条就够了，不需要多次获取
        ==================================================
        """
        if self.cache is not None:
            logger.debug("✅【币安修复区】【半成品修复】 第1步：使用现有缓存")
            return True

        logger.debug("【币安修复区】【半成品修复】第1步：缓存为空，从门外存储区读取币安数据")

        # 从门外存储区获取币安数据
        binance_data = self._get_binance_from_snapshot()
        if not binance_data:
            logger.warning("⚠️【币安修复区】【半成品修复】 门外存储区中没有币安数据")
            return False

        # 保存到缓存
        self.cache = binance_data.copy()
        logger.debug(f"✅【币安修复区】【半成品修复】 第1步：从门外存储区读取到币安数据，开仓合约名: {self.cache.get(FIELD_OPEN_CONTRACT)}")
        return True

    async def _step2_get_prices(self) -> bool:
        """
        第2步：从行情数据提取最新价和标记价
        ==================================================
        根据缓存数据中的开仓合约名，去门外存储区的行情数据中：
            - binance_trade_price → 对应币安数据的最新价
            - binance_mark_price  → 对应币安数据的标记价

        把这两个值覆盖到缓存中
        ==================================================
        """
        logger.debug("【币安修复区】【半成品修复】第2步：从行情数据提取最新价和标记价")

        contract = self.cache.get(FIELD_OPEN_CONTRACT)
        if not contract:
            logger.error("❌【币安修复区】【半成品修复】 缓存中没有开仓合约名")
            return False

        # 从门外存储区获取行情数据
        market_data = self._get_market_data_from_snapshot(contract)
        if not market_data:
            logger.error(f"❌ 【币安修复区】【半成品修复】无法获取合约 {contract} 的行情数据")
            return False

        # 提取币安的行情字段
        latest_price = market_data.get('binance_trade_price')
        mark_price = market_data.get('binance_mark_price')

        if latest_price is None or mark_price is None:
            logger.error(f"❌【币安修复区】【半成品修复】 行情数据中缺少必要字段: latest_price={latest_price}, mark_price={mark_price}")
            return False

        # 转换为float（如果是字符串）
        try:
            latest_price = float(latest_price)
            mark_price = float(mark_price)
        except (TypeError, ValueError):
            logger.error(f"❌ 【币安修复区】【半成品修复】行情数据格式错误: latest_price={latest_price}, mark_price={mark_price}")
            return False

        # 覆盖到缓存
        self.cache[FIELD_LATEST_PRICE] = latest_price
        self.cache[FIELD_MARK_PRICE] = mark_price

        logger.debug(f"✅ 【币安修复区】【半成品修复】第2步：获取到行情数据 - 最新价: {latest_price}, 标记价: {mark_price}")
        return True

    async def _step3_calc_fields(self):
        """
        第3步：计算6个字段
        ==================================================
        【严格按照原始方案，独立计算，不复用任何中间结果】

        需要计算的字段：
            1. 标记价涨跌盈亏幅
            2. 最新价涨跌盈亏幅
            3. 最新价保证金
            4. 最新价仓位价值
            5. 最新价浮盈
            6. 最新价浮盈百分比

        计算公式（严格按照你的文档，每个字段独立计算）：

        当开仓方向为 "LONG" 时：
            标记价涨跌盈亏幅 = (标记价 - 开仓价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (最新价 - 开仓价) * 100 / 开仓价
            最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            最新价仓位价值 = 最新价 * 持仓币数
            最新价浮盈 = 最新价 * 持仓币数 - 开仓价仓位价值
            最新价浮盈百分比 = (最新价 * 持仓币数 - 开仓价仓位价值) * 100 / 开仓保证金

        当开仓方向为 "SHORT" 时：
            标记价涨跌盈亏幅 = (开仓价 - 标记价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (开仓价 - 最新价) * 100 / 开仓价
            最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            最新价仓位价值 = 最新价 * 持仓币数
            最新价浮盈 = 开仓价仓位价值 - (最新价 * 持仓币数)
            最新价浮盈百分比 = [开仓价仓位价值 - (最新价 * 持仓币数)] * 100 / 开仓保证金
        ==================================================
        """
        logger.debug("【币安修复区】【半成品修复】第3步：计算6个字段（严格按照原始方案，独立计算）")

        cache = self.cache

        # 获取原始字段值（直接从缓存读取，不复用任何计算结果）
        latest_price = cache.get(FIELD_LATEST_PRICE, 0)
        mark_price = cache.get(FIELD_MARK_PRICE, 0)
        position_size = cache.get(FIELD_POSITION_SIZE, 0) or 0
        leverage = cache.get(FIELD_LEVERAGE, 1) or 1
        open_price = cache.get(FIELD_OPEN_PRICE, 0) or 0
        open_position_value = cache.get(FIELD_OPEN_POSITION_VALUE, 0) or 0
        open_margin = cache.get(FIELD_OPEN_MARGIN, 1) or 1
        direction = cache.get(FIELD_OPEN_DIRECTION)

        # 根据方向计算 - 每个字段独立计算，严格按照原始公式
        if direction == "LONG":
            # 多头 - 严格按照原始公式，独立计算每个字段

            # 1. 标记价涨跌盈亏幅 = (标记价 - 开仓价) * 100 / 开仓价
            mark_pnl_percent = (mark_price - open_price) * 100 / open_price if open_price else 0

            # 2. 最新价涨跌盈亏幅 = (最新价 - 开仓价) * 100 / 开仓价
            latest_pnl_percent = (latest_price - open_price) * 100 / open_price if open_price else 0

            # 3. 最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            latest_margin = (latest_price * position_size / leverage) if leverage else 0

            # 4. 最新价仓位价值 = 最新价 * 持仓币数
            latest_position_value = latest_price * position_size

            # 5. 最新价浮盈 = 最新价 * 持仓币数 - 开仓价仓位价值
            latest_pnl = (latest_price * position_size) - open_position_value

            # 6. 最新价浮盈百分比 = (最新价 * 持仓币数 - 开仓价仓位价值) * 100 / 开仓保证金
            latest_pnl_percent_of_margin = ((latest_price * position_size - open_position_value) * 100 / open_margin) if open_margin else 0

        else:  # direction == "SHORT"
            # 空头 - 严格按照原始公式，独立计算每个字段

            # 1. 标记价涨跌盈亏幅 = (开仓价 - 标记价) * 100 / 开仓价
            mark_pnl_percent = (open_price - mark_price) * 100 / open_price if open_price else 0

            # 2. 最新价涨跌盈亏幅 = (开仓价 - 最新价) * 100 / 开仓价
            latest_pnl_percent = (open_price - latest_price) * 100 / open_price if open_price else 0

            # 3. 最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            latest_margin = (latest_price * position_size / leverage) if leverage else 0

            # 4. 最新价仓位价值 = 最新价 * 持仓币数
            latest_position_value = latest_price * position_size

            # 5. 最新价浮盈 = 开仓价仓位价值 - (最新价 * 持仓币数)
            latest_pnl = open_position_value - (latest_price * position_size)

            # 6. 最新价浮盈百分比 = [开仓价仓位价值 - (最新价 * 持仓币数)] * 100 / 开仓保证金
            latest_pnl_percent_of_margin = ((open_position_value - latest_price * position_size) * 100 / open_margin) if open_margin else 0

        # 保存计算结果到缓存 - 每个字段只赋值一次
        cache[FIELD_MARK_PNL_PERCENT] = mark_pnl_percent
        cache[FIELD_LATEST_PNL_PERCENT] = latest_pnl_percent
        cache[FIELD_LATEST_MARGIN] = latest_margin
        cache[FIELD_LATEST_POSITION_VALUE] = latest_position_value
        cache[FIELD_LATEST_PNL] = latest_pnl
        cache[FIELD_LATEST_PNL_PERCENT_OF_MARGIN] = latest_pnl_percent_of_margin

        logger.debug(f" 【币安修复区】【半成品修复】  计算完成 - 标记价涨跌盈亏幅: {mark_pnl_percent:.2f}%, "
                   f"最新价涨跌盈亏幅: {latest_pnl_percent:.2f}%, "
                   f"最新价保证金: {latest_margin:.2f}, "
                   f"最新价仓位价值: {latest_position_value:.2f}, "
                   f"最新价浮盈: {latest_pnl:.2f}, "
                   f"最新价浮盈百分比: {latest_pnl_percent_of_margin:.2f}%, "
                   f"开仓方向: {direction}")

    async def _step4_merge_and_push(self):
        """
        第4步：融合修复并推送
        ==================================================
        做了三件事：
            1. 从门外存储区读取最新的币安数据
            2. 把缓存中的8个字段值填充进去
            3. 打上"持仓完整"标签推送给调度器

        需要填充的8个字段：
            - 最新价 (已从行情获取)
            - 标记价 (已从行情获取)
            - 标记价涨跌盈亏幅 (计算得到)
            - 最新价涨跌盈亏幅 (计算得到)
            - 最新价保证金 (计算得到)
            - 最新价仓位价值 (计算得到)
            - 最新价浮盈 (计算得到)
            - 最新价浮盈百分比 (计算得到)
        ==================================================
        """
        logger.debug("【币安修复区】【半成品修复】第4步：融合修复并推送")

        # 从门外存储区获取最新的币安数据
        latest_binance = self._get_binance_from_snapshot()
        if not latest_binance:
            logger.error("❌【币安修复区】【半成品修复】 无法获取最新的币安数据，融合失败")
            return

        # 创建副本（要推送的数据）
        merged_data = latest_binance.copy()

        # 要填充的8个字段（严格按照设计文档）
        fields_to_fill = [
            FIELD_LATEST_PRICE,                    # 1. 最新价
            FIELD_MARK_PRICE,                      # 2. 标记价
            FIELD_MARK_PNL_PERCENT,                # 3. 标记价涨跌盈亏幅
            FIELD_LATEST_PNL_PERCENT,              # 4. 最新价涨跌盈亏幅
            FIELD_LATEST_MARGIN,                   # 5. 最新价保证金
            FIELD_LATEST_POSITION_VALUE,           # 6. 最新价仓位价值
            FIELD_LATEST_PNL,                      # 7. 最新价浮盈
            FIELD_LATEST_PNL_PERCENT_OF_MARGIN,    # 8. 最新价浮盈百分比
        ]

        # 从缓存中获取这些字段的值，填充到合并数据中
        fill_count = 0
        for field in fields_to_fill:
            if field in self.cache and self.cache[field] is not None:
                merged_data[field] = self.cache[field]
                fill_count += 1

        logger.debug(f" 【币安修复区】【半成品修复】  已填充 {fill_count} 个字段")

        # 打标签推送
        await self.scheduler.handle({
            'tag': TAG_COMPLETE,
            'data': merged_data
        })

        contract = merged_data.get(FIELD_OPEN_CONTRACT, 'unknown')
        logger.info(f"✅ 【币安修复区】【半成品修复】已推送持仓完整数据: {EXCHANGE_BINANCE} - {contract}")

    # ==================== 辅助方法 ====================

    def _get_binance_from_snapshot(self) -> Optional[Dict]:
        """
        从门外存储区快照中获取最新的币安数据
        ==================================================
        存储区快照格式：
            {
                'user_data': {
                    'binance_user': {
                        'exchange': 'binance',
                        'data': {...}  # 真正的业务数据
                    }
                }
            }
        ==================================================
        """
        if not self.latest_snapshot:
            logger.debug("【币安修复区】【半成品修复】门外还没有存储区数据")
            return None

        user_data = self.latest_snapshot.get('user_data', {})
        binance_key = f"{EXCHANGE_BINANCE}_user"
        binance_item = user_data.get(binance_key, {})

        if not binance_item:
            logger.debug("【币安修复区】【半成品修复】存储区中没有币安用户数据")
            return None

        return binance_item.get('data')

    def _get_market_data_from_snapshot(self, contract: str) -> Optional[Dict]:
        """
        从门外存储区快照中获取指定合约的行情数据
        ==================================================
        存储区快照格式：
            {
                'market_data': {
                    'BTCUSDT': {
                        'binance_trade_price': '69457.10',
                        'binance_mark_price': '69466.68',
                        ...
                    },
                    ...
                }
            }
        ==================================================

        :param contract: 合约名，如 'BTCUSDT'
        :return: 该合约的行情数据，或None
        """
        if not self.latest_snapshot:
            logger.debug("【币安修复区】【半成品修复】门外还没有存储区数据")
            return None

        market_data = self.latest_snapshot.get('market_data', {})
        if not market_data:
            logger.debug("【币安修复区】【半成品修复】存储区中没有行情数据")
            return None

        return market_data.get(contract)
