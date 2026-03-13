"""
币安持仓缺失修复
==================================================
【文件职责】
这个文件是币安持仓缺失修复的核心，负责处理两种信息标签：
1. "币安持仓缺失" - 启动修复流程（循环运行）
2. "币安已平仓"   - 停止修复流程

【重要提醒】
这个文件会被币安修复区入口调用：
- update_snapshot() - 接收存储区快照
- handle_info() - 接收信息标签

【什么是持仓缺失？】
币安数据出现"开仓合约名空 + 标记价保证金有值 + 平仓时间空"的情况，
说明有持仓但丢失了开仓合约名等关键信息，需要从数据库和历史数据中修复。

【数据来源】
1. 数据库持仓区：第1步从这里读取历史持仓数据（如果缓存为空）
2. 门外存储区快照：第2、4、6步从这里获取最新数据
   - user_data：获取最新的币安数据（用于资金费检测和字段提取）
   - market_data：获取币安最新价和标记价
3. 本文件缓存：保存修复过程中的中间数据

【修复流程 - 共6步】（完全按照你的设计文档）
第1步：获取缓存数据（从数据库或直接用缓存）
第2步：检测资金费状态（4种情况）
第3步：资金费融合（只有情况D需要）
第4步：提取最新价和标记价（从行情数据）
第5步：计算8个固定字段（根据多空方向，严格按照原始公式，独立计算）
第6步：提取3个特定字段并推送
==================================================
"""

import os
import logging
import asyncio
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime

# 导入常量 - 使用修正后的常量名
from ...constants import (
    TAG_COMPLETE,
    INFO_BINANCE_MISSING,
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
    FIELD_MARK_POSITION_VALUE,
    FIELD_MARK_MARGIN,
    FIELD_MARK_PNL,
    FIELD_MARK_PNL_PERCENT,              # 标记价涨跌盈亏幅
    FIELD_LATEST_PNL_PERCENT,            # 最新价涨跌盈亏幅
    FIELD_MARK_PNL_PERCENT_OF_MARGIN,    # 标记价浮盈百分比（基于保证金）
    FIELD_LATEST_PNL_PERCENT_OF_MARGIN,  # 最新价浮盈百分比（基于保证金）
    FIELD_LATEST_MARGIN,
    FIELD_LATEST_POSITION_VALUE,
    FIELD_LATEST_PNL,
    FIELD_AVG_FUNDING_RATE,
    FIELD_FUNDING_THIS,
    FIELD_FUNDING_TOTAL,
    FIELD_FUNDING_COUNT,
    FIELD_FUNDING_TIME,
)

logger = logging.getLogger(__name__)


class BinanceMissingRepair:
    """
    币安持仓缺失修复类
    ==================================================
    这个类负责：
        1. 接收门外标签（持仓缺失/已平仓）- 通过 handle_info()
        2. 接收门外存储区快照（最新数据）- 通过 update_snapshot()
        3. 根据标签启动/停止修复循环
        4. 执行6步修复流程

    门外标签规则：
        - 永远只有1个标签（覆盖更新）
        - 持仓缺失 = 开（启动循环）
        - 已平仓   = 关（停止循环）

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

        # ===== 数据库连接信息（从环境变量读取）=====
        # 修复区需要直接读取数据库，不经过database.py
        self.db_url = os.getenv('TURSO_DATABASE_URL')
        self.db_token = os.getenv('TURSO_DATABASE_TOKEN')

        if not self.db_url or not self.db_token:
            logger.error("❌ 环境变量TURSO_DATABASE_URL和TURSO_DATABASE_TOKEN未设置")

        # ===== 测试数据库连接 =====
        if self.db_url and self.db_token:
            self._test_database_connection()

        # 临时存储门外数据（供第3步使用）
        self._snapshot_data = None

        logger.info("✅ 币安持仓缺失修复区初始化完成")

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
            logger.warning("⚠️ 收到空快照")
            return

        self.latest_snapshot = snapshot
        logger.debug(f"📦 币安持仓缺失修复收到存储区快照，时间戳: {snapshot.get('timestamp')}")

    async def handle_info(self, info: str):
        """
        接收调度器推送的信息标签
        ==================================================
        可能收到两种标签：
            - "币安持仓缺失" - 启动修复循环
            - "币安已平仓" - 停止修复循环

        门外标签规则：
            - 永远只有1个标签（覆盖更新）
            - 新标签到来直接覆盖旧标签
        ==================================================

        :param info: 信息标签
        """
        if not info:
            logger.warning("⚠️ 收到空标签")
            return

        old_info = self.current_info
        self.current_info = info

        logger.info(f"📨 币安持仓缺失修复门外标签更新: {old_info} → {info}")

        if info == INFO_BINANCE_MISSING:
            await self._start_repair()
        elif info == INFO_BINANCE_CLOSED:
            await self._stop_repair()
        else:
            logger.warning(f"⚠️ 币安持仓缺失修复收到未知标签: {info}")

    # ==================== 修复循环控制 ====================

    async def _start_repair(self):
        """启动修复流程（循环运行）"""
        if self.is_running:
            logger.debug("修复流程已在运行中")
            return

        self.is_running = True
        self.repair_task = asyncio.create_task(self._repair_loop())
        logger.info("🚀 币安持仓缺失修复流程已启动（循环运行）")

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
        logger.info("🛑 币安持仓缺失修复流程已停止")

    async def _repair_loop(self):
        """
        修复循环
        ==================================================
        只要门外标签是"币安持仓缺失"，就一直运行

        循环频率：每秒执行一次
        安全机制：
            - 每次执行前检查门外标签
            - 如果标签变了，自己停止
            - 如果修复过程出错，等待5秒后重试
        ==================================================
        """
        logger.info("🔄 币安持仓缺失修复循环开始")

        while self.is_running:
            try:
                if self.current_info != INFO_BINANCE_MISSING:
                    logger.info("门外标签已不是币安持仓缺失，停止修复循环")
                    await self._stop_repair()
                    break

                await self._repair_once()
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("修复循环被取消")
                break
            except Exception as e:
                logger.error(f"❌ 修复循环出错: {e}", exc_info=True)
                await asyncio.sleep(5)

        logger.info("🔄 币安持仓缺失修复循环结束")

    async def _repair_once(self):
        """
        执行一次修复流程
        ==================================================
        完全按照你的6步设计文档：
            第1步：获取缓存数据（从数据库或直接用缓存）
            第2步：检测资金费状态（4种情况）
            第3步：资金费融合（只有情况D需要）
            第4步：提取最新价和标记价（从行情数据）
            第5步：计算8个固定字段（根据多空方向，严格按照原始公式，独立计算）
            第6步：提取3个特定字段并推送
        ==================================================
        """
        logger.debug("执行一次币安持仓缺失修复")

        # 检查门外是否有存储区数据
        if not self.latest_snapshot:
            logger.warning("⚠️ 门外还没有存储区数据，等待下次循环")
            return

        if not await self._step1_get_cache():
            logger.error("❌ 第1步失败：无法获取缓存数据，本次修复终止")
            return

        funding_action = await self._step2_check_funding()
        if funding_action is None:
            logger.error("❌ 第2步失败：检测资金费状态出错，本次修复终止")
            return

        if funding_action == 'do_fusion':
            await self._step3_funding_fusion()

        if not await self._step4_get_prices():
            logger.error("❌ 第4步失败：无法获取行情数据，本次修复终止")
            return

        await self._step5_calc_fields()
        await self._step6_extract_and_push()

        logger.debug("一次币安持仓缺失修复执行完成")

    # ==================== 6步修复流程 ====================

    async def _step1_get_cache(self) -> bool:
        """
        第1步：获取缓存数据
        ==================================================
        规则：
            - 如果已经有缓存，直接用
            - 如果没有缓存，从数据库持仓区读取1条币安数据

        数据库查询：
            SELECT * FROM active_positions WHERE 交易所 = 'binance'

        注意：
            数据库表字段全是中文，SQL必须用中文字段名
        ==================================================
        """
        if self.cache is not None:
            logger.debug("✅ 第1步：使用现有缓存")
            return True

        logger.info("第1步：缓存为空，从数据库读取币安持仓数据")

        if not self.db_url or not self.db_token:
            logger.error("❌ 数据库连接信息不完整，无法读取")
            return False

        try:
            sql = "SELECT * FROM active_positions WHERE 交易所 = 'binance' LIMIT 1"
            result = self._query_database(sql)

            if not result:
                logger.warning("⚠️ 数据库查询失败或无数据")
                return False

            rows = result.get('rows', [])
            if not rows:
                logger.warning("⚠️ 数据库中没有币安持仓数据")
                return False

            cols = result.get('cols', [])
            row = rows[0]
            self.cache = self._row_to_dict(row, cols)

            logger.info(f"✅ 第1步：从数据库读取到币安数据，开仓合约名: {self.cache.get(FIELD_OPEN_CONTRACT)}")
            return True

        except Exception as e:
            logger.error(f"❌ 第1步：读取数据库失败: {e}")
            return False

    async def _step2_check_funding(self) -> Optional[str]:
        """
        第2步：检测资金费状态
        ==================================================
        检测两个维度：
            1. 有无历史：缓存本次资金费是否不等于0
            2. 有无新结算：存储区本次资金费是否不等于0

        4种情况：
            A. 无历史 + 无新结算 → 返回 'skip_to_step4'
            B. 无历史 + 有新结算 → 更新4个资金费字段 → 返回 'skip_to_step4'
            C. 有历史 + 无新结算 → 返回 'skip_to_step4'
            D. 有历史 + 有新结算 → 返回 'do_fusion'
        ==================================================
        """
        logger.info("第2步：检测资金费状态")

        # 从门外存储区快照获取最新的币安数据
        snapshot_data = self._get_binance_from_snapshot()
        if not snapshot_data:
            logger.error("❌ 门外存储区中没有币安数据")
            return None

        # 判断有无历史（缓存本次资金费是否为0）
        cache_funding = self.cache.get(FIELD_FUNDING_THIS, 0)
        if cache_funding is None:
            cache_funding = 0
        has_history = (cache_funding != 0)

        # 判断有无新结算（存储区本次资金费是否为0）
        snapshot_funding = snapshot_data.get(FIELD_FUNDING_THIS, 0)
        if snapshot_funding is None:
            snapshot_funding = 0
        has_new = (snapshot_funding != 0)

        logger.info(f"   缓存本次资金费: {cache_funding}, 存储区本次资金费: {snapshot_funding}")
        logger.info(f"   有无历史: {has_history}, 有无新结算: {has_new}")

        # 保存门外数据供后续步骤使用
        self._snapshot_data = snapshot_data

        if not has_history and not has_new:
            # 情况A：无历史 + 无新结算
            logger.info("   情况A：无历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'

        elif not has_history and has_new:
            # 情况B：无历史 + 有新结算
            logger.info("   情况B：无历史 + 有新结算，更新4个资金费字段后跳到第4步")
            self._update_funding_fields(snapshot_data)
            return 'skip_to_step4'

        elif has_history and not has_new:
            # 情况C：有历史 + 无新结算
            logger.info("   情况C：有历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'

        else:  # has_history and has_new
            # 情况D：有历史 + 有新结算
            logger.info("   情况D：有历史 + 有新结算，进入第3步资金费融合")
            return 'do_fusion'

    async def _step3_funding_fusion(self):
        """
        第3步：资金费融合流程
        ==================================================
        只有情况D（有历史+有新结算）才会执行这一步。

        融合规则：
            1. 本次资金费和本次结算时间 → 直接从存储区覆盖
            2. 累计资金费 = 存储区累计资金费 + 缓存累计资金费
            3. 资金费结算次数 = 存储区次数 + 缓存次数
        ==================================================
        """
        logger.info("第3步：执行资金费融合")

        snapshot = self._snapshot_data
        cache = self.cache

        # 获取数值，处理None
        snapshot_funding_this = snapshot.get(FIELD_FUNDING_THIS, 0) or 0
        snapshot_funding_total = snapshot.get(FIELD_FUNDING_TOTAL, 0) or 0
        snapshot_funding_count = snapshot.get(FIELD_FUNDING_COUNT, 0) or 0
        snapshot_funding_time = snapshot.get(FIELD_FUNDING_TIME)

        cache_funding_total = cache.get(FIELD_FUNDING_TOTAL, 0) or 0
        cache_funding_count = cache.get(FIELD_FUNDING_COUNT, 0) or 0

        # 1. 本次资金费和本次结算时间直接覆盖
        cache[FIELD_FUNDING_THIS] = snapshot_funding_this
        if snapshot_funding_time is not None:
            cache[FIELD_FUNDING_TIME] = snapshot_funding_time

        # 2. 累计资金费相加
        cache[FIELD_FUNDING_TOTAL] = snapshot_funding_total + cache_funding_total

        # 3. 资金费结算次数相加
        cache[FIELD_FUNDING_COUNT] = snapshot_funding_count + cache_funding_count

        logger.info(f"   融合后 - 本次资金费: {cache[FIELD_FUNDING_THIS]}, "
                   f"累计资金费: {cache[FIELD_FUNDING_TOTAL]}, "
                   f"结算次数: {cache[FIELD_FUNDING_COUNT]}")

    async def _step4_get_prices(self) -> bool:
        """
        第4步：提取最新价和标记价
        ==================================================
        根据缓存数据中的开仓合约名，去门外存储区的行情数据中：
            - binance_trade_price → 对应币安数据的最新价
            - binance_mark_price  → 对应币安数据的标记价

        把这两个值覆盖到缓存中
        ==================================================
        """
        logger.info("第4步：从行情数据提取最新价和标记价")

        contract = self.cache.get(FIELD_OPEN_CONTRACT)
        if not contract:
            logger.error("❌ 缓存中没有开仓合约名")
            return False

        # 从门外存储区获取行情数据
        market_data = self._get_market_data_from_snapshot(contract)
        if not market_data:
            logger.error(f"❌ 无法获取合约 {contract} 的行情数据")
            return False

        # 提取币安的行情字段
        latest_price = market_data.get('binance_trade_price')
        mark_price = market_data.get('binance_mark_price')

        if latest_price is None or mark_price is None:
            logger.error(f"❌ 行情数据中缺少必要字段: latest_price={latest_price}, mark_price={mark_price}")
            return False

        # 转换为float（如果是字符串）
        try:
            latest_price = float(latest_price)
            mark_price = float(mark_price)
        except (TypeError, ValueError):
            logger.error(f"❌ 行情数据格式错误: latest_price={latest_price}, mark_price={mark_price}")
            return False

        # 覆盖到缓存
        self.cache[FIELD_LATEST_PRICE] = latest_price
        self.cache[FIELD_MARK_PRICE] = mark_price

        logger.info(f"✅ 第4步：获取到行情数据 - 最新价: {latest_price}, 标记价: {mark_price}")
        return True

    async def _step5_calc_fields(self):
        """
        第5步：计算8个固定字段
        ==================================================
        【严格按照原始方案，独立计算，不复用任何中间结果】

        需要计算的字段：
            1. 标记价涨跌盈亏幅
            2. 最新价涨跌盈亏幅
            3. 最新价保证金
            4. 最新价仓位价值
            5. 最新价浮盈
            6. 最新价浮盈百分比
            7. 标记价浮盈百分比
            8. 平均资金费率

        计算公式（严格按照你的文档，每个字段独立计算）：

        当开仓方向为 "LONG" 时：
            标记价涨跌盈亏幅 = (标记价 - 开仓价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (最新价 - 开仓价) * 100 / 开仓价
            最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            最新价仓位价值 = 最新价 * 持仓币数
            最新价浮盈 = 最新价 * 持仓币数 - 开仓价仓位价值
            最新价浮盈百分比 = (最新价 * 持仓币数 - 开仓价仓位价值) * 100 / 开仓保证金
            标记价浮盈百分比 = 标记价浮盈 * 100 / 开仓保证金
            平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值

        当开仓方向为 "SHORT" 时：
            标记价涨跌盈亏幅 = (开仓价 - 标记价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (开仓价 - 最新价) * 100 / 开仓价
            最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            最新价仓位价值 = 最新价 * 持仓币数
            最新价浮盈 = 开仓价仓位价值 - (最新价 * 持仓币数)
            最新价浮盈百分比 = [开仓价仓位价值 - (最新价 * 持仓币数)] * 100 / 开仓保证金
            标记价浮盈百分比 = 标记价浮盈 * 100 / 开仓保证金
            平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值
        ==================================================
        """
        logger.info("第5步：计算8个固定字段（严格按照原始方案，独立计算）")

        cache = self.cache

        # 获取原始字段值（直接从缓存读取，不复用任何计算结果）
        latest_price = cache.get(FIELD_LATEST_PRICE, 0)
        mark_price = cache.get(FIELD_MARK_PRICE, 0)
        position_size = cache.get(FIELD_POSITION_SIZE, 0) or 0
        leverage = cache.get(FIELD_LEVERAGE, 1) or 1
        open_price = cache.get(FIELD_OPEN_PRICE, 0) or 0
        open_position_value = cache.get(FIELD_OPEN_POSITION_VALUE, 0) or 0
        open_margin = cache.get(FIELD_OPEN_MARGIN, 1) or 1
        mark_pnl = cache.get(FIELD_MARK_PNL, 0) or 0
        total_funding = cache.get(FIELD_FUNDING_TOTAL, 0) or 0
        direction = cache.get(FIELD_OPEN_DIRECTION)

        # 根据方向计算 - 每个字段严格按照原始公式独立计算
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

            # 7. 标记价浮盈百分比 = 标记价浮盈 * 100 / 开仓保证金
            mark_pnl_percent_of_margin = (mark_pnl * 100 / open_margin) if open_margin else 0

            # 8. 平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值
            avg_funding_rate = (total_funding * 100 / open_position_value) if open_position_value else 0

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

            # 7. 标记价浮盈百分比 = 标记价浮盈 * 100 / 开仓保证金
            mark_pnl_percent_of_margin = (mark_pnl * 100 / open_margin) if open_margin else 0

            # 8. 平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值
            avg_funding_rate = (total_funding * 100 / open_position_value) if open_position_value else 0

        # 保存计算结果到缓存 - 每个字段只赋值一次，使用正确的常量名
        cache[FIELD_MARK_PNL_PERCENT] = mark_pnl_percent                    # 1. 标记价涨跌盈亏幅
        cache[FIELD_LATEST_PNL_PERCENT] = latest_pnl_percent              # 2. 最新价涨跌盈亏幅
        cache[FIELD_LATEST_MARGIN] = latest_margin                         # 3. 最新价保证金
        cache[FIELD_LATEST_POSITION_VALUE] = latest_position_value         # 4. 最新价仓位价值
        cache[FIELD_LATEST_PNL] = latest_pnl                               # 5. 最新价浮盈
        cache[FIELD_LATEST_PNL_PERCENT_OF_MARGIN] = latest_pnl_percent_of_margin  # 6. 最新价浮盈百分比
        cache[FIELD_MARK_PNL_PERCENT_OF_MARGIN] = mark_pnl_percent_of_margin    # 7. 标记价浮盈百分比
        cache[FIELD_AVG_FUNDING_RATE] = avg_funding_rate                   # 8. 平均资金费率

        logger.info(f"   计算完成 - 标记价涨跌盈亏幅: {mark_pnl_percent:.2f}%, "
                   f"最新价涨跌盈亏幅: {latest_pnl_percent:.2f}%, "
                   f"最新价保证金: {latest_margin:.2f}, "
                   f"最新价仓位价值: {latest_position_value:.2f}, "
                   f"最新价浮盈: {latest_pnl:.2f}, "
                   f"最新价浮盈百分比: {latest_pnl_percent_of_margin:.2f}%, "
                   f"标记价浮盈百分比: {mark_pnl_percent_of_margin:.2f}%, "
                   f"平均资金费率: {avg_funding_rate:.4f}%, "
                   f"开仓方向: {direction}")

    async def _step6_extract_and_push(self):
        """
        第6步：提取3个特定字段并推送
        ==================================================
        做了四件事：
            1. 从门外存储区读取最新的币安数据
            2. 提取3个字段：标记价保证金、标记价仓位价值、标记价浮盈
            3. 如果提取不到或为空值，保留原缓存值（不报错）
            4. 创建缓存副本，打上"持仓完整"标签，推送给调度器

        需要提取的3个字段：
            - 标记价保证金
            - 标记价仓位价值
            - 标记价浮盈
        ==================================================
        """
        logger.info("第6步：提取3个特定字段并推送")

        # 从门外存储区获取最新的币安数据
        latest_binance = self._get_binance_from_snapshot()
        if not latest_binance:
            logger.warning("⚠️ 无法获取最新的币安数据，跳过字段提取")
        else:
            # 要提取的3个字段
            fields_to_extract = [
                FIELD_MARK_MARGIN,
                FIELD_MARK_POSITION_VALUE,
                FIELD_MARK_PNL
            ]

            # 尝试提取，如果提取不到或为空值，保留原缓存值
            extract_count = 0
            for field in fields_to_extract:
                if field in latest_binance and latest_binance[field] is not None and latest_binance[field] != '':
                    self.cache[field] = latest_binance[field]
                    extract_count += 1
                else:
                    logger.debug(f"   字段 {field} 提取不到或为空值，保留原缓存值")

            logger.info(f"   成功提取 {extract_count} 个字段")

        # 创建缓存副本
        data_copy = self.cache.copy()

        # 打标签推送
        await self.scheduler.handle({
            'tag': TAG_COMPLETE,
            'data': data_copy
        })

        contract = data_copy.get(FIELD_OPEN_CONTRACT, 'unknown')
        logger.info(f"✅ 已推送持仓完整数据: {EXCHANGE_BINANCE} - {contract}")

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
            logger.debug("门外还没有存储区数据")
            return None

        user_data = self.latest_snapshot.get('user_data', {})
        binance_key = f"{EXCHANGE_BINANCE}_user"
        binance_item = user_data.get(binance_key, {})

        if not binance_item:
            logger.debug("存储区中没有币安用户数据")
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
            logger.debug("门外还没有存储区数据")
            return None

        market_data = self.latest_snapshot.get('market_data', {})
        if not market_data:
            logger.debug("存储区中没有行情数据")
            return None

        return market_data.get(contract)

    def _query_database(self, sql: str, params: List = None) -> Optional[Dict]:
        """
        查询Turso数据库
        ==================================================
        执行SQL查询，返回结果。
        如果查询失败，返回None并记录错误。
        ==================================================
        """
        if params is None:
            params = []

        args = []
        for p in params:
            if p is None:
                args.append({"type": "null", "value": None})
            elif isinstance(p, (int, float)):
                args.append({"type": "integer", "value": p})
            else:
                args.append({"type": "text", "value": str(p)})

        payload = {
            "requests": [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": sql,
                        "args": args
                    }
                }
            ]
        }

        try:
            response = requests.post(
                f"{self.db_url}/v2/pipeline",
                headers={
                    "Authorization": f"Bearer {self.db_token}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            result = response.json()

            if result and 'results' in result and len(result['results']) > 0:
                return result['results'][0].get('result', {})
            return None

        except requests.exceptions.Timeout:
            logger.error(f"❌ 数据库查询超时: {sql[:50]}...")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ 数据库请求失败: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ 数据库查询未知错误: {e}")
            return None

    def _row_to_dict(self, row: List, cols: List) -> Dict:
        """
        将数据库行转换为字典
        ==================================================
        Turso返回的数据格式：
            cols: [{"name": "字段名"}, ...]
            rows: [[{"value": "值"}, ...], ...]
        ==================================================
        """
        result = {}
        for i, col in enumerate(cols):
            if i < len(row):
                col_name = col.get('name')
                value_cell = row[i]
                value = value_cell.get('value') if value_cell else None
                result[col_name] = value
        return result

    def _update_funding_fields(self, snapshot_data: Dict):
        """
        更新4个资金费字段（用于情况B）
        ==================================================
        当无历史但有新结算时，直接把存储区的4个资金费字段
        覆盖到缓存。

        情况B：无历史 + 有新结算
        ==================================================
        """
        fields = [
            FIELD_FUNDING_THIS,
            FIELD_FUNDING_TOTAL,
            FIELD_FUNDING_COUNT,
            FIELD_FUNDING_TIME
        ]

        update_count = 0
        for field in fields:
            if field in snapshot_data and snapshot_data[field] is not None:
                self.cache[field] = snapshot_data[field]
                update_count += 1

        logger.debug(f"   已更新 {update_count} 个资金费字段")

    def _test_database_connection(self):
        """测试数据库连接是否正常"""
        try:
            result = self._query_database("SELECT 1")
            if result:
                logger.info("✅ 数据库连接测试成功")
            else:
                logger.warning("⚠️ 数据库连接测试返回空结果")
        except Exception as e:
            logger.error(f"❌ 数据库连接测试失败: {e}")
