"""
币安持仓缺失修复
==================================================
【文件职责】
这个文件是币安持仓缺失修复的核心，负责处理两种信息标签：
1. "币安持仓缺失" - 启动修复流程（循环运行）
2. "币安空仓"     - 停止修复流程

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

【修复流程 - 共7步】（完全按照你的设计文档）
第1步：获取缓存数据（从数据库或直接用缓存）
第2步：检测资金费状态（4种情况）
第3步：资金费融合（只有情况D需要）
第4步：提取存储区平仓字段（新增）
第5步：提取最新价和标记价（从行情数据）
第6步：计算固定字段（原有8个字段 + 新增4个平仓相关字段）
第7步：提取3个特定字段并检测平仓价打对应标签推送

【数据库迁移 - 从Turso到MongoDB】
2026-03-20 修改：将数据库查询从Turso改为MongoDB
- 环境变量从 TURSO_DATABASE_URL/TOKEN 改为 MONGODB_URI
- 查询方式从 SQL 改为直接字典查询
- 连接方式从 aiohttp 改为 pymongo（按需连接，用完即弃）
- 结果处理从 _row_to_dict() 转换改为直接使用（MongoDB返回就是字典）
- 读取逻辑优化：按开仓时间倒序取最新一条，防止残留数据干扰
==================================================
"""

import os
import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
from pymongo import MongoClient  # ✅ 新增MongoDB驱动

# 导入常量 - 使用修正后的常量名
from ...constants import (
    TAG_COMPLETE,
    TAG_CLOSED_COMPLETE,
    INFO_BINANCE_MISSING,
    INFO_BINANCE_EMPTY,
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
    FIELD_CLOSE_TIME,
    FIELD_CLOSE_PRICE,                    # 平仓价
    FIELD_CLOSE_POSITION_VALUE,           # 平仓价仓位价值
    FIELD_CLOSE_PNL_PERCENT,               # 平仓价涨跌盈亏幅
    FIELD_CLOSE_PNL,                       # 平仓收益
    FIELD_CLOSE_PNL_PERCENT_OF_MARGIN,     # 平仓收益率
    FIELD_CLOSE_EXEC_TYPE,                 # 平仓执行方式
    FIELD_CLOSE_FEE,                        # 平仓手续费
    FIELD_CLOSE_FEE_CURRENCY,               # 平仓手续费币种
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
        1. 接收门外标签（持仓缺失/空仓）- 通过 handle_info()
        2. 接收门外存储区快照（最新数据）- 通过 update_snapshot()
        3. 根据标签启动/停止修复循环
        4. 执行7步修复流程

    门外标签规则：
        - 永远只有1个标签（覆盖更新）
        - 持仓缺失 = 开（启动循环）
        - 空仓     = 关（停止循环）

    门外数据规则：
        - 永远只有1份存储区快照（覆盖更新）
        - 包含 market_data 和 user_data

    修复循环规则：
        - 每秒执行一次
        - 每次执行前检查门外标签
        - 如果标签变了，自己停止
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

        # ===== 【MongoDB迁移】不再保存数据库连接，改为按需连接 =====
        # 临时存储门外数据（供第3步使用）
        self._snapshot_data = None

        logger.info("✅【币安修复区】【持仓缺失修复】 修复区初始化完成")

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
            logger.warning("⚠️ 【币安修复区】【持仓缺失修复】收到空快照")
            return

        self.latest_snapshot = snapshot
        logger.debug(f"📦【币安修复区】【持仓缺失修复】 收到存储区快照，时间戳: {snapshot.get('timestamp')}")

    async def handle_info(self, info: str):
        """
        接收调度器推送的信息标签
        ==================================================
        可能收到两种标签：
            - "币安持仓缺失" - 启动修复循环
            - "币安空仓"     - 停止修复循环

        门外标签规则：
            - 永远只有1个标签（覆盖更新）
            - 新标签到来直接覆盖旧标签
        ==================================================

        :param info: 信息标签
        """
        if not info:
            logger.warning("⚠️【币安修复区】【持仓缺失修复】 收到空标签")
            return

        old_info = self.current_info
        self.current_info = info

        logger.debug(f"📨【币安修复区】【持仓缺失修复】 门外标签更新: {old_info} → {info}")

        if info == INFO_BINANCE_MISSING:
            await self._start_repair()
        elif info == INFO_BINANCE_EMPTY:
            await self._stop_repair()
        else:
            logger.warning(f"⚠️ 【币安修复区】【持仓缺失修复】收到未知标签: {info}")

    # ==================== 修复循环控制 ====================

    async def _start_repair(self):
        """启动修复流程（循环运行）"""
        if self.is_running:
            logger.debug("【币安修复区】【持仓缺失修复】修复流程已在运行中")
            return

        self.is_running = True
        self.repair_task = asyncio.create_task(self._repair_loop())
        logger.info("🚀【币安修复区】【持仓缺失修复】 修复流程已启动（循环运行）")

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
        logger.info("🛑 【币安修复区】【持仓缺失修复】修复流程已停止")

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
        logger.info("🔄【币安修复区】【持仓缺失修复】 修复循环开始")

        while self.is_running:
            await asyncio.sleep(0)  # ✅ 循环开始让出CPU，避免长时间占用
            try:
                if self.current_info != INFO_BINANCE_MISSING:
                    logger.info("【币安修复区】【持仓缺失修复】门外标签已不是币安持仓缺失，停止修复循环")
                    await self._stop_repair()
                    break

                await self._repair_once()
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("【币安修复区】【持仓缺失修复】修复循环被取消")
                break
            except Exception as e:
                logger.error(f"❌ 【币安修复区】【持仓缺失修复】修复循环出错: {e}", exc_info=True)
                await asyncio.sleep(5)

        logger.info("🔄【币安修复区】【持仓缺失修复】 修复循环结束")

    async def _repair_once(self):
        """
        执行一次修复流程
        ==================================================
        完全按照你的7步设计文档：
            第1步：获取缓存数据（从数据库或直接用缓存）
            第2步：检测资金费状态（4种情况）
            第3步：资金费融合（只有情况D需要）
            第4步：提取存储区平仓字段（新增）
            第5步：提取最新价和标记价（从行情数据）
            第6步：计算固定字段（原有8个字段 + 新增4个平仓相关字段）
            第7步：提取3个特定字段并检测平仓价打对应标签推送
        ==================================================
        """
        logger.debug("【币安修复区】【持仓缺失修复】执行一次修复流程")

        # 检查门外是否有存储区数据
        if not self.latest_snapshot:
            logger.warning("⚠️【币安修复区】【持仓缺失修复】 门外还没有存储区数据，等待下次循环")
            return

        if not await self._step1_get_cache():
            logger.error("❌【币安修复区】【持仓缺失修复】 第1步失败：无法获取缓存数据，本次修复终止")
            return

        funding_action = await self._step2_check_funding()
        if funding_action is None:
            logger.error("❌【币安修复区】【持仓缺失修复】 第2步失败：检测资金费状态出错，本次修复终止")
            return

        if funding_action == 'do_fusion':
            await self._step3_funding_fusion()

        # 第4步：提取存储区平仓字段（新增）
        await self._step4_extract_close_fields()

        # 第5步：提取最新价和标记价（原有的第4步）
        if not await self._step5_get_prices():
            logger.error("❌【币安修复区】【持仓缺失修复】 第5步失败：无法获取行情数据，本次修复终止")
            return

        # 第6步：计算固定字段（原有的第5步）
        await self._step6_calc_fields()

        # 第7步：提取3个特定字段并检测平仓价打对应标签推送（原有的第6步）
        await self._step7_extract_and_push()

        logger.debug("【币安修复区】【持仓缺失修复】一次修复流程执行完成")

    # ==================== 7步修复流程 ====================

    async def _step1_get_cache(self) -> bool:
        """
        第1步：获取缓存数据
        ==================================================
        【MongoDB迁移说明】
        按照"按需连接"原则重构：
            1. 先检查缓存是否有数据
            2. 如果有缓存，直接使用，不需要连接数据库
            3. 如果没缓存，才去连接MongoDB读取
        
        【读取逻辑优化】
        - 按开仓时间倒序排序，取最新一条数据
        - 防止平仓失败时残留的旧数据干扰修复流程
        ==================================================
        """
        # ----- 第1层：检查缓存 -----
        if self.cache is not None:
            logger.debug("✅【币安修复区】【持仓缺失修复】 第1步：使用现有缓存")
            return True

        logger.info("🔍【币安修复区】【持仓缺失修复】 第1步：缓存为空，准备从MongoDB读取")

        # ----- 第2层：获取MongoDB连接信息（按需读取环境变量）-----
        mongo_uri = os.getenv('MONGODB_URI')

        if not mongo_uri:
            logger.error("❌【币安修复区】【持仓缺失修复】 环境变量 MONGODB_URI 未设置")
            return False

        logger.info("✅【币安修复区】【持仓缺失修复】 成功读取MongoDB连接信息")

        # ----- 第3层：连接MongoDB并查询币安数据 -----
        loop = asyncio.get_event_loop()
        client = None
        try:
            # 临时连接MongoDB（用完即关）
            client = await loop.run_in_executor(
                None,
                lambda: MongoClient(
                    mongo_uri,
                    serverSelectionTimeoutMS=5000,  # 5秒连接超时
                    connectTimeoutMS=5000
                )
            )
            
            # 测试连接
            await loop.run_in_executor(
                None,
                lambda: client.admin.command('ping')
            )
            logger.debug("✅【币安修复区】【持仓缺失修复】 MongoDB连接成功")
            
            db = client["trading_db"]
            collection = db["active_positions"]
            
            # ✅ 查询币安数据，按开仓时间倒序取最新一条
            # 防止平仓失败时残留的旧数据干扰修复流程
            cursor = await loop.run_in_executor(
                None,
                lambda: collection.find({"交易所": "binance"}).sort("开仓时间", -1).limit(1)
            )
            
            results = await loop.run_in_executor(
                None,
                lambda: list(cursor)
            )
            
            result = results[0] if results else None
            
            # 如果没有找到，尝试其他可能的交易所名称
            if not result:
                logger.warning("⚠️【币安修复区】【持仓缺失修复】 未找到交易所为 'binance' 的数据，尝试其他写法")
                test_exchanges = ['BINANCE', 'Binance', '币安']
                for test_exchange in test_exchanges:
                    await asyncio.sleep(0)
                    cursor = await loop.run_in_executor(
                        None,
                        lambda: collection.find({"交易所": test_exchange}).sort("开仓时间", -1).limit(1)
                    )
                    results = await loop.run_in_executor(
                        None,
                        lambda: list(cursor)
                    )
                    if results:
                        result = results[0]
                        logger.debug(f"✅【币安修复区】【持仓缺失修复】 找到数据！交易所字段实际为: {test_exchange}")
                        break
            
            if not result:
                logger.error("❌【币安修复区】【持仓缺失修复】 尝试了所有可能的交易所名称，都没有找到数据")
                return False
            
            # ----- 第4层：提取数据到缓存 -----
            # MongoDB返回的已经是字典，直接使用，不需要转换！
            
            # 🔥 关键修复：删除 MongoDB 的 _id 字段，避免 JSON 序列化报错
            if '_id' in result:
                del result['_id']

            self.cache = result

            logger.info(f"✅【币安修复区】【持仓缺失修复】 第1步：成功读取到币安数据（最新开仓时间）")
            logger.info(f"   交易所: {self.cache.get(FIELD_EXCHANGE)}")
            logger.info(f"   开仓合约名: {self.cache.get(FIELD_OPEN_CONTRACT)}")
            logger.info(f"   开仓时间: {self.cache.get('开仓时间')}")
            logger.info(f"   ID: {self.cache.get('id')}")
            
            # 打印关键字段
            logger.debug(f"   开仓价: {self.cache.get(FIELD_OPEN_PRICE)}")
            logger.debug(f"   持仓张数: {self.cache.get(FIELD_POSITION_CONTRACTS)}")
            logger.debug(f"   累计资金费: {self.cache.get(FIELD_FUNDING_TOTAL)}")
            
            return True

        except Exception as e:
            logger.error(f"❌【币安修复区】【持仓缺失修复】 第1步：读取MongoDB失败: {e}", exc_info=True)
            return False
        finally:
            # 无论成功失败，都关闭连接
            if client:
                await loop.run_in_executor(None, client.close)
                logger.debug("🔌【币安修复区】【持仓缺失修复】 MongoDB连接已关闭")

    async def _step2_check_funding(self) -> Optional[str]:
        """
        第2步：检测资金费状态
        ==================================================
        【修正】2026.03.19 - 修复判断逻辑
        检测两个维度：
            1. 有无历史：缓存本次资金费是否不等于0
            2. 有无新结算：存储区本次资金费 != 0 AND 存储区本次资金费 != 缓存本次资金费

        4种情况：
            A. 无历史 + 无新结算 → 返回 'skip_to_step4'
            B. 无历史 + 有新结算 → 更新4个资金费字段 → 返回 'skip_to_step4'
            C. 有历史 + 无新结算 → 返回 'skip_to_step4'
            D. 有历史 + 有新结算 → 返回 'do_fusion'
        ==================================================
        """
        logger.debug("【币安修复区】【持仓缺失修复】第2步：检测资金费状态")

        # 从门外存储区快照获取最新的币安数据
        snapshot_data = self._get_binance_from_snapshot()
        if not snapshot_data:
            logger.error("❌【币安修复区】【持仓缺失修复】 门外存储区中没有币安数据")
            return None

        # 判断有无历史（缓存本次资金费是否为0）
        cache_funding = self.cache.get(FIELD_FUNDING_THIS, 0)
        if cache_funding is None:
            cache_funding = 0
        has_history = (cache_funding != 0)

        # 判断有无新结算（存储区本次资金费 != 0 AND 存储区本次资金费 != 缓存本次资金费）
        snapshot_funding = snapshot_data.get(FIELD_FUNDING_THIS, 0)
        if snapshot_funding is None:
            snapshot_funding = 0
        has_new = (snapshot_funding != 0) and (snapshot_funding != cache_funding)

        logger.debug(f" 【币安修复区】【持仓缺失修复】  缓存本次资金费: {cache_funding}, 存储区本次资金费: {snapshot_funding}")
        logger.debug(f"  【币安修复区】【持仓缺失修复】 有无历史: {has_history}, 有无新结算: {has_new}")

        # 保存门外数据供后续步骤使用
        self._snapshot_data = snapshot_data

        if not has_history and not has_new:
            # 情况A：无历史 + 无新结算
            logger.info(" 【币安修复区】【持仓缺失修复】  情况A：无历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'

        elif not has_history and has_new:
            # 情况B：无历史 + 有新结算
            logger.info(" 【币安修复区】【持仓缺失修复】  情况B：无历史 + 有新结算，更新4个资金费字段后跳到第4步")
            self._update_funding_fields(snapshot_data)
            return 'skip_to_step4'

        elif has_history and not has_new:
            # 情况C：有历史 + 无新结算
            logger.info(" 【币安修复区】【持仓缺失修复】  情况C：有历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'

        else:  # has_history and has_new
            # 情况D：有历史 + 有新结算
            logger.info("  【币安修复区】【持仓缺失修复】 情况D：有历史 + 有新结算，进入第3步资金费融合")
            return 'do_fusion'

    async def _step3_funding_fusion(self):
        """
        第3步：资金费融合流程
        ==================================================
        只有情况D（有历史+有新结算）才会执行这一步。

        融合规则：
            1. 本次资金费和本次结算时间 → 直接从存储区覆盖
            2. 累计资金费 = 存储区累计资金费 + 缓存累计资金费
            3. 资金费结算次数 = 缓存次数 + 1

        【重要】允许部分更新：即使累计资金费计算失败，本次资金费和结算时间也会更新
        ==================================================
        """
        logger.debug("【币安修复区】【持仓缺失修复】第3步：执行资金费融合")

        snapshot = self._snapshot_data
        cache = self.cache

        # ===== 第1部分：直接覆盖的字段（即使后续失败也要更新）=====
        # 本次资金费直接覆盖
        if FIELD_FUNDING_THIS in snapshot:
            cache[FIELD_FUNDING_THIS] = snapshot[FIELD_FUNDING_THIS]
            logger.debug(f" ✅ 本次资金费已覆盖: {cache[FIELD_FUNDING_THIS]}")
        
        # 本次结算时间直接覆盖
        if FIELD_FUNDING_TIME in snapshot and snapshot[FIELD_FUNDING_TIME] is not None:
            cache[FIELD_FUNDING_TIME] = snapshot[FIELD_FUNDING_TIME]
            logger.debug(f" ✅ 本次结算时间已覆盖: {cache[FIELD_FUNDING_TIME]}")

        # ===== 第2部分：需要计算的字段（用安全转换保护）=====
        try:
            # 累计资金费相加
            snapshot_total = self._safe_float(snapshot.get(FIELD_FUNDING_TOTAL))
            cache_total = self._safe_float(cache.get(FIELD_FUNDING_TOTAL))
            cache[FIELD_FUNDING_TOTAL] = snapshot_total + cache_total
            logger.debug(f" ✅ 累计资金费计算: {snapshot_total} + {cache_total} = {cache[FIELD_FUNDING_TOTAL]}")
        except Exception as e:
            logger.error(f" ❌ 累计资金费计算失败: {e}，保持原值 {cache.get(FIELD_FUNDING_TOTAL)}")

        try:
            # 资金费结算次数加1
            cache_count = self._safe_float(cache.get(FIELD_FUNDING_COUNT))
            cache[FIELD_FUNDING_COUNT] = cache_count + 1
            logger.debug(f" ✅ 结算次数计算: {cache_count} + 1 = {cache[FIELD_FUNDING_COUNT]}")
        except Exception as e:
            logger.error(f" ❌ 结算次数计算失败: {e}，保持原值 {cache.get(FIELD_FUNDING_COUNT)}")

        logger.debug(f" 【币安修复区】【持仓缺失修复】  融合后 - 本次资金费: {cache.get(FIELD_FUNDING_THIS)}, "
                   f"累计资金费: {cache.get(FIELD_FUNDING_TOTAL)}, "
                   f"结算次数: {cache.get(FIELD_FUNDING_COUNT)}")

    # ==================== 新增第4步：提取存储区平仓字段 ====================

    async def _step4_extract_close_fields(self):
        """
        第4步：提取存储区中的平仓字段
        ==================================================
        从门外存储区快照获取币安用户数据，提取5个平仓相关字段：
            - 平仓执行方式 (FIELD_CLOSE_EXEC_TYPE)
            - 平仓价 (FIELD_CLOSE_PRICE)
            - 平仓手续费 (FIELD_CLOSE_FEE)
            - 平仓手续费币种 (FIELD_CLOSE_FEE_CURRENCY)
            - 平仓时间 (FIELD_CLOSE_TIME)
        
        如果平仓执行方式为空，说明没有平仓，跳过提取。
        如果有值，则覆盖到缓存中。
        ==================================================
        """
        logger.debug("【币安修复区】【持仓缺失修复】第4步：提取存储区平仓字段")

        snapshot_data = self._get_binance_from_snapshot()
        if not snapshot_data:
            logger.debug("⚠️ 门外存储区中没有币安数据，跳过平仓字段提取")
            return

        # 检查是否有平仓执行方式
        close_exec_type = snapshot_data.get(FIELD_CLOSE_EXEC_TYPE)
        if close_exec_type is None or close_exec_type == '':
            logger.debug("⏩ 平仓执行方式为空，没有平仓事件，跳过提取")
            return

        # 要提取的5个字段
        fields_to_extract = [
            FIELD_CLOSE_EXEC_TYPE,      # 平仓执行方式
            FIELD_CLOSE_PRICE,          # 平仓价
            FIELD_CLOSE_FEE,            # 平仓手续费
            FIELD_CLOSE_FEE_CURRENCY,   # 平仓手续费币种
            FIELD_CLOSE_TIME,           # 平仓时间
        ]

        update_count = 0
        for field in fields_to_extract:
            if field in snapshot_data and snapshot_data[field] is not None and snapshot_data[field] != '':
                self.cache[field] = snapshot_data[field]
                logger.debug(f"✅ 更新平仓字段 {field}: {snapshot_data[field]}")
                update_count += 1

        logger.info(f"📊 平仓字段提取完成，更新 {update_count} 个字段")

    # ==================== 辅助函数：安全转换 ====================
    
    def _safe_float(self, value, default=0.0):
        """安全转换为float，如果是字符串则转换，如果是None则返回默认值"""
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            logger.debug(f"【币安修复区】【持仓缺失修复】 类型转换失败: {value}, 使用默认值 {default}")
            return default

    # ==================== 原第4步改为第5步：提取行情数据 ====================

    async def _step5_get_prices(self) -> bool:
        """
        第5步：提取最新价和标记价
        ==================================================
        根据缓存数据中的开仓合约名，去门外存储区的行情数据中：
            - binance_trade_price → 对应币安数据的最新价
            - binance_mark_price  → 对应币安数据的标记价

        把这两个值覆盖到缓存中
        ==================================================
        """
        logger.debug("【币安修复区】【持仓缺失修复】第5步：从行情数据提取最新价和标记价")

        contract = self.cache.get(FIELD_OPEN_CONTRACT)
        if not contract:
            logger.error("❌【币安修复区】【持仓缺失修复】 缓存中没有开仓合约名")
            return False

        # 从门外存储区获取行情数据
        market_data = self._get_market_data_from_snapshot(contract)
        if not market_data:
            logger.error(f"❌ 【币安修复区】【持仓缺失修复】无法获取合约 {contract} 的行情数据")
            return False

        # 提取币安的行情字段
        latest_price = market_data.get('binance_trade_price')
        mark_price = market_data.get('binance_mark_price')

        if latest_price is None or mark_price is None:
            logger.error(f"❌【币安修复区】【持仓缺失修复】 行情数据中缺少必要字段: latest_price={latest_price}, mark_price={mark_price}")
            return False

        # 使用安全转换
        try:
            latest_price = self._safe_float(latest_price)
            mark_price = self._safe_float(mark_price)
        except Exception as e:
            logger.error(f"❌【币安修复区】【持仓缺失修复】 行情数据格式错误: {e}")
            return False

        # 覆盖到缓存
        self.cache[FIELD_LATEST_PRICE] = latest_price
        self.cache[FIELD_MARK_PRICE] = mark_price

        logger.debug(f"✅ 【币安修复区】【持仓缺失修复】第5步：获取到行情数据 - 最新价: {latest_price}, 标记价: {mark_price}")
        return True

    # ==================== 原第5步改为第6步：计算字段 ====================

    async def _step6_calc_fields(self):
        """
        第6步：计算固定字段
        ==================================================
        【严格按照原始方案，独立计算，不复用任何中间结果】

        需要计算的字段（共12个）：
            原有8个字段：
                1. 标记价涨跌盈亏幅
                2. 最新价涨跌盈亏幅
                3. 最新价保证金
                4. 最新价仓位价值
                5. 最新价浮盈
                6. 最新价浮盈百分比
                7. 标记价浮盈百分比（用缓存中的标记价重新计算，不从存储区取）
                8. 平均资金费率
            
            新增4个平仓相关字段（仅在平仓价不为空时计算）：
                9. 平仓价仓位价值
                10. 平仓价涨跌盈亏幅
                11. 平仓收益
                12. 平仓收益率

        计算公式（严格按照你的文档，每个字段独立计算）：

        当开仓方向为 "LONG" 时：
            标记价涨跌盈亏幅 = (标记价 - 开仓价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (最新价 - 开仓价) * 100 / 开仓价
            最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            最新价仓位价值 = 最新价 * 持仓币数
            最新价浮盈 = 最新价 * 持仓币数 - 开仓价仓位价值
            最新价浮盈百分比 = (最新价 * 持仓币数 - 开仓价仓位价值) * 100 / 开仓保证金
            标记价浮盈百分比 = (标记价 * 持仓币数 - 开仓价仓位价值) * 100 / 开仓保证金
            平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值

            平仓价仓位价值 = 平仓价 * 持仓币数
            平仓价涨跌盈亏幅 = (平仓价 - 开仓价) * 100 / 开仓价
            平仓收益 = (平仓价 - 开仓价) * 持仓币数
            平仓收益率 = (平仓价 - 开仓价) * |标记价仓位价值| * 100 / (开仓价 * 标记价保证金)

        当开仓方向为 "SHORT" 时：
            标记价涨跌盈亏幅 = (开仓价 - 标记价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (开仓价 - 最新价) * 100 / 开仓价
            最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            最新价仓位价值 = 最新价 * 持仓币数
            最新价浮盈 = 开仓价仓位价值 - (最新价 * 持仓币数)
            最新价浮盈百分比 = [开仓价仓位价值 - (最新价 * 持仓币数)] * 100 / 开仓保证金
            标记价浮盈百分比 = [开仓价仓位价值 - (标记价 * 持仓币数)] * 100 / 开仓保证金
            平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值

            平仓价仓位价值 = 平仓价 * 持仓币数
            平仓价涨跌盈亏幅 = (开仓价 - 平仓价) * 100 / 开仓价
            平仓收益 = (开仓价 - 平仓价) * 持仓币数
            平仓收益率 = (开仓价 - 平仓价) * |标记价仓位价值| * 100 / (开仓价 * 标记价保证金)

        【重要】所有计算前都使用安全转换
        ==================================================
        """
        logger.debug("【币安修复区】【持仓缺失修复】第6步：计算固定字段（严格按照原始方案，独立计算）")

        cache = self.cache

        # ===== 使用安全转换获取所有原始字段值 =====
        latest_price = self._safe_float(cache.get(FIELD_LATEST_PRICE))
        mark_price = self._safe_float(cache.get(FIELD_MARK_PRICE))
        close_price = cache.get(FIELD_CLOSE_PRICE)  # 平仓价可能为None，保持原样
        position_size = self._safe_float(cache.get(FIELD_POSITION_SIZE))
        leverage = self._safe_float(cache.get(FIELD_LEVERAGE), 1.0)
        open_price = self._safe_float(cache.get(FIELD_OPEN_PRICE))
        open_position_value = self._safe_float(cache.get(FIELD_OPEN_POSITION_VALUE))
        open_margin = self._safe_float(cache.get(FIELD_OPEN_MARGIN), 1.0)
        total_funding = self._safe_float(cache.get(FIELD_FUNDING_TOTAL))
        direction = cache.get(FIELD_OPEN_DIRECTION)
        mark_position_value = self._safe_float(cache.get(FIELD_MARK_POSITION_VALUE))
        mark_margin = self._safe_float(cache.get(FIELD_MARK_MARGIN))

        # 根据方向计算 - 每个字段严格按照原始公式独立计算
        if direction == "LONG":
            # 多头 - 严格按照原始公式，独立计算每个字段

            # 1. 标记价涨跌盈亏幅 = (标记价 - 开仓价) * 100 / 开仓价
            mark_pnl_percent = (mark_price - open_price) * 100 / open_price if open_price else 0
            mark_pnl_percent = round(mark_pnl_percent, 4)  # 四舍五入保留4位小数

            # 2. 最新价涨跌盈亏幅 = (最新价 - 开仓价) * 100 / 开仓价
            latest_pnl_percent = (latest_price - open_price) * 100 / open_price if open_price else 0
            latest_pnl_percent = round(latest_pnl_percent, 4)  # 四舍五入保留4位小数

            # 3. 最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            latest_margin = (latest_price * position_size / leverage) if leverage else 0
            latest_margin = round(latest_margin, 4)  # 四舍五入保留4位小数

            # 4. 最新价仓位价值 = 最新价 * 持仓币数
            latest_position_value = latest_price * position_size
            latest_position_value = round(latest_position_value, 4)  # 四舍五入保留4位小数

            # 5. 最新价浮盈 = 最新价 * 持仓币数 - 开仓价仓位价值
            latest_pnl = (latest_price * position_size) - open_position_value
            latest_pnl = round(latest_pnl, 4)  # 四舍五入保留4位小数

            # 6. 最新价浮盈百分比 = (最新价 * 持仓币数 - 开仓价仓位价值) * 100 / 开仓保证金
            latest_pnl_percent_of_margin = ((latest_price * position_size - open_position_value) * 100 / open_margin) if open_margin else 0
            latest_pnl_percent_of_margin = round(latest_pnl_percent_of_margin, 4)  # 四舍五入保留4位小数

            # 7. 标记价浮盈百分比 = (标记价 * 持仓币数 - 开仓价仓位价值) * 100 / 开仓保证金
            mark_pnl_percent_of_margin = ((mark_price * position_size - open_position_value) * 100 / open_margin) if open_margin else 0
            mark_pnl_percent_of_margin = round(mark_pnl_percent_of_margin, 4)  # 四舍五入保留4位小数

            # 8. 平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值
            avg_funding_rate = (total_funding * 100 / open_position_value) if open_position_value else 0
            avg_funding_rate = round(avg_funding_rate, 4)  # 四舍五入保留4位小数

            # ===== 新增：平仓相关字段（仅在平仓价不为空时计算）=====
            if close_price is not None:
                close_price_float = self._safe_float(close_price)
                # 9. 平仓价仓位价值 = 平仓价 * 持仓币数
                close_position_value = close_price_float * position_size
                close_position_value = round(close_position_value, 4)
                cache[FIELD_CLOSE_POSITION_VALUE] = close_position_value

                # 10. 平仓价涨跌盈亏幅 = (平仓价 - 开仓价) * 100 / 开仓价
                close_pnl_percent = (close_price_float - open_price) * 100 / open_price if open_price else 0
                close_pnl_percent = round(close_pnl_percent, 4)
                cache[FIELD_CLOSE_PNL_PERCENT] = close_pnl_percent

                # 11. 平仓收益 = (平仓价 - 开仓价) * 持仓币数
                close_pnl = (close_price_float - open_price) * position_size
                close_pnl = round(close_pnl, 4)
                cache[FIELD_CLOSE_PNL] = close_pnl

                # 12. 平仓收益率 = (平仓价 - 开仓价) * |标记价仓位价值| * 100 / (开仓价 * 标记价保证金)
                mark_position_value_abs = abs(mark_position_value)
                close_pnl_percent_of_margin = (close_price_float - open_price) * mark_position_value_abs * 100 / (open_price * mark_margin) if (open_price and mark_margin) else 0
                close_pnl_percent_of_margin = round(close_pnl_percent_of_margin, 4)
                cache[FIELD_CLOSE_PNL_PERCENT_OF_MARGIN] = close_pnl_percent_of_margin

                logger.debug(f"  【币安修复区】【持仓缺失修复】 平仓计算完成 - 平仓价: {close_price_float}, "
                           f"平仓价仓位价值: {close_position_value:.2f}, "
                           f"平仓价涨跌盈亏幅: {close_pnl_percent:.2f}%, "
                           f"平仓收益: {close_pnl:.2f}, "
                           f"平仓收益率: {close_pnl_percent_of_margin:.2f}%")
            else:
                logger.debug("  【币安修复区】【持仓缺失修复】 平仓价为空，跳过平仓相关字段计算")

        else:  # direction == "SHORT"
            # 空头 - 严格按照原始公式，独立计算每个字段

            # 1. 标记价涨跌盈亏幅 = (开仓价 - 标记价) * 100 / 开仓价
            mark_pnl_percent = (open_price - mark_price) * 100 / open_price if open_price else 0
            mark_pnl_percent = round(mark_pnl_percent, 4)  # 四舍五入保留4位小数

            # 2. 最新价涨跌盈亏幅 = (开仓价 - 最新价) * 100 / 开仓价
            latest_pnl_percent = (open_price - latest_price) * 100 / open_price if open_price else 0
            latest_pnl_percent = round(latest_pnl_percent, 4)  # 四舍五入保留4位小数

            # 3. 最新价保证金 = 最新价 * 持仓币数 ÷ 杠杆
            latest_margin = (latest_price * position_size / leverage) if leverage else 0
            latest_margin = round(latest_margin, 4)  # 四舍五入保留4位小数

            # 4. 最新价仓位价值 = 最新价 * 持仓币数
            latest_position_value = latest_price * position_size
            latest_position_value = round(latest_position_value, 4)  # 四舍五入保留4位小数

            # 5. 最新价浮盈 = 开仓价仓位价值 - (最新价 * 持仓币数)
            latest_pnl = open_position_value - (latest_price * position_size)
            latest_pnl = round(latest_pnl, 4)  # 四舍五入保留4位小数

            # 6. 最新价浮盈百分比 = [开仓价仓位价值 - (最新价 * 持仓币数)] * 100 / 开仓保证金
            latest_pnl_percent_of_margin = ((open_position_value - latest_price * position_size) * 100 / open_margin) if open_margin else 0
            latest_pnl_percent_of_margin = round(latest_pnl_percent_of_margin, 4)  # 四舍五入保留4位小数

            # 7. 标记价浮盈百分比 = [开仓价仓位价值 - (标记价 * 持仓币数)] * 100 / 开仓保证金
            mark_pnl_percent_of_margin = ((open_position_value - mark_price * position_size) * 100 / open_margin) if open_margin else 0
            mark_pnl_percent_of_margin = round(mark_pnl_percent_of_margin, 4)  # 四舍五入保留4位小数

            # 8. 平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值
            avg_funding_rate = (total_funding * 100 / open_position_value) if open_position_value else 0
            avg_funding_rate = round(avg_funding_rate, 4)  # 四舍五入保留4位小数

            # ===== 新增：平仓相关字段（仅在平仓价不为空时计算）=====
            if close_price is not None:
                close_price_float = self._safe_float(close_price)
                # 9. 平仓价仓位价值 = 平仓价 * 持仓币数
                close_position_value = close_price_float * position_size
                close_position_value = round(close_position_value, 4)
                cache[FIELD_CLOSE_POSITION_VALUE] = close_position_value

                # 10. 平仓价涨跌盈亏幅 = (开仓价 - 平仓价) * 100 / 开仓价
                close_pnl_percent = (open_price - close_price_float) * 100 / open_price if open_price else 0
                close_pnl_percent = round(close_pnl_percent, 4)
                cache[FIELD_CLOSE_PNL_PERCENT] = close_pnl_percent

                # 11. 平仓收益 = (开仓价 - 平仓价) * 持仓币数
                close_pnl = (open_price - close_price_float) * position_size
                close_pnl = round(close_pnl, 4)
                cache[FIELD_CLOSE_PNL] = close_pnl

                # 12. 平仓收益率 = (开仓价 - 平仓价) * |标记价仓位价值| * 100 / (开仓价 * 标记价保证金)
                mark_position_value_abs = abs(mark_position_value)
                close_pnl_percent_of_margin = (open_price - close_price_float) * mark_position_value_abs * 100 / (open_price * mark_margin) if (open_price and mark_margin) else 0
                close_pnl_percent_of_margin = round(close_pnl_percent_of_margin, 4)
                cache[FIELD_CLOSE_PNL_PERCENT_OF_MARGIN] = close_pnl_percent_of_margin

                logger.debug(f"  【币安修复区】【持仓缺失修复】 平仓计算完成 - 平仓价: {close_price_float}, "
                           f"平仓价仓位价值: {close_position_value:.2f}, "
                           f"平仓价涨跌盈亏幅: {close_pnl_percent:.2f}%, "
                           f"平仓收益: {close_pnl:.2f}, "
                           f"平仓收益率: {close_pnl_percent_of_margin:.2f}%")
            else:
                logger.debug("  【币安修复区】【持仓缺失修复】 平仓价为空，跳过平仓相关字段计算")

        # 保存原有8个字段的计算结果到缓存 - 每个字段只赋值一次，使用正确的常量名
        cache[FIELD_MARK_PNL_PERCENT] = mark_pnl_percent                    # 1. 标记价涨跌盈亏幅
        cache[FIELD_LATEST_PNL_PERCENT] = latest_pnl_percent              # 2. 最新价涨跌盈亏幅
        cache[FIELD_LATEST_MARGIN] = latest_margin                         # 3. 最新价保证金
        cache[FIELD_LATEST_POSITION_VALUE] = latest_position_value         # 4. 最新价仓位价值
        cache[FIELD_LATEST_PNL] = latest_pnl                               # 5. 最新价浮盈
        cache[FIELD_LATEST_PNL_PERCENT_OF_MARGIN] = latest_pnl_percent_of_margin  # 6. 最新价浮盈百分比
        cache[FIELD_MARK_PNL_PERCENT_OF_MARGIN] = mark_pnl_percent_of_margin    # 7. 标记价浮盈百分比（重新计算）
        cache[FIELD_AVG_FUNDING_RATE] = avg_funding_rate                   # 8. 平均资金费率

        logger.debug(f" 【币安修复区】【持仓缺失修复】  计算完成 - 标记价涨跌盈亏幅: {mark_pnl_percent:.2f}%, "
                   f"最新价涨跌盈亏幅: {latest_pnl_percent:.2f}%, "
                   f"最新价保证金: {latest_margin:.2f}, "
                   f"最新价仓位价值: {latest_position_value:.2f}, "
                   f"最新价浮盈: {latest_pnl:.2f}, "
                   f"最新价浮盈百分比: {latest_pnl_percent_of_margin:.2f}%, "
                   f"标记价浮盈百分比: {mark_pnl_percent_of_margin:.2f}%, "
                   f"平均资金费率: {avg_funding_rate:.4f}%, "
                   f"开仓方向: {direction}")

    # ==================== 原第6步改为第7步：提取并推送 ====================

    async def _step7_extract_and_push(self):
        """
        第7步：提取3个特定字段并检测平仓价打对应标签推送
        ==================================================
        做了五件事：
            1. 从门外存储区读取最新的币安数据
            2. 提取3个字段：标记价保证金、标记价仓位价值、标记价浮盈
            3. 如果提取不到或为空值，保留原缓存值（不报错）
            4. 检测平仓价字段：
               - 若平仓价为空，打标签"持仓完整"
               - 若平仓价不为空，打标签"平仓完整"
            5. 创建缓存副本，打上对应标签，推送给调度器

        需要提取的3个字段：
            - 标记价保证金
            - 标记价仓位价值
            - 标记价浮盈
        ==================================================
        """
        logger.debug("【币安修复区】【持仓缺失修复】第7步：提取3个特定字段并检测平仓价打对应标签推送")

        # 从门外存储区获取最新的币安数据
        latest_binance = self._get_binance_from_snapshot()
        if not latest_binance:
            logger.warning("⚠️【币安修复区】【持仓缺失修复】 无法获取最新的币安数据，跳过字段提取")
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
                await asyncio.sleep(0)  # ✅ 循环内让出CPU
                if field in latest_binance and latest_binance[field] is not None and latest_binance[field] != '':
                    self.cache[field] = latest_binance[field]
                    extract_count += 1
                else:
                    logger.debug(f"【币安修复区】【持仓缺失修复】   字段 {field} 提取不到或为空值，保留原缓存值")

            logger.debug(f"【币安修复区】【持仓缺失修复】   成功提取 {extract_count} 个字段")

        # 创建缓存副本
        data_copy = self.cache.copy()
        
        # 检测平仓价字段
        close_price = data_copy.get(FIELD_CLOSE_PRICE)
        close_time = data_copy.get(FIELD_CLOSE_TIME)
        
        # 确定要打的标签
        if close_price is not None and close_price != '' and close_time is not None and close_time != '':
            # 平仓价和平仓时间都有值，说明已平仓
            tag = TAG_CLOSED_COMPLETE
            logger.debug(f"  【币安修复区】【持仓缺失修复】 检测到平仓价有值，打标签: {tag}")
        else:
            # 平仓价或平仓时间为空，说明还在持仓中
            tag = TAG_COMPLETE
            logger.debug(f"  【币安修复区】【持仓缺失修复】 检测到平仓价为空，打标签: {tag}")

        # 打标签推送
        await self.scheduler.handle({
            'tag': tag,
            'data': data_copy
        })

        contract = data_copy.get(FIELD_OPEN_CONTRACT, 'unknown')
        logger.info(f"✅ 【币安修复区】【持仓缺失修复】已推送{tag}数据: {EXCHANGE_BINANCE} - {contract}")

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
            logger.debug("【币安修复区】【持仓缺失修复】门外还没有存储区数据")
            return None

        user_data = self.latest_snapshot.get('user_data', {})
        binance_key = f"{EXCHANGE_BINANCE}_user"
        binance_item = user_data.get(binance_key, {})

        if not binance_item:
            logger.debug("【币安修复区】【持仓缺失修复】存储区中没有币安用户数据")
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
            logger.debug("【币安修复区】【持仓缺失修复】门外还没有存储区数据")
            return None

        market_data = self.latest_snapshot.get('market_data', {})
        if not market_data:
            logger.debug("【币安修复区】【持仓缺失修复】存储区中没有行情数据")
            return None

        return market_data.get(contract)

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

        logger.debug(f" 【币安修复区】【持仓缺失修复】  已更新 {update_count} 个资金费字段")


# ==================== 已移除的方法 ====================
"""
【已移除的方法 - 不再需要】

以下方法在原Turso版本中存在，MongoDB版本不再需要：

1. _query_database() - 改用MongoDB的直接查询
2. _row_to_dict() - MongoDB返回的就是字典，不需要转换
3. _get_session() - 不再需要aiohttp会话
4. close() - 不再需要关闭aiohttp会话

所有数据库操作都在第1步按需完成，用完即关，不保留长连接。
"""