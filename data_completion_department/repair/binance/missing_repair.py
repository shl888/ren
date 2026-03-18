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

【修复流程 - 共6步】（完全按照你的设计文档）
第1步：获取缓存数据（从数据库或直接用缓存）
第2步：检测资金费状态（4种情况）
第3步：资金费融合（只有情况D需要）
第4步：提取最新价和标记价（从行情数据）
第5步：计算固定字段（原有8个字段 + 新增4个平仓相关字段）
第6步：提取3个特定字段并检测平仓价打对应标签推送
==================================================
"""

import os
import logging
import asyncio
import aiohttp  # ✅ [蚂蚁基因修复] 改用异步HTTP库
from typing import Dict, Any, Optional, List
from datetime import datetime

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
        4. 执行6步修复流程

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

        # ===== 【修改】不再在初始化时读取和存储数据库连接信息 =====
        # 数据库连接改为按需使用，只在第1步缓存为空时才读取环境变量
        # 这样可以避免不必要的数据库连接，也符合"用完即弃"的原则

        # ✅ [蚂蚁基因修复] 添加aiohttp会话管理
        self._session = None

        # 临时存储门外数据（供第3步使用）
        self._snapshot_data = None

        logger.info("✅【币安修复区】【持仓缺失修复】 修复区初始化完成")

    # ✅ [蚂蚁基因修复] 获取或创建aiohttp会话
    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建aiohttp会话"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    # ✅ [蚂蚁基因修复] 关闭会话
    async def close(self):
        """关闭aiohttp会话"""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("🔌【币安修复区】【持仓缺失修复】 HTTP会话已关闭")

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
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU，避免长时间占用
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
        完全按照你的6步设计文档：
            第1步：获取缓存数据（从数据库或直接用缓存）
            第2步：检测资金费状态（4种情况）
            第3步：资金费融合（只有情况D需要）
            第4步：提取最新价和标记价（从行情数据）
            第5步：计算固定字段（原有8个字段 + 新增4个平仓相关字段）
            第6步：提取3个特定字段并检测平仓价打对应标签推送
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

        if not await self._step4_get_prices():
            logger.error("❌【币安修复区】【持仓缺失修复】 第4步失败：无法获取行情数据，本次修复终止")
            return

        await self._step5_calc_fields()
        await self._step6_extract_and_push()

        logger.debug("【币安修复区】【持仓缺失修复】一次修复流程执行完成")

    # ==================== 6步修复流程 ====================

    async def _step1_get_cache(self) -> bool:
        """
        第1步：获取缓存数据
        ==================================================
        【修改说明】
        按照"按需连接"原则重构：
            1. 先检查缓存是否有数据
            2. 如果有缓存，直接使用，不需要连接数据库
            3. 如果没缓存，才去连接数据库读取

        【5层检查】
            第1层：是否拿到环境变量
            第2层：数据库连接是否成功
            第3层：持仓表是否存在
            第4层：表中有什么数据
            第5层：是否找到币安数据并提取成功
        ==================================================
        """
        # ----- 第1层：检查缓存 -----
        if self.cache is not None:
            logger.debug("✅【币安修复区】【持仓缺失修复】 第1步：使用现有缓存")
            return True

        logger.info("🔍【币安修复区】【持仓缺失修复】 第1步：缓存为空，准备从数据库读取")

        # ----- 第2层：获取数据库连接信息（按需读取环境变量）-----
        db_url = os.getenv('TURSO_DATABASE_URL')
        db_token = os.getenv('TURSO_DATABASE_TOKEN')

        if not db_url:
            logger.error("❌【币安修复区】【持仓缺失修复】 环境变量 TURSO_DATABASE_URL 未设置")
            return False
        
        if not db_token:
            logger.error("❌【币安修复区】【持仓缺失修复】 环境变量 TURSO_DATABASE_TOKEN 未设置")
            return False

        logger.info("✅【币安修复区】【持仓缺失修复】 成功读取数据库连接信息")
        logger.debug(f"   数据库URL: {db_url}")

        # ----- 第3层：测试数据库连接是否成功 -----
        try:
            test_result = await self._query_database("SELECT 1", db_url, db_token)
            if test_result is None:
                logger.error("❌【币安修复区】【持仓缺失修复】 数据库连接失败（网络问题或令牌错误）")
                return False
            logger.info("✅【币安修复区】【持仓缺失修复】 数据库连接成功")
        except Exception as e:
            logger.error(f"❌【币安修复区】【持仓缺失修复】 数据库连接异常: {e}")
            return False

        # ----- 第4层：检查所有表 -----
        try:
            tables_result = await self._query_database(
                "SELECT name FROM sqlite_master WHERE type='table'", 
                db_url, db_token
            )
            
            if tables_result is None:
                logger.error("❌【币安修复区】【持仓缺失修复】 查询表失败")
                return False
            
            rows = tables_result.get('rows', [])
            cols = tables_result.get('cols', [])
            
            all_tables = []
            for row in rows:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                if row and len(row) > 0:
                    if isinstance(row[0], dict):
                        table_name = row[0].get('value')
                    else:
                        table_name = row[0]
                    all_tables.append(table_name)
            
            logger.info(f"📋【币安修复区】【持仓缺失修复】 数据库中的所有表: {all_tables}")
            
            if 'active_positions' not in all_tables:
                logger.error("❌【币安修复区】【持仓缺失修复】 数据库中没有 active_positions 表")
                return False
            
            logger.info("✅【币安修复区】【持仓缺失修复】 持仓表存在")
        except Exception as e:
            logger.error(f"❌【币安修复区】【持仓缺失修复】 检查表失败: {e}")
            return False

        # ----- 第5层：查看表中所有数据 -----
        try:
            all_data = await self._query_database(
                "SELECT 交易所, 开仓合约名, id FROM active_positions", 
                db_url, db_token
            )
            
            if all_data and 'rows' in all_data:
                rows = all_data.get('rows', [])
                cols = all_data.get('cols', [])
                
                logger.info(f"📊【币安修复区】【持仓缺失修复】 active_positions 表共有 {len(rows)} 条数据")
                
                # 找出交易所字段的索引位置
                exchange_idx = None
                for i, col in enumerate(cols):
                    await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                    if isinstance(col, dict):
                        col_name = col.get('name')
                    else:
                        col_name = col
                    if col_name == '交易所':
                        exchange_idx = i
                        break
                
                if exchange_idx is not None:
                    exchanges = []
                    for row in rows:
                        await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                        if row and len(row) > exchange_idx:
                            if isinstance(row[exchange_idx], dict):
                                exchange = row[exchange_idx].get('value')
                            else:
                                exchange = row[exchange_idx]
                            exchanges.append(exchange)
                    
                    logger.info(f"📋【币安修复区】【持仓缺失修复】 表中的交易所值: {exchanges}")
                else:
                    logger.warning("⚠️【币安修复区】【持仓缺失修复】 找不到'交易所'字段")
            else:
                logger.warning("⚠️【币安修复区】【持仓缺失修复】 active_positions 表是空的")
        except Exception as e:
            logger.warning(f"⚠️【币安修复区】【持仓缺失修复】 查询所有数据失败: {e}")
            # 继续执行，不因为调试查询失败而终止

        # ----- 第6层：查询币安数据（尝试不同写法）-----
        try:
            # 先尝试小写 binance
            sql = "SELECT * FROM active_positions WHERE 交易所 = 'binance' LIMIT 1"
            logger.debug(f"🔍【币安修复区】【持仓缺失修复】 执行查询: {sql}")
            
            result = await self._query_database(sql, db_url, db_token)

            if result is None:
                logger.error("❌【币安修复区】【持仓缺失修复】 查询币安数据失败")
                return False

            rows = result.get('rows', [])
            cols = result.get('cols', [])
            
            # 如果没有找到数据，尝试其他可能的写法
            if not rows:
                logger.warning("⚠️【币安修复区】【持仓缺失修复】 查询成功，但没有找到交易所为 'binance' 的数据")
                
                # 尝试其他可能的交易所名称
                test_exchanges = ['BINANCE', 'Binance', '币安']
                found = False
                
                for test_exchange in test_exchanges:
                    await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                    logger.debug(f"🔍【币安修复区】【持仓缺失修复】 尝试查询: {test_exchange}")
                    test_result = await self._query_database(
                        f"SELECT * FROM active_positions WHERE 交易所 = '{test_exchange}' LIMIT 1",
                        db_url, db_token
                    )
                    
                    if test_result and test_result.get('rows'):
                        logger.debug(f"✅【币安修复区】【持仓缺失修复】 找到数据！交易所字段实际为: {test_exchange}")
                        rows = test_result.get('rows', [])
                        cols = test_result.get('cols', [])
                        found = True
                        break
                
                if not found:
                    logger.error("❌【币安修复区】【持仓缺失修复】 尝试了所有可能的交易所名称，都没有找到数据")
                    return False

            # ----- 第7层：提取数据到缓存 -----
            row = rows[0]
            self.cache = self._row_to_dict(row, cols)

            logger.info(f"✅【币安修复区】【持仓缺失修复】 第1步：成功读取到币安数据")
            logger.info(f"   交易所: {self.cache.get(FIELD_EXCHANGE)}")
            logger.info(f"   开仓合约名: {self.cache.get(FIELD_OPEN_CONTRACT)}")
            logger.info(f"   ID: {self.cache.get('id')}")
            
            # 打印关键字段
            logger.debug(f"   开仓时间: {self.cache.get('开仓时间')}")
            logger.debug(f"   开仓价: {self.cache.get(FIELD_OPEN_PRICE)}")
            logger.debug(f"   持仓张数: {self.cache.get(FIELD_POSITION_CONTRACTS)}")
            logger.debug(f"   累计资金费: {self.cache.get(FIELD_FUNDING_TOTAL)}")
            
            return True

        except Exception as e:
            logger.error(f"❌【币安修复区】【持仓缺失修复】 第1步：读取数据库失败: {e}", exc_info=True)
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

        # 判断有无新结算（存储区本次资金费是否为0）
        snapshot_funding = snapshot_data.get(FIELD_FUNDING_THIS, 0)
        if snapshot_funding is None:
            snapshot_funding = 0
        has_new = (snapshot_funding != 0)

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

    async def _step3_funding_fusion(self):
        """
        第3步：资金费融合流程
        ==================================================
        只有情况D（有历史+有新结算）才会执行这一步。

        融合规则：
            1. 本次资金费和本次结算时间 → 直接从存储区覆盖（允许部分更新）
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
        logger.debug("第4步：【币安修复区】【持仓缺失修复】从行情数据提取最新价和标记价")

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

        logger.debug(f"✅ 【币安修复区】【持仓缺失修复】第4步：获取到行情数据 - 最新价: {latest_price}, 标记价: {mark_price}")
        return True

    async def _step5_calc_fields(self):
        """
        第5步：计算固定字段
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
        logger.debug("【币安修复区】【持仓缺失修复】第5步：计算固定字段（严格按照原始方案，独立计算）")

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

    async def _step6_extract_and_push(self):
        """
        第6步：提取3个特定字段并检测平仓价打对应标签推送
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
        logger.debug("【币安修复区】【持仓缺失修复】第6步：提取3个特定字段并检测平仓价打对应标签推送")

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
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
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

    async def _query_database(self, sql: str, db_url: str, db_token: str, params: List = None) -> Optional[Dict]:
        """
        ✅ [蚂蚁基因修复] 异步查询Turso数据库
        ==================================================
        【修改说明】
        接收数据库连接信息作为参数，而不是使用实例变量。
        这样符合"按需连接、用完即弃"的原则。
        
        使用异步 aiohttp 替代同步 requests

        使用正确的Turso API返回格式解析：
            {
                "results": [
                    {
                        "type": "ok",
                        "response": {
                            "type": "execute",
                            "result": {
                                "cols": [...],
                                "rows": [...]   # 数据在这里！
                            }
                        }
                    }
                ]
            }

        :param sql: SQL查询语句
        :param db_url: 数据库URL
        :param db_token: 数据库令牌
        :param params: 查询参数列表
        :return: 包含 cols 和 rows 的结果字典，失败返回None
        ==================================================
        """
        if params is None:
            params = []

        # 转换参数格式
        args = []
        for p in params:
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            if p is None:
                args.append({"type": "null", "value": None})
            else:
                args.append({"type": "text", "value": str(p)})

        # 构建请求体
        request_item = {
            "type": "execute",
            "stmt": {
                "sql": sql
            }
        }
        
        # 只有在有参数时才添加 args
        if args:
            request_item["stmt"]["args"] = args
        
        payload = {
            "requests": [request_item]
        }

        session = await self._get_session()

        try:
            async with session.post(
                f"{db_url}/v2/pipeline",
                headers={
                    "Authorization": f"Bearer {db_token}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=10
            ) as response:
                
                response.raise_for_status()
                result = await response.json()

                # ✅ 正确的解析路径：results[0].response.result
                if result and 'results' in result and len(result['results']) > 0:
                    first_result = result['results'][0]
                    if 'response' in first_result and 'result' in first_result['response']:
                        return first_result['response']['result']  # 返回 {cols, rows}
                
                return None

        except asyncio.TimeoutError:
            logger.error(f"❌ 【币安修复区】【持仓缺失修复】数据库查询超时: {sql[:50]}...")
            return None
        except aiohttp.ClientError as e:
            logger.error(f"❌ 【币安修复区】【持仓缺失修复】数据库请求失败: {e}")
            return None
        except Exception as e:
            logger.error(f"❌【币安修复区】【持仓缺失修复】 数据库查询未知错误: {e}")
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
                col_name = col.get('name') if isinstance(col, dict) else col
                value_cell = row[i]
                if isinstance(value_cell, dict):
                    value = value_cell.get('value')
                else:
                    value = value_cell
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

        logger.debug(f" 【币安修复区】【持仓缺失修复】  已更新 {update_count} 个资金费字段")