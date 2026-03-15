"""
欧意持仓缺失修复
==================================================
【文件职责】
这个文件是欧意修复区的核心，负责处理两种信息标签：
1. "欧意持仓缺失" - 启动修复流程（循环运行）
2. "欧意已平仓"   - 停止修复流程

【重要提醒】
这个文件会被两个地方调用：
1. 调度器直接调用 handle_info() 推送信息标签
2. receiver 通过币安修复区入口间接调用 handle_store_snapshot() 推送数据

【数据来源】
1. 门外存储区快照：通过 handle_store_snapshot() 接收
2. 数据库持仓区：从Turso数据库读取历史持仓数据（第1步需要）
3. 本文件缓存：保存修复过程中的中间数据

【门外标签】
调度器推送的信息标签，永远只有1个（覆盖更新）
   - 持仓缺失标签 = 开（启动循环）
   - 已平仓标签   = 关（停止循环）

【门外数据】
receiver推送的存储区快照，永远只有1份（覆盖更新）
   - 包含最新的user_data
   - 修复区从这里获取欧意数据

【修复流程 - 共6步】（完全按照你的设计文档）
第1步：获取缓存数据（从数据库或直接用缓存）
第2步：检测资金费状态（4种情况）
第3步：资金费融合（只有情况D需要）
第4步：覆盖更新（从门外存储区读最新数据，保护资金费4字段）
第5步：计算6个固定字段（严格按照原始方案，独立计算）
第6步：打标签推送
==================================================
"""

import os
import logging
import asyncio
import requests
import json
from typing import Dict, Any, Optional, List
from datetime import datetime

# 导入常量 - 使用修正后的常量名
from ...constants import (
    TAG_COMPLETE,
    INFO_OKX_MISSING,
    INFO_OKX_CLOSED,
    EXCHANGE_OKX,
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
    FIELD_MARK_POSITION_VALUE,
    FIELD_LATEST_PRICE,
    FIELD_LATEST_POSITION_VALUE,
    FIELD_LATEST_MARGIN,
    FIELD_MARK_PNL_PERCENT,              # 标记价涨跌盈亏幅
    FIELD_LATEST_PNL_PERCENT,            # 最新价涨跌盈亏幅
    FIELD_AVG_FUNDING_RATE,
    FIELD_FUNDING_THIS,
    FIELD_FUNDING_TOTAL,
    FIELD_FUNDING_COUNT,
    FIELD_FUNDING_TIME,
)

logger = logging.getLogger(__name__)


class OkxMissingRepair:
    """
    欧意持仓缺失修复类
    ==================================================
    这个类负责：
        1. 接收门外标签（通过 handle_info）
        2. 接收门外存储区快照（通过 handle_store_snapshot）
        3. 根据标签启动/停止修复循环
        4. 执行6步修复流程

    门外标签规则：
        - 永远只有1个标签（覆盖更新）
        - 持仓缺失 = 开（启动循环）
        - 已平仓   = 关（停止循环）

    门外数据规则：
        - 永远只有1份存储区快照（覆盖更新）
        - 包含最新的 user_data

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

        # ===== 门外存储区快照（receiver推送过来的）=====
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

        # 临时存储门外数据（供第3步使用）
        self._snapshot_data = None

        logger.info("✅【欧易持仓缺失修复区】 初始化完成")

    # ==================== 对外入口 ====================

    async def handle_info(self, info: str):
        """
        接收调度器推送的信息标签
        ==================================================
        这是修复区的标签入口，调度器会把两种标签推送到这里：
            - "欧意持仓缺失"
            - "欧意已平仓"

        门外标签规则：
            - 永远只有1个标签（覆盖更新）
            - 新标签到来直接覆盖旧标签

        动作：
            - 记录当前标签（self.current_info）
            - 根据标签类型启动或停止修复循环
        ==================================================

        :param info: 信息标签，只能是"欧意持仓缺失"或"欧意已平仓"
        """
        old_info = self.current_info
        self.current_info = info  # 覆盖更新

        logger.debug(f"📨【欧易持仓缺失修复区】 门外标签更新: {old_info} → {info}")

        # ===== 根据标签决定开关 =====
        if info == INFO_OKX_MISSING:
            await self._start_repair()
        elif info == INFO_OKX_CLOSED:
            await self._stop_repair()
        else:
            logger.warning(f"⚠️【欧易持仓缺失修复区】 收到未知信息标签: {info}")

    async def handle_store_snapshot(self, snapshot: Dict):
        """
        接收receiver推送的存储区快照
        ==================================================
        这是修复区的数据入口，receiver会把整个存储区推送到这里：
            {
                'market_data': {...},  # 所有合约的行情数据（欧易不需要）
                'user_data': {...},     # 所有交易所的私人数据
                'timestamp': '...'
            }

        门外数据规则：
            - 永远只有1份快照（覆盖更新）
            - 新快照到来直接覆盖旧快照

        动作：
            - 保存最新的快照（self.latest_snapshot）
        ==================================================

        :param snapshot: 完整的存储区快照
        """
        self.latest_snapshot = snapshot
        logger.debug(f"📦【欧易持仓缺失修复区】 收到存储区快照，时间戳: {snapshot.get('timestamp')}")

    # ==================== 修复循环控制 ====================

    async def _start_repair(self):
        """启动修复流程（循环运行）"""
        if self.is_running:
            logger.debug("【欧易持仓缺失修复区】修复流程已在运行中")
            return

        self.is_running = True
        self.repair_task = asyncio.create_task(self._repair_loop())
        logger.info("🚀【欧易持仓缺失修复区】 修复流程已启动（循环运行）")

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
        logger.info("🛑 【欧易持仓缺失修复区】修复流程已停止")

    async def _repair_loop(self):
        """
        修复循环
        ==================================================
        只要门外标签是"欧意持仓缺失"，就一直运行

        循环频率：每秒执行一次
        安全机制：
            - 每次执行前检查门外标签
            - 如果标签变了，自己停止
            - 如果修复过程出错，等待5秒后重试
        ==================================================
        """
        logger.debug("🔄【欧易持仓缺失修复区】 修复循环开始")

        while self.is_running:
            try:
                if self.current_info != INFO_OKX_MISSING:
                    logger.info("【欧易持仓缺失修复区】门外标签已不是持仓缺失，停止修复循环")
                    await self._stop_repair()
                    break

                await self._repair_once()
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("【欧易持仓缺失修复区】修复循环被取消")
                break
            except Exception as e:
                logger.error(f"❌ 【欧易持仓缺失修复区】修复循环出错: {e}", exc_info=True)
                await asyncio.sleep(5)

        logger.info("🔄【欧易持仓缺失修复区】 修复循环结束")

    async def _repair_once(self):
        """
        执行一轮修复流程
        ==================================================
        完全按照你的6步设计文档：
            第1步：获取缓存数据
            第2步：检测资金费状态
            第3步：资金费融合（仅情况D需要）
            第4步：覆盖更新（从门外存储区读最新数据，保护资金费4字段）
            第5步：计算6个固定字段（严格按照原始方案，独立计算）
            第6步：打标签推送
        ==================================================
        """
        logger.debug("执行一轮欧意持仓缺失修复")

        if not await self._step1_get_cache():
            logger.error("❌【欧易持仓缺失修复区】 第1步失败：无法获取缓存数据，本次修复终止")
            return

        # 检查门外是否有存储区数据
        if not self.latest_snapshot:
            logger.warning("⚠️【欧易持仓缺失修复区】 门外还没有存储区数据，等待下次循环")
            return

        funding_action = await self._step2_check_funding()
        if funding_action is None:
            logger.error("❌【欧易持仓缺失修复区】 第2步失败：检测资金费状态出错，本次修复终止")
            return

        if funding_action == 'do_fusion':
            await self._step3_funding_fusion()

        await self._step4_update_from_snapshot()
        await self._step5_calc_fixed_fields()
        await self._step6_push_complete()

        logger.debug("【欧易持仓缺失修复区】一轮修复流程执行完成")

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
            第5层：是否找到欧意数据并提取成功
        ==================================================
        """
        # ----- 第1层：检查缓存 -----
        if self.cache is not None:
            logger.debug("✅【欧易持仓缺失修复区】 第1步：使用现有缓存")
            return True

        logger.info("🔍【欧易持仓缺失修复区】 第1步：缓存为空，准备从数据库读取")

        # ----- 第2层：获取数据库连接信息（按需读取环境变量）-----
        db_url = os.getenv('TURSO_DATABASE_URL')
        db_token = os.getenv('TURSO_DATABASE_TOKEN')

        if not db_url:
            logger.error("❌【欧易持仓缺失修复区】 环境变量 TURSO_DATABASE_URL 未设置")
            return False
        
        if not db_token:
            logger.error("❌【欧易持仓缺失修复区】 环境变量 TURSO_DATABASE_TOKEN 未设置")
            return False

        logger.info("✅【欧易持仓缺失修复区】 成功读取数据库连接信息")
        logger.debug(f"   数据库URL: {db_url}")

        # ----- 第3层：测试数据库连接是否成功 -----
        try:
            test_result = self._query_database("SELECT 1", db_url, db_token)
            if test_result is None:
                logger.error("❌【欧易持仓缺失修复区】 数据库连接失败（网络问题或令牌错误）")
                return False
            logger.info("✅【欧易持仓缺失修复区】 数据库连接成功")
        except Exception as e:
            logger.error(f"❌【欧易持仓缺失修复区】 数据库连接异常: {e}")
            return False

        # ----- 第4层：检查所有表 -----
        try:
            tables_result = self._query_database(
                "SELECT name FROM sqlite_master WHERE type='table'", 
                db_url, db_token
            )
            
            if tables_result is None:
                logger.error("❌【欧易持仓缺失修复区】 查询表失败")
                return False
            
            rows = tables_result.get('rows', [])
            cols = tables_result.get('cols', [])
            
            all_tables = []
            for row in rows:
                if row and len(row) > 0:
                    if isinstance(row[0], dict):
                        table_name = row[0].get('value')
                    else:
                        table_name = row[0]
                    all_tables.append(table_name)
            
            logger.debug(f"📋【欧易持仓缺失修复区】 数据库中的所有表: {all_tables}")
            
            if 'active_positions' not in all_tables:
                logger.error("❌【欧易持仓缺失修复区】 数据库中没有 active_positions 表")
                return False
            
            logger.info("✅【欧易持仓缺失修复区】 持仓表存在")
        except Exception as e:
            logger.error(f"❌【欧易持仓缺失修复区】 检查表失败: {e}")
            return False

        # ----- 第5层：查看表中所有数据 -----
        try:
            all_data = self._query_database(
                "SELECT * FROM active_positions", 
                db_url, db_token
            )
            
            if all_data and 'rows' in all_data:
                rows = all_data.get('rows', [])
                cols = all_data.get('cols', [])
                
                logger.info(f"📊【欧易持仓缺失修复区】 active_positions 表共有 {len(rows)} 条数据")
                
                # 打印列名
                column_names = []
                for col in cols:
                    if isinstance(col, dict):
                        column_names.append(col.get('name'))
                    else:
                        column_names.append(col)
                logger.debug(f" 【欧易持仓缺失修复区】  列名: {column_names}")
                
                # 打印前几条数据
                for i, row in enumerate(rows[:3]):  # 只打印前3条
                    row_data = {}
                    for j, col in enumerate(column_names):
                        if j < len(row):
                            if isinstance(row[j], dict):
                                row_data[col] = row[j].get('value')
                            else:
                                row_data[col] = row[j]
                    logger.debug(f" 【欧易持仓缺失修复区】  第{i+1}条: {row_data}")
                
                # 找出交易所字段的索引位置
                exchange_idx = None
                for i, col in enumerate(column_names):
                    if col == '交易所':
                        exchange_idx = i
                        break
                
                if exchange_idx is not None:
                    exchanges = []
                    for row in rows:
                        if row and len(row) > exchange_idx:
                            if isinstance(row[exchange_idx], dict):
                                exchange = row[exchange_idx].get('value')
                            else:
                                exchange = row[exchange_idx]
                            exchanges.append(exchange)
                    
                    logger.debug(f"📋【欧易持仓缺失修复区】 表中的交易所值: {exchanges}")
                else:
                    logger.warning("⚠️【欧易持仓缺失修复区】 找不到'交易所'字段")
            else:
                logger.warning("⚠️【欧易持仓缺失修复区】 active_positions 表是空的")
        except Exception as e:
            logger.warning(f"⚠️【欧易持仓缺失修复区】 查询所有数据失败: {e}")
            # 继续执行，不因为调试查询失败而终止

        # ----- 第6层：查询欧意数据（尝试不同写法）-----
        try:
            # 先尝试小写 okx
            sql = "SELECT * FROM active_positions WHERE 交易所 = 'okx' LIMIT 1"
            logger.debug(f"🔍【欧易持仓缺失修复区】 执行查询: {sql}")
            
            result = self._query_database(sql, db_url, db_token)

            if result is None:
                logger.error("❌【欧易持仓缺失修复区】 查询欧意数据失败")
                return False

            rows = result.get('rows', [])
            cols = result.get('cols', [])
            
            # 如果没有找到数据，尝试其他可能的写法
            if not rows:
                logger.warning("⚠️【欧易持仓缺失修复区】 查询成功，但没有找到交易所为 'okx' 的数据")
                
                # 尝试其他可能的交易所名称
                test_exchanges = ['OKX', 'Okx', 'okex', 'OKEX', '欧意', '欧易']
                found = False
                
                for test_exchange in test_exchanges:
                    logger.debug(f"🔍【欧易持仓缺失修复区】 尝试查询: {test_exchange}")
                    test_result = self._query_database(
                        f"SELECT * FROM active_positions WHERE 交易所 = '{test_exchange}' LIMIT 1",
                        db_url, db_token
                    )
                    
                    if test_result and test_result.get('rows'):
                        logger.debug(f"✅【欧易持仓缺失修复区】 找到数据！交易所字段实际为: {test_exchange}")
                        rows = test_result.get('rows', [])
                        cols = test_result.get('cols', [])
                        found = True
                        break
                
                if not found:
                    logger.error("❌【欧易持仓缺失修复区】 尝试了所有可能的交易所名称，都没有找到数据")
                    return False

            # ----- 第7层：提取数据到缓存 -----
            row = rows[0]
            self.cache = self._row_to_dict(row, cols)

            logger.info(f"✅【欧易持仓缺失修复区】 第1步：成功读取到欧意数据")
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
            logger.error(f"❌【欧易持仓缺失修复区】 第1步：读取数据库失败: {e}", exc_info=True)
            return False

    async def _step2_check_funding(self) -> Optional[str]:
        """
        第2步：检测资金费状态
        ==================================================
        从门外存储区快照获取最新的欧意数据，然后检测：
            1. 有无历史：缓存累计资金费是否等于0
            2. 有无新结算：存储区累计资金费是否等于缓存累计资金费

        4种情况：
            A. 无历史 + 无新结算 → 返回 'skip_to_step4'
            B. 无历史 + 有新结算 → 更新4个资金费字段 → 返回 'skip_to_step4'
            C. 有历史 + 无新结算 → 返回 'skip_to_step4'
            D. 有历史 + 有新结算 → 返回 'do_fusion'
        ==================================================
        """
        logger.debug("【欧易持仓缺失修复区】第2步：检测资金费状态")

        # 从门外存储区快照获取最新的欧意数据
        snapshot_data = self._get_okx_from_snapshot()
        if not snapshot_data:
            logger.error("❌【欧易持仓缺失修复区】 门外存储区中没有欧意数据")
            return None

        cache_total = self.cache.get(FIELD_FUNDING_TOTAL, 0)
        if cache_total is None:
            cache_total = 0
        has_history = (cache_total != 0)

        snapshot_total = snapshot_data.get(FIELD_FUNDING_TOTAL, 0)
        if snapshot_total is None:
            snapshot_total = 0
        has_new = (snapshot_total != cache_total)

        logger.info(f" 【欧易持仓缺失修复区】  缓存累计资金费: {cache_total}, 门外存储区累计资金费: {snapshot_total}")
        logger.info(f" 【欧易持仓缺失修复区】  有无历史: {has_history}, 有无新结算: {has_new}")

        # 保存门外数据供后续步骤使用
        self._snapshot_data = snapshot_data

        if not has_history and not has_new:
            logger.info(" 【欧易持仓缺失修复区】  情况A：无历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'
        elif not has_history and has_new:
            logger.info(" 【欧易持仓缺失修复区】  情况B：无历史 + 有新结算，更新4个资金费字段后跳到第4步")
            self._update_funding_fields(snapshot_data)
            return 'skip_to_step4'
        elif has_history and not has_new:
            logger.info(" 【欧易持仓缺失修复区】  情况C：有历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'
        else:
            logger.info(" 【欧易持仓缺失修复区】  情况D：有历史 + 有新结算，进入第3步资金费融合")
            return 'do_fusion'

    async def _step3_funding_fusion(self):
        """
        第3步：资金费融合流程
        ==================================================
        只有情况D（有历史+有新结算）才会执行这一步。

        融合规则：
            1. 累计资金费、本次结算时间 → 直接从门外存储区覆盖
            2. 本次资金费 = 门外存储区累计资金费 - 缓存累计资金费
            3. 资金费结算次数 = 门外存储区次数 + 缓存次数
        ==================================================
        """
        logger.info("【欧易持仓缺失修复区】第3步：执行资金费融合")

        snapshot = self._snapshot_data
        cache = self.cache

        snapshot_total = snapshot.get(FIELD_FUNDING_TOTAL, 0) or 0
        cache_total = cache.get(FIELD_FUNDING_TOTAL, 0) or 0
        snapshot_count = snapshot.get(FIELD_FUNDING_COUNT, 0) or 0
        cache_count = cache.get(FIELD_FUNDING_COUNT, 0) or 0
        snapshot_time = snapshot.get(FIELD_FUNDING_TIME)

        # 1. 累计资金费直接覆盖
        cache[FIELD_FUNDING_TOTAL] = snapshot_total

        # 2. 本次结算时间直接覆盖
        if snapshot_time is not None:
            cache[FIELD_FUNDING_TIME] = snapshot_time

        # 3. 计算本次资金费
        cache[FIELD_FUNDING_THIS] = snapshot_total - cache_total

        # 4. 资金费结算次数相加
        cache[FIELD_FUNDING_COUNT] = snapshot_count + cache_count

        logger.info(f" 【欧易持仓缺失修复区】  融合后 - 累计资金费: {cache[FIELD_FUNDING_TOTAL]}, "
                   f"本次资金费: {cache[FIELD_FUNDING_THIS]}, "
                   f"结算次数: {cache[FIELD_FUNDING_COUNT]}")

    async def _step4_update_from_snapshot(self):
        """
        第4步：从门外存储区快照覆盖更新缓存
        ==================================================
        规则：
            1. 从门外存储区快照获取最新的欧意数据
            2. 把有值的字段（不为None/空）覆盖到缓存
            3. 保护4个资金费字段不被覆盖
        ==================================================
        """
        logger.debug("【欧易持仓缺失修复区】第4步：从门外存储区覆盖更新缓存")

        snapshot_data = self._get_okx_from_snapshot()
        if not snapshot_data:
            logger.warning("⚠️【欧易持仓缺失修复区】 门外存储区中没有欧意数据，跳过第4步")
            return

        protected_fields = [
            FIELD_FUNDING_THIS,
            FIELD_FUNDING_TOTAL,
            FIELD_FUNDING_COUNT,
            FIELD_FUNDING_TIME
        ]

        update_count = 0
        skip_count = 0

        for key, value in snapshot_data.items():
            if key in protected_fields:
                skip_count += 1
                continue

            if value is not None and value != '':
                self.cache[key] = value
                update_count += 1

        logger.debug(f" 【欧易持仓缺失修复区】  已覆盖 {update_count} 个字段，跳过 {skip_count} 个保护字段")

    async def _step5_calc_fixed_fields(self):
        """
        第5步：计算6个固定字段
        ==================================================
        【严格按照原始方案，独立计算，不复用任何中间结果】

        需要计算的字段：
            1. 标记价涨跌盈亏幅
            2. 标记价仓位价值
            3. 最新价涨跌盈亏幅
            4. 最新价保证金
            5. 最新价仓位价值
            6. 平均资金费率

        计算公式（严格按照你的文档，每个字段独立计算）：

        当开仓方向为 "LONG" 时：
            标记价涨跌盈亏幅 = (标记价 - 开仓价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (最新价 - 开仓价) * 100 / 开仓价
            标记价仓位价值 = 标记价 * 合约面值 * 持仓张数
            最新价仓位价值 = 最新价 * 合约面值 * 持仓张数
            最新价保证金 = 最新价 * 合约面值 * 持仓张数 ÷ 杠杆
            平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值

        当开仓方向为 "SHORT" 时：
            标记价涨跌盈亏幅 = (开仓价 - 标记价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (开仓价 - 最新价) * 100 / 开仓价
            标记价仓位价值 = 标记价 * 合约面值 * 持仓张数
            最新价仓位价值 = 最新价 * 合约面值 * 持仓张数
            最新价保证金 = 最新价 * 合约面值 * 持仓张数 ÷ 杠杆
            平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值

        注意：
            - 欧易数据本身已经包含标记价和最新价，直接从缓存中获取
            - 不需要从行情数据中获取
        ==================================================
        """
        logger.debug("【欧易持仓缺失修复区】第5步：计算6个固定字段（严格按照原始方案，独立计算）")

        cache = self.cache

        # 直接从缓存中获取所有需要的原始字段值
        mark_price = cache.get(FIELD_MARK_PRICE, 0) or 0
        latest_price = cache.get(FIELD_LATEST_PRICE, 0) or 0
        contract_value = cache.get(FIELD_CONTRACT_VALUE, 0) or 0
        contracts = cache.get(FIELD_POSITION_CONTRACTS, 0) or 0
        leverage = cache.get(FIELD_LEVERAGE, 1) or 1
        open_price = cache.get(FIELD_OPEN_PRICE, 0) or 0
        open_position_value = cache.get(FIELD_OPEN_POSITION_VALUE, 0) or 0
        direction = cache.get(FIELD_OPEN_DIRECTION)  # 值为 "LONG" 或 "SHORT"
        total_funding = cache.get(FIELD_FUNDING_TOTAL, 0) or 0

        # 根据开仓方向计算 - 每个字段严格按照原始公式独立计算
        if direction == "LONG":
            # 多头 - 严格按照原始公式，独立计算每个字段

            # 1. 标记价涨跌盈亏幅 = (标记价 - 开仓价) * 100 / 开仓价
            mark_pnl_percent = (mark_price - open_price) * 100 / open_price if open_price else 0
            mark_pnl_percent = round(mark_pnl_percent, 4)  # 四舍五入保留4位小数

            # 2. 最新价涨跌盈亏幅 = (最新价 - 开仓价) * 100 / 开仓价
            latest_pnl_percent = (latest_price - open_price) * 100 / open_price if open_price else 0
            latest_pnl_percent = round(latest_pnl_percent, 4)  # 四舍五入保留4位小数

            # 3. 标记价仓位价值 = 标记价 * 合约面值 * 持仓张数
            mark_position_value = mark_price * contract_value * contracts
            mark_position_value = round(mark_position_value, 4)  # 四舍五入保留4位小数

            # 4. 最新价仓位价值 = 最新价 * 合约面值 * 持仓张数
            latest_position_value = latest_price * contract_value * contracts
            latest_position_value = round(latest_position_value, 4)  # 四舍五入保留4位小数

            # 5. 最新价保证金 = 最新价 * 合约面值 * 持仓张数 ÷ 杠杆
            latest_margin = (latest_price * contract_value * contracts / leverage) if leverage else 0
            latest_margin = round(latest_margin, 4)  # 四舍五入保留4位小数

            # 6. 平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值
            avg_funding_rate = (total_funding * 100 / open_position_value) if open_position_value else 0
            avg_funding_rate = round(avg_funding_rate, 4)  # 四舍五入保留4位小数

        else:  # direction == "SHORT"
            # 空头 - 严格按照原始公式，独立计算每个字段

            # 1. 标记价涨跌盈亏幅 = (开仓价 - 标记价) * 100 / 开仓价
            mark_pnl_percent = (open_price - mark_price) * 100 / open_price if open_price else 0
            mark_pnl_percent = round(mark_pnl_percent, 4)  # 四舍五入保留4位小数

            # 2. 最新价涨跌盈亏幅 = (开仓价 - 最新价) * 100 / 开仓价
            latest_pnl_percent = (open_price - latest_price) * 100 / open_price if open_price else 0
            latest_pnl_percent = round(latest_pnl_percent, 4)  # 四舍五入保留4位小数

            # 3. 标记价仓位价值 = 标记价 * 合约面值 * 持仓张数
            mark_position_value = mark_price * contract_value * contracts
            mark_position_value = round(mark_position_value, 4)  # 四舍五入保留4位小数

            # 4. 最新价仓位价值 = 最新价 * 合约面值 * 持仓张数
            latest_position_value = latest_price * contract_value * contracts
            latest_position_value = round(latest_position_value, 4)  # 四舍五入保留4位小数

            # 5. 最新价保证金 = 最新价 * 合约面值 * 持仓张数 ÷ 杠杆
            latest_margin = (latest_price * contract_value * contracts / leverage) if leverage else 0
            latest_margin = round(latest_margin, 4)  # 四舍五入保留4位小数

            # 6. 平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值
            avg_funding_rate = (total_funding * 100 / open_position_value) if open_position_value else 0
            avg_funding_rate = round(avg_funding_rate, 4)  # 四舍五入保留4位小数

        # 保存计算结果到缓存 - 每个字段只赋值一次
        cache[FIELD_MARK_PNL_PERCENT] = mark_pnl_percent              # 1. 标记价涨跌盈亏幅
        cache[FIELD_LATEST_PNL_PERCENT] = latest_pnl_percent          # 2. 最新价涨跌盈亏幅
        cache[FIELD_MARK_POSITION_VALUE] = mark_position_value        # 3. 标记价仓位价值
        cache[FIELD_LATEST_POSITION_VALUE] = latest_position_value    # 4. 最新价仓位价值
        cache[FIELD_LATEST_MARGIN] = latest_margin                     # 5. 最新价保证金
        cache[FIELD_AVG_FUNDING_RATE] = avg_funding_rate               # 6. 平均资金费率

        logger.debug(f"  【欧易持仓缺失修复区】 计算完成 - 标记价: {mark_price}, 最新价: {latest_price}, "
                   f"标记价仓位价值: {mark_position_value:.2f}, "
                   f"最新价仓位价值: {latest_position_value:.2f}, "
                   f"最新价保证金: {latest_margin:.2f}, "
                   f"最新价涨跌盈亏幅: {latest_pnl_percent:.2f}%, "
                   f"平均资金费率: {avg_funding_rate:.4f}%, "
                   f"开仓方向: {direction}")

    async def _step6_push_complete(self):
        """
        第6步：打标签推送
        ==================================================
        做了两件事：
            1. 创建缓存数据的副本
            2. 打上"持仓完整"标签
            3. 推送给调度器
        ==================================================
        """
        logger.debug("【欧易持仓缺失修复区】第6步：打标签推送")

        data_copy = self.cache.copy()

        await self.scheduler.handle({
            'tag': TAG_COMPLETE,
            'data': data_copy
        })

        exchange = data_copy.get(FIELD_EXCHANGE, 'unknown')
        contract = data_copy.get(FIELD_OPEN_CONTRACT, 'unknown')
        logger.info(f"✅ 已推送持仓完整数据: {exchange} - {contract}")

    # ==================== 辅助方法 ====================

    def _get_okx_from_snapshot(self) -> Optional[Dict]:
        """
        从门外存储区快照中获取最新的欧意数据
        ==================================================
        存储区快照格式：
            {
                'user_data': {
                    'okx_user': {
                        'exchange': 'okx',
                        'data': {...}  # 这是真正的业务数据
                    }
                }
            }
        ==================================================
        """
        if not self.latest_snapshot:
            logger.debug("【欧易持仓缺失修复区】门外还没有存储区数据")
            return None

        user_data = self.latest_snapshot.get('user_data', {})
        okx_key = f"{EXCHANGE_OKX}_user"
        okx_item = user_data.get(okx_key, {})

        # 返回实际的业务数据
        return okx_item.get('data')

    def _query_database(self, sql: str, db_url: str, db_token: str, params: List = None) -> Optional[Dict]:
        """
        查询Turso数据库
        ==================================================
        【修改说明】
        接收数据库连接信息作为参数，而不是使用实例变量。
        这样符合"按需连接、用完即弃"的原则。

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

        try:
            response = requests.post(
                f"{db_url}/v2/pipeline",
                headers={
                    "Authorization": f"Bearer {db_token}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            result = response.json()

            # ✅ 正确的解析路径：results[0].response.result
            if result and 'results' in result and len(result['results']) > 0:
                first_result = result['results'][0]
                if 'response' in first_result and 'result' in first_result['response']:
                    return first_result['response']['result']  # 返回 {cols, rows}
            
            return None

        except requests.exceptions.Timeout:
            logger.error(f"❌ 【欧易持仓缺失修复区】数据库查询超时: {sql[:50]}...")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ 【欧易持仓缺失修复区】数据库请求失败: {e}")
            return None
        except Exception as e:
            logger.error(f"❌【欧易持仓缺失修复区】 数据库查询未知错误: {e}")
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

        logger.info(f" 【欧易持仓缺失修复区】  已更新 {update_count} 个资金费字段")