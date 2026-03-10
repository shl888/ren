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
第5步：计算6个固定字段
第6步：打标签推送
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
    FIELD_OPEN_MARGIN,  # 添加这个，用于计算平均资金费率
    FIELD_MARK_PRICE,
    FIELD_MARK_POSITION_VALUE,
    FIELD_LATEST_PRICE,
    FIELD_LATEST_POSITION_VALUE,
    FIELD_LATEST_MARGIN,
    FIELD_MARK_PNL_PERCENT,
    FIELD_LATEST_PNL_PERCENT,
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
        
        # ===== 数据库连接信息（从环境变量读取）=====
        # 修复区需要直接读取数据库，不经过database.py
        self.db_url = os.getenv('TURSO_DATABASE_URL')
        self.db_token = os.getenv('TURSO_DATABASE_TOKEN')
        
        if not self.db_url or not self.db_token:
            logger.error("❌ 环境变量TURSO_DATABASE_URL和TURSO_DATABASE_TOKEN未设置")
            # 不抛出异常，让程序继续运行但记录错误
        
        # ===== 测试数据库连接 =====
        if self.db_url and self.db_token:
            self._test_database_connection()
        
        logger.info("✅ 欧意持仓缺失修复区初始化完成")
    
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
        
        logger.info(f"📨 门外标签更新: {old_info} → {info}")
        
        # ===== 根据标签决定开关 =====
        if info == INFO_OKX_MISSING:
            await self._start_repair()
        elif info == INFO_OKX_CLOSED:
            await self._stop_repair()
        else:
            logger.warning(f"⚠️ 收到未知信息标签: {info}")
    
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
        logger.debug(f"📦 收到存储区快照，时间戳: {snapshot.get('timestamp')}")
    
    # ==================== 修复循环控制 ====================
    
    async def _start_repair(self):
        """启动修复流程（循环运行）"""
        if self.is_running:
            logger.debug("修复流程已在运行中")
            return
        
        self.is_running = True
        self.repair_task = asyncio.create_task(self._repair_loop())
        logger.info("🚀 欧意持仓缺失修复流程已启动（循环运行）")
    
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
        logger.info("🛑 欧意持仓缺失修复流程已停止")
    
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
        logger.info("🔄 欧意持仓缺失修复循环开始")
        
        while self.is_running:
            try:
                if self.current_info != INFO_OKX_MISSING:
                    logger.info("门外标签已不是持仓缺失，停止修复循环")
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
        
        logger.info("🔄 欧意持仓缺失修复循环结束")
    
    async def _repair_once(self):
        """
        执行一次修复流程
        ==================================================
        完全按照你的6步设计文档：
            第1步：获取缓存数据
            第2步：检测资金费状态
            第3步：资金费融合（仅情况D需要）
            第4步：覆盖更新（从门外存储区读最新数据，保护资金费4字段）
            第5步：计算6个固定字段
            第6步：打标签推送
        ==================================================
        """
        logger.debug("执行一次欧意持仓缺失修复")
        
        if not await self._step1_get_cache():
            logger.error("❌ 第1步失败：无法获取缓存数据，本次修复终止")
            return
        
        # 检查门外是否有存储区数据
        if not self.latest_snapshot:
            logger.warning("⚠️ 门外还没有存储区数据，等待下次循环")
            return
        
        funding_action = await self._step2_check_funding()
        if funding_action is None:
            logger.error("❌ 第2步失败：检测资金费状态出错，本次修复终止")
            return
        
        if funding_action == 'do_fusion':
            await self._step3_funding_fusion()
        
        await self._step4_update_from_snapshot()
        await self._step5_calc_fixed_fields()
        await self._step6_push_complete()
        
        logger.debug("一次欧意持仓缺失修复执行完成")
    
    # ==================== 6步修复流程 ====================
    
    async def _step1_get_cache(self) -> bool:
        """
        第1步：获取缓存数据
        ==================================================
        规则：
            - 如果已经有缓存，直接用
            - 如果没有缓存，从数据库持仓区读取1条欧意数据
        
        数据库查询：
            SELECT * FROM active_positions WHERE 交易所 = 'okx'
        
        注意：
            数据库表字段全是中文，SQL必须用中文字段名
        ==================================================
        """
        if self.cache is not None:
            logger.debug("✅ 第1步：使用现有缓存")
            return True
        
        logger.info("第1步：缓存为空，从数据库读取欧意持仓数据")
        
        if not self.db_url or not self.db_token:
            logger.error("❌ 数据库连接信息不完整，无法读取")
            return False
        
        try:
            sql = "SELECT * FROM active_positions WHERE 交易所 = 'okx' LIMIT 1"
            result = self._query_database(sql)
            
            if not result:
                logger.warning("⚠️ 数据库查询失败或无数据")
                return False
            
            rows = result.get('rows', [])
            if not rows:
                logger.warning("⚠️ 数据库中没有欧意持仓数据")
                return False
            
            cols = result.get('cols', [])
            row = rows[0]
            self.cache = self._row_to_dict(row, cols)
            
            logger.info(f"✅ 第1步：从数据库读取到欧意数据，开仓合约名: {self.cache.get(FIELD_OPEN_CONTRACT)}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 第1步：读取数据库失败: {e}")
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
        logger.info("第2步：检测资金费状态")
        
        # 从门外存储区快照获取最新的欧意数据
        snapshot_data = self._get_okx_from_snapshot()
        if not snapshot_data:
            logger.error("❌ 门外存储区中没有欧意数据")
            return None
        
        cache_total = self.cache.get(FIELD_FUNDING_TOTAL, 0)
        if cache_total is None:
            cache_total = 0
        has_history = (cache_total != 0)
        
        snapshot_total = snapshot_data.get(FIELD_FUNDING_TOTAL, 0)
        if snapshot_total is None:
            snapshot_total = 0
        has_new = (snapshot_total != cache_total)
        
        logger.info(f"   缓存累计资金费: {cache_total}, 门外存储区累计资金费: {snapshot_total}")
        logger.info(f"   有无历史: {has_history}, 有无新结算: {has_new}")
        
        # 保存门外数据供后续步骤使用
        self._snapshot_data = snapshot_data
        
        if not has_history and not has_new:
            logger.info("   情况A：无历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'
        elif not has_history and has_new:
            logger.info("   情况B：无历史 + 有新结算，更新4个资金费字段后跳到第4步")
            self._update_funding_fields(snapshot_data)
            return 'skip_to_step4'
        elif has_history and not has_new:
            logger.info("   情况C：有历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'
        else:
            logger.info("   情况D：有历史 + 有新结算，进入第3步资金费融合")
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
        logger.info("第3步：执行资金费融合")
        
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
        
        logger.info(f"   融合后 - 累计资金费: {cache[FIELD_FUNDING_TOTAL]}, "
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
        logger.info("第4步：从门外存储区覆盖更新缓存")
        
        snapshot_data = self._get_okx_from_snapshot()
        if not snapshot_data:
            logger.warning("⚠️ 门外存储区中没有欧意数据，跳过第4步")
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
        
        logger.info(f"   已覆盖 {update_count} 个字段，跳过 {skip_count} 个保护字段")
    
    async def _step5_calc_fixed_fields(self):
        """
        第5步：计算6个固定字段
        ==================================================
        需要计算的字段：
            1. 标记价涨跌盈亏幅
            2. 标记价仓位价值
            3. 最新价涨跌盈亏幅
            4. 最新价保证金
            5. 最新价仓位价值
            6. 平均资金费率
        
        计算公式：
            标记价仓位价值 = 标记价 * 合约面值 * 持仓张数
            最新价仓位价值 = 最新价 * 合约面值 * 持仓张数
            最新价保证金 = 最新价 * 合约面值 * 持仓张数 ÷ 杠杆
            平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值
            
            当开仓方向为 "LONG" 时：
                标记价涨跌盈亏幅 = (标记价 - 开仓价) * 100 / 开仓价
                最新价涨跌盈亏幅 = (最新价 - 开仓价) * 100 / 开仓价
            
            当开仓方向为 "SHORT" 时：
                标记价涨跌盈亏幅 = (开仓价 - 标记价) * 100 / 开仓价
                最新价涨跌盈亏幅 = (开仓价 - 最新价) * 100 / 开仓价
        
        注意：
            - 欧易数据本身已经包含标记价和最新价，直接从缓存中获取
            - 不需要从行情数据中获取
        ==================================================
        """
        logger.info("第5步：计算6个固定字段")
        
        cache = self.cache
        
        # 直接从缓存中获取所有需要的字段
        mark_price = cache.get(FIELD_MARK_PRICE, 0) or 0
        latest_price = cache.get(FIELD_LATEST_PRICE, 0) or 0
        contract_value = cache.get(FIELD_CONTRACT_VALUE, 0) or 0
        contracts = cache.get(FIELD_POSITION_CONTRACTS, 0) or 0
        leverage = cache.get(FIELD_LEVERAGE, 1) or 1
        open_price = cache.get(FIELD_OPEN_PRICE, 0) or 0
        open_position_value = cache.get(FIELD_OPEN_POSITION_VALUE, 1) or 1
        direction = cache.get(FIELD_OPEN_DIRECTION)  # 值为 "LONG" 或 "SHORT"
        total_funding = cache.get(FIELD_FUNDING_TOTAL, 0) or 0
        
        # 1. 标记价仓位价值
        mark_position_value = mark_price * contract_value * contracts
        cache[FIELD_MARK_POSITION_VALUE] = mark_position_value
        
        # 2. 最新价仓位价值
        latest_position_value = latest_price * contract_value * contracts
        cache[FIELD_LATEST_POSITION_VALUE] = latest_position_value
        
        # 3. 最新价保证金
        if leverage > 0:
            latest_margin = latest_price * contract_value * contracts / leverage
        else:
            latest_margin = 0
        cache[FIELD_LATEST_MARGIN] = latest_margin
        
        # 4. 平均资金费率
        if open_position_value > 0:
            avg_funding_rate = total_funding * 100 / open_position_value
        else:
            avg_funding_rate = 0
        cache[FIELD_AVG_FUNDING_RATE] = avg_funding_rate
        
        # 5. 根据开仓方向计算涨跌盈亏幅
        if direction == "LONG":
            # 多头
            mark_pnl_percent = (mark_price - open_price) * 100 / open_price
            latest_pnl_percent = (latest_price - open_price) * 100 / open_price
        else:  # direction == "SHORT"
            # 空头
            mark_pnl_percent = (open_price - mark_price) * 100 / open_price
            latest_pnl_percent = (open_price - latest_price) * 100 / open_price
        
        cache[FIELD_MARK_PNL_PERCENT] = mark_pnl_percent
        cache[FIELD_LATEST_PNL_PERCENT] = latest_pnl_percent
        
        logger.info(f"   计算结果 - 标记价: {mark_price}, 最新价: {latest_price}, "
                   f"标记价仓位价值: {mark_position_value:.2f}, "
                   f"最新价涨跌盈亏幅: {latest_pnl_percent:.2f}%, "
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
        logger.info("第6步：打标签推送")
        
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
            logger.debug("门外还没有存储区数据")
            return None
        
        user_data = self.latest_snapshot.get('user_data', {})
        okx_key = f"{EXCHANGE_OKX}_user"
        okx_item = user_data.get(okx_key, {})
        
        # 返回实际的业务数据
        return okx_item.get('data')
    
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
        
        # 转换参数格式
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