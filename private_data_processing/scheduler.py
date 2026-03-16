"""
私人调度器 - 私人数据处理模块的核心调度中心
==================================================
【文件职责】
1. 启动和管理步骤1-2-3-4的流水线
2. 接收Manager的完整存储区，直接驱动Step1-2-3-4
3. 将最终处理后的数据推送到数据完成部门

【重要变更 - 2026.03.14】
==================================================
🔴 修改点：去掉 await gather，改为纯转发模式
- 收到step1的结果后，只创建任务转发
- 不等待任何任务完成，立即返回
- 实现收一条转一条的纯转发器功能
==================================================
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class PrivateDataScheduler:
    """
    私人数据私人调度器 - 真正的调度中心
    ==================================================
    负责：
        1. 创建并管理Step1-4的实例
        2. 直接从Manager接收完整存储区
        3. 驱动Step1-2-3-4的流水线处理
        4. 将最终数据推送到数据完成部门
    ==================================================
    """

    def __init__(self):
        """初始化私人调度器（不启动流水线）"""
        self.step1 = None
        self.step2 = None
        self.step3 = None
        self.step4 = None
        self.running = False
        
        # 添加就绪事件，用于等待私人调度器完全启动
        self._ready = asyncio.Event()
        
        # 统计信息（用于监控）
        self.stats = {
            "total_processed": 0,
            "total_results": 0,
            "last_process_time": None
        }
        
        logger.info("✅【私人调度器】实例已创建")

    async def start(self):
        """
        启动私人调度器 - 启动所有步骤和工作流
        ==================================================
        做了三件事：
            1. 导入所有Step模块
            2. 创建Step1-4的实例
            3. 标记运行状态
        ==================================================
        """
        if self.running:
            logger.warning("⚠️【私人调度器】已经启动，跳过")
            self._ready.set()
            return
            
        # 导入所有步骤
        from .pipeline.step1_extract import Step1Extract
        from .pipeline.step2_fusion import Step2Fusion
        from .pipeline.step3_calc import Step3Calc
        from .pipeline.step4_funding import Step4Funding
        
        # 创建所有步骤实例
        self.step1 = Step1Extract()
        self.step2 = Step2Fusion()
        self.step3 = Step3Calc()
        self.step4 = Step4Funding()
        
        self.running = True
        logger.info("🚀【私人调度器】已启动 - step1, step2, step3, step4 已就绪")
        
        # 标记就绪
        self._ready.set()

    async def stop(self):
        """停止私人调度器"""
        self.running = False
        logger.error("🛑【私人调度器】已停止")

    async def feed_step1(self, stored_item: Dict[str, Any]):
        """
        Manager直接调用这个，把完整存储区塞给Step1
        ==================================================
        🔴【修改点】改为纯转发模式：收到就转，绝不等待！
        
        原来：await asyncio.gather(*process_tasks) 会等待所有任务完成
        现在：只创建任务，不等待，立即返回
        
        :param stored_item: 完整存储区数据，格式：
            {
                'full_storage': {
                    'binance_order_update': {...},
                    'okx_order_update': {...},
                    ...
                }
            }
        ==================================================
        """
        logger.info(f"🎯【私人调度器】feed_step1被调用！存储区包含数据项: {len(stored_item.get('full_storage', {}))}")
        
        if not self.running:
            logger.error("❌【私人调度器】未启动，无法处理数据")
            return
            
        if not self.step1:
            logger.error("❌【私人调度器】Step1未初始化")
            return
            
        try:
            # ===== 步骤1：提取字段（直接await，等待处理完成）=====
            # Step1会并行处理所有key，返回提取结果列表
            results = await self.step1.receive(stored_item)
            
            if not results:
                logger.debug("⏭️【私人调度器】step1没有返回任何结果")
                return
                
            logger.debug(f"📊【私人调度器】step1返回 {len(results)} 条提取结果")
            
            # ===== 🔴 关键修改：只转发，不等待！ =====
            # 对每条提取结果，创建独立任务执行后续步骤
            for result in results:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU，避免大量结果阻塞事件循环
                asyncio.create_task(self._process_single_result(result))
            
            # 立即返回，不等待任何任务完成！
            logger.debug(f"✅【私人调度器】已转发 {len(results)} 条结果到后台处理")
            
            # 更新统计
            self.stats["total_processed"] += 1
            self.stats["total_results"] += len(results)
            self.stats["last_process_time"] = datetime.now().isoformat()
            
        except Exception as e:
            logger.error(f"❌【私人调度器】处理失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

    async def _process_single_result(self, extracted: Dict[str, Any]):
        """
        处理单条提取结果：Step2 → Step3 → Step4 → 推送
        ==================================================
        为每条结果独立执行完整的流水线处理。
        所有步骤都在线程池中执行，避免阻塞事件循环。
        
        :param extracted: Step1提取的单条结果
        ==================================================
        """
        try:
            exchange = extracted.get('交易所', 'unknown')
            event_type = extracted.get('event_type', extracted.get('data_type', 'unknown'))
            
            logger.debug(f"🔨【私人调度器】开始处理单条结果: {exchange} {event_type}")
            
            # 获取事件循环
            loop = asyncio.get_event_loop()
            
            # ===== 步骤2：融合更新（在线程池执行，避免阻塞）=====
            container = await loop.run_in_executor(
                None, self.step2.process, extracted
            )
            
            if not container:
                logger.warning(f"⚠️【私人调度器】Step2返回空，跳过本条结果")
                return
                
            logger.info(f"✅【私人调度器】Step2完成: {exchange}")
            
            # ===== 步骤3：计算衍生字段 =====
            await loop.run_in_executor(
                None, self.step3.process, container
            )
            logger.debug(f"✅【私人调度器】Step3完成: {exchange}")
            
            # ===== 步骤4：资金费处理 =====
            final_container = await loop.run_in_executor(
                None, self.step4.process, container
            )
            logger.info(f"✅【私人调度器】Step4完成: {exchange}")
            
            # ===== 推送数据到完成部门 =====
            await self._push_to_data_completion(final_container, event_type)
            
            logger.debug(f"✅【私人调度器】单条结果处理完成: {exchange} {event_type}")
            
        except Exception as e:
            logger.error(f"❌【私人调度器】处理单条结果失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

    async def _push_to_data_completion(self, container: Dict[str, Any], event_type: str):
        """
        推送数据到数据完成部门的接收器
        ==================================================
        做了三件事：
            1. 把数字字段转换为 Python 数字类型（供大脑计算）
            2. 组装符合receiver要求的数据格式
            3. 调用 receive_private_data 推送
        ==================================================
        
        :param container: Step4处理后的完整数据容器
        :param event_type: 事件类型（用于日志）
        """
        try:
            # 判断数据类型（成品或半成品）
            exchange = container.get("交易所", "unknown")
            
            if exchange == "okx":
                data_type = "okx_complete"      # 欧意是成品
            elif exchange == "binance":
                data_type = "binance_semi"      # 币安是半成品
            else:
                data_type = "unknown"
            
            # ===== 关键步骤：把数字字段转换为Python数字类型 =====
            # 供大脑模块计算使用
            # ✅ [蚂蚁基因修复] 将同步的转换方法放到线程池执行，避免阻塞事件循环
            loop = asyncio.get_event_loop()
            converted_container = await loop.run_in_executor(
                None, self._convert_numeric_fields, container
            )
            
            # 组装数据包
            completion_data = {
                "exchange": exchange,
                "data_type": data_type,
                "event_type": event_type,
                "data": converted_container,      # 使用转换后的数据
                "timestamp": datetime.now().isoformat()
            }
            
            # 推送到数据完成部门
            from data_completion_department import receive_private_data
            await receive_private_data(completion_data)
            
            logger.info(f"✅【私人调度器】已推送 {exchange} 数据到数据完成部门")
            
        except Exception as e:
            logger.error(f"❌【私人调度器】推送数据到数据完成部门失败: {e}")

    def _convert_numeric_fields(self, data: dict) -> dict:
        """
        把数字字段转换为 Python 数字类型（不带双引号）
        ==================================================
        【目的】
        大脑模块需要计算盈亏、保证金等，必须使用数字类型。
        如果字段值是字符串 "69789.4"，计算时会报错。
        
        【转换规则】
        1. 数字字段（数据库中是 REAL/INTEGER）→ 转换为 Python 数字类型
           - 字符串 "69789.4" → 69789.4 (float)
           - 字符串 "10" → 10 (int)
           - 已经是数字的保持不变
        
        2. 文本字段 → 保持为字符串
           - 交易所、开仓方向、时间等保持不变
        
        3. None值 → 保持 None
        
        【数字字段列表 - 基于数据库表定义】
        - 价格类：开仓价、标记价、最新价、平仓价
        - 数量类：持仓币数、持仓张数、合约面值
        - 价值类：开仓价仓位价值、标记价仓位价值、最新价仓位价值、平仓价仓位价值
        - 保证金类：开仓保证金、标记价保证金、最新价保证金
        - 盈亏类：标记价涨跌盈亏幅、最新价涨跌盈亏幅、平仓价涨跌盈亏幅
        - 盈亏金额类：标记价浮盈、最新价浮盈、平仓收益
        - 盈亏百分比类：标记价浮盈百分比、最新价浮盈百分比、平仓收益率
        - 杠杆费率类：杠杆、本次资金费、累计资金费、平均资金费率
        - 整数类：资金费结算次数
        - 止损止盈类：止损触发价、止损幅度、止盈触发价、止盈幅度
        - 手续费类：开仓手续费、平仓手续费
        - 资产类：账户资产额
        ==================================================
        
        :param data: 原始数据字典
        :return: 数字字段转换为Python数字类型后的数据字典
        """
        # 创建副本，避免修改原始数据
        converted = data.copy()
        
        # ----- 数字字段列表（必须转成 int/float）-----
        numeric_fields = {
            # 价格和数量类
            '开仓价', '标记价', '最新价', '平仓价',
            '持仓币数', '持仓张数', '合约面值',
            
            # 价值类
            '开仓价仓位价值', '标记价仓位价值', '最新价仓位价值', '平仓价仓位价值',
            
            # 保证金类
            '开仓保证金', '标记价保证金', '最新价保证金',
            
            # 盈亏幅类
            '标记价涨跌盈亏幅', '最新价涨跌盈亏幅', '平仓价涨跌盈亏幅',
            
            # 盈亏金额类
            '标记价浮盈', '最新价浮盈', '平仓收益',
            
            # 盈亏百分比类
            '标记价浮盈百分比', '最新价浮盈百分比', '平仓收益率',
            
            # 杠杆和费率
            '杠杆', '本次资金费', '累计资金费', '平均资金费率',
            
            # 整数类
            '资金费结算次数',
            
            # 止损止盈
            '止损触发价', '止损幅度', '止盈触发价', '止盈幅度',
            
            # 手续费
            '开仓手续费', '平仓手续费',
            
            # 账户资产
            '账户资产额',
        }
        
        # ----- 转换统计 -----
        convert_count = 0
        none_count = 0
        
        for key, value in converted.items():
            # 这个循环在同步方法中，但此方法现在在线程池中执行，不会阻塞事件循环
            if value is None:
                # None 保持 None
                none_count += 1
                logger.debug(f"【私人调度器】字段 [{key}]: None (保持null)")
                
            elif key in numeric_fields:
                # 数字字段：必须转成 int/float（不带双引号）
                try:
                    if isinstance(value, str):
                        # 字符串数字转成数字
                        cleaned = value.strip().replace(',', '')
                        if '.' in cleaned:
                            converted[key] = float(cleaned)
                            logger.debug(f"【私人调度器】字段 [{key}]: '{value}' → {converted[key]} (float)")
                        else:
                            # 先尝试转int，失败则转float
                            try:
                                converted[key] = int(cleaned)
                                logger.debug(f"【私人调度器】字段 [{key}]: '{value}' → {converted[key]} (int)")
                            except ValueError:
                                converted[key] = float(cleaned)
                                logger.debug(f"【私人调度器】字段 [{key}]: '{value}' → {converted[key]} (float)")
                        convert_count += 1
                    elif isinstance(value, (int, float)):
                        # 已经是数字，保持原样
                        logger.debug(f"【私人调度器】字段 [{key}]: {value} (已是{type(value).__name__})")
                        convert_count += 1
                    else:
                        # 意外类型，尝试转数字
                        converted[key] = float(value)
                        logger.warning(f"【私人调度器】字段 [{key}]: 意外类型 {type(value).__name__}，强制转float")
                        convert_count += 1
                except (ValueError, TypeError) as e:
                    logger.error(f"【私人调度器】字段 [{key}] 转换失败: {value}, 错误: {e}")
                    # 转换失败就保持原样，但记录错误
            else:
                # 文本字段：保持原样（字符串、时间等）
                logger.debug(f"【私人调度器】字段 [{key}]: {value} ({type(value).__name__}) - 文本字段保持原样")
        
        # 记录转换统计
        logger.debug(f"📊 【私人调度器】数字字段转换统计: {convert_count}个字段转成数字, {none_count}个null")
        
        # 记录关键字段示例
        sample_fields = ['交易所', '开仓合约名', '杠杆', '开仓价', '开仓时间']
        for field in sample_fields:
            # 这个循环很小（只有5个字段），不会造成阻塞，保持原样
            if field in converted:
                logger.debug(f" 【私人调度器】 📌 {field}: {converted[field]} ({type(converted[field]).__name__})")
        
        return converted

    async def wait_until_ready(self):
        """等待私人调度器完全就绪"""
        await self._ready.wait()
    
    def get_stats(self) -> Dict:
        """获取统计信息（调试用）"""
        return self.stats.copy()


# ========== 单例模式 ==========
_scheduler_instance: Optional[PrivateDataScheduler] = None


def get_scheduler() -> PrivateDataScheduler:
    """
    获取私人调度器单例
    ==================================================
    确保整个模块只使用一个私人调度器实例。
    其他组件通过此函数获取私人调度器，而不是直接创建。
    ==================================================
    """
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = PrivateDataScheduler()
        logger.info("🔥【私人调度器】单例已创建")
    
    return _scheduler_instance