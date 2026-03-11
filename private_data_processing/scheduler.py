"""
调度器 - 私人数据处理模块的核心调度中心
==================================================
【文件职责】
1. 启动和管理步骤1-2-3-4的流水线
2. 接收Step1的输出，驱动Step2-3-4
3. 将最终处理后的数据推送到数据完成部门

【数据流向】
Manager.feed_step1() 
    ↓
Step1 (提取) → 队列
    ↓
流水线工作线程:
    Step2 (融合更新) 
        ↓
    Step3 (计算衍生字段)
        ↓
    Step4 (资金费处理)
        ↓
    数据完成部门 (receiver.py)

【重要规则 - 数据格式转换】
推送前必须将所有数字字段转换为Python原生数字类型：
    - 字符串 "69789.4" → float 69789.4
    - 字符串 "20" → int 20
    - 字符串 "0.0817" → float 0.0817

这样数据库才能正确接收（表定义中这些字段是 REAL/INTEGER）
==================================================
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class PrivateDataScheduler:
    """
    私人数据调度器 - 真正的调度中心
    ==================================================
    负责：
        1. 创建并管理Step1-4的实例
        2. 从Step1输出队列获取数据
        3. 驱动Step2-3-4的流水线处理
        4. 将最终数据推送到数据完成部门
        5. 在推送前统一转换数据类型
    ==================================================
    """

    def __init__(self):
        """初始化调度器（不启动流水线）"""
        self.step1 = None
        self.step2 = None
        self.step3 = None
        self.step4 = None
        self.running = False
        
        # Step1输出队列：Step1提取后推到这里，调度器从这里取
        self.step1_output_queue = asyncio.Queue()
        
        # 添加就绪事件，用于等待调度器完全启动
        self._ready = asyncio.Event()
        
        logger.info("✅【调度器】实例已创建")

    async def start(self):
        """
        启动调度器 - 启动所有步骤和工作流
        ==================================================
        做了四件事：
            1. 导入所有Step模块
            2. 创建Step1-4的实例
            3. 标记运行状态
            4. 启动流水线工作线程
        ==================================================
        """
        if self.running:
            logger.warning("⚠️【调度器】已经启动，跳过")
            self._ready.set()
            return
            
        # 导入所有步骤
        from .pipeline.step1_extract import Step1Extract
        from .pipeline.step2_fusion import Step2Fusion
        from .pipeline.step3_calc import Step3Calc
        from .pipeline.step4_funding import Step4Funding
        
        # 创建所有步骤实例
        self.step1 = Step1Extract(self.step1_output_queue)
        self.step2 = Step2Fusion()
        self.step3 = Step3Calc()
        self.step4 = Step4Funding()
        
        self.running = True
        logger.info("🚀【调度器】已启动 - step1, step2, step3, step4 已就绪")
        
        # 标记就绪
        self._ready.set()
        
        # 启动流水线工作线程
        asyncio.create_task(self._pipeline_worker())

    async def stop(self):
        """停止调度器"""
        self.running = False
        logger.info("🛑【调度器】已停止")

    def feed_step1(self, stored_item: Dict[str, Any]):
        """
        Manager直接调用这个，往Step1嘴里塞数据
        ==================================================
        这是调度器的输入入口，Manager收到原始数据后调用此方法。
        
        :param stored_item: 原始数据，格式：
            {
                'exchange': 'binance',
                'data_type': 'account_update',
                'data': {...},  # 真正的业务数据
                'timestamp': '...'
            }
        ==================================================
        """
        logger.info(f"🎯【调度器】feed_step1被调用！数据类型: {stored_item.get('data_type')}, 交易所: {stored_item.get('exchange')}")
        
        if self.step1:
            try:
                # 创建异步任务交给Step1处理，不阻塞当前调用
                asyncio.create_task(self.step1.receive(stored_item))
                logger.info(f"📥【调度器】已创建任务塞给Step1，队列当前大小: {self.step1_output_queue.qsize()}")
            except Exception as e:
                logger.error(f"❌【调度器】创建任务失败: {e}")
        else:
            logger.error(f"❌【调度器】Step1未初始化！无法处理数据: {stored_item.get('data_type')}")

    async def _pipeline_worker(self):
        """
        流水线工作线程
        ==================================================
        这是调度器的核心处理循环：
            1. 从Step1输出队列取数据
            2. Step2：融合更新（合并新旧数据）
            3. Step3：计算衍生字段（盈亏、保证金等）
            4. Step4：资金费处理
            5. 转换数据类型并推送到数据完成部门
        ==================================================
        """
        logger.info("🏭【流水线工作线程】已启动")
        
        while self.running:
            try:
                # 从Step1输出队列取数据（等待1秒超时）
                extracted = await asyncio.wait_for(self.step1_output_queue.get(), timeout=1.0)
                
                # 获取事件类型（兼容币安和欧易）
                event_type = extracted.get('event_type')
                if not event_type:
                    event_type = extracted.get('data_type', 'unknown')
                
                logger.info(f"✅【调度器】从队列取到数据！类型: {event_type}, 交易所: {extracted.get('交易所')}")
                
                # ===== 步骤2：融合更新 =====
                container = self.step2.process(extracted)
                if not container:
                    logger.warning(f"⚠️【调度器】Step2返回空，跳过")
                    self.step1_output_queue.task_done()
                    continue
                logger.info(f"✅【调度器】Step2完成")

                # ===== 步骤3：计算衍生字段 =====
                self.step3.process(container)
                logger.info(f"✅【调度器】Step3完成")

                # ===== 步骤4：资金费处理 =====
                final_container = self.step4.process(container)
                logger.info(f"✅【调度器】Step4完成")

                # ===== 推送数据完成部门 =====
                await self._push_to_data_completion(final_container, event_type)
                
                self.step1_output_queue.task_done()
                
            except asyncio.TimeoutError:
                # 超时正常，继续循环
                continue
            except Exception as e:
                logger.error(f"❌【流水线工作线程】错误: {e}")
                import traceback
                logger.error(traceback.format_exc())

    async def _push_to_data_completion(self, container: Dict[str, Any], event_type: str):
        """
        推送数据到数据完成部门的接收器
        ==================================================
        做了三件事：
            1. 将容器中的所有数字字段转换为Python原生数字类型
            2. 组装符合receiver要求的数据格式
            3. 调用 receive_private_data 推送
        
        为什么需要转换数字字段？
            数据库表定义中，这些字段是 REAL 或 INTEGER，
            如果传字符串会导致 Turso HTTP API 400错误。
        
        转换规则：
            - 字符串 "69789.4" → float 69789.4
            - 字符串 "20" → int 20
            - 字符串 "0.0817" → float 0.0817
            - None/null → 保持 None
            - 已经是数字的保持原样
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
            
            # ===== 关键步骤：转换数字字段类型 =====
            converted_container = self._convert_numeric_fields(container)
            
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
            
            logger.info(f"✅【调度器】已推送 {exchange} 数据到数据完成部门")
            
        except Exception as e:
            logger.error(f"❌【调度器】推送数据到数据完成部门失败: {e}")

    def _convert_numeric_fields(self, data: dict) -> dict:
        """
        将数字字段统一转换为 Python 数字类型
        ==================================================
        这是解决数据库400错误的关键方法！
        
        转换规则：
            1. 字符串数字 → float 或 int
            2. 已经是数字的 → 保持原样
            3. None → 保持 None
            4. 其他类型 → 保持原样并警告
        
        字段分类：
            - 数字字段：价格、数量、价值、保证金、盈亏等
            - 字符串字段：交易所、币种、模式、合约名等
            - 时间字段：保持字符串格式
        
        :param data: 原始数据字典
        :return: 转换后的数据字典
        ==================================================
        """
        # 创建副本，避免修改原始数据
        converted = data.copy()
        
        # ===== 定义字段分类 =====
        # 1. 必须转换为数字的字段（对应数据库 REAL/INTEGER）
        numeric_fields = [
            # 价格和数量类
            '开仓价', '标记价', '最新价', '平仓价',
            '持仓币数', '持仓张数', '合约面值',
            
            # 价值类
            '开仓价仓位价值', '标记价仓位价值', '最新价仓位价值', '平仓价仓位价值',
            
            # 保证金类
            '开仓保证金', '标记价保证金', '最新价保证金',
            
            # 盈亏类
            '标记价涨跌盈亏幅', '最新价涨跌盈亏幅', '平仓价涨跌盈亏幅',
            '标记价浮盈', '最新价浮盈', '平仓收益',
            '标记价浮盈百分比', '最新价浮盈百分比', '平仓收益率',
            
            # 杠杆和费率
            '杠杆', '本次资金费', '累计资金费', '平均资金费率',
            '资金费结算次数',  # INTEGER
            
            # 止损止盈
            '止损触发价', '止损幅度', '止盈触发价', '止盈幅度',
            
            # 手续费
            '开仓手续费', '平仓手续费',
            
            # 账户资产
            '账户资产额',
        ]
        
        # 2. 保持为字符串的字段
        string_fields = [
            '交易所', '资产币种', '保证金模式', '保证金币种',
            '开仓合约名', '开仓方向', '开仓执行方式', '开仓手续费币种',
            '止损触发方式', '止盈触发方式', '平仓执行方式', '平仓手续费币种',
        ]
        
        # 3. 时间字段（保持字符串格式）
        time_fields = [
            '开仓时间', '本次资金费结算时间', '平仓时间'
        ]
        
        # ===== 转换数字字段 =====
        for field in numeric_fields:
            if field in converted and converted[field] is not None:
                value = converted[field]
                try:
                    # 如果是字符串，尝试转成数字
                    if isinstance(value, str):
                        # 去除可能的逗号、空格
                        cleaned = value.strip().replace(',', '')
                        
                        # 判断是否包含小数点
                        if '.' in cleaned:
                            converted[field] = float(cleaned)
                        else:
                            # 尝试转int，如果失败就转float
                            try:
                                converted[field] = int(cleaned)
                            except ValueError:
                                converted[field] = float(cleaned)
                    
                    # 如果已经是数字，确保是float（数据库REAL类型）
                    elif isinstance(value, (int, float)):
                        # 对于资金费结算次数这样的整数字段，保持int
                        if field == '资金费结算次数':
                            converted[field] = int(value)
                        else:
                            converted[field] = float(value)
                            
                except (ValueError, TypeError) as e:
                    logger.warning(f"⚠️ 字段 {field} 转换失败: {value} (错误: {e})，保持原样")
                    # 转换失败就保持原样，但记录警告
        
        # ===== 确保字符串字段确实是字符串 =====
        for field in string_fields:
            if field in converted and converted[field] is not None:
                if not isinstance(converted[field], str):
                    converted[field] = str(converted[field])
        
        # ===== 记录转换统计 =====
        numeric_count = sum(1 for f in numeric_fields if f in converted and converted[f] is not None)
        logger.debug(f"📊 已转换 {numeric_count} 个数字字段")
        
        return converted

    async def wait_until_ready(self):
        """等待调度器完全就绪"""
        await self._ready.wait()


# ========== 单例模式 ==========
_scheduler_instance: Optional[PrivateDataScheduler] = None


def get_scheduler() -> PrivateDataScheduler:
    """
    获取调度器单例
    ==================================================
    确保整个模块只使用一个调度器实例。
    其他组件通过此函数获取调度器，而不是直接创建。
    ==================================================
    """
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = PrivateDataScheduler()
        logger.info("🔥【调度器】单例已创建")
    
    return _scheduler_instance