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

【重要规则 - 数据格式转换 - 2024-03-11 更新】
Turso API 要求：
    1. 所有值都必须作为字符串传递（避免JSON精度丢失）
    2. 数据库文件会根据字段类型标记为 integer/float/text

因此推送前必须将所有字段值转换为字符串格式：
    - 整数 20 → "20"
    - 浮点数 69789.4 → "69789.4" 
    - 字符串 "binance" → "binance"（保持不变）
    - None → None（保持null）
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
        5. 在推送前统一转换数据格式为字符串（符合Turso API要求）
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
            1. 将所有字段值转换为字符串格式（符合Turso API要求）
            2. 组装符合receiver要求的数据格式
            3. 调用 receive_private_data 推送
        
        【重要 - Turso API 要求】
        Turso官方文档明确要求：
            "In JSON, the value is a String to avoid losing precision"
        所有值都必须作为字符串传递，即使是数字也要转成字符串。
        
        转换规则（2024-03-11 更新）：
            - 整数 20 → "20"
            - 浮点数 69789.4 → "69789.4"
            - 字符串 "binance" → "binance"（保持不变）
            - None → None（保持null）
            - 其他类型 → str() 转换
        
        为什么这样转换？
            database.py 中的 _run_sql 方法会根据字段类型
            （integer/float/text）正确标记参数，但值必须是字符串。
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
            
            # ===== 关键步骤：将所有字段值转换为字符串格式 =====
            # 符合Turso API要求：所有值都作为字符串传递
            converted_container = self._convert_all_to_strings(container)
            
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

    def _convert_all_to_strings(self, data: dict) -> dict:
        """
        将所有字段值统一转换为字符串格式
        ==================================================
        【Turso API 要求】
        官方文档明确指出：所有值在JSON中必须作为字符串传递，
        这是为了避免精度丢失（because some JSON implementations 
        treat all numbers as 64-bit floats）。
        
        转换规则：
            - 整数 20 → "20"
            - 浮点数 69789.4 → "69789.4"
            - 字符串 "binance" → "binance"（保持不变）
            - None → None（保持null，不转换）
            - 其他类型（bool、list等）→ str() 转换
        
        为什么不用之前的分字段转换？
            之前尝试区分 numeric_fields 和 string_fields，
            但Turso API要求所有值都是字符串，所以统一转换最简单可靠。
        
        为什么保留 None？
            None 对应数据库的 null，需要保持原样，
            在 database.py 中会标记为 "null" 类型。
        
        :param data: 原始数据字典
        :return: 所有值都转换为字符串的数据字典
        ==================================================
        """
        # 创建副本，避免修改原始数据
        converted = data.copy()
        
        # 转换计数器，用于日志
        convert_count = 0
        none_count = 0
        already_str_count = 0
        
        for key, value in converted.items():
            if value is None:
                # None 保持 None，不转换
                none_count += 1
                logger.debug(f"字段 [{key}]: None (保持null)")
                
            elif isinstance(value, str):
                # 已经是字符串，保持不变
                already_str_count += 1
                logger.debug(f"字段 [{key}]: '{value}' (已经是字符串)")
                
            else:
                # 其他所有类型都转成字符串
                original_type = type(value).__name__
                str_value = str(value)
                converted[key] = str_value
                convert_count += 1
                logger.debug(f"字段 [{key}]: {value} ({original_type}) → '{str_value}'")
        
        # 记录转换统计
        logger.info(f"📊 数据转换统计: {convert_count}个字段转字符串, {already_str_count}个已是字符串, {none_count}个null")
        
        # 记录几个关键字段的示例
        sample_fields = ['交易所', '开仓合约名', '杠杆', '开仓价', 'id']
        for field in sample_fields:
            if field in converted:
                logger.debug(f"  📌 {field}: {converted[field]} ({type(converted[field]).__name__})")
        
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