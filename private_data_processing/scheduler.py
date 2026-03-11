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

【重要规则 - 数据格式转换 - 2024-03-11 最终修正】
根据Turso API要求和数据库表定义，必须按字段类型转换：

1. 数字字段（数据库中是 REAL/INTEGER）→ 转换为 Python 数字类型（int/float）
   - 原因：Turso API要求数字字段的值必须是数字类型
   - 例如：杠杆 "20" → 20 (int)，开仓价 "69789.4" → 69789.4 (float)

2. 文本字段（数据库中是 TEXT）→ 保持为字符串
   - 原因：Turso API要求文本字段的值必须是字符串
   - 例如：交易所 "binance" → "binance"，id "binance_BTCUSDT_..." → 保持字符串

3. 时间字段（数据库中是 DATETIME，本质是TEXT）→ 保持为字符串
   - 例如：开仓时间 "2024-03-11T07:39:25" → 保持字符串

4. None值 → 保持 None（对应数据库的 null）

【为什么不能统一转字符串或统一转数字？】
- 第一个错误：integer 20, expected string → 文本字段收到了数字
- 第二个错误：string 92045, expected f64 → 数字字段收到了字符串
- 结论：必须按字段类型分别处理！
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
        5. 在推送前按字段类型转换数据格式
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
            5. 按字段类型转换数据并推送到数据完成部门
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
            1. 按字段类型转换数据格式（数字字段转数字，文本字段保持字符串）
            2. 组装符合receiver要求的数据格式
            3. 调用 receive_private_data 推送
        
        【为什么必须按字段类型转换？】
        根据两次错误经验：
        - 错误1: integer 20, expected string → 文本字段收到了数字
        - 错误2: string 92045, expected f64 → 数字字段收到了字符串
        
        结论：必须严格区分字段类型，分别处理！
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
            
            # ===== 关键步骤：按字段类型转换数据格式 =====
            # 根据数据库表定义，分别处理数字字段和文本字段
            converted_container = self._convert_by_field_type(container)
            
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

    def _convert_by_field_type(self, data: dict) -> dict:
        """
        根据字段类型转换数据格式
        ==================================================
        【字段类型分类 - 基于数据库表定义】
        
        1. 数字字段（REAL/INTEGER）- 必须转为 Python 数字类型
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
        
        2. 文本字段（TEXT）- 必须保持为字符串
           - 标识类：id、交易所、资产币种
           - 模式类：保证金模式、保证金币种
           - 合约类：开仓合约名
           - 方向类：开仓方向、开仓执行方式
           - 币种类：开仓手续费币种、平仓手续费币种
           - 触发方式类：止损触发方式、止盈触发方式、平仓执行方式
           - 时间类：开仓时间、本次资金费结算时间、平仓时间（DATETIME本质是TEXT）
        
        3. None值 - 保持 None（对应数据库null）
        
        :param data: 原始数据字典
        :return: 按字段类型转换后的数据字典
        ==================================================
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
        }
        
        # ----- 文本字段列表（必须保持字符串）-----
        # 这些字段即使内容是数字（如时间戳），也要保持为字符串
        text_fields = {
            # 标识类
            'id', '交易所', '资产币种',
            
            # 模式类
            '保证金模式', '保证金币种',
            
            # 合约类
            '开仓合约名',
            
            # 方向类
            '开仓方向', '开仓执行方式',
            
            # 币种类
            '开仓手续费币种', '平仓手续费币种',
            
            # 触发方式类
            '止损触发方式', '止盈触发方式', '平仓执行方式',
            
            # 时间类（DATETIME本质是TEXT）
            '开仓时间', '本次资金费结算时间', '平仓时间',
        }
        
        # ----- 转换统计 -----
        convert_to_num_count = 0
        keep_as_str_count = 0
        none_count = 0
        other_count = 0
        
        for key, value in converted.items():
            if value is None:
                # None 保持 None
                none_count += 1
                logger.debug(f"字段 [{key}]: None (保持null)")
                
            elif key in numeric_fields:
                # 数字字段：必须转成 int/float
                try:
                    if isinstance(value, str):
                        # 字符串数字转成数字
                        cleaned = value.strip().replace(',', '')
                        if '.' in cleaned:
                            converted[key] = float(cleaned)
                            logger.debug(f"字段 [{key}]: '{value}' → {converted[key]} (float)")
                        else:
                            # 先尝试转int，失败则转float
                            try:
                                converted[key] = int(cleaned)
                                logger.debug(f"字段 [{key}]: '{value}' → {converted[key]} (int)")
                            except ValueError:
                                converted[key] = float(cleaned)
                                logger.debug(f"字段 [{key}]: '{value}' → {converted[key]} (float)")
                        convert_to_num_count += 1
                    elif isinstance(value, (int, float)):
                        # 已经是数字，保持原样，但确保资金费结算次数是int
                        if key == '资金费结算次数' and isinstance(value, float):
                            converted[key] = int(value)
                            logger.debug(f"字段 [{key}]: {value} → {converted[key]} (强制转int)")
                        else:
                            logger.debug(f"字段 [{key}]: {value} (已是{type(value).__name__})")
                        convert_to_num_count += 1
                    else:
                        # 意外类型，尝试转数字
                        converted[key] = float(value)
                        logger.warning(f"字段 [{key}]: 意外类型 {type(value).__name__}，强制转float")
                        convert_to_num_count += 1
                except (ValueError, TypeError) as e:
                    logger.error(f"字段 [{key}] 转换失败: {value}, 错误: {e}")
                    # 转换失败就保持原样，但记录错误
                    
            elif key in text_fields:
                # 文本字段：确保是字符串
                if not isinstance(value, str):
                    converted[key] = str(value)
                    logger.debug(f"字段 [{key}]: {value} ({type(value).__name__}) → '{converted[key]}'")
                else:
                    logger.debug(f"字段 [{key}]: '{value}' (已是字符串)")
                keep_as_str_count += 1
                
            else:
                # 未知字段：默认保持原样，但记录警告
                other_count += 1
                logger.warning(f"字段 [{key}] 不在分类中，保持原样: {value} ({type(value).__name__})")
        
        # 记录转换统计
        logger.info(f"📊 数据转换统计: {convert_to_num_count}个数字字段, {keep_as_str_count}个文本字段, {none_count}个null, {other_count}个未知字段")
        
        # 记录关键字段示例
        sample_fields = ['交易所', '开仓合约名', '杠杆', '开仓价', 'id', '开仓时间']
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