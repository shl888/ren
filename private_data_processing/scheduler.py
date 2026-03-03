"""
调度器 - 负责启动步骤1-2-3-4，接收步骤1的输出，执行步骤2-3-4，推送到大脑
"""
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class PrivateDataScheduler:
    """私人数据调度器 - 真正的调度中心"""

    def __init__(self, brain: Optional[Any] = None):
        self.brain = brain
        self.step1 = None
        self.step2 = None
        self.step3 = None
        self.step4 = None
        self.running = False
        
        # Step1输出队列：Step1提取后推到这里，调度器从这里取
        self.step1_output_queue = asyncio.Queue()
        
        logger.info("✅【调度器】实例已创建")

    def set_brain(self, brain: Any):
        """设置大脑模块"""
        self.brain = brain
        logger.info("🧠【调度器】大脑已设置")

    async def start(self):
        """启动调度器 - 启动所有步骤和工作流"""
        if self.running:
            logger.warning("⚠️【调度器】已经启动，跳过")
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
        
        # 启动流水线工作线程
        asyncio.create_task(self._pipeline_worker())

    async def stop(self):
        self.running = False
        logger.info("🛑【调度器】已停止")

    def feed_step1(self, stored_item: Dict[str, Any]):
        """
        Manager直接调用这个，往Step1嘴里塞数据
        """
        # ===== 调试日志1：确认feed_step1被调用 =====
        logger.info(f"🎯【调度器】feed_step1被调用！数据类型: {stored_item.get('data_type')}, 交易所: {stored_item.get('exchange')}")
        
        if self.step1:
            try:
                # ===== 调试日志2：确认创建任务 =====
                asyncio.create_task(self.step1.receive(stored_item))
                logger.info(f"📥【调度器】已创建任务塞给Step1，队列当前大小: {self.step1_output_queue.qsize()}")
            except Exception as e:
                logger.error(f"❌【调度器】创建任务失败: {e}")
        else:
            logger.error(f"❌【调度器】Step1未初始化！无法处理数据: {stored_item.get('data_type')}")

    async def _pipeline_worker(self):
        """流水线工作线程：从Step1输出队列取数据，走step2-3-4，推给大脑"""
        logger.info("🏭【流水线工作线程】已启动")
        
        empty_count = 0  # 计数器，记录空队列次数
        
        while self.running:
            try:
                # ===== 调试日志3：检查队列状态（每10秒打印一次）=====
                queue_size = self.step1_output_queue.qsize()
                if queue_size > 0:
                    logger.info(f"📊【调度器】队列有 {queue_size} 条数据待处理")
                    empty_count = 0
                else:
                    empty_count += 1
                    if empty_count % 10 == 0:  # 每10秒（10次超时）打印一次
                        logger.info(f"⏳【调度器】队列持续为空，已等待 {empty_count} 秒")
                
                # 从Step1输出队列取数据
                extracted = await asyncio.wait_for(self.step1_output_queue.get(), timeout=1.0)
                
                # ===== 调试日志4：确认取到数据 =====
                logger.info(f"✅【调度器】从队列取到数据！事件类型: {extracted.get('event_type')}, 交易所: {extracted.get('交易所')}")
                
                # ===== 步骤2：融合更新 =====
                logger.info(f"🔜【调度器】开始Step2融合...")
                container = self.step2.process(extracted)
                if not container:
                    logger.warning(f"⚠️【调度器】Step2返回空，跳过")
                    self.step1_output_queue.task_done()
                    continue
                logger.info(f"✅【调度器】Step2完成，容器交易所: {container.get('交易所')}")

                # ===== 步骤3：计算衍生字段 =====
                logger.info(f"🔜【调度器】开始Step3计算...")
                self.step3.process(container)
                logger.info(f"✅【调度器】Step3完成")

                # ===== 步骤4：资金费处理 =====
                logger.info(f"🔜【调度器】开始Step4资金费...")
                self.step4.process(container)
                logger.info(f"✅【调度器】Step4完成")

                # ===== 推送大脑 =====
                logger.info(f"🔜【调度器】准备推给大脑...")
                await self._push_to_brain(container)
                
                self.step1_output_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"❌【流水线工作线程】错误: {e}")
                import traceback
                logger.error(traceback.format_exc())

    async def _push_to_brain(self, container: Dict[str, Any]):
        """推送成品数据到大脑"""
        if not self.brain:
            # ===== 调试日志5：确认大脑未设置 =====
            logger.error(f"❌【调度器】大脑未设置！数据无法推送: {container.get('交易所')}")
            return

        try:
            await self.brain.receive_private_data({
                "exchange": container.get("交易所", "unknown"),
                "data_type": "user_summary",
                "data": container,
                "timestamp": datetime.now().isoformat()
            })
            logger.info(f"✅【调度器】已推送 {container.get('交易所')} 数据到大脑")
        except Exception as e:
            logger.error(f"❌【调度器】推送大脑失败: {e}")


# ========== 单例模式 ==========
_scheduler_instance: Optional[PrivateDataScheduler] = None


def get_scheduler(brain: Optional[Any] = None) -> PrivateDataScheduler:
    """获取调度器单例"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = PrivateDataScheduler(brain)
        logger.info("🔥【调度器】单例已创建")
    
    return _scheduler_instance
