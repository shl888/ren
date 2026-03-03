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
        # Step1需要知道往哪输出（调度器的队列）
        self.step1 = Step1Extract(self.step1_output_queue)
        self.step2 = Step2Fusion()
        self.step3 = Step3Calc()
        self.step4 = Step4Funding()
        
        self.running = True
        logger.info("🚀【调度器】已启动 - step1, step2, step3, step4 已就绪")
        
        # 启动流水线工作线程：从Step1输出队列取数据，走step2-3-4，推给大脑
        asyncio.create_task(self._pipeline_worker())

    async def stop(self):
        self.running = False
        logger.info("🛑【调度器】已停止")

    def feed_step1(self, stored_item: Dict[str, Any]):
        """
        Manager直接调用这个，往Step1嘴里塞数据
        不管Step1是否启动，只管塞
        """
        if self.step1:
            # 异步塞数据，不阻塞Manager
            asyncio.create_task(self.step1.receive(stored_item))
            logger.debug(f"📥【调度器】数据已塞给Step1: {stored_item.get('data_type')}")
        else:
            logger.warning("⚠️【调度器】Step1未初始化，数据丢弃")

    async def _pipeline_worker(self):
        """流水线工作线程：从Step1输出队列取数据，走step2-3-4，推给大脑"""
        logger.info("🏭【流水线工作线程】已启动")
        
        while self.running:
            try:
                # 从Step1输出队列取数据（阻塞等待）
                extracted = await asyncio.wait_for(self.step1_output_queue.get(), timeout=1.0)
                
                # ===== 步骤2：融合更新 =====
                container = self.step2.process(extracted)
                if not container:
                    self.step1_output_queue.task_done()
                    continue

                # ===== 步骤3：计算衍生字段 =====
                self.step3.process(container)

                # ===== 步骤4：资金费处理 =====
                self.step4.process(container)

                # ===== 推送大脑 =====
                await self._push_to_brain(container)
                
                self.step1_output_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"❌【流水线工作线程】错误: {e}")

    async def _push_to_brain(self, container: Dict[str, Any]):
        """推送成品数据到大脑"""
        if not self.brain:
            logger.debug("⚠️【调度器】大脑未设置，跳过推送")
            return

        try:
            await self.brain.receive_private_data({
                "exchange": container.get("交易所", "unknown"),
                "data_type": "user_summary",
                "data": container,
                "timestamp": datetime.now().isoformat()
            })
            logger.debug(f"✅【调度器】已推送 {container.get('交易所')} 数据到大脑")
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
