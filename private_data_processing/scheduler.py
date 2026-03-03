"""
调度器 - 负责启动步骤1-2-3-4，接收步骤1的输出，执行步骤2-3-4，推送到大脑
"""
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class PrivateDataScheduler:
    """私人数据调度器"""

    def __init__(self, brain: Optional[Any] = None):
        self.brain = brain
        self.step1 = None
        self.step2 = None
        self.step3 = None
        self.step4 = None
        self.running = False
        logger.info("✅【调度器】实例已创建")

    def set_brain(self, brain: Any):
        """设置大脑模块"""
        self.brain = brain
        logger.info("🧠【调度器】大脑已设置")

    async def start(self):
        """启动调度器 - 启动所有步骤"""
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
        logger.info("🚀【调度器】已启动 - step1, step2, step3, step4 已就绪")

    async def stop(self):
        self.running = False
        logger.info("🛑【调度器】已停止")

    async def process_extracted(self, extracted_list: list):
        """
        接收步骤1的输出，处理后续步骤
        """
        if not extracted_list or not self.running:
            return

        try:
            for extracted in extracted_list:
                # ===== 步骤2：融合更新 =====
                container = self.step2.process(extracted)
                if not container:
                    continue

                # ===== 步骤3：计算衍生字段 =====
                self.step3.process(container)

                # ===== 步骤4：资金费处理 =====
                self.step4.process(container)

                # ===== 推送大脑 =====
                await self._push_to_brain(container)

        except Exception as e:
            logger.error(f"❌【调度器】处理失败: {e}")

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


# ========== 单例模式 + 自动启动 ==========
_scheduler_instance: Optional[PrivateDataScheduler] = None


def get_scheduler(brain: Optional[Any] = None) -> PrivateDataScheduler:
    """获取调度器单例"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = PrivateDataScheduler(brain)
        
        # 自动启动！
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_scheduler_instance.start())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_scheduler_instance.start())
        
        logger.info("🔥【调度器】模块加载时自动启动")
    
    return _scheduler_instance