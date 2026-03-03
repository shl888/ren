"""
调度器 - 负责接收步骤1的输出，执行步骤2-3-4，推送到大脑
"""
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional

from .pipeline.step2_fusion import Step2Fusion
from .pipeline.step3_calc import Step3Calc
from .pipeline.step4_funding import Step4Funding

logger = logging.getLogger(__name__)


class PrivateDataScheduler:
    """私人数据调度器"""

    def __init__(self, brain: Optional[Any] = None):
        self.brain = brain
        self.step2 = Step2Fusion()
        self.step3 = Step3Calc()
        self.step4 = Step4Funding()
        self.running = False
        self._task: Optional[asyncio.Task] = None
        logger.info("✅【调度器】初始化完成")

    def set_brain(self, brain: Any):
        """设置大脑模块"""
        self.brain = brain
        logger.info("🧠【调度器】大脑已设置")

    async def start(self):
        """启动调度器（实际不需要循环，保留以兼容）"""
        self.running = True
        logger.info("🚀【调度器】已就绪")

    async def stop(self):
        self.running = False
        logger.info("🛑【调度器】已停止")

    async def process_extracted(self, extracted_list: list):
        """
        接收步骤1的输出，处理后续步骤
        
        Args:
            extracted_list: 步骤1提取的结果列表（可能多条）
        """
        if not extracted_list:
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


# ========== 单例模式 ==========
_scheduler_instance: Optional[PrivateDataScheduler] = None


def get_scheduler(brain: Optional[Any] = None) -> PrivateDataScheduler:
    """获取调度器单例"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = PrivateDataScheduler(brain)
        logger.info("✅【调度器】全局实例已创建")
    elif brain is not None and _scheduler_instance.brain is None:
        _scheduler_instance.set_brain(brain)
    return _scheduler_instance


async def start_scheduler(brain: Optional[Any] = None):
    """启动调度器"""
    scheduler = get_scheduler(brain)
    await scheduler.start()
    return scheduler