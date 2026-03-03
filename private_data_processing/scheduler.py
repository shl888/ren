"""
调度器 - 负责调用步骤1-4
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from .pipeline.step1_extract import Step1Extract
from .pipeline.step2_fusion import Step2Fusion
from .pipeline.step3_calc import Step3Calc
from .pipeline.step4_funding import Step4Funding

logger = logging.getLogger(__name__)


class PrivateDataScheduler:
    """私人数据调度器"""
    
    def __init__(self, brain: Optional[Any] = None):
        self.brain = brain
        self.step1 = Step1Extract()
        self.step2 = Step2Fusion()
        self.step3 = Step3Calc()
        self.step4 = Step4Funding()
        logger.info("✅【调度器】初始化完成")
    
    def set_brain(self, brain: Any):
        """设置大脑模块"""
        self.brain = brain
    
    async def process(self, raw_data: Dict[str, Any]) -> None:
        """
        处理原始数据
        """
        try:
            exchange = raw_data.get('exchange', '')
            
            # 步骤1：提取字段
            extracted = self.step1.extract(raw_data)
            if not extracted:
                return
            
            # 步骤2：融合更新
            container = self.step2.process(extracted)
            if not container:
                return
            
            # 步骤3：计算衍生字段
            self.step3.process(container)
            
            # 步骤4：资金费处理
            self.step4.process(container)
            
            # 推送大脑
            if self.brain:
                await self.brain.receive_private_data({
                    "exchange": exchange,
                    "data_type": "user_summary",
                    "data": container,
                    "timestamp": datetime.now().isoformat()
                })
                
        except Exception as e:
            logger.error(f"❌【调度器】处理失败: {e}")


# ========== 单例模式 ==========
_scheduler_instance = None

def get_scheduler(brain: Optional[Any] = None) -> PrivateDataScheduler:
    """获取调度器单例"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = PrivateDataScheduler(brain)
        logger.info("✅【调度器】全局实例已创建")
    elif brain is not None and _scheduler_instance.brain is None:
        # 如果之前没有brain，现在设置
        _scheduler_instance.set_brain(brain)
    
    return _scheduler_instance