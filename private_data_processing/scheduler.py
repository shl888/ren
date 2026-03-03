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

# ===== 强制输出 =====
print("🔥🔥🔥【调度器文件】正在被导入！")
import sys
print(f"📂 导入路径: {__file__}")

logger = logging.getLogger(__name__)
# 强制设置日志级别
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)


class PrivateDataScheduler:
    """私人数据调度器"""
    
    def __init__(self, brain: Optional[Any] = None):
        print("🔥🔥🔥【调度器】__init__ 被调用了！")
        print(f"🧠 传入brain: {brain is not None}")
        
        self.brain = brain
        self.step1 = Step1Extract()
        self.step2 = Step2Fusion()
        self.step3 = Step3Calc()
        self.step4 = Step4Funding()
        
        print("✅【调度器】初始化完成（print版）")
        logger.info("✅【调度器】初始化完成（log版）")
    
    def set_brain(self, brain: Any):
        """设置大脑模块"""
        print(f"🔄【调度器】设置brain: {brain is not None}")
        self.brain = brain
    
    async def process(self, raw_data: Dict[str, Any]) -> None:
        """
        处理原始数据
        """
        print(f"📥【调度器】收到数据: {raw_data.get('exchange')}.{raw_data.get('data_type')}")
        
        try:
            exchange = raw_data.get('exchange', '')
            
            # 步骤1：提取字段
            print("🔍 调用 step1.extract()")
            extracted = self.step1.extract(raw_data)
            print(f"📦 step1 返回: {extracted is not None}")
            if not extracted:
                return
            
            # 步骤2：融合更新
            print("🔄 调用 step2.process()")
            container = self.step2.process(extracted)
            print(f"📦 step2 返回: {container is not None}")
            if not container:
                return
            
            # 步骤3：计算衍生字段
            print("🧮 调用 step3.process()")
            self.step3.process(container)
            
            # 步骤4：资金费处理
            print("💰 调用 step4.process()")
            self.step4.process(container)
            
            # 推送大脑
            if self.brain:
                print(f"📤 推送数据到大脑: {exchange}")
                await self.brain.receive_private_data({
                    "exchange": exchange,
                    "data_type": "user_summary",
                    "data": container,
                    "timestamp": datetime.now().isoformat()
                })
                print("✅ 推送完成")
            else:
                print("⚠️ brain 不存在，无法推送")
                
        except Exception as e:
            print(f"❌ 错误: {e}")
            logger.error(f"❌【调度器】处理失败: {e}")


# ========== 单例模式 ==========
_scheduler_instance = None

def get_scheduler(brain: Optional[Any] = None) -> PrivateDataScheduler:
    """获取调度器单例"""
    print("🔍 get_scheduler() 被调用")
    global _scheduler_instance
    if _scheduler_instance is None:
        print("🆕 创建新调度器实例")
        _scheduler_instance = PrivateDataScheduler(brain)
        print("✅ 调度器实例创建完成")
    elif brain is not None and _scheduler_instance.brain is None:
        print("🔄 设置brain到现有实例")
        _scheduler_instance.set_brain(brain)
    
    print(f"📤 返回调度器实例: {_scheduler_instance}")
    return _scheduler_instance


# 文件末尾再加一个确认
print("✅【调度器文件】加载完成！")