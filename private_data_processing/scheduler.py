"""
调度器 - 负责启动步骤1-2-3-4，接收步骤1的输出，执行步骤2-3-4，推送到大脑
"""
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ===== 导入大脑接收函数（如果可用）=====
try:
    from smart_brain import receive_private_data as brain_receive_data
    logger.info("✅【调度器】大脑模块已导入")
except ImportError:
    brain_receive_data = None
    logger.warning("⚠️【调度器】大脑模块未安装，数据将无处可推")


class PrivateDataScheduler:
    """私人数据调度器 - 真正的调度中心"""

    def __init__(self):
        # 不再依赖外部传入brain，直接调用全局函数
        self.step1 = None
        self.step2 = None
        self.step3 = None
        self.step4 = None
        self.running = False
        
        # Step1输出队列：Step1提取后推到这里，调度器从这里取
        self.step1_output_queue = asyncio.Queue()
        
        logger.info("✅【调度器】实例已创建")

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
        logger.info(f"🎯【调度器】feed_step1被调用！数据类型: {stored_item.get('data_type')}, 交易所: {stored_item.get('exchange')}")
        
        if self.step1:
            try:
                asyncio.create_task(self.step1.receive(stored_item))
                logger.info(f"📥【调度器】已创建任务塞给Step1，队列当前大小: {self.step1_output_queue.qsize()}")
            except Exception as e:
                logger.error(f"❌【调度器】创建任务失败: {e}")
        else:
            logger.error(f"❌【调度器】Step1未初始化！无法处理数据: {stored_item.get('data_type')}")

    async def _pipeline_worker(self):
        """流水线工作线程：从Step1输出队列取数据，走step2-3-4，推给大脑"""
        logger.info("🏭【流水线工作线程】已启动")
        
        while self.running:
            try:
                # 从Step1输出队列取数据
                extracted = await asyncio.wait_for(self.step1_output_queue.get(), timeout=1.0)
                
                logger.info(f"✅【调度器】从队列取到数据！事件类型: {extracted.get('event_type')}, 交易所: {extracted.get('交易所')}")
                
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
                self.step4.process(container)
                logger.info(f"✅【调度器】Step4完成")

                # ===== 推送大脑 =====
                await self._push_to_brain(container)
                
                self.step1_output_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"❌【流水线工作线程】错误: {e}")
                import traceback
                logger.error(traceback.format_exc())

    async def _push_to_brain(self, container: Dict[str, Any]):
        """推送成品数据到大脑（使用全局函数，不依赖brain实例）"""
        if brain_receive_data is None:
            logger.warning(f"⚠️【调度器】大脑未安装，数据丢弃: {container.get('交易所')}")
            # 后续这里可以改成推送到其他目的地
            return

        try:
            await brain_receive_data({
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


def get_scheduler() -> PrivateDataScheduler:
    """获取调度器单例（不再接受brain参数）"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = PrivateDataScheduler()
        logger.info("🔥【调度器】单例已创建")
    
    return _scheduler_instance
