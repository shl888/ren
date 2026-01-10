#!/usr/bin/env python3
"""
PipelineManager 终极降压版 - 流式处理 + 零缓存 + 无队列
内存占用：<100MB，适合512MB实例
"""

import asyncio
from enum import Enum
from typing import Dict, Any, Optional, Callable
import logging
import time  # ✅ 修复：必须导入

# 5个步骤
from shared_data.step1_filter import Step1Filter
from shared_data.step2_fusion import Step2Fusion
from shared_data.step3_align import Step3Align
from shared_data.step4_calc import Step4Calc
from shared_data.step5_cross_calc import Step5CrossCalc

logger = logging.getLogger(__name__)

class DataType(Enum):
    """极简数据类型分类"""
    MARKET = "market"
    ACCOUNT = "account"

class PipelineManager:
    """终极降压版 - 流式处理，无队列，无缓冲"""
    
    _instance: Optional['PipelineManager'] = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def instance(cls) -> 'PipelineManager':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self, brain_callback: Optional[Callable] = None):
        # 防止重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        self.brain_callback = brain_callback
        
        # 5个步骤（无状态）
        self.step1 = Step1Filter()
        self.step2 = Step2Fusion()
        self.step3 = Step3Align()
        self.step4 = Step4Calc()  # 保留必需缓存
        self.step5 = Step5CrossCalc()
        
        # 单条处理锁（确保顺序）
        self.processing_lock = asyncio.Lock()
        
        # 计数器（无历史记录）
        self.counters = {
            'market_processed': 0,
            'account_processed': 0,
            'errors': 0,
            'start_time': time.time()  # ✅ 现在time已导入
        }
        
        self.running = False
        
        logger.info("✅ 终极降压版PipelineManager初始化完成（流式处理，无队列）")
        self._initialized = True
    
    async def start(self):
        """启动（流式版不需要后台循环）"""
        if self.running:
            return
        
        logger.info("🚀 终极降压版PipelineManager启动...")
        self.running = True
        
        # 流式版：不需要消费者循环，数据来时直接处理
        
        logger.info("✅ 流式处理已就绪（来一条处理一条）")
    
    async def stop(self):
        """停止"""
        logger.info("🛑 PipelineManager停止中...")
        self.running = False
        await asyncio.sleep(1)
        logger.info("✅ PipelineManager已停止")
    
    async def ingest_data(self, data: Dict[str, Any]) -> bool:
        """
        流式处理入口：
        - 来一条立即处理
        - 不缓冲、不等待、不积压
        - 内存占用=原始数据的1.2倍
        """
        try:
            # 快速分类
            data_type = data.get("data_type", "")
            if data_type.startswith(("ticker", "funding_rate", "mark_price",
                                   "okx_", "binance_")):
                category = DataType.MARKET
            elif data_type.startswith(("account", "position", "order", "trade")):
                category = DataType.ACCOUNT
            else:
                category = DataType.MARKET
            
            # 立即处理（无队列）
            async with self.processing_lock:
                if category == DataType.MARKET:
                    await self._process_market_data(data)
                elif category == DataType.ACCOUNT:
                    await self._process_account_data(data)
            
            return True
            
        except Exception as e:
            logger.error(f"处理失败: {data.get('symbol', 'N/A')} - {e}")
            self.counters['errors'] += 1
            return False
    
    async def _process_market_data(self, data: Dict[str, Any]):
        """市场数据处理：5步流水线，流式"""
        # Step1: 提取
        step1_results = self.step1.process([data])
        if not step1_results:
            return
        
        # Step2: 融合
        step2_results = self.step2.process(step1_results)
        if not step2_results:
            return
        
        # Step3: 对齐
        step3_results = self.step3.process(step2_results)
        if not step3_results:
            return
        
        # Step4: 计算（内部缓存自动工作）
        step4_results = self.step4.process(step3_results)
        if not step4_results:
            return
        
        # Step5: 跨平台计算
        final_results = self.step5.process(step4_results)
        if not final_results:
            return
        
        # 推送大脑
        if self.brain_callback:
            for result in final_results:
                await self.brain_callback(result.__dict__)
        
        self.counters['market_processed'] += 1
        logger.debug(f"📊 处理完成: {data.get('symbol', 'N/A')}")
    
    async def _process_account_data(self, data: Dict[str, Any]):
        """账户数据：直连大脑"""
        if self.brain_callback:
            await self.brain_callback(data)
        
        self.counters['account_processed'] += 1
        logger.debug(f"💰 账户数据直达: {data.get('exchange', 'N/A')}")
    
    def get_status(self) -> Dict[str, Any]:
        uptime = time.time() - self.counters['start_time']
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "market_processed": self.counters['market_processed'],
            "account_processed": self.counters['account_processed'],
            "errors": self.counters['errors'],
            "memory_mode": "流式处理，无队列积压",
            "step4_cache_size": len(self.step4.binance_cache) if hasattr(self.step4, 'binance_cache') else 0
        }

# 使用示例
async def main():
    async def brain_callback(data):
        print(f"🧠 收到: {data.get('symbol', 'N/A')}")
    
    manager = PipelineManager(brain_callback=brain_callback)
    await manager.start()
    
    test_data = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "data_type": "funding_rate",
        "raw_data": {"fundingRate": 0.0001}
    }
    
    await manager.ingest_data(test_data)
    await asyncio.sleep(2)
    
    print(manager.get_status())
    await manager.stop()

if __name__ == "__main__":
    asyncio.run(main())
