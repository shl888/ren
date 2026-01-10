#!/usr/bin/env python3
"""
PipelineManager 定时全量版
功能：每500ms让data_store推送全量数据，控制币安历史费率每个合约只流出1次
"""

import asyncio
import time
import logging
from typing import Dict, Any, Optional, Callable

# 5个步骤
from shared_data.step1_filter import Step1Filter
from shared_data.step2_fusion import Step2Fusion
from shared_data.step3_align import Step3Align
from shared_data.step4_calc import Step4Calc
from shared_data.step5_cross_calc import Step5CrossCalc

logger = logging.getLogger(__name__)

class PipelineManager:
    """定时全量版 - 每500ms推送一次"""
    
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
        
        # 单条处理锁
        self.processing_lock = asyncio.Lock()
        
        # 计数器
        self.counters = {
            'market_processed': 0,
            'account_processed': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # 定时全量控制
        self.history_flowed_contracts = set()  # 已流出历史费率的合约
        self.push_interval = 0.5  # 500ms，保持不变！
        self.push_task = None
        self.running = False
        
        logger.info("✅ PipelineManager初始化完成（定时全量模式）")
        self._initialized = True
    
    async def start(self):
        """启动定时推送"""
        if self.running:
            return
        
        self.running = True
        self.push_task = asyncio.create_task(self._push_loop())
        logger.info(f"🚀 启动定时全量推送，间隔{self.push_interval}秒（500ms）")
    
    async def _push_loop(self):
        """定时推送循环"""
        while self.running:
            try:
                # 获取data_store实例
                from shared_data.data_store import data_store
                
                # 让data_store按规则推送全量数据
                await data_store.push_all_data_by_rules(
                    self.history_flowed_contracts.copy()
                )
                
            except Exception as e:
                logger.error(f"定时推送失败: {e}")
                self.counters['errors'] += 1
            
            await asyncio.sleep(self.push_interval)  # 500ms
    
    async def ingest_data(self, data: Dict[str, Any]) -> bool:
        """
        数据处理入口（由data_store调用）
        ✅ 修改：区分账户数据和市场数据
        """
        try:
            # 快速分类
            data_type = data.get("data_type", "")
            
            # 🚨 账户/订单数据：直接推送到大脑（不经过流水线）
            if data_type.startswith(("account_", "position", "order", "trade")):
                await self._process_account_data(data)
            else:
                # 📊 市场数据：走完整5步流水线
                await self._process_market_data(data)
            
            return True
            
        except Exception as e:
            logger.error(f"处理失败: {data.get('symbol', 'N/A')} - {e}")
            self.counters['errors'] += 1
            return False
    
    async def _process_market_data(self, data: Dict[str, Any]):
        """市场数据处理：5步流水线"""
        async with self.processing_lock:
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
            
            # Step4: 计算
            step4_results = self.step4.process(step3_results)
            if not step4_results:
                return
            
            # Step5: 跨平台计算
            final_results = self.step5.process(step4_results)
            if not final_results:
                return
            
            # 推送大脑：成品套利数据
            if self.brain_callback:
                for result in final_results:
                    await self.brain_callback(result.__dict__)
            
            self.counters['market_processed'] += 1
            logger.debug(f"📊 市场数据处理完成: {data.get('symbol', 'N/A')}")
    
    async def _process_account_data(self, data: Dict[str, Any]):
        """账户数据：直接推送到大脑"""
        if self.brain_callback:
            await self.brain_callback(data)
        
        self.counters['account_processed'] += 1
        logger.debug(f"💰 账户数据直达: {data.get('exchange', 'N/A')}")
    
    def mark_history_flowed(self, exchange: str, symbol: str):
        """标记合约历史费率已流出"""
        if exchange == "binance":
            contract_key = f"{exchange}_{symbol}"
            self.history_flowed_contracts.add(contract_key)
            logger.debug(f"📝 记录: {contract_key} 历史费率已流出")
    
    def get_status(self) -> Dict[str, Any]:
        uptime = time.time() - self.counters['start_time']
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "market_processed": self.counters['market_processed'],
            "account_processed": self.counters['account_processed'],
            "errors": self.counters['errors'],
            "history_flowed_count": len(self.history_flowed_contracts),
            "mode": "定时全量模式（500ms间隔）",
            "step4_cache_size": len(self.step4.binance_cache) if hasattr(self.step4, 'binance_cache') else 0
        }
    
    async def stop(self):
        """停止"""
        logger.info("🛑 PipelineManager停止中...")
        self.running = False
        if self.push_task:
            self.push_task.cancel()
        await asyncio.sleep(0.1)
        logger.info("✅ PipelineManager已停止")