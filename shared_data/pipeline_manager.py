#!/usr/bin/env python3
"""
PipelineManager 终极降压版 - 流式处理 + 零缓存 + 无队列
内存占用：<100MB，适合512MB实例
稳定性增强版：在原始文件基础上添加异步推送和超时保护
"""

import asyncio
from enum import Enum
from typing import Dict, Any, Optional, Callable
import logging
import time

# 5个步骤
from shared_data.step1_filter import Step1Filter
from shared_data.step2_fusion import Step2Fusion
from shared_data.step3_align import Step3Align
from shared_data.step4_calc import Step4Calc
from shared_data.step5_cross_calc import Step5CrossCalc

logger = logging.getLogger(__name__)

# ✅ 添加：统一的日志工具函数
def log_data_process(module: str, action: str, message: str, level: str = "INFO"):
    """统一的数据处理日志格式"""
    prefix = f"[数据处理][{module}][{action}]"
    full_message = f"{prefix} {message}"
    
    if level == "INFO":
        logger.info(full_message)
    elif level == "ERROR":
        logger.error(full_message)
    elif level == "WARNING":
        logger.warning(full_message)
    elif level == "DEBUG":
        logger.debug(full_message)

class DataType(Enum):
    """极简数据类型分类"""
    MARKET = "market"
    ACCOUNT = "account"

class PipelineManager:
    """终极降压版 - 流式处理，无队列，无缓冲（稳定性增强版）"""
    
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
        
        # 5个步骤（保持原样）
        self.step1 = Step1Filter()
        self.step2 = Step2Fusion()
        self.step3 = Step3Align()
        self.step4 = Step4Calc()  # 保留必需缓存
        self.step5 = Step5CrossCalc()
        
        # ✅ 稳定性增强：带超时的处理锁
        self.processing_lock = asyncio.Lock()
        self.lock_timeout = 30.0  # 30秒超时
        
        # ✅ 稳定性增强：异步推送管理
        self._async_push_enabled = True  # 默认启用异步推送
        self._max_concurrent_pushes = 10
        self._active_push_tasks = set()
        
        # 计数器（保持原样）
        self.counters = {
            'market_processed': 0,
            'account_processed': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # ✅ 添加：5分钟统计
        self.last_report_time = time.time()
        self.data_received_count = 0
        
        self.running = False
        
        # ✅ 修改：统一日志格式
        log_data_process("流水线", "启动", "初始化完成（稳定性增强版）")
        self._initialized = True
    
    def enable_async_push(self, enabled: bool = True):
        """启用或禁用异步推送"""
        self._async_push_enabled = enabled
        log_data_process("流水线", "设置", f"异步推送: {'启用' if enabled else '禁用'}")
    
    async def start(self):
        """启动（流式版不需要后台循环）"""
        if self.running:
            return
        
        # ✅ 修改：统一日志格式
        log_data_process("流水线", "启动", "开始运行...")
        self.running = True
        
        # 流式版：不需要消费者循环，数据来时直接处理
        log_data_process("流水线", "运行", "流式处理已就绪（来一条处理一条）")
    
    async def stop(self):
        """停止"""
        log_data_process("流水线", "停止", "正在停止...")
        self.running = False
        
        # 等待异步推送任务完成
        if self._active_push_tasks:
            log_data_process("流水线", "等待", f"等待 {len(self._active_push_tasks)} 个异步推送任务完成...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._active_push_tasks, return_exceptions=True),
                    timeout=5.0
                )
            except asyncio.TimeoutError:
                log_data_process("流水线", "警告", "异步推送任务超时，强制取消", "WARNING")
        
        await asyncio.sleep(1)
        log_data_process("流水线", "停止", "已停止")
    
    async def ingest_data(self, data: Dict[str, Any]) -> bool:
        """
        流式处理入口（保持原逻辑，添加超时保护）
        """
        try:
            # 快速分类（保持原样）
            data_type = data.get("data_type", "")
            if data_type.startswith(("ticker", "funding_rate", "mark_price",
                                   "okx_", "binance_")):
                category = DataType.MARKET
            elif data_type.startswith(("account", "position", "order", "trade")):
                category = DataType.ACCOUNT
            else:
                category = DataType.MARKET
            
            # ✅ 添加：5分钟统计
            self.data_received_count += 1
            current_time = time.time()
            if current_time - self.last_report_time >= 300:  # 5分钟
                log_data_process("流水线", "统计", 
                               f"5分钟: 接收{self.data_received_count}条数据, "
                               f"处理市场{self.counters['market_processed']}条, "
                               f"处理账户{self.counters['account_processed']}条")
                self.last_report_time = current_time
                self.data_received_count = 0
            
            # ✅ 稳定性增强：带超时的锁
            try:
                async with asyncio.timeout(self.lock_timeout):
                    async with self.processing_lock:
                        if category == DataType.MARKET:
                            await self._process_market_data(data)
                        elif category == DataType.ACCOUNT:
                            await self._process_account_data(data)
            
            except asyncio.TimeoutError:
                symbol = data.get('symbol', 'N/A')
                log_data_process("流水线", "错误", f"处理锁超时 ({self.lock_timeout}秒)，数据丢弃: {symbol}", "ERROR")
                self.counters['errors'] += 1
                return False
            
            return True
            
        except Exception as e:
            symbol = data.get('symbol', 'N/A')
            log_data_process("流水线", "错误", f"处理失败: {symbol} - {e}", "ERROR")
            self.counters['errors'] += 1
            return False
    
    async def _process_market_data(self, data: Dict[str, Any]):
        """市场数据处理：5步流水线，流式（添加异步推送选项）"""
        # Step1: 提取（保持原样）
        step1_results = self.step1.process([data])
        if not step1_results:
            return
        
        # Step2: 融合（保持原样）
        step2_results = self.step2.process(step1_results)
        if not step2_results:
            return
        
        # Step3: 对齐（保持原样）
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
        
        # 推送大脑（✅ 添加异步推送选项）
        if self.brain_callback:
            for result in final_results:
                if self._async_push_enabled and len(self._active_push_tasks) < self._max_concurrent_pushes:
                    # 异步推送（不阻塞）
                    self._push_async(result)
                else:
                    # 同步推送（保持原行为）
                    try:
                        await self.brain_callback(result.__dict__)
                    except Exception as e:
                        log_data_process("流水线", "错误", f"同步推送失败: {e}", "ERROR")
        
        self.counters['market_processed'] += 1
        
        # ✅ 移除原有的调试日志
        # 改为在5分钟统计中显示
    
    def _push_async(self, result):
        """异步推送（不阻塞流水线）"""
        if not self.brain_callback:
            return
        
        async def safe_push():
            try:
                await self.brain_callback(result.__dict__)
            except Exception as e:
                log_data_process("流水线", "错误", f"异步推送失败: {e}", "ERROR")
            finally:
                self._active_push_tasks.discard(task)
        
        task = asyncio.create_task(safe_push())
        self._active_push_tasks.add(task)
    
    async def _process_account_data(self, data: Dict[str, Any]):
        """账户数据：直连大脑（添加异步推送选项）"""
        if self.brain_callback:
            if self._async_push_enabled and len(self._active_push_tasks) < self._max_concurrent_pushes:
                # 异步推送
                self._push_async_account(data)
            else:
                # 同步推送（保持原行为）
                try:
                    await self.brain_callback(data)
                except Exception as e:
                    log_data_process("流水线", "错误", f"账户数据推送失败: {e}", "ERROR")
        
        self.counters['account_processed'] += 1
        
        # ✅ 移除原有的调试日志
        # 账户数据已经在data_store中实时打印了
    
    def _push_async_account(self, data):
        """异步推送账户数据"""
        if not self.brain_callback:
            return
        
        async def safe_push_account():
            try:
                await self.brain_callback(data)
            except Exception as e:
                log_data_process("流水线", "错误", f"异步账户推送失败: {e}", "ERROR")
            finally:
                self._active_push_tasks.discard(task)
        
        task = asyncio.create_task(safe_push_account())
        self._active_push_tasks.add(task)
    
    def get_status(self) -> Dict[str, Any]:
        """获取状态信息（添加异步推送状态）"""
        uptime = time.time() - self.counters['start_time']
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "market_processed": self.counters['market_processed'],
            "account_processed": self.counters['account_processed'],
            "errors": self.counters['errors'],
            "memory_mode": "流式处理，无队列积压",
            "step4_cache_size": len(self.step4.binance_cache) if hasattr(self.step4, 'binance_cache') else 0,
            "async_push": {
                "enabled": self._async_push_enabled,
                "active_tasks": len(self._active_push_tasks),
                "max_concurrent": self._max_concurrent_pushes
            }
        }

# 使用示例（保持原样）
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