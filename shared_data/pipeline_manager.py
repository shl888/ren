#!/usr/bin/env python3
"""
PipelineManager - 管理员/立法者
功能：1. 制定规则 2. 启动系统 3. 监督运行
"""

import asyncio
import time
from typing import Dict, Any, Optional, Callable
import logging

# 导入5个步骤
from shared_data.step1_filter import Step1Filter
from shared_data.step2_fusion import Step2Fusion
from shared_data.step3_align import Step3Align
from shared_data.step4_calc import Step4Calc
from shared_data.step5_cross_calc import Step5CrossCalc

logger = logging.getLogger(__name__)

class PipelineManager:
    """管理员：制定规则，启动系统"""
    
    _instance: Optional['PipelineManager'] = None
    
    @classmethod
    def instance(cls) -> 'PipelineManager':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self, brain_callback: Optional[Callable] = None):
        if hasattr(self, '_initialized'):
            return
        
        # 大脑回调
        self.brain_callback = brain_callback
        
        # 立法：制定核心规则
        self.rules = {
            # 放水规则
            "flow": {
                "interval_seconds": 1.0,      # 1秒放一次水
                "enabled": True,              # 是否放水
            },
            
            # 币安历史费率特殊规则
            "binance_history": {
                "max_flows_per_contract": 1,  # 每个合约最多流1次
                "expected_total_contracts": 600,  # 预计总共600个合约
                "complete_threshold": 500,    # 收到500个就认为完成了
            },
            
            # 流水线规则
            "pipeline": {
                "enabled": True,
                "log_statistics": True,       # 记录统计信息
            }
        }
        
        # 流水线工人
        self.step1 = Step1Filter()
        self.step2 = Step2Fusion()
        self.step3 = Step3Align()
        self.step4 = Step4Calc()
        self.step5 = Step5CrossCalc()
        
        # 系统状态
        self.system_running = False
        self.stats = {
            "total_processed": 0,
            "last_processed_time": 0,
            "errors": 0,
            "start_time": time.time()
        }
        
        logger.info("✅【数据处理管理员】初始化完成")
        self._initialized = True
    
    # ==================== 管理员核心功能 ====================
    
    async def start(self):
        """启动整个系统（保持接口兼容）"""
        if self.system_running:
            logger.warning("⚠️【数据处理管理员】系统已经在运行中")
            return
        
        logger.info("🚀【数据处理管理员】开始启动系统...")
        self.system_running = True
        
        try:
            # 1. 把规则发给DataStore
            from shared_data.data_store import data_store
            await data_store.receive_rules(self.rules)
            logger.info("📋【数据处理管理员】规则已下达给DataStore")
            
            # 2. 启动DataStore的放水系统
            await data_store.start_flowing(self._receive_water_callback)
            logger.info("🚰【数据处理管理员】DataStore放水系统已启动")
            
            # 3. 流水线工人已就绪（步骤1-5）
            logger.info("🔧【数据处理管理员】流水线工人已就位")
            
            # 4. 系统运行中
            logger.info("🎉【数据处理管理员】系统启动完成，开始自动运行")
            
            # 5. 启动状态监控（可选）
            self._monitor_task = asyncio.create_task(self._monitor_system())
            
        except Exception as e:
            logger.error(f"❌【数据处理管理员】系统启动失败: {e}")
            self.system_running = False
            raise
    
    async def stop(self):
        """停止系统（保持接口兼容）"""
        logger.info("🛑【数据处理管理员】正在停止系统...")
        self.system_running = False
        
        # 停止DataStore放水
        from shared_data.data_store import data_store
        await data_store.stop_flowing()
        
        # 停止监控
        if hasattr(self, '_monitor_task'):
            self._monitor_task.cancel()
        
        logger.info("✅【数据处理管理员】系统已停止")
    
    async def update_rule(self, rule_key: str, rule_value: Any):
        """更新规则（动态调整）"""
        if rule_key in self.rules:
            old_value = self.rules[rule_key]
            self.rules[rule_key] = rule_value
            
            logger.info(f"📝【数据处理管理员】规则更新: {rule_key} = {rule_value}")
            
            # 通知DataStore规则更新
            from shared_data.data_store import data_store
            await data_store.receive_rule_update(rule_key, rule_value)
        else:
            logger.warning(f"⚠️【数据处理管理员】未知规则: {rule_key}")
    
    # ==================== 回调函数 ====================
    
    async def _receive_water_callback(self, water_data: list):
        """
        接收DataStore放过来的水
        水已经按照规则过滤好了
        """
        if not water_data:
            return
        
        try:
            # 步骤1：过滤提取
            step1_results = self.step1.process(water_data)
            if not step1_results:
                return
            
            # 步骤2：融合
            step2_results = self.step2.process(step1_results)
            if not step2_results:
                return
            
            # 步骤3：对齐
            step3_results = self.step3.process(step2_results)
            if not step3_results:
                return
            
            # 步骤4：计算
            step4_results = self.step4.process(step3_results)
            if not step4_results:
                return
            
            # 步骤5：跨平台计算
            step5_results = self.step5.process(step4_results)
            if not step5_results:
                return
            
            # 统计
            self.stats["total_processed"] += len(step5_results)
            self.stats["last_processed_time"] = time.time()
            
            # 给大脑
            if self.brain_callback:
                for result in step5_results:
                    await self.brain_callback(result.__dict__)
            
        except Exception as e:
            logger.error(f"❌【数据处理管理员】流水线处理失败: {e}")
            self.stats["errors"] += 1
    
    # ==================== 系统监控 ====================
    
    async def _monitor_system(self):
        """监控系统运行状态"""
        while self.system_running:
            try:
                # 每分钟报告一次状态
                await asyncio.sleep(60)
                
                uptime = time.time() - self.stats["start_time"]
                logger.info(f"📈【数据处理管理员】系统运行报告 - 运行时间: {uptime:.0f}秒, "
                          f"处理总数: {self.stats['total_processed']}, "
                          f"错误数: {self.stats['errors']}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【数据处理管理员】监控错误: {e}")
                await asyncio.sleep(10)
    
    # ==================== 状态查询 ====================
    
    def get_status(self) -> Dict[str, Any]:
        """获取系统状态（保持接口兼容）"""
        uptime = time.time() - self.stats["start_time"]
        
        return {
            "running": self.system_running,
            "uptime_seconds": uptime,
            "market_processed": self.stats["total_processed"],
            "errors": self.stats["errors"],
            "memory_mode": "定时全量处理，1秒间隔",
            "step4_cache_size": len(self.step4.binance_cache) if hasattr(self.step4, 'binance_cache') else 0,
            "timestamp": time.time()
        }
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态（详细版）"""
        uptime = time.time() - self.stats["start_time"]
        
        return {
            "system_running": self.system_running,
            "uptime_seconds": uptime,
            "stats": self.stats.copy(),
            "rules": self.rules.copy(),
            "timestamp": time.time()
        }
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """获取流水线统计"""
        return {
            "step1_stats": dict(self.step1.stats) if hasattr(self.step1, 'stats') else {},
            "step2_stats": dict(self.step2.stats) if hasattr(self.step2, 'stats') else {},
            "step3_stats": self.step3.stats if hasattr(self.step3, 'stats') else {},
            "step4_stats": self.step4.stats if hasattr(self.step4, 'stats') else {},
            "step5_stats": self.step5.stats if hasattr(self.step5, 'stats') else {},
        }
    
    # ==================== 兼容原有接口 ====================
    
    async def ingest_data(self, data: Dict[str, Any]) -> bool:
        """接收数据（保持接口兼容，但实际由DataStore控制）"""
        return True

# 使用示例
async def main():
    async def brain_callback(data):
        pass
    
    # 获取管理员实例
    manager = PipelineManager.instance()
    manager.brain_callback = brain_callback
    
    # 启动系统（一次）
    await manager.start()
    
    # 运行一段时间
    await asyncio.sleep(30)
    
    # 查看状态
    print("系统状态:", manager.get_status())
    
    # 停止系统
    await manager.stop()

if __name__ == "__main__":
    asyncio.run(main())