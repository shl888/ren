"""
PipelineManager - 管理员/立法者
功能：1. 制定规则 2. 启动系统 3. 监督运行（已添加Step0）
"""

import asyncio
import time
from typing import Dict, Any, Optional, Callable
import logging

# ✅ 导入6个步骤（新增Step0）
from shared_data.step0_rate_limiter import Step0RateLimiter
from shared_data.step1_filter import Step1Filter
from shared_data.step2_fusion import Step2Fusion
from shared_data.step3_align import Step3Align
from shared_data.step4_calc import Step4Calc
from shared_data.step5_cross_calc import Step5CrossCalc

logger = logging.getLogger(__name__)

class PipelineManager:
    """管理员：制定规则，启动系统，管理双数据管道（已集成Step0）"""
    
    _instance: Optional['PipelineManager'] = None
    
    @classmethod
    def instance(cls) -> 'PipelineManager':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self, 
                 brain_callback: Optional[Callable] = None,
                 private_data_callback: Optional[Callable] = None):
        """✅ 已集成Step0限流器"""
        if hasattr(self, '_initialized'):
            return
        
        # 大脑双通道回调
        self.brain_callback = brain_callback
        self.private_data_callback = private_data_callback
        
        # 每小时重置计时器
        self._last_hourly_reset = time.time()
        self._hourly_reset_interval = 3600  # 1小时 = 3600秒
        
        # 立法：制定核心规则
        self.rules = {
            # 放水规则
            "flow": {
                "interval_seconds": 1.0,
                "enabled": True,
            },
            
            # 流水线规则
            "pipeline": {
                "enabled": True,
                "log_statistics": True,
            },
            
            # ✅ 新增：Step0限流规则
            "step0_limit": {
                "binance_funding_settlement_limit": 150,  # 币安历史费率数据限制次数
                "enabled": True
            },
            
            # 私人数据规则
            "private_data": {
                "enabled": True,
                "immediate_flow": True,
                "log_updates": True
            }
        }
        
        # ✅ 流水线工人（现在有6个步骤！）
        self.step0 = Step0RateLimiter(limit_times=self.rules["step0_limit"]["binance_funding_settlement_limit"])
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
            "start_time": time.time(),
            # ✅ 新增：Step0统计
            "step0_stats": {
                "total_in": 0,
                "total_out": 0,
                "binance_funding_blocked": 0,
                "binance_funding_passed": 0
            },
            # 私人数据统计
            "private_data": {
                "account_updates": 0,
                "order_updates": 0,
                "last_account_update": 0,
                "last_order_update": 0,
                "errors": 0
            }
        }
        
        logger.info("✅【数据处理管理员】初始化完成（已集成Step0限流器）")
        self._initialized = True
    
    # ==================== 管理员核心功能 ====================
    
    async def start(self):
        """启动整个系统（已集成Step0）"""
        if self.system_running:
            logger.warning("⚠️【数据处理管理员】系统已经在运行中")
            return
        
        logger.info("🚀【数据处理管理员】开始启动系统...")
        self.system_running = True
        self._last_hourly_reset = time.time()  # 重置计时器
        
        try:
            # 1. 把规则发给DataStore
            from shared_data.data_store import data_store
            await data_store.receive_rules(self.rules)
            logger.info("📋【数据处理管理员】规则已下达给DataStore")
            
            # 2. 启动DataStore的市场数据放水系统
            await data_store.start_flowing(self._receive_water_callback)
            logger.info("🚰【数据处理管理员】DataStore市场数据放水系统已启动")
            
            # 3. 连接私人数据管道
            data_store.set_private_water_callback(self._receive_private_water)
            logger.info("🔄【数据处理管理员】私人数据管道已连接")
            
            # ✅ 4. 流水线工人已就绪（步骤0-5）
            logger.info("🔧【数据处理管理员】流水线工人已就位（步骤0-5）")
            
            # 5. 系统运行中
            logger.info("🎉【数据处理管理员】系统启动完成，开始自动运行")
            
            # 6. 启动状态监控（包含每小时重置检查）
            self._monitor_task = asyncio.create_task(self._monitor_system())
            
        except Exception as e:
            logger.error(f"❌【数据处理管理员】系统启动失败: {e}")
            self.system_running = False
            raise
    
    async def stop(self):
        """停止系统"""
        logger.info("🛑【数据处理管理员】正在停止系统...")
        self.system_running = False
        
        # 停止DataStore放水
        from shared_data.data_store import data_store
        await data_store.stop_flowing()
        
        # 关闭私人数据管道
        data_store.set_private_flowing(False)
        
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
            
            # 特殊处理：更新Step0限流次数
            if rule_key == "step0_limit" and "binance_funding_settlement_limit" in rule_value:
                new_limit = rule_value["binance_funding_settlement_limit"]
                self.step0.update_limit(new_limit)
                logger.info(f"🔧【数据处理管理员】Step0限流次数已更新为: {new_limit}")
            
            # 通知DataStore规则更新
            from shared_data.data_store import data_store
            await data_store.receive_rule_update(rule_key, rule_value)
        else:
            logger.warning(f"⚠️【数据处理管理员】未知规则: {rule_key}")
    
    # ==================== 市场数据处理回调（已集成Step0） ====================
    
    async def _receive_water_callback(self, water_data: list):
        """
        接收DataStore放过来的市场数据水
        ✅ 已集成Step0限流器
        """
        if not water_data:
            return
        
        try:
            # 检查是否需要每小时重置统计
            self._check_hourly_reset()
            
            # ✅ 步骤0：币安历史费率限流
            step0_results = self.step0.process(water_data)
            
            # 记录Step0统计
            step0_status = self.step0.get_status()
            self.stats["step0_stats"]["total_in"] += len(water_data)
            self.stats["step0_stats"]["total_out"] += len(step0_results)
            self.stats["step0_stats"]["binance_funding_passed"] = step0_status["binance_funding_passed"]
            self.stats["step0_stats"]["binance_funding_blocked"] = step0_status.get("binance_funding_blocked", 0)
            
            if not step0_results:
                # 如果Step0过滤后没有数据了，直接返回
                logger.debug("🔄【数据处理管理员】Step0过滤后无数据，跳过本次处理")
                return
            
            # ✅ 步骤1：过滤提取（接收Step0的输出！）
            step1_results = self.step1.process(step0_results)
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
                all_results = [result.__dict__ for result in step5_results]
                await self.brain_callback(all_results)
            
        except Exception as e:
            logger.error(f"❌【数据处理管理员】流水线处理失败: {e}")
            self.stats["errors"] += 1
    
    # ==================== 私人数据处理回调 ====================
    
    async def _receive_private_water(self, private_data: Dict):
        """接收DataStore放过来的私人数据水（保持不变）"""
        if not private_data:
            return
        
        try:
            # 检查是否需要每小时重置统计
            self._check_hourly_reset()
            
            data_type = private_data.get('data_type', 'unknown')
            
            # 统计
            if data_type == 'account_update':
                self.stats["private_data"]["account_updates"] += 1
                self.stats["private_data"]["last_account_update"] = time.time()
            elif data_type == 'order_update':
                self.stats["private_data"]["order_updates"] += 1
                self.stats["private_data"]["last_order_update"] = time.time()
            
            # 可选：记录私人数据更新
            if self.rules["private_data"]["log_updates"]:
                logger.debug(f"📨【数据处理管理员】【私人数据】收到 {data_type}: {private_data.get('exchange')}")
            
            # 立即推送给大脑
            if self.private_data_callback:
                await self.private_data_callback(private_data)
            else:
                logger.warning(f"⚠️【数据处理管理员】【私人数据】收到{data_type}但无回调，数据丢失")
            
        except Exception as e:
            logger.error(f"❌【数据处理管理员】私人数据处理失败: {e}")
            self.stats["private_data"]["errors"] += 1
    
    # ==================== 每小时重置方法 ====================
    
    def _check_hourly_reset(self):
        """检查并执行每小时统计重置"""
        current_time = time.time()
        time_since_reset = current_time - self._last_hourly_reset
        
        if time_since_reset >= self._hourly_reset_interval:
            self._reset_hourly_stats()
            self._last_hourly_reset = current_time
    
    def _reset_hourly_stats(self):
        """每小时重置统计计数"""
        logger.info("🕐【数据处理管理员】每小时统计重置开始")
        
        # 重置所有统计计数
        self.stats["total_processed"] = 0
        self.stats["errors"] = 0
        
        # 重置Step0统计
        self.stats["step0_stats"]["total_in"] = 0
        self.stats["step0_stats"]["total_out"] = 0
        self.stats["step0_stats"]["binance_funding_blocked"] = 0
        self.stats["step0_stats"]["binance_funding_passed"] = 0
        
        # 重置Step0限流器内部计数
        if hasattr(self, 'step0') and hasattr(self.step0, 'reset_counters'):
            self.step0.reset_counters()
        
        # 重置私人数据统计
        self.stats["private_data"]["account_updates"] = 0
        self.stats["private_data"]["order_updates"] = 0
        self.stats["private_data"]["errors"] = 0
        
        logger.info("✅【数据处理管理员】每小时统计重置完成")
    
    # ==================== 系统监控 ====================
    
    async def _monitor_system(self):
        """监控系统运行状态（包含每小时重置检查）"""
        while self.system_running:
            try:
                # 每分钟报告一次状态
                await asyncio.sleep(60)
                
                # 检查每小时重置
                self._check_hourly_reset()
                
                # 格式化运行时间
                uptime_seconds = time.time() - self.stats["start_time"]
                uptime_str = self._format_uptime(uptime_seconds)
                
                market_total = self.stats["total_processed"]
                step0_in = self.stats["step0_stats"]["total_in"]
                step0_out = self.stats["step0_stats"]["total_out"]
                step0_blocked = step0_in - step0_out
                private_account = self.stats["private_data"]["account_updates"]
                private_order = self.stats["private_data"]["order_updates"]
                
                logger.info(f"📈【数据处理管理员】系统运行报告 - "
                          f"运行时间: {uptime_str}, "
                          f"Step0: 输入{step0_in}/输出{step0_out}/拦截{step0_blocked}, "
                          f"市场处理: {market_total}条, "
                          f"私人数据(账户: {private_account}, 交易: {private_order})")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【数据处理管理员】监控错误: {e}")
                await asyncio.sleep(10)
    
    def _format_uptime(self, seconds: float) -> str:
        """将秒数格式化为 小时:分钟:秒 格式"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds_remain = int(seconds % 60)
        
        return f"{hours}小时{minutes:02d}分{seconds_remain:02d}秒"
    
    # ==================== 状态查询 ====================
    
    def get_status(self) -> Dict[str, Any]:
        """获取系统状态（保持接口兼容）"""
        self._check_hourly_reset()  # 确保统计是最新的
        
        uptime_seconds = time.time() - self.stats["start_time"]
        uptime_str = self._format_uptime(uptime_seconds)
        
        return {
            "running": self.system_running,
            "uptime": uptime_str,  # 改为格式化字符串
            "uptime_seconds": uptime_seconds,  # 保留秒数版本
            "market_processed": self.stats["total_processed"],
            "errors": self.stats["errors"],
            "step0_status": self.step0.get_status(),
            "memory_mode": "定时全量处理，1秒间隔",
            "step4_cache_size": len(self.step4.binance_cache) if hasattr(self.step4, 'binance_cache') else 0,
            "timestamp": time.time()
        }
    
    def get_system_status(self) -> Dict[str, Any]:
        """✅ 增强：获取系统状态（详细版，包含Step0）"""
        self._check_hourly_reset()  # 确保统计是最新的
        
        uptime_seconds = time.time() - self.stats["start_time"]
        uptime_str = self._format_uptime(uptime_seconds)
        
        return {
            "system_running": self.system_running,
            "uptime": uptime_str,
            "uptime_seconds": uptime_seconds,
            "stats": self.stats.copy(),
            "rules": self.rules.copy(),
            "step0_status": self.step0.get_status(),
            "timestamp": time.time(),
            "next_hourly_reset_in": max(0, self._hourly_reset_interval - (time.time() - self._last_hourly_reset))
        }
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """获取流水线统计（包含Step0）"""
        self._check_hourly_reset()  # 确保统计是最新的
        
        return {
            "step0_stats": self.step0.get_status() if hasattr(self.step0, 'get_status') else {},
            "step1_stats": dict(self.step1.stats) if hasattr(self.step1, 'stats') else {},
            "step2_stats": dict(self.step2.stats) if hasattr(self.step2, 'stats') else {},
            "step3_stats": self.step3.stats if hasattr(self.step3, 'stats') else {},
            "step4_stats": self.step4.stats if hasattr(self.step4, 'stats') else {},
            "step5_stats": self.step5.stats if hasattr(self.step5, 'stats') else {},
        }
    
    # ==================== 回调设置方法 ====================
    
    def set_brain_callback(self, callback: Callable):
        """设置市场数据大脑回调"""
        self.brain_callback = callback
        logger.info("✅【数据处理管理员】市场数据大脑回调已设置")
    
    def set_private_data_callback(self, callback: Callable):
        """设置私人数据大脑回调"""
        self.private_data_callback = callback
        logger.info("✅【数据处理管理员】私人数据大脑回调已设置")
    
    def has_private_data_callback(self) -> bool:
        """检查是否有私人数据回调"""
        return self.private_data_callback is not None
    
    # ==================== Step0相关方法 ====================
    
    def reset_step0_limit(self):
        """重置Step0限流器（用于测试）"""
        if hasattr(self, 'step0'):
            self.step0.reset_limit()
            logger.info("🔄【数据处理管理员】Step0限流器已重置")
    
    def get_step0_status(self) -> Dict[str, Any]:
        """获取Step0状态"""
        if hasattr(self, 'step0'):
            return self.step0.get_status()
        return {"error": "Step0 not initialized"}
    
    # ==================== 兼容原有接口 ====================
    
    async def ingest_data(self, data: Dict[str, Any]) -> bool:
        """接收数据（保持接口兼容）"""
        return True

# 使用示例
async def main():
    # 大脑双回调函数
    async def brain_callback(data):
        """处理市场数据"""
        print(f"📈【数据处理管理员】 收到市场数据: {len(data)}条")
    
    async def private_data_callback(data):
        """处理私人数据"""
        data_type = data.get('data_type', 'unknown')
        if data_type == 'account_update':
            print(f"💰 【数据处理管理员】收到账户更新: {data.get('exchange')}")
        elif data_type == 'order_update':
            print(f"📝 【数据处理管理员】收到交易更新: {data.get('order_id')}")
    
    # 获取管理员实例
    manager = PipelineManager.instance()
    
    # 设置双回调
    manager.set_brain_callback(brain_callback)
    manager.set_private_data_callback(private_data_callback)
    
    # 启动系统（一次）
    await manager.start()
    
    # 运行一段时间
    await asyncio.sleep(30)
    
    # 查看状态
    print("系统状态:", manager.get_system_status())
    
    # 停止系统
    await manager.stop()

if __name__ == "__main__":
    asyncio.run(main())