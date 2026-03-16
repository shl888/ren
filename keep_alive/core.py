import asyncio  # ✅ [蚂蚁基因修复] 导入asyncio
import time
import sys
from .config import Config
from .pinger import Pinger
from .scheduler import Scheduler
from .monitor import Monitor
from .utils import print_banner, format_time, format_timestamp, check_simple_memory

class KeepAlive:
    """保活主类 - 异步优化版"""
    
    def __init__(self, background_mode=False):
        self.background_mode = background_mode
        self.running = True
        
        # 初始化组件
        self.config = Config
        self.pinger = Pinger()
        self.monitor = Monitor(max_history=10)  # ✅ 只保留10条记录
        self.scheduler = Scheduler(self.monitor)  # ✅ 传入monitor
        
        if not background_mode:
            print_banner()
            print(f"[保活] 应用地址: {self.config.APP_URL}")
            print(f"[保活] 端点策略: {len(self.config.SELF_ENDPOINTS)}个优先级")
            print(f"[保活] 记录限制: 最近{self.monitor.recent_results.maxlen}条")
    
    # ✅ [蚂蚁基因修复] 改为异步方法
    async def run_cycle(self):
        """异步执行一个保活周期"""
        cycle_start = time.time()
        timestamp = format_timestamp(cycle_start)
        
        print(f"[{timestamp}] 开始保活周期...")
        
        # 步骤1: 自ping（带端点回退）- 异步调用
        self_ping_success, self_endpoint = await self.pinger.self_ping_async()
        
        # 步骤2: 外ping（保持不变）- 异步调用
        external_ping_success, external_endpoint = await self.pinger.external_ping_async()
        
        # 判断本次周期是否成功
        cycle_success = self_ping_success or external_ping_success
        
        # 更新调度器状态
        self.scheduler.update_failure_count(cycle_success)
        
        # 记录到监控
        endpoint_info = f"自:{self_endpoint[:30]},外:{external_endpoint[:20]}"
        alert = self.monitor.record_result(cycle_success, endpoint_info)
        
        # 打印结果
        cycle_time = time.time() - cycle_start
        status_symbol = "✅" if cycle_success else "❌"
        
        print(f"[{timestamp}] 周期完成 {status_symbol}")
        print(f"      自ping: {'✅' if self_ping_success else '❌'} {self_endpoint}")
        print(f"      外ping: {'✅' if external_ping_success else '❌'} {external_endpoint}")
        print(f"      总耗时: {cycle_time:.1f}s")
        
        # 检查简单告警
        if alert:
            print(f"[警告] {alert}")
        
        # 简单统计（每10次或每天重置时）
        if self.monitor.reset_if_needed() or self.monitor.total_attempts % 10 == 0:
            stats = self.monitor.get_simple_stats()
            print(f"[统计] 最近{stats['recent_count']}次: {stats['recent_success']}成功")
            print(f"[统计] UptimeRobot检测: {stats['uptimerobot_detected']}")
            
            # 简单内存检查
            mem_usage = check_simple_memory()
            print(f"[内存] 使用: {mem_usage}")
        
        return cycle_success
    
    # ✅ [蚂蚁基因修复] 改为异步方法
    async def _run_main_loop(self):
        """异步主运行循环"""
        print("[保活] 🚀 开始保活循环...")
        
        cycle_count = 0
        try:
            while self.running:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU
                cycle_count += 1
                
                # 执行一个周期
                await self.run_cycle()
                
                # 计算并等待下次执行
                next_interval, reason = self.scheduler.calculate_interval()
                next_time = time.time() + next_interval
                
                print(f"[等待] 下次: {format_timestamp(next_time)}")
                print(f"      间隔: {format_time(next_interval)} ({reason})")
                print("-" * 50)
                
                # 异步等待（支持优雅中断）
                await self._sleep_with_interrupt(next_interval)
                
        except asyncio.CancelledError:  # ✅ [蚂蚁基因修复] 捕获取消异常
            print("\n[保活] 🛑 任务被取消")
            self.stop()
        except Exception as e:
            print(f"[错误] ❗ 运行异常: {e}")
            print("[保活] ⏳ 30秒后重启...")
            await asyncio.sleep(30)
            await self._run_main_loop()  # 重启
    
    # ✅ [蚂蚁基因修复] 改为异步方法
    async def _sleep_with_interrupt(self, seconds):
        """异步可中断的睡眠"""
        interval = 1  # 每次检查间隔
        for _ in range(int(seconds / interval)):
            if not self.running:
                break
            await asyncio.sleep(interval)  # ✅ [蚂蚁基因修复] 异步sleep
    
    # ✅ [蚂蚁基因修复] 改为异步方法
    async def run(self):
        """异步运行入口"""
        # 直接开始主循环（HTTP就绪检查在外部完成）
        await self._run_main_loop()
    
    def stop(self):
        """停止保活"""
        self.running = False
        print("[保活] 🛑 服务停止")
    
    def get_simple_status(self):
        """获取简单状态"""
        stats = self.monitor.get_simple_stats()
        scheduler_status = self.scheduler.get_status()
        
        return {
            'running': self.running,
            'total_attempts': self.monitor.total_attempts,
            'recent_stats': stats,
            'scheduler': scheduler_status,
            'config': {
                'app_url': self.config.APP_URL,
                'self_endpoints': len(self.config.SELF_ENDPOINTS),
                'external_targets': len(self.config.EXTERNAL_TARGETS)
            }
        }