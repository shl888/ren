import time
import random
from .config import Config

class Scheduler:
    """调度器 - 智能错峰版"""
    
    def __init__(self, monitor=None):
        self.failures_in_row = 0
        self.monitor = monitor  # ✅ 传入monitor以检测UptimeRobot
        self.last_interval = Config.BASE_INTERVAL
    
    def calculate_interval(self):
        """智能计算下次间隔 - 与UptimeRobot错峰"""
        
        # 如果刚有UptimeRobot访问，延迟执行（形成互补）
        if self.monitor and self.monitor.should_delay_self_ping():
            # UptimeRobot刚来过，我们等2.5分钟后执行
            interval = 150  # 2.5分钟
            reason = "UptimeRobot互补"
        
        else:
            # 正常情况：在5分钟周期内随机
            interval = random.randint(Config.MIN_INTERVAL, Config.MAX_INTERVAL)
            reason = "正常随机"
            
            # 根据连续失败次数微调
            if self.failures_in_row > 0:
                # 失败时稍微缩短间隔（但保持最低2分钟）
                reduction = min(60, self.failures_in_row * 15)
                interval = max(120, interval - reduction)
                reason = f"失败调整(-{reduction}s)"
            elif self.failures_in_row < 0:
                # 连续成功时稍微延长间隔（但最多7分钟）
                increase = min(60, abs(self.failures_in_row) * 10)
                interval = min(420, interval + increase)
                reason = f"成功调整(+{increase}s)"
        
        self.last_interval = interval
        return interval, reason
    
    def update_failure_count(self, success):
        """更新连续失败/成功计数"""
        if success:
            self.failures_in_row = min(0, self.failures_in_row - 1)  # 负数表示连续成功
        else:
            self.failures_in_row = max(0, self.failures_in_row + 1)
        
        return self.failures_in_row
    
    def get_status(self):
        """获取调度器状态"""
        return {
            'failures_in_row': self.failures_in_row,
            'last_interval': self.last_interval,
            'monitor_has_uptimerobot': self.monitor.uptimerobot_count > 0 if self.monitor else False
        }