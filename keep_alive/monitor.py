import time
from collections import deque

class Monitor:
    """监控和统计 - 简化版（只保留最近10条记录）"""
    
    def __init__(self, max_history=10):  # ✅ 只保留10条记录
        self.start_time = time.time()
        self.total_attempts = 0
        self.success_count = 0
        self.last_success_time = time.time()
        self.recent_results = deque(maxlen=max_history)  # ✅ 固定10条
        self.alerts_enabled = True
        
        # UptimeRobot检测
        self.uptimerobot_last_seen = None
        self.uptimerobot_count = 0
    
    def record_result(self, success, endpoint_used="", source="self"):
        """记录一次执行结果"""
        self.total_attempts += 1
        
        if success:
            self.success_count += 1
            self.last_success_time = time.time()
        
        # 保存最近结果（只保留10条）
        result = {
            'timestamp': time.time(),
            'success': success,
            'endpoint': endpoint_used,
            'source': source
        }
        self.recent_results.append(result)
        
        # 简单分析（只检查最近连续失败）
        return self._check_simple_alert()
    
    def record_uptimerobot_access(self):
        """记录UptimeRobot访问"""
        self.uptimerobot_last_seen = time.time()
        self.uptimerobot_count += 1
        
        # 记录为外部保活成功
        self.record_result(True, "/public/ping", "uptimerobot")
    
    def should_delay_self_ping(self):
        """
        判断是否应该延迟自ping
        与UptimeRobot形成时间互补
        """
        if not self.uptimerobot_last_seen:
            return False
        
        time_since_uptimerobot = time.time() - self.uptimerobot_last_seen
        # 如果UptimeRobot最近2分钟内访问过，我们延迟到2.5分钟后执行
        return time_since_uptimerobot < 120
    
    def _check_simple_alert(self):
        """简单告警检查（只检查连续失败）"""
        if len(self.recent_results) < 3:
            return None
        
        # 检查最近3次是否都失败
        recent_failures = 0
        for result in list(self.recent_results)[-3:]:
            if not result['success']:
                recent_failures += 1
        
        if recent_failures >= 3:
            return "连续3次保活失败"
        
        return None
    
    def get_simple_stats(self):
        """获取简单统计信息"""
        if not self.recent_results:
            return {
                'recent_count': 0,
                'recent_success': 0,
                'uptimerobot_detected': self.uptimerobot_count > 0
            }
        
        recent_success = sum(1 for r in self.recent_results if r['success'])
        recent_total = len(self.recent_results)
        
        return {
            'recent_count': recent_total,
            'recent_success': recent_success,
            'recent_rate': f"{(recent_success/recent_total*100):.1f}%" if recent_total > 0 else "0%",
            'uptimerobot_detected': self.uptimerobot_count > 0,
            'uptimerobot_last_seen': time.ctime(self.uptimerobot_last_seen) if self.uptimerobot_last_seen else "从未"
        }
    
    def reset_if_needed(self):
        """必要时重置计数（防止整数溢出）"""
        if self.total_attempts > 10000:  # 每10000次重置
            self.total_attempts = 0
            self.success_count = 0
            return True
        return False