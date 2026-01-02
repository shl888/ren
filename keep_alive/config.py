import os
import random

class Config:
    """配置管理 - 优化版"""
    
    # 应用URL（从环境变量获取）
    APP_URL = os.environ.get("APP_URL", "https://your-app.onrender.com")
    
    # 自ping端点（按优先级排序）- 优化顺序
    SELF_ENDPOINTS = [
        f"{APP_URL}/public/ping",  # ✅ 第一优先级：专为监控设计
        f"{APP_URL}/",             # ✅ 第二优先级：首页（轻量）
        f"{APP_URL}/health",       # ✅ 第三优先级：健康检查（备用）
    ]
    
    # 外部ping目标（小文件、稳定）- 保持不变
    EXTERNAL_TARGETS = [
        "https://www.bing.com/favicon.ico",
        "https://www.google.com/favicon.ico", 
        "https://api.github.com/zen",
        "https://httpbin.org/status/200",
        "https://1.1.1.1/",
    ]
    
    # User-Agent轮换
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/537.36",
    ]
    
    # 时间间隔配置（秒）- 调整为与UptimeRobot互补
    BASE_INTERVAL = 300      # 5分钟基础间隔（与UptimeRobot对齐）
    MIN_INTERVAL = 270       # 4.5分钟
    MAX_INTERVAL = 330       # 5.5分钟
    
    # UptimeRobot检测
    UPTIMEROBOT_USER_AGENT = "UptimeRobot"
    UPTIMEROBOT_DETECTION_WINDOW = 300  # 检测最近5分钟的访问
    
    # 重试配置（简化）
    MAX_RETRIES = 1          # 快速重试1次（端点切换更快）
    REQUEST_TIMEOUT = 5      # 请求超时
    
    @classmethod
    def get_random_user_agent(cls):
        """获取随机User-Agent"""
        return random.choice(cls.USER_AGENTS)
    
    @classmethod
    def get_random_external_target(cls):
        """获取随机外部目标"""
        return random.choice(cls.EXTERNAL_TARGETS)
    
    @classmethod
    def validate_config(cls):
        """验证配置"""
        if not cls.APP_URL or "your-app" in cls.APP_URL:
            print("[警告] ⚠️  请设置正确的APP_URL环境变量")
            print("[提示] 💡 在Render环境变量中设置: APP_URL=https://你的应用.onrender.com")
            return False
        return True