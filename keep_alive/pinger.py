import urllib.request
import time
import random
from urllib.error import URLError, HTTPError
import socket
from .config import Config

class Pinger:
    """Ping执行器 - 优化版"""
    
    @staticmethod
    def ping_single(url, timeout=None):
        """执行单次ping"""
        if timeout is None:
            timeout = Config.REQUEST_TIMEOUT
        
        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', Config.get_random_user_agent())
            
            response = urllib.request.urlopen(req, timeout=timeout)
            status = response.getcode()
            
            # 读取少量数据确认响应
            if status == 200 or status == 204:
                try:
                    response.read(100)  # 只读100字节
                except:
                    pass  # 读取失败也视为成功（有响应）
                return True, url  # 返回成功和使用的URL
            
            return False, url
            
        except (URLError, HTTPError, socket.timeout, ConnectionError):
            # 静默处理常见网络错误
            return False, url
        except Exception:
            return False, url
    
    @classmethod
    def ping_with_retry(cls, url, max_retries=None):
        """带重试的ping（快速版）"""
        if max_retries is None:
            max_retries = Config.MAX_RETRIES
        
        for attempt in range(max_retries + 1):  # 包括首次尝试
            success, used_url = cls.ping_single(url)
            if success:
                return True, used_url
            
            # 快速重试等待（如果还有重试次数）
            if attempt < max_retries:
                time.sleep(1)  # 只等1秒
        
        return False, url
    
    @classmethod
    def self_ping(cls):
        """执行自ping - 带端点回退策略"""
        # 按优先级尝试所有端点
        for endpoint in Config.SELF_ENDPOINTS:
            success, used_url = cls.ping_with_retry(endpoint, max_retries=1)
            if success:
                return True, used_url
            # 立即尝试下一个端点，不等待
        
        return False, "all_failed"
    
    @classmethod
    def external_ping(cls):
        """执行外ping - 保持不变"""
        target = Config.get_random_external_target()
        success, used_url = cls.ping_with_retry(target, max_retries=2)
        return success, used_url
    
    @staticmethod
    def detect_uptimerobot(request_headers):
        """检测是否为UptimeRobot访问（仅检测，不修改行为）"""
        user_agent = request_headers.get('User-Agent', '')
        return Config.UPTIMEROBOT_USER_AGENT in user_agent