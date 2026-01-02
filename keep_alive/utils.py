import sys
import os

def print_banner():
    """打印横幅"""
    banner = """
    ╔══════════════════════════════════════════╗
    ║      Render实例保活服务 v1.1            ║
    ║      智能错峰版（与UptimeRobot互补）    ║
    ║      目标保活率: 99.99%+                ║
    ║      内存限制: <3MB                     ║
    ╚══════════════════════════════════════════╝
    """
    print(banner)

def format_time(seconds):
    """格式化时间"""
    if seconds < 60:
        return f"{int(seconds)}秒"
    elif seconds < 3600:
        minutes = int(seconds / 60)
        seconds_left = int(seconds % 60)
        return f"{minutes}分{seconds_left}秒"
    else:
        hours = int(seconds / 3600)
        minutes = int((seconds % 3600) / 60)
        return f"{hours}小时{minutes}分"

def format_timestamp(timestamp=None):
    """格式化时间戳"""
    import time
    if timestamp is None:
        timestamp = time.time()
    
    return time.strftime('%H:%M:%S', time.localtime(timestamp))

def check_simple_memory():
    """简单内存检查"""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / 1024 / 1024
        return f"{mem_mb:.1f}MB"
    except ImportError:
        return "未知"

def get_simple_status():
    """获取简单状态"""
    import time
    return {
        'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
        'python_version': sys.version.split()[0],
        'platform': sys.platform
    }