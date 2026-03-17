"""
异步日志专员 - 即插即用，防蚂蚁隐患版
干活的只管丢纸条，专员负责写
修正：保持标准 logging API 兼容性
"""

import asyncio
import threading
import time
from datetime import datetime
from typing import Optional
from collections import deque
import os
import logging as _logging  # 别名避免冲突

class AsyncLogger:
    """
    异步日志专员
    特点：
    1. 所有操作非阻塞
    2. 队列满时自动丢弃旧日志
    3. 批量写入提升性能
    4. 线程安全
    """
    
    _instance: Optional['AsyncLogger'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """单例模式，确保全局只有一个日志专员"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """初始化日志专员"""
        if hasattr(self, '_initialized'):
            return
            
        # ========== 核心组件 ==========
        # 使用 deque 替代 Queue，更轻量，可设置最大长度
        self._queue = deque(maxlen=1000)  # 最多保留10000条，防止内存爆炸
        self._lock = threading.Lock()  # 线程锁保护队列
        self._running = True
        self._batch_size = 20  # 每20条写一次
        self._flush_interval = 1.0  # 最多等1秒
        
        # ========== 统计信息 ==========
        self._total_logged = 0
        self._total_dropped = 0
        self._last_flush_time = time.time()
        
        # ========== 启动后台线程 ==========
        self._thread = threading.Thread(target=self._worker, daemon=True, name="AsyncLogger")
        self._thread.start()
        
        self._initialized = True
        
        # 测试日志
        self._async_log("INFO", "异步日志专员启动完成，最大队列1000条，批量20条")
    
    # ========== 对外接口（供干活的调用）==========
    
    def info(self, msg: str):
        """记录INFO级别日志"""
        self._async_log("INFO", msg)
    
    def error(self, msg: str):
        """记录ERROR级别日志"""
        self._async_log("ERROR", msg)
    
    def warning(self, msg: str):
        """记录WARNING级别日志"""
        self._async_log("WARNING", msg)
    
    def debug(self, msg: str):
        """记录DEBUG级别日志"""
        self._async_log("DEBUG", msg)
    
    def critical(self, msg: str):
        """记录CRITICAL级别日志"""
        self._async_log("CRITICAL", msg)
    
    # ========== 核心丢纸条方法 ==========
    
    def _async_log(self, level: str, msg: str):
        """
        丢纸条到队列（核心！绝对不阻塞）
        """
        try:
            # 格式化日志
            log_entry = f"{datetime.now().isoformat()} - {level} - {msg}"
            
            # 丢到队列（用deque，自动处理满的情况）
            with self._lock:
                if len(self._queue) >= self._queue.maxlen:
                    # 队列满了，丢弃最旧的
                    self._queue.popleft()
                    self._total_dropped += 1
                
                self._queue.append(log_entry)
                self._total_logged += 1
                
        except Exception:
            # 日志异常绝对不能影响主任务
            pass
    
    # ========== 后台专员线程 ==========
    
    def _worker(self):
        """
        专员线程：慢慢写日志，批量处理
        """
        buffer = []
        last_flush = time.time()
        
        while self._running:
            try:
                # 从队列取一批日志
                with self._lock:
                    while self._queue and len(buffer) < self._batch_size:
                        buffer.append(self._queue.popleft())
                
                # 如果有日志，写一批
                if buffer:
                    self._write_batch(buffer)
                    buffer = []
                    last_flush = time.time()
                
                # 如果队列空，或者没攒够，就等一会儿
                time.sleep(0.01)  # 10ms，不占CPU
                
                # 强制刷：超过1秒没写，把buffer里的写了
                if buffer and time.time() - last_flush > self._flush_interval:
                    self._write_batch(buffer)
                    buffer = []
                    last_flush = time.time()
                    
            except Exception as e:
                # 专员异常不能崩溃
                print(f"⚠️ 日志专员异常: {e}")
                time.sleep(1)
        
        # 退出前把剩下的写了
        if buffer:
            self._write_batch(buffer)
    
    def _write_batch(self, logs):
        """
        批量写日志 - 同时写入文件和打印到控制台
        """
        if not logs:
            return
        
        # ===== 1. 打印到控制台（Railway能看到）=====
        for log in logs:
            print(log)  # 这行让日志出现在Railway控制台
        
        # ===== 2. 同时写入文件（永久保存）=====
        try:
            # 确保日志目录存在
            log_dir = "logs"
            os.makedirs(log_dir, exist_ok=True)
            
            # 按天分文件
            today = datetime.now().strftime("%Y%m%d")
            log_file = f"{log_dir}/app_{today}.log"
            
            # 批量写入
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write("\n".join(logs) + "\n")
                
        except Exception as e:
            # 写文件失败时，至少控制台能看到
            print(f"⚠️ 写日志文件失败: {e}")
    
    # ========== 管理接口 ==========
    
    def get_stats(self):
        """获取统计信息"""
        with self._lock:
            return {
                "queue_size": len(self._queue),
                "queue_max": self._queue.maxlen,
                "total_logged": self._total_logged,
                "total_dropped": self._total_dropped,
                "running": self._running
            }
    
    def shutdown(self):
        """关闭日志专员"""
        self._running = False
        self._thread.join(timeout=5)
        self._async_log("INFO", "异步日志专员关闭")
    
    def flush(self):
        """强制刷日志"""
        # 取出所有日志
        logs = []
        with self._lock:
            while self._queue:
                logs.append(self._queue.popleft())
        
        if logs:
            self._write_batch(logs)


# ========== 全局单例 ==========
_async_logger = AsyncLogger()


class AsyncLogHandler(_logging.Handler):
    """将标准 logging 日志消息路由到 AsyncLogger"""
    def __init__(self, level=_logging.NOTSET):
        super().__init__(level)
        self.async_logger = _async_logger
    
    def emit(self, record):
        try:
            msg = self.format(record)
            level = record.levelname
            self.async_logger._async_log(level, msg)
        except Exception:
            # 日志异常绝对不能影响主任务
            pass


# ========== 极简补丁函数 ==========

def patch_logging(show_debug=False):
    """
    🎯 极简日志补丁 - 只控制是否显示DEBUG
    
    Args:
        show_debug: True=显示所有日志（包括DEBUG）
                   False=屏蔽DEBUG，显示INFO及以上（默认）
    """
    import sys
    
    # 获取root logger
    root = _logging.getLogger()
    
    # 记录原有handler数量
    original_count = len(root.handlers)
    
    # 移除已有的AsyncLogHandler（避免重复）
    removed = 0
    for handler in root.handlers[:]:
        if isinstance(handler, AsyncLogHandler):
            root.removeHandler(handler)
            removed += 1
    
    # 创建并添加新的AsyncLogHandler
    handler = AsyncLogHandler()
    formatter = _logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    handler.setLevel(_logging.NOTSET)  # handler接收所有日志
    root.addHandler(handler)
    
    # 设置日志级别
    if show_debug:
        root.setLevel(_logging.DEBUG)
        level_name = "DEBUG"
    else:
        root.setLevel(_logging.INFO)
        level_name = "INFO (DEBUG已屏蔽)"
    
    # 添加控制台handler（确保能看见）
    console = _logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    console.setLevel(root.level)
    root.addHandler(console)
    
    # 输出简洁提示
    print(f"\n📢 日志级别: {level_name} | 移除旧handler: {removed}个 | 当前handler: {len(root.handlers)}个\n")
    
    # 测试日志
    test_logger = _logging.getLogger("patch_test")
    test_logger.debug("🐛 DEBUG - 这条只有show_debug=True时才显示")
    test_logger.info("ℹ️ INFO - 这条总会显示")
    test_logger.warning("⚠️ WARNING - 这条总会显示")
    
    return {
        "level": level_name,
        "show_debug": show_debug,
        "handlers_removed": removed
    }


# ========== 快捷开关 ==========

def enable_debug():
    """开启DEBUG模式（显示所有日志）"""
    return patch_logging(show_debug=True)


def enable_normal():
    """正常模式（屏蔽DEBUG，显示INFO及以上）"""
    return patch_logging(show_debug=False)


# ===== 自动执行（如果直接运行此文件）=====
if __name__ == "__main__":
    print("🧪 测试异步日志专员...")
    patch_logging(show_debug=True)
    time.sleep(2)
    _async_logger.shutdown()