"""
智能日志配置 - 只针对Railway添加stdout handler，其他环境不变
"""
import os
import sys
import logging

def setup_logging():
    """只针对Railway添加日志适配"""
    
    # 防止重复执行
    if hasattr(setup_logging, '_done'):
        return logging.getLogger()
    setup_logging._done = True
    
    # 只判断是不是 Railway
    is_railway = bool(os.environ.get('RAILWAY_SERVICE_ID'))
    
    # 不是 Railway 就直接返回
    if not is_railway:
        return logging.getLogger()
    
    # 获取根日志器
    root_logger = logging.getLogger()
    
    # 格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # ========== Railway 环境：添加 stdout handler ==========
    # 检查是否已经有 stdout handler
    has_stdout = any(
        isinstance(h, logging.StreamHandler) and h.stream == sys.stdout 
        for h in root_logger.handlers
    )
    
    if not has_stdout:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
        # 用 print 避免循环引用
        print("✅ Railway 模式：添加 stdout handler")
    
    return root_logger