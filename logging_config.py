"""
智能日志配置 - 自动适配 Railway/Render/本地环境
只添加 handler，不删除原有的
"""
import os
import sys
import logging

def setup_logging():
    """根据运行环境添加日志适配（不覆盖原有配置）"""
    
    # 防止重复执行
    if hasattr(setup_logging, '_done'):
        return logging.getLogger()
    setup_logging._done = True
    
    # 判断当前环境
    is_railway = bool(os.environ.get('RAILWAY_SERVICE_ID'))
    is_render = bool(os.environ.get('RENDER'))
    
    # 获取根日志器
    root_logger = logging.getLogger()
    
    # 通用格式化器（和 launcher.py 保持一致）
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    if is_railway:
        # ========== Railway 环境：添加 stdout handler ==========
        # 检查是否已经有 stdout handler，避免重复
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
        
    elif is_render:
        # ========== Render 环境：错误保留红色 ==========
        class RenderHandler(logging.Handler):
            def emit(self, record):
                log_entry = self.format(record) + '\n'
                if record.levelno >= logging.ERROR:
                    sys.stderr.write(log_entry)
                    sys.stderr.flush()
                else:
                    sys.stdout.write(log_entry)
                    sys.stdout.flush()
        
        # 检查是否已经有自定义 handler
        has_render_handler = any(
            isinstance(h, RenderHandler) for h in root_logger.handlers
        )
        
        if not has_render_handler:
            handler = RenderHandler()
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
            print("✅ Render 模式：添加分级 handler")
    
    # 其他环境保持不变，用原来的 basicConfig
    
    return root_logger

# 便捷函数：获取当前环境信息
def get_logging_env():
    """返回当前运行环境名称"""
    if os.environ.get('RAILWAY_SERVICE_ID'):
        return 'railway'
    if os.environ.get('RENDER'):
        return 'render'
    return 'local'
    