"""
保活模块包
用于保持Render免费实例活跃 - 优化版
与UptimeRobot互补，确保99.99%+保活率
"""
from .core import KeepAlive
from .config import Config

__version__ = "1.1.0"
__all__ = ['KeepAlive', 'Config', 'start_with_http_check']


def start_with_http_check():
    """
    带HTTP就绪检查的启动函数
    确保HTTP服务就绪后再开始保活循环
    """
    import time
    import asyncio
    import urllib.request
    from .core import KeepAlive
    from .config import Config
    
    app_url = Config.APP_URL
    
    # 检查配置
    if not app_url or "your-app" in app_url:
        print("[保活] ⚠️  APP_URL未正确配置，使用默认保活逻辑")
        keeper = KeepAlive()
        # ✅ 修复：在同步函数中用 asyncio.run 运行异步代码
        asyncio.run(keeper.run())
        return
    
    print(f"[保活] 检测到APP_URL: {app_url}")
    print("[保活] 检查HTTP服务状态...")
    
    # 尝试连接HTTP服务（最多30秒）
    max_attempts = 6  # 6×5秒=30秒
    for i in range(max_attempts):
        try:
            # 尝试访问/public/ping端点
            urllib.request.urlopen(f"{app_url}/public/ping", timeout=3)
            print(f"[保活] ✅ HTTP服务连接成功")
            
            # HTTP就绪，启动保活
            keeper = KeepAlive()
            # ✅ 修复：在同步函数中用 asyncio.run 运行异步代码
            asyncio.run(keeper.run())
            return
            
        except Exception as e:
            if i < max_attempts - 1:
                print(f"[保活] ⏳ HTTP服务未就绪，5秒后重试 ({i+1}/{max_attempts})")
                time.sleep(5)
            else:
                print(f"[保活] ⚠️  HTTP服务等待超时: {e}")
    
    # 最终尝试（即使HTTP未就绪也启动保活）
    print("[保活] 🚀 启动基础保活模式（HTTP服务可能未完全就绪）")
    keeper = KeepAlive()
    # ✅ 修复：在同步函数中用 asyncio.run 运行异步代码
    asyncio.run(keeper.run())