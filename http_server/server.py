"""
HTTP服务器 - Render优化版
先启动HTTP服务，WebSocket连接池在后台初始化
"""
import asyncio
import logging
import sys
import os
from aiohttp import web
import signal
from typing import Dict, Any

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # smart_brain目录
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store
from websocket_pool.pool_manager import WebSocketPoolManager
from .routes import setup_routes
from shared_data.routes import setup_public_data_routes  # <-- 新增导入

logger = logging.getLogger(__name__)

class HTTPServer:
    """HTTP服务器，内部包含WebSocket连接池"""
    
    def __init__(self, host='0.0.0.0', port=None, brain=None):
        # 如果没有指定端口，使用环境变量或Render默认端口
        if port is None:
            port = int(os.getenv('PORT', 10000))  # Render默认端口
        
        self.host = host
        self.port = port
        self.brain = brain  # 保存大脑引用
        self.app = web.Application()
        self.runner = None
        self.site = None
        
        # WebSocket连接池（隐藏在HTTP服务内部）
        self.ws_pool_manager = None
        
        # 设置基础路由
        setup_routes(self.app)
        
        # ✅ 新增：注册公开数据路由（从shared_data模块）
        setup_public_data_routes(self.app)
        
        # 如果提供了大脑实例，注册大脑路由
        if self.brain:
            self._setup_brain_routes()
        
        # 添加启动和关闭钩子
        self.app.on_startup.append(self.on_startup)
        self.app.on_shutdown.append(self.on_shutdown)
        self.app.on_cleanup.append(self.on_cleanup)
        
        # ❌ 移除信号处理，由bsmart_brain统一管理
    
    def _setup_brain_routes(self):
        """设置大脑数据路由 - 完全替换为新结构"""
        try:
            from .routes.brain import BrainRoutes
            brain_routes = BrainRoutes(self.brain)
            
            # ===== 基础路由 =====
            self.app.router.add_get('/api/brain/', brain_routes.api_root)
            self.app.router.add_get('/api/brain/health', brain_routes.health)
            
            # ===== 数据大纲路由 =====
            self.app.router.add_get('/api/brain/data', brain_routes.get_data_summary)
            
            # ===== 按来源分类的详情路由 =====
            # 1. 公开市场数据
            self.app.router.add_get('/api/brain/data/public_market', brain_routes.get_public_market_data)
            
            # 2. 私人用户数据
            self.app.router.add_get('/api/brain/data/private_user', brain_routes.get_private_user_data)
            
            # 3. 参考数据（欧易面值）
            self.app.router.add_get('/api/brain/data/okx_contracts', brain_routes.get_okx_contracts_data)
            
            # ===== 系统管理路由 =====
            self.app.router.add_get('/api/brain/apis', brain_routes.get_apis)
            self.app.router.add_get('/api/brain/status', brain_routes.get_status)
            self.app.router.add_delete('/api/brain/data/clear', brain_routes.clear_data)
            self.app.router.add_delete('/api/brain/data/clear/{data_type}', brain_routes.clear_data_type)
            
            logger.info(f"✅ 已注册大脑数据API路由（3个来源：public_market/private_user/okx_contracts）")
            
        except ImportError as e:
            logger.warning(f"无法导入大脑路由: {e}")
        except Exception as e:
            logger.error(f"设置大脑路由失败: {e}")
    
    async def on_startup(self, app):
        """应用启动时 - 快速初始化"""
        logger.info("✅ HTTP服务器启动成功，端口已监听")
        
        # ✅ 标记HTTP服务已就绪（让健康检查立即通过）
        data_store.set_http_server_ready(True)
        
        logger.info(f"HTTP服务器已就绪，监听在 {self.host}:{self.port}")
        
        # WebSocket连接池将在smart_brain中后台初始化
        # 这里不初始化，保证HTTP服务快速启动
    
    async def handle_websocket_data(self, data: Dict[str, Any]):
        """处理WebSocket数据 - 占位方法，实际由smart_brain处理"""
        # 这个方法保留，但实际处理逻辑在smart_brain中
        pass
    
    async def on_shutdown(self, app):
        """应用关闭时清理资源"""
        logger.info("HTTP服务器关闭中...")
        
        # 关闭WebSocket连接池（如果有）
        if self.ws_pool_manager:
            await self.ws_pool_manager.shutdown()
    
    async def on_cleanup(self, app):
        """应用清理"""
        logger.info("HTTP服务器清理完成")
    
    async def shutdown(self):
        """优雅关闭"""
        logger.info("HTTP服务器关闭中...")
        
        # 关闭WebSocket连接池
        if hasattr(self, 'ws_pool_manager') and self.ws_pool_manager:
            await self.ws_pool_manager.shutdown()
        
        # 关闭HTTP服务器
        if self.runner:
            await self.runner.cleanup()
        if self.site:
            await self.site.stop()
        
        logger.info("HTTP服务器已关闭")
        # ❌ 不调用 sys.exit(0)，由smart_brain控制进程退出
    
    async def get_ws_pool_status(self) -> Dict[str, Any]:
        """获取WebSocket连接池状态"""
        if self.ws_pool_manager:
            return await self.ws_pool_manager.get_all_status()
        return {"error": "WebSocket连接池未初始化"}
    
    def run(self):
        """运行HTTP服务器 - 快速启动版本"""
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        logger.info("=" * 60)
        logger.info("🚀 启动HTTP服务器（快速启动模式）")
        logger.info(f"端口: {self.port}")
        logger.info("=" * 60)
        
        try:
            # 快速启动，不等待其他组件
            web.run_app(
                self.app,
                host=self.host,
                port=self.port,
                access_log=logger,
                shutdown_timeout=60,
                print=None  # 禁用默认的启动信息
            )
        except KeyboardInterrupt:
            logger.info("收到键盘中断")
        except Exception as e:
            logger.error(f"服务器运行错误: {e}")
            sys.exit(1)