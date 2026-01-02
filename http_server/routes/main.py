"""
基础路由模块
处理根路径、健康检查、公开接口等
"""
from aiohttp import web
import datetime
import logging

from ..welcome_page import get_welcome_page  # 删除 get_funding_history_test_page

logger = logging.getLogger(__name__)


async def root_handler(request: web.Request) -> web.Response:
    """根路径处理器 - 返回友好的欢迎页面"""
    html_content = get_welcome_page()
    return web.Response(text=html_content, content_type='text/html')


async def public_ping(request: web.Request) -> web.Response:
    """
    完全公开的健康检查接口
    用于外部监控网站保持服务器活跃
    只返回最简单的状态，不含任何敏感信息
    """
    data = {
        "status": "alive",
        "timestamp": datetime.datetime.now().isoformat()
    }
    return web.json_response(data, status=200)


async def health_check(request: web.Request) -> web.Response:
    """健康检查 - Render优化版"""
    from shared_data.data_store import data_store
    
    # 检查HTTP服务状态
    http_ready = data_store.is_http_server_ready()
    
    status_info = {
        "status": "ok" if http_ready else "starting",
        "service": "brain-core-trading",
        "http_server_ready": http_ready,
        "timestamp": datetime.datetime.now().isoformat()
    }
    
    # 如果HTTP服务已就绪，返回200，否则返回202（已接受，但还在处理）
    status_code = 200 if http_ready else 202
    return web.json_response(status_info, status=status_code)


def setup_main_routes(app: web.Application):
    """设置基础路由"""
    app.router.add_get('/', root_handler)
    # 删除这行：app.router.add_get('/test/funding_history', funding_history_test_page)
    app.router.add_get('/public/ping', public_ping)
    app.router.add_get('/health', health_check)
    
    logger.info("✅ 基础路由已加载: /, /health, /public/ping")
