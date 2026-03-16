"""
调试接口模块 - 只保留真正的调试接口
"""
from aiohttp import web
import datetime
import logging
import asyncio  # ✅ [蚂蚁基因修复] 导入asyncio

from shared_data.data_store import data_store

logger = logging.getLogger(__name__)


# ============ 只保留这一个函数 ============
async def get_websocket_status(request: web.Request) -> web.Response:
    """
    【调试接口】查看WebSocket连接池状态
    地址：GET /api/debug/websocket_status
    """
    try:
        # 获取连接状态（异步方法，可以直接await）
        connection_status = await data_store.get_connection_status()
        
        # ✅ [蚂蚁基因修复] 在线程池中执行同步方法
        loop = asyncio.get_event_loop()
        data_stats = await loop.run_in_executor(None, data_store.get_market_data_stats)
        
        # 统计信息
        stats = {
            "total_exchanges": len(connection_status),
            "exchanges": list(connection_status.keys()),
            "data_statistics": data_stats
        }
        
        return web.json_response({
            "success": True,
            "timestamp": datetime.datetime.now().isoformat(),
            "stats": stats,
            "connection_status": connection_status
        })
        
    except Exception as e:
        logger.error(f"获取WebSocket状态失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)


def setup_debug_routes(app: web.Application):
    """设置调试接口路由 - 只保留真正的调试接口"""
    app.router.add_get('/api/debug/websocket_status', get_websocket_status)
    
    logger.info("✅ 调试路由已加载 (精简版):")
    logger.info("   GET /api/debug/websocket_status")