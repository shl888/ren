"""
私人数据查看API - 通过HTTP提供原始数据查看接口
"""
import json
import os
from aiohttp import web
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class PrivateDataAPI:
    """私人数据查看API服务器"""
    
    def __init__(self, raw_data_cache, host: str = '0.0.0.0', port: int = 10002):
        self.raw_data_cache = raw_data_cache
        self.host = host
        self.port = port
        self.runner = None
        self.site = None
        
        logger.info(f"[数据查看API] 初始化完成，端口: {port}")
    
    async def start(self):
        """启动API服务器"""
        app = web.Application()
        
        # 注册路由
        app.router.add_get('/', self.handle_root)
        app.router.add_get('/health', self.handle_health)
        app.router.add_get('/latest', self.handle_latest_all)
        app.router.add_get('/latest/{exchange}', self.handle_latest_exchange)
        app.router.add_get('/stats', self.handle_stats)
        
        # 创建并启动服务器
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        
        logger.info(f"✅ [数据查看API] 已启动: http://{self.host}:{self.port}")
        logger.info(f"📊 [数据查看API] 可用端点:")
        logger.info(f"   http://{self.host}:{self.port}/latest          # 查看所有最新数据")
        logger.info(f"   http://{self.host}:{self.port}/latest/binance # 查看币安最新数据")
        logger.info(f"   http://{self.host}:{self.port}/latest/okx     # 查看欧意最新数据")
        logger.info(f"   http://{self.host}:{self.port}/stats          # 查看缓存统计")
    
    async def stop(self):
        """停止API服务器"""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        logger.info("🛑 [数据查看API] 已停止")
    
    # ==================== 请求处理函数 ====================
    
    async def handle_root(self, request):
        """根路径 - 显示可用端点"""
        endpoints = {
            "endpoints": {
                "/": "显示此帮助信息",
                "/health": "健康检查",
                "/latest": "查看所有交易所的最新数据",
                "/latest/{exchange}": "查看指定交易所的最新数据",
                "/stats": "查看缓存统计信息"
            },
            "note": "私人数据查看API - 用于调试和数据分析"
        }
        return web.json_response(endpoints)
    
    async def handle_health(self, request):
        """健康检查"""
        return web.json_response({
            "status": "healthy",
            "service": "private_data_api",
            "timestamp": datetime.now().isoformat()
        })
    
    async def handle_latest_all(self, request):
        """查看所有交易所的最新数据"""
        try:
            data = self.raw_data_cache.get_latest()
            return web.json_response({
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "data": data
            })
        except Exception as e:
            return web.json_response({
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def handle_latest_exchange(self, request):
        """查看指定交易所的最新数据"""
        try:
            exchange = request.match_info['exchange']
            if exchange not in ['binance', 'okx']:
                return web.json_response({
                    "success": False,
                    "error": f"不支持的交易所: {exchange}",
                    "supported_exchanges": ["binance", "okx"]
                }, status=400)
            
            data = self.raw_data_cache.get_latest(exchange)
            if not data:
                return web.json_response({
                    "success": False,
                    "error": f"没有找到{exchange}的数据",
                    "timestamp": datetime.now().isoformat()
                }, status=404)
            
            return web.json_response({
                "success": True,
                "exchange": exchange,
                "timestamp": datetime.now().isoformat(),
                "data": data
            })
            
        except Exception as e:
            return web.json_response({
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def handle_stats(self, request):
        """查看缓存统计"""
        try:
            stats = self.raw_data_cache.get_stats()
            return web.json_response({
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "stats": stats
            })
        except Exception as e:
            return web.json_response({
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)


# 工具函数：创建并启动API服务器
async def start_private_data_api(raw_data_cache, port: int = 10002):
    """启动私人数据查看API（用于在launcher.py中调用）"""
    api = PrivateDataAPI(raw_data_cache, port=port)
    await api.start()
    return PrivateDataAPI
    