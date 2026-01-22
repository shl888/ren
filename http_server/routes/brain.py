"""
大脑数据路由 - 处理所有大脑数据的HTTP请求
"""
import logging
from aiohttp import web
from datetime import datetime

logger = logging.getLogger(__name__)

class BrainRoutes:
    """大脑数据路由处理器"""
    
    def __init__(self, brain):
        self.brain = brain
    
    async def api_root(self, request):
        """API根路径"""
        api_docs = {
            "service": "智能大脑数据管理器API",
            "version": "1.0.0",
            "endpoints": {
                "/api/brain/health": "健康检查",
                "/api/brain/data": "查看所有存储数据",
                "/api/brain/data/market": "查看市场数据",
                "/api/brain/data/private": "查看私人数据",
                "/api/brain/status": "查看数据状态",
                "/api/brain/apis": "查看API凭证状态",
                "/api/brain/data/clear": "清空数据（谨慎使用）",
                "/api/brain/data/clear/{data_type}": "清空特定类型数据"
            },
            "current_time": datetime.now().isoformat()
        }
        return web.json_response(api_docs)
    
    async def health(self, request):
        """健康检查"""
        try:
            # 获取数据管理器状态
            from shared_data.data_store import data_store
            connection_status = await data_store.get_connection_status(None)
            
            return web.json_response({
                "status": "healthy",
                "service": "brain_data_api",
                "timestamp": datetime.now().isoformat(),
                "http_server": "running",
                "websocket_connections": connection_status.get('total_connections', 0),
                "note": "大脑数据API运行正常"
            })
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return web.json_response({
                "status": "error",
                "error": str(e)
            }, status=500)
    
    async def get_all_data(self, request):
        """查看所有存储数据（概览）"""
        try:
            data = await self.brain.data_manager.get_market_data_summary()
            private_data = await self.brain.data_manager.get_private_data_summary()
            
            response = {
                "timestamp": datetime.now().isoformat(),
                "market_data": data,
                "private_data": private_data
            }
            return web.json_response(response)
        except Exception as e:
            logger.error(f"获取所有数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_market_data(self, request):
        """查看所有市场数据"""
        try:
            data = await self.brain.data_manager.get_market_data_summary()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取市场数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_market_data_by_exchange(self, request):
        """按交易所查看市场数据"""
        try:
            exchange = request.match_info.get('exchange', '').lower()
            if not exchange:
                return web.json_response({
                    "error": "需要指定交易所参数",
                    "timestamp": datetime.now().isoformat()
                }, status=400)
            
            data = await self.brain.data_manager.get_market_data_by_exchange(exchange)
            return web.json_response(data)
        except Exception as e:
            logger.error(f"按交易所获取市场数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_market_data_detail(self, request):
        """查看特定市场数据详情"""
        try:
            exchange = request.match_info.get('exchange', '').lower()
            symbol = request.match_info.get('symbol', '').upper()
            
            if not exchange or not symbol:
                return web.json_response({
                    "error": "需要指定交易所和交易对参数",
                    "timestamp": datetime.now().isoformat()
                }, status=400)
            
            data = await self.brain.data_manager.get_market_data_detail(exchange, symbol)
            
            if "error" in data:
                return web.json_response(data, status=404)
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取市场数据详情失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_private_data(self, request):
        """查看所有私人数据"""
        try:
            data = await self.brain.data_manager.get_private_data_summary()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取私人数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_private_data_by_exchange(self, request):
        """按交易所查看私人数据"""
        try:
            exchange = request.match_info.get('exchange', '').lower()
            if not exchange:
                return web.json_response({
                    "error": "需要指定交易所参数",
                    "timestamp": datetime.now().isoformat()
                }, status=400)
            
            data = await self.brain.data_manager.get_private_data_by_exchange(exchange)
            return web.json_response(data)
        except Exception as e:
            logger.error(f"按交易所获取私人数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_private_data_detail(self, request):
        """查看特定私人数据详情"""
        try:
            exchange = request.match_info.get('exchange', '').lower()
            data_type = request.match_info.get('data_type', '').lower()
            
            if not exchange or not data_type:
                return web.json_response({
                    "error": "需要指定交易所和数据类型参数",
                    "timestamp": datetime.now().isoformat()
                }, status=400)
            
            data = await self.brain.data_manager.get_private_data_detail(exchange, data_type)
            
            if "error" in data:
                return web.json_response(data, status=404)
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取私人数据详情失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_apis(self, request):
        """查看API凭证状态（隐藏敏感信息）"""
        try:
            data = await self.brain.data_manager.get_api_credentials_status()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取API状态失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_status(self, request):
        """查看数据状态"""
        try:
            data = await self.brain.data_manager.get_system_status()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取系统状态失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def clear_data(self, request):
        """清空所有数据"""
        try:
            result = await self.brain.data_manager.clear_stored_data()
            
            if result.get("success"):
                return web.json_response(result)
            else:
                return web.json_response(result, status=400)
        except Exception as e:
            logger.error(f"清空数据失败: {e}")
            return web.json_response({
                "success": False,
                "error": str(e)
            }, status=500)
    
    async def clear_data_type(self, request):
        """清空特定类型数据"""
        try:
            data_type = request.match_info.get('data_type', '').lower()
            
            if data_type not in ['market', 'private']:
                return web.json_response({
                    "success": False,
                    "error": f"不支持的数据类型: {data_type}",
                    "supported_types": ["market", "private"]
                }, status=400)
            
            result = await self.brain.data_manager.clear_stored_data(data_type)
            
            if result.get("success"):
                return web.json_response(result)
            else:
                return web.json_response(result, status=400)
        except Exception as e:
            logger.error(f"清空{data_type}数据失败: {e}")
            return web.json_response({
                "success": False,
                "error": str(e)
            }, status=500)