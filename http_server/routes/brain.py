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
                "/api/brain/": "API文档（本页）",
                "/api/brain/health": "健康检查",
                "/api/brain/data": "查看所有数据大纲（按来源分类）",
                "/api/brain/data/public_market": "查看公开市场数据详情（239个币种）",
                "/api/brain/data/private_user": "查看私人用户数据详情（币安+欧易）",
                "/api/brain/data/okx_contracts": "查看OKX合约面值数据详情（262个合约）",
                "/api/brain/data/binance_contracts": "查看币安合约精度数据详情",
                "/api/brain/apis": "查看API凭证状态",
                "/api/brain/status": "查看系统状态",
                "/api/brain/data/clear": "清空所有数据（谨慎使用）",
                "/api/brain/data/clear/{data_type}": "清空特定类型数据"
            },
            "current_time": datetime.now().isoformat(),
            "note": "数据按来源分类：public_market（公开市场）、private_user（私人用户）、okx_contracts/binance_contracts（参考数据）"
        }
        return web.json_response(api_docs)
    
    async def health(self, request):
        """健康检查"""
        try:
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
    
    async def get_data_summary(self, request):
        """获取所有数据的大纲（按来源分类）"""
        try:
            data = await self.brain.data_manager.get_data_summary()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取数据大纲失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_public_market_data(self, request):
        """获取公开市场数据详情"""
        try:
            data = await self.brain.data_manager.get_public_market_data()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取公开市场数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_private_user_data(self, request):
        """获取私人用户数据详情"""
        try:
            data = await self.brain.data_manager.get_private_user_data()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取私人用户数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_okx_contracts_data(self, request):
        """获取OKX合约面值数据详情"""
        try:
            data = await self.brain.data_manager.get_okx_contracts_data()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取OKX合约面值数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_binance_contracts_data(self, request):
        """获取币安合约精度数据详情"""
        try:
            data = await self.brain.data_manager.get_binance_contracts_data()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取币安合约精度数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_apis(self, request):
        """查看API凭证状态"""
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
        """查看系统状态"""
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
            valid_types = ['market', 'user', 'reference', 'tokens']
            
            if data_type not in valid_types:
                return web.json_response({
                    "success": False,
                    "error": f"不支持的数据类型: {data_type}",
                    "supported_types": valid_types
                }, status=400)
            
            result = await self.brain.data_manager.clear_stored_data(data_type)
            if result.get("success"):
                return web.json_response(result)
            return web.json_response(result, status=400)
        except Exception as e:
            logger.error(f"清空{data_type}数据失败: {e}")
            return web.json_response({
                "success": False,
                "error": str(e)
            }, status=500)