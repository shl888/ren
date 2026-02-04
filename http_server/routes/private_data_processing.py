"""
私人数据处理模块路由 - 仿制大脑格式，但完全独立（极简版）
只提供数据查看功能，不包含状态和清空功能
"""
import logging
from aiohttp import web
from datetime import datetime

logger = logging.getLogger(__name__)

class PrivateDataProcessingRoutes:
    """私人数据处理路由处理器 - 独立单例模式"""
    
    def __init__(self):
        # 🔴 关键区别：不需要大脑实例，使用全局单例
        from private_data_processing.manager import get_processor
        self.processor = get_processor()
        logger.info("✅ [私人数据处理路由] 初始化完成（独立单例模式）")
    
    async def api_root(self, request):
        """API根路径 - 显示所有可用端点"""
        api_docs = {
            "service": "私人数据处理模块API",
            "version": "1.0.0",
            "module_type": "独立数据处理模块（全局单例）",
            "data_source": "硬接收私人连接池推送数据",
            "function": "只接收、存储、查看私人数据（最新一份）",
            "endpoints": {
                "/api/private_data_processing/": "API文档（本页）",
                "/api/private_data_processing/health": "健康检查",
                "/api/private_data_processing/data/private": "查看所有私人数据",
                "/api/private_data_processing/data/private/{exchange}": "按交易所查看私人数据",
                "/api/private_data_processing/data/private/{exchange}/{data_type}": "查看特定私人数据详情"
            },
            "current_time": datetime.now().isoformat(),
            "note": "独立模块，与大脑数据完全分离，只处理私人连接池推送的数据"
        }
        return web.json_response(api_docs)
    
    async def health(self, request):
        """健康检查 - 只检查本模块"""
        try:
            # 简单检查处理器是否已初始化
            processor = self.processor
            return web.json_response({
                "status": "healthy",
                "service": "private_data_processing_api",
                "timestamp": datetime.now().isoformat(),
                "module": "running",
                "storage_initialized": hasattr(processor, 'memory_store'),
                "data_types_count": len(processor.memory_store.get('private_data', {})) if hasattr(processor, 'memory_store') else 0,
                "note": "私人数据处理API运行正常"
            })
        except Exception as e:
            logger.error(f"[私人数据处理路由] 健康检查失败: {e}")
            return web.json_response({
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_all_private_data(self, request):
        """查看所有私人数据（概览）"""
        try:
            data = await self.processor.get_all_data()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"[私人数据处理路由] 获取所有私人数据失败: {e}")
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
            
            data = await self.processor.get_data_by_exchange(exchange)
            return web.json_response(data)
        except Exception as e:
            logger.error(f"[私人数据处理路由] 按交易所获取私人数据失败: {e}")
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
            
            data = await self.processor.get_data_detail(exchange, data_type)
            
            if "error" in data:
                return web.json_response(data, status=404)
            return web.json_response(data)
        except Exception as e:
            logger.error(f"[私人数据处理路由] 获取私人数据详情失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)