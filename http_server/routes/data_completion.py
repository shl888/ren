"""
数据完成部门 - HTTP路由（按大脑模块标准重构）
"""
from aiohttp import web
import logging
from datetime import datetime
from data_completion_department.receiver import get_receiver

logger = logging.getLogger(__name__)


def setup_data_completion_routes(app: web.Application):
    """设置数据完成部门路由（和 brain.py 风格一致）"""
    receiver = get_receiver()
    
    # ===== 根路由：数据大纲 =====
    async def get_data_summary(request):
        """获取所有数据的大纲（按来源分类）"""
        try:
            data = receiver.get_data_summary()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取数据大纲失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    # ===== 行情数据分路由 =====
    async def get_public_market_data(request):
        """获取公开市场数据详情"""
        try:
            data = receiver.get_public_market_data()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取行情数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    # ===== 私人数据分路由 =====
    async def get_private_user_data(request):
        """获取私人用户数据详情"""
        try:
            data = receiver.get_private_user_data()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"获取私人数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    # ===== 注册路由 =====
    # 根路由
    app.router.add_get('/api/completion', get_data_summary)
    
    # 数据分路由（路径和大脑模块对齐：/data/源名称）
    app.router.add_get('/api/completion/data/public_market', get_public_market_data)
    app.router.add_get('/api/completion/data/private_user', get_private_user_data)
    
    logger.info("✅ 数据完成部门路由（重构版）注册完成")
    logger.info("   - GET /api/completion                 # 数据大纲")
    logger.info("   - GET /api/completion/data/public_market  # 行情数据")
    logger.info("   - GET /api/completion/data/private_user   # 私人数据")