"""
数据完成部门 - HTTP路由
"""
from aiohttp import web
import logging
from datetime import datetime
from data_completion_department.receiver import get_receiver

logger = logging.getLogger(__name__)

def setup_data_completion_routes(app: web.Application):
    """设置数据完成部门路由"""
    
    # 根路由：/api/completion
    async def get_root(request):
        receiver = get_receiver()
        store = receiver.memory_store
        sources = []
        
        if store.get('private_data'):
            sources.append({
                "name": "private_data",
                "description": "私人数据（欧易+币安）",
                "endpoint": "/api/completion/private",
                "last_update": store['private_data'].get('received_at')
            })
        
        if store.get('market_data'):
            sources.append({
                "name": "market_data",
                "description": "行情数据",
                "endpoint": "/api/completion/market",
                "last_update": store['market_data'].get('received_at')
            })
        
        return web.json_response({
            "timestamp": datetime.now().isoformat(),
            "source_count": len(sources),
            "sources": sources
        })
    
    # 私人数据分路由
    async def get_private(request):
        receiver = get_receiver()
        data = receiver.memory_store.get('private_data')
        return web.json_response(data if data else {"note": "暂无数据"})
    
    # 行情数据分路由
    async def get_market(request):
        receiver = get_receiver()
        data = receiver.memory_store.get('market_data')
        return web.json_response(data if data else {"note": "暂无数据"})
    
    app.router.add_get('/api/completion', get_root)
    app.router.add_get('/api/completion/private', get_private)
    app.router.add_get('/api/completion/market', get_market)
    
    logger.info("✅ 数据完成部门路由注册完成")