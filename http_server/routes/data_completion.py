"""
数据完成部门 - HTTP路由 (aiohttp版)
"""
from aiohttp import web
import logging
from datetime import datetime
from data_completion_department.receiver import get_receiver

logger = logging.getLogger(__name__)

def setup_data_completion_routes(app: web.Application):
    """设置数据完成部门路由"""
    
    # ===== 根路由：/api/completion =====
    async def get_completion_root(request):
        """返回数据源概览"""
        receiver = get_receiver()
        store = receiver.memory_store
        sources = []
        
        # 私人数据源
        if store.get('private_data'):
            private_item = store['private_data']
            private_content = private_item.get('data', {})
            inner_data = private_content.get('data', {})
            
            exchanges = []
            if inner_data.get('okx'):
                exchanges.append('okx')
            if inner_data.get('binance'):
                exchanges.append('binance')
            
            sources.append({
                "name": "private_data",
                "description": f"私人数据 - 包含: {', '.join(exchanges)}",
                "endpoint": "/api/completion/private",
                "last_update": private_item.get('received_at')
            })
        
        # 行情数据源
        if store.get('market_data'):
            market_item = store['market_data']
            market_content = market_item.get('data', {})
            market_data = market_content.get('data', {})
            contract_count = market_data.get('total_contracts', 0) if isinstance(market_data, dict) else 0
            
            sources.append({
                "name": "market_data",
                "description": "聚合行情数据",
                "contract_count": contract_count,
                "endpoint": "/api/completion/market",
                "last_update": market_item.get('received_at')
            })
        
        return web.json_response({
            "timestamp": datetime.now().isoformat(),
            "source_count": len(sources),
            "sources": sources,
            "note": "数据完成部门根路由"
        })
    
    # ===== 分路由：/api/completion/private =====
    async def get_private_detail(request):
        """返回私人数据详情（完整的一条数据）"""
        receiver = get_receiver()
        private_data = receiver.memory_store.get('private_data')
        if private_data:
            return web.json_response(private_data)
        return web.json_response({
            "timestamp": datetime.now().isoformat(),
            "note": "暂无私人数据"
        })
    
    # ===== 分路由：/api/completion/market =====
    async def get_market_detail(request):
        """返回行情数据详情"""
        receiver = get_receiver()
        market_data = receiver.memory_store.get('market_data')
        if market_data:
            return web.json_response(market_data)
        return web.json_response({
            "timestamp": datetime.now().isoformat(),
            "note": "暂无行情数据"
        })
    
    # 注册路由
    app.router.add_get('/api/completion', get_completion_root)
    app.router.add_get('/api/completion/private', get_private_detail)
    app.router.add_get('/api/completion/market', get_market_detail)
    
    logger.info("✅ 数据完成部门路由注册完成: /api/completion/*")