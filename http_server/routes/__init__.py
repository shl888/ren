"""
HTTP路由聚合模块
集中管理所有路由的导入和注册
"""
from aiohttp import web
import logging
import sys
import os
from datetime import datetime

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# ============ 导入各模块路由 ============
from .main import setup_main_routes
from .debug import setup_debug_routes
from .monitor import setup_monitor_routes
from public_http_fetcher.binance_funding_rate.api_routes import setup_funding_settlement_routes

logger = logging.getLogger(__name__)

def setup_private_data_processing_routes(app: web.Application):
    """设置私人数据处理模块路由"""
    try:
        from .private_data_processing import PrivateDataProcessingRoutes
        private_data_routes = PrivateDataProcessingRoutes()
        
        # 注册私人数据处理模块路由
        app.router.add_get('/api/private_data_processing/', private_data_routes.api_root)
        app.router.add_get('/api/private_data_processing/health', private_data_routes.health)
        app.router.add_get('/api/private_data_processing/data/private', private_data_routes.get_all_private_data)
        app.router.add_get('/api/private_data_processing/data/private/{exchange}', private_data_routes.get_private_data_by_exchange)
        app.router.add_get('/api/private_data_processing/data/private/{exchange}/{data_type}', private_data_routes.get_private_data_detail)
        
        logger.info("✅ 已注册私人数据处理模块路由（共5个端点）")
        return True
        
    except ImportError as e:
        logger.warning(f"无法导入私人数据处理路由: {e}")
        return False
    except Exception as e:
        logger.error(f"设置私人数据处理路由失败: {e}")
        return False

# ============ 数据完成部门路由 ============
def setup_data_completion_routes(app: web.Application):
    """设置数据完成部门路由"""
    try:
        from data_completion_department.receiver import get_receiver
        
        # ===== 根路由：/api/completion =====
        async def get_completion_root(request):
            """根路由：返回所有数据源概览"""
            receiver = get_receiver()
            store = receiver.memory_store
            sources = []
            
            # 私人数据源
            if store.get('private_data'):
                private_item = store['private_data']
                data_content = private_item.get('data', {})
                
                # 判断数据类型
                name = "private_data"
                if data_content.get('exchange') == 'okx':
                    name = "okx_complete"
                elif data_content.get('exchange') == 'binance':
                    name = "binance_semi"
                
                sources.append({
                    "name": name,
                    "description": f"私人数据 - {data_content.get('exchange', 'unknown')}",
                    "endpoint": "/api/completion/private",
                    "last_update": private_item.get('received_at')
                })
            
            # 行情数据源
            if store.get('market_data'):
                market_item = store['market_data']
                market_content = market_item.get('data', {})
                contract_count = 0
                if isinstance(market_content, dict):
                    contract_count = market_content.get('total_contracts', 0)
                
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
            """返回私人数据详情"""
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
        app.router.add_get('/api/completion', get_completion_root)        # 根
        app.router.add_get('/api/completion/private', get_private_detail) # 分
        app.router.add_get('/api/completion/market', get_market_detail)   # 分
        
        logger.info("✅ 已注册数据完成部门路由（根 + 2个分路由）")
        return True
        
    except ImportError as e:
        logger.warning(f"无法导入数据完成部门路由: {e}")
        return False
    except Exception as e:
        logger.error(f"设置数据完成部门路由失败: {e}")
        return False

def setup_routes(app: web.Application):
    """
    主路由设置函数 - 聚合所有模块
    """
    logger.info("开始加载路由模块...")
    
    # 基础路由
    setup_main_routes(app)
    
    # 功能路由
    setup_debug_routes(app)           # 已精简，只有websocket_status
    setup_monitor_routes(app)
    
    # 资金费率结算路由
    setup_funding_settlement_routes(app)
    
    # 私人数据处理模块路由
    setup_private_data_processing_routes(app)
    
    # 数据完成部门路由
    setup_data_completion_routes(app)
    
    # 获取当前路由总数
    total_routes = len(app.router.routes())
    
    logger.info("=" * 60)
    logger.info("✅ 所有路由模块加载完成")
    logger.info("📊 路由统计:")
    logger.info(f"   - 总路由数: {total_routes}")
    logger.info(f"   - 基础接口: /, /health, /public/ping (3个)")
    logger.info(f"   - 调试接口: /api/debug/websocket_status (1个)")
    logger.info(f"   - 监控接口: /api/monitor/* (3个)")
    logger.info(f"   - 资金费率: /api/funding/settlement/* (4个)")
    logger.info(f"   - 私人数据处理: /api/private_data_processing/* (5个)")
    logger.info(f"   - 数据完成部门: /api/completion/* (3个)")
    logger.info("=" * 60)
    logger.info("📌 公开数据路由已在 server.py 中注册: /api/public/data/* (2个)")
    logger.info("=" * 60)