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
    """设置数据完成部门路由 - 根路由返回数据目录"""
    try:
        from data_completion_department.receiver import get_receiver
        
        async def get_completion_status(request):
            """根路由：返回数据目录，列出所有子数据源"""
            receiver = get_receiver()
            store = receiver.memory_store
            
            sources_list = []
            
            # 1. 私人数据源（如果存在）
            if store.get('private_data'):
                private_item = store['private_data']
                private_content = private_item.get('data', {})
                
                # 判断是欧易成品还是币安半成品
                data_type = "unknown"
                exchange_name = private_content.get('exchange', 'unknown')
                
                if exchange_name == 'okx':
                    data_type = "okx_complete"
                    description = "欧易交易所完整成品数据"
                elif exchange_name == 'binance':
                    data_type = "binance_semi"
                    description = "币安交易所半成品数据"
                else:
                    description = "私人数据"
                
                sources_list.append({
                    "name": data_type,
                    "description": description,
                    "endpoint": "/api/completion/data/private",
                    "last_update": private_item.get('received_at')
                })
            
            # 2. 行情数据源（如果存在）
            if store.get('market_data'):
                market_item = store['market_data']
                market_content = market_item.get('data', {})
                
                # 获取合约数量
                contract_count = 0
                if isinstance(market_content, dict):
                    contract_count = market_content.get('total_contracts', 0)
                
                sources_list.append({
                    "name": "market_data",
                    "description": "聚合行情数据（用于补全网安）",
                    "contract_count": contract_count,
                    "endpoint": "/api/completion/data/market",
                    "last_update": market_item.get('received_at')
                })
            
            return web.json_response({
                "timestamp": datetime.now().isoformat(),
                "source_count": len(sources_list),
                "sources": sources_list,
                "note": "数据完成部门状态目录，点击endpoint查看详情"
            })
        
        # 私人数据详情路由
        async def get_private_data(request):
            """返回私人数据详情"""
            receiver = get_receiver()
            private_data = receiver.memory_store.get('private_data')
            
            if private_data:
                return web.json_response(private_data)
            else:
                return web.json_response({
                    "timestamp": datetime.now().isoformat(),
                    "note": "暂无私人数据"
                })
        
        # 行情数据详情路由
        async def get_market_data(request):
            """返回行情数据详情"""
            receiver = get_receiver()
            market_data = receiver.memory_store.get('market_data')
            
            if market_data:
                return web.json_response(market_data)
            else:
                return web.json_response({
                    "timestamp": datetime.now().isoformat(),
                    "note": "暂无行情数据"
                })
        
        # 注册所有路由
        app.router.add_get('/api/completion/status', get_completion_status)
        app.router.add_get('/api/completion/data/private', get_private_data)
        app.router.add_get('/api/completion/data/market', get_market_data)
        
        logger.info("✅ 已注册数据完成部门路由（根目录 + 2个数据端点）")
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
    logger.info(f"   - 数据完成部门: /api/completion/* (3个)")  # status + 2个数据端点
    logger.info("=" * 60)
    logger.info("📌 公开数据路由已在 server.py 中注册: /api/public/data/* (2个)")
    logger.info("=" * 60)