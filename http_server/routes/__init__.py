"""
HTTP路由聚合模块
集中管理所有路由的导入和注册
"""
from aiohttp import web
import logging
import sys
import os

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# ============ 导入各模块路由 ============
from .main import setup_main_routes
from .debug import setup_debug_routes
from .monitor import setup_monitor_routes
from funding_settlement.api_routes import setup_funding_settlement_routes

logger = logging.getLogger(__name__)

def setup_private_data_processing_routes(app: web.Application):
    """设置私人数据处理模块路由"""
    try:
        from .private_data_processing import PrivateDataProcessingRoutes
        private_data_routes = PrivateDataProcessingRoutes()
        
        # 注册私人数据处理模块路由（仿制brain.py格式，但只保留5个端点）
        app.router.add_get('/api/private_data_processing/', private_data_routes.api_root)
        app.router.add_get('/api/private_data_processing/health', private_data_routes.health)
        app.router.add_get('/api/private_data_processing/data/private', private_data_routes.get_all_private_data)
        app.router.add_get('/api/private_data_processing/data/private/{exchange}', private_data_routes.get_private_data_by_exchange)
        app.router.add_get('/api/private_data_processing/data/private/{exchange}/{data_type}', private_data_routes.get_private_data_detail)
        # 删除status和clear端点
        
        logger.info("✅ 已注册私人数据处理模块路由（共5个端点）")
        return True
        
    except ImportError as e:
        logger.warning(f"无法导入私人数据处理路由: {e}")
        return False
    except Exception as e:
        logger.error(f"设置私人数据处理路由失败: {e}")
        return False

def setup_routes(app: web.Application):
    """
    主路由设置函数 - 聚合所有模块
    """
    logger.info("开始加载路由模块...")
    
    # 基础路由
    setup_main_routes(app)
    
    # 功能路由
    setup_debug_routes(app)
    setup_monitor_routes(app)
    
    # 资金费率结算路由
    setup_funding_settlement_routes(app)
    
    # ✅ 新增：私人数据处理模块路由
    setup_private_data_processing_routes(app)
    
    logger.info("=" * 60)
    logger.info("✅ 所有路由模块加载完成")
    logger.info("📊 路由统计:")
    logger.info(f"   - 总路由数: {len(app.router.routes())}")
    logger.info(f"   - 调试接口: /api/debug/* (4个)")
    logger.info(f"   - 监控接口: /api/monitor/* (3个)")
    logger.info(f"   - 资金费率: /api/funding/settlement/* (3个)")
    logger.info(f"   - 私人数据处理: /api/private_data_processing/* (5个)")
    logger.info(f"   - 基础接口: /, /health, /public/ping (3个)")
    logger.info("=" * 60)
    