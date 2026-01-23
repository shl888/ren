"""
HTTP路由聚合模块
集中管理所有路由的导入和注册
"""
from aiohttp import web
import logging
import datetime
import sys
import os
from typing import Dict, Any

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

# ============ 导入各模块路由 ============
from .main import setup_main_routes
from .debug import setup_debug_routes
# ❌ 删除交易相关路由导入
# from .trade import setup_trade_routes  
# from .account import setup_account_routes
from .monitor import setup_monitor_routes
from funding_settlement.api_routes import setup_funding_settlement_routes  # ✅ 新增

logger = logging.getLogger(__name__)


def setup_routes(app: web.Application):
    """
    主路由设置函数 - 聚合所有模块
    保持与原始routes.py完全兼容的接口
    """
    logger.info("开始加载路由模块...")
    
    # 基础路由
    setup_main_routes(app)
    
    # 功能路由
    setup_debug_routes(app)
    # ❌ 删除交易相关路由设置
    # setup_trade_routes(app)
    # setup_account_routes(app)
    setup_monitor_routes(app)
    
    # ✅ 新增：资金费率结算路由
    setup_funding_settlement_routes(app)
    
    logger.info("=" * 60)
    logger.info("✅ 所有路由模块加载完成")
    logger.info("📊 路由统计:")
    logger.info(f"   - 总路由数: {len(app.router.routes())}")
    logger.info(f"   - 调试接口: /api/debug/* (4个)")
    # ❌ 删除交易相关路由统计
    # logger.info(f"   - 交易接口: /api/trade/* (5个)")
    # logger.info(f"   - 账户接口: /api/account/* (2个)")
    logger.info(f"   - 市场数据: /api/market/*, /api/data/* (3个)")
    logger.info(f"   - 监控接口: /api/monitor/* (3个)")
    logger.info(f"   - 资金费率: /api/funding/settlement/* (3个)")
    logger.info(f"   - 基础接口: /, /health, /public/ping (3个)")
    logger.info("=" * 60)