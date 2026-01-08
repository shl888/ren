"""
历史资金费率结算HTTP接口 - 精简版（无需密码）
"""
from aiohttp import web
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any
import asyncio

# 设置导入路径
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from .manager import FundingSettlementManager
from .templates import get_html_page

logger = logging.getLogger(__name__)

# 创建管理器实例
_manager = FundingSettlementManager()


# ✅ 新增：启动时自动获取的任务
async def _startup_auto_fetch(app: web.Application):
    """
    服务器启动时自动获取一次历史资金费率数据
    """
    logger.info("=" * 60)
    logger.info("📝【历史费率】 启动时自动获取历史资金费率数据...")
    logger.info(f"   时间: {datetime.now().isoformat()}")
    logger.info("=" * 60)
    
    try:
        # 检查是否已经自动获取过
        if _manager.is_auto_fetched:
            logger.info("⏭️【历史费率】  已经自动获取过，跳过本次启动获取")
            return
        
        # ✅【修改】延迟3分钟启动，确保所有初始化完成，避免被封IP
        logger.info("⏳ 延迟3分钟启动，确保市场数据加载完成...")
        await asyncio.sleep(180)  # 180秒 = 3分钟
        
        logger.info("📡【历史费率】 开始获取币安资金费率结算数据...")
        result = await _manager.fetch_funding_settlement()
        
        if result["success"]:
            logger.info(f"✅【历史费率】 启动自动获取成功！获取到币安 {result.get('filtered_count', 0)} 个合约")
            logger.info(f"   ️🤔【历史费率】权重消耗: {result.get('weight_used', 0)}")
            # 标记为已自动获取
            _manager.is_auto_fetched = True
        else:
            logger.warning(f"️❌ 【历史费率】 启动自动获取失败: {result.get('error')}")
            logger.warning("⚠️【历史费率】 将在第一次手动获取时重试")
            
    except Exception as e:
        logger.error(f"⚠️ 【历史费率】启动自动获取异常: {e}")
        import traceback
        logger.error(traceback.format_exc())


# ✅ 公开的API（无需密码）
async def get_settlement_public(request: web.Request) -> web.Response:
    """
    获取所有历史资金费率结算数据（无需密码）
    GET /api/funding/settlement/public
    """
    try:
        from shared_data.data_store import data_store
        
        funding_data = data_store.funding_settlement.get('binance', {})
        
        # 格式化为详细数据
        formatted_data = []
        for symbol, data in funding_data.items():
            formatted_data.append({
                "exchange": "binance",
                "symbol": symbol,
                "data_type": "funding_settlement",
                "funding_rate": data.get('funding_rate'),
                "funding_time": data.get('funding_time'),
                "next_funding_time": data.get('next_funding_time'),
                "timestamp": datetime.now().isoformat(),
                "source": "api"
            })
        
        return web.json_response({
            "success": True,
            "count": len(formatted_data),
            "data": formatted_data
        })
        
    except Exception as e:
        logger.error(f"⚠️【历史费率】公共API错误: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "data": []
        })


# ✅ 查看状态（无需密码）
async def get_settlement_status(request: web.Request) -> web.Response:
    """获取历史资金费率结算状态（无需密码）"""
    try:
        status = _manager.get_status()
        from shared_data.data_store import data_store
        
        contracts = data_store.funding_settlement.get('binance', {})
        sample_contracts = list(contracts.keys())[:5] if contracts else []
        
        return web.json_response({
            "success": True,
            "status": status,
            "sample_contracts": sample_contracts,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌【历史费率】获取状态失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, status=500)


# ✅ 手动触发获取（无需密码）
async def post_fetch_settlement(request: web.Request) -> web.Response:
    """手动触发获取历史资金费率结算数据（无需密码）"""
    try:
        result = await _manager.manual_fetch()
        
        # ✅ 无论是手动还是自动，只要成功就标记为已获取
        if result.get("success"):
            _manager.is_auto_fetched = True
        
        return web.json_response(result)
        
    except Exception as e:
        logger.error(f"❌【历史费率】手动获取失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, status=500)


# ✅ HTML页面（无需密码）
async def get_settlement_page(request: web.Request) -> web.Response:
    """历史资金费率结算管理HTML页面（无需密码）"""
    try:
        from shared_data.data_store import data_store
        
        contracts = data_store.funding_settlement.get('binance', {})
        html_content = get_html_page(_manager)
        return web.Response(text=html_content, content_type='text/html')
        
    except Exception as e:
        logger.error(f"❌【历史费率】生成页面失败: {e}")
        return web.Response(text=f"❌【历史费率】页面生成错误: {e}", status=500)


def setup_funding_settlement_routes(app: web.Application):
    """
    设置历史资金费率结算路由（精简版，无需密码）
    """
    # ✅ 注册启动时自动获取任务
    app.on_startup.append(_startup_auto_fetch)
    
    # ✅ 所有接口都无需密码
    app.router.add_get('/api/funding/settlement/public', get_settlement_public)
    app.router.add_get('/api/funding/settlement/status', get_settlement_status)
    app.router.add_post('/api/funding/settlement/fetch', post_fetch_settlement)
    app.router.add_get('/funding/settlement', get_settlement_page)
    
    logger.info("✅ 历史资金费率结算路由已加载（无需密码）:")
    logger.info("   - GET  /api/funding/settlement/public")
    logger.info("   - GET  /api/funding/settlement/status")
    logger.info("   - POST /api/funding/settlement/fetch")
    logger.info("   - GET  /funding/settlement")
    logger.info("   - 📝【历史费率】 服务器启动时自动获取一次")