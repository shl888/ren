"""
历史资金费率结算HTTP接口 - 正规化改造版（统一从market_data读取）
"""
from aiohttp import web
import logging
import os
import sys
import asyncio  # ✅ 加上这行！
from datetime import datetime
from typing import Dict, Any

# 设置导入路径
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from .manager import FundingSettlementManager
from .templates import get_html_page
from shared_data.data_store import data_store

logger = logging.getLogger(__name__)

_manager = FundingSettlementManager()

async def _startup_auto_fetch(app: web.Application):
    """服务器启动时自动获取历史资金费率数据（后台任务）"""
    
    async def background_fetch_task():
        try:
            if _manager.is_auto_fetched:
                return
            
            await asyncio.sleep(180)
            result = await _manager.fetch_funding_settlement()
            
            if result["success"]:
                logger.info(f"✅【历史费率】启动自动获取成功！获取币安 {result.get('filtered_count', 0)} 个合约")
                _manager.is_auto_fetched = True
            else:
                logger.warning(f"❌【历史费率】启动自动获取失败: {result.get('error')}")
        except Exception as e:
            logger.error(f"⚠️ 【历史费率】启动自动获取异常: {e}")
    
    asyncio.create_task(background_fetch_task())

async def get_settlement_public(request: web.Request) -> web.Response:
    """获取所有历史资金费率结算数据（从market_data读取）"""
    try:
        funding_data = await data_store.get_market_data(exchange="binance", data_type="funding_settlement")
        
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
        return web.json_response({"success": False, "error": str(e), "data": []})

async def get_settlement_status(request: web.Request) -> web.Response:
    """获取历史资金费率结算状态"""
    try:
        status = _manager.get_status()
        contracts = await data_store.get_market_data(exchange="binance", data_type="funding_settlement")
        sample_contracts = list(contracts.keys())[:5] if contracts else []
        
        return web.json_response({
            "success": True,
            "status": status,
            "sample_contracts": sample_contracts,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"❌【历史费率】获取状态失败: {e}")
        return web.json_response({"success": False, "error": str(e)}, status=500)

async def post_fetch_settlement(request: web.Request) -> web.Response:
    """手动触发获取历史资金费率结算数据"""
    try:
        result = await _manager.manual_fetch()
        if result.get("success"):
            _manager.is_auto_fetched = True
        return web.json_response(result)
    except Exception as e:
        logger.error(f"❌【历史费率】手动获取失败: {e}")
        return web.json_response({"success": False, "error": str(e)}, status=500)

async def get_settlement_page(request: web.Request) -> web.Response:
    """历史资金费率结算管理HTML页面"""
    try:
        contracts = await data_store.get_market_data(exchange="binance", data_type="funding_settlement")
        html_content = get_html_page(_manager, contracts)
        return web.Response(text=html_content, content_type='text/html')
    except Exception as e:
        logger.error(f"❌【历史费率】生成页面失败: {e}")
        return web.Response(text=f"❌【历史费率】页面生成错误: {e}", status=500)

def setup_funding_settlement_routes(app: web.Application):
    """设置历史资金费率结算路由"""
    app.on_startup.append(_startup_auto_fetch)
    app.router.add_get('/api/funding/settlement/public', get_settlement_public)
    app.router.add_get('/api/funding/settlement/status', get_settlement_status)
    app.router.add_post('/api/funding/settlement/fetch', post_fetch_settlement)
    app.router.add_get('/funding/settlement', get_settlement_page)
    
    logger.info("✅ 历史资金费率结算路由已加载（统一从market_data读取）")
