"""
账户与市场数据接口模块
处理账户余额、持仓、市场数据等查询
"""
from aiohttp import web
import datetime
import logging

from shared_data.data_store import data_store
from ..exchange_api import ExchangeAPI
from ..auth import require_auth

logger = logging.getLogger(__name__)


@require_auth
async def get_market_data(request: web.Request) -> web.Response:
    """获取市场数据"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.query.get('symbol')
        
        market_data = await data_store.get_market_data(exchange, symbol)
        
        return web.json_response({
            "exchange": exchange,
            "symbol": symbol or "all",
            "data": market_data,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取市场数据失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


@require_auth
async def get_account_balance(request: web.Request) -> web.Response:
    """获取账户余额"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        
        # 初始化交易所API
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        balance = await api.fetch_account_balance()
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "balance": balance,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取账户余额失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


@require_auth
async def get_positions(request: web.Request) -> web.Response:
    """获取持仓"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        positions = await api.fetch_positions()
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "positions": positions,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取持仓失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


@require_auth
async def get_ticker(request: web.Request) -> web.Response:
    """获取ticker数据"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.query.get('symbol')
        
        if not symbol:
            return web.json_response({"error": "缺少symbol参数"}, status=400)
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        ticker = await api.fetch_ticker(symbol)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "ticker": ticker,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取ticker失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


@require_auth
async def get_connection_status(request: web.Request) -> web.Response:
    """获取连接状态"""
    try:
        exchange = request.match_info.get('exchange', '')
        
        connection_status = await data_store.get_connection_status(exchange or None)
        
        return web.json_response({
            "connection_status": connection_status,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取连接状态失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


def setup_account_routes(app: web.Application):
    """设置账户与市场数据路由"""
    # 市场数据
    app.router.add_get('/api/market/{exchange}', get_market_data)
    app.router.add_get('/api/market/{exchange}/{symbol}', get_market_data)
    
    # 账户数据
    app.router.add_get('/api/account/{exchange}/balance', get_account_balance)
    app.router.add_get('/api/account/{exchange}/positions', get_positions)
    
    # 实时数据
    app.router.add_get('/api/data/{exchange}/ticker', get_ticker)
    
    # 连接状态
    app.router.add_get('/api/status/connections', get_connection_status)
    app.router.add_get('/api/status/connections/{exchange}', get_connection_status)
    
    logger.info("✅ 账户与市场路由已加载: /api/market/*, /api/account/*, /api/data/*/ticker, /api/status/connections")
