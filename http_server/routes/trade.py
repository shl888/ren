"""
交易接口模块
处理订单创建、取消、杠杆设置等交易操作
"""
from aiohttp import web
import datetime
import logging
from typing import Optional

from ..exchange_api import ExchangeAPI
from ..auth import require_auth

logger = logging.getLogger(__name__)


@require_auth
async def create_order(request: web.Request) -> web.Response:
    """创建订单"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        data = await request.json()
        
        # 验证必要参数
        required = ['symbol', 'type', 'side', 'amount']
        for field in required:
            if field not in data:
                return web.json_response({"error": f"缺少必要参数: {field}"}, status=400)
        
        symbol = data['symbol']
        order_type = data['type']
        side = data['side']
        amount = float(data['amount'])
        price = float(data.get('price', 0))
        params = data.get('params', {})
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        order = await api.create_order(symbol, order_type, side, amount, price, params)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "order": order,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"创建订单失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


@require_auth
async def cancel_order(request: web.Request) -> web.Response:
    """取消订单"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        data = await request.json()
        
        if 'symbol' not in data or 'order_id' not in data:
            return web.json_response({"error": "缺少symbol或order_id参数"}, status=400)
        
        symbol = data['symbol']
        order_id = data['order_id']
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        result = await api.cancel_order(symbol, order_id)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "result": result,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"取消订单失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


@require_auth
async def get_open_orders(request: web.Request) -> web.Response:
    """获取未成交订单"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.query.get('symbol')
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        orders = await api.fetch_open_orders(symbol)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "open_orders": orders,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取未成交订单失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


@require_auth
async def get_order_history(request: web.Request) -> web.Response:
    """获取订单历史"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.query.get('symbol')
        limit = int(request.query.get('limit', 100))
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        orders = await api.fetch_order_history(symbol, limit=limit)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "order_history": orders,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"获取订单历史失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


@require_auth
async def set_leverage(request: web.Request) -> web.Response:
    """设置杠杆"""
    try:
        exchange = request.match_info.get('exchange', '').lower()
        data = await request.json()
        
        if 'symbol' not in data or 'leverage' not in data:
            return web.json_response({"error": "缺少symbol或leverage参数"}, status=400)
        
        symbol = data['symbol']
        leverage = int(data['leverage'])
        
        api = ExchangeAPI(exchange)
        if not await api.initialize():
            return web.json_response({"error": f"{exchange} API初始化失败"}, status=400)
        
        result = await api.set_leverage(symbol, leverage)
        await api.close()
        
        return web.json_response({
            "exchange": exchange,
            "result": result,
            "timestamp": datetime.datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"设置杠杆失败: {e}")
        return web.json_response({"error": str(e)}, status=500)


def setup_trade_routes(app: web.Application):
    """设置交易接口路由"""
    app.router.add_post('/api/trade/{exchange}/order', create_order)
    app.router.add_post('/api/trade/{exchange}/cancel', cancel_order)
    app.router.add_get('/api/trade/{exchange}/open-orders', get_open_orders)
    app.router.add_get('/api/trade/{exchange}/order-history', get_order_history)
    app.router.add_post('/api/trade/{exchange}/leverage', set_leverage)
    
    logger.info("✅ 交易路由已加载: /api/trade/{exchange}/order, /api/trade/{exchange}/cancel, /api/trade/{exchange}/open-orders, /api/trade/{exchange}/order-history, /api/trade/{exchange}/leverage")
