"""
调试接口模块
提供WebSocket数据查看、资金费率查询等调试功能
"""
from aiohttp import web
import datetime
import logging
from typing import Dict, Any

from shared_data.data_store import data_store

logger = logging.getLogger(__name__)


# ============ 辅助函数（直接定义在本文件） ============
def _calculate_data_age(timestamp_str: str) -> float:
    """计算数据年龄（秒）"""
    if not timestamp_str:
        return float('inf')
    
    try:
        if 'T' in timestamp_str:
            # ISO格式
            try:
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                data_time = datetime.datetime.fromisoformat(timestamp_str)
            except ValueError:
                try:
                    if '.' in timestamp_str:
                        timestamp_str = timestamp_str.split('.')[0]
                    data_time = datetime.datetime.fromisoformat(timestamp_str)
                except:
                    return float('inf')
        else:
            try:
                ts = float(timestamp_str)
                if ts > 1e12:
                    ts = ts / 1000
                data_time = datetime.datetime.fromtimestamp(ts)
            except:
                return float('inf')
        
        now = datetime.datetime.now(datetime.timezone.utc)
        if data_time.tzinfo is None:
            data_time = data_time.replace(tzinfo=datetime.timezone.utc)
        
        return (now - data_time).total_seconds()
    except Exception:
        return float('inf')


def _count_data_types(exchange_data: Dict) -> Dict[str, int]:
    """统计数据类型数量"""
    stats = {
        "total_symbols": 0,
        "ticker": 0,
        "funding_rate": 0,
        "mark_price": 0,
        "other": 0
    }
    
    if not exchange_data:
        return stats
    
    stats["total_symbols"] = len(exchange_data)
    
    for symbol, data_dict in exchange_data.items():
        if isinstance(data_dict, dict):
            for data_type in data_dict:
                if data_type in stats:
                    stats[data_type] += 1
                elif data_type not in ['latest', 'store_timestamp']:
                    stats["other"] += 1
    
    return stats


def _get_sample_data(exchange_data: Dict, sample_size: int, show_types: bool = False) -> Dict:
    """获取抽样数据"""
    if not exchange_data:
        return {}
    
    sample = {}
    count = 0
    
    for symbol, data_dict in exchange_data.items():
        if count >= sample_size:
            break
            
        if not show_types and isinstance(data_dict, dict) and 'latest' in data_dict:
            # 只显示最新数据
            latest_type = data_dict['latest']
            if latest_type in data_dict and latest_type != 'latest':
                sample[symbol] = data_dict[latest_type]
                count += 1
        else:
            # 显示所有数据类型
            sample[symbol] = data_dict
            count += 1
    
    return sample


def _get_sort_key(data_item: Dict, sort_by: str) -> Any:
    """获取排序键"""
    if sort_by == 'rate':
        return data_item.get('funding_rate', 0)
    elif sort_by == 'abs_rate':
        return abs(data_item.get('funding_rate', 0))
    elif sort_by == 'symbol':
        return data_item.get('symbol', '')
    elif sort_by == 'age':
        return data_item.get('age_seconds', float('inf'))
    else:
        return 0


# ============ 主接口 ============
async def get_all_websocket_data(request: web.Request) -> web.Response:
    """
    【核心调试接口】查看WebSocket获取的所有市场数据
    地址：GET /api/debug/all_websocket_data
    修复版：显示所有数据类型，不覆盖
    """
    try:
        # 获取查询参数
        query = request.query
        show_all = query.get('show_all', '').lower() == 'true'
        show_types = query.get('show_types', '').lower() == 'true'
        sample_size = min(int(query.get('sample', 3)), 10)
        
        # 从共享存储中获取数据
        binance_all_data = await data_store.get_market_data("binance", get_latest=False)
        okx_all_data = await data_store.get_market_data("okx", get_latest=False)
        
        # 统计不同类型的数据量
        binance_stats = _count_data_types(binance_all_data)
        okx_stats = _count_data_types(okx_all_data)
        
        # 准备返回的数据
        response_data = {
            "success": True,
            "timestamp": datetime.datetime.now().isoformat(),
            "summary": {
                "binance_symbols_count": len(binance_all_data),
                "okx_symbols_count": len(okx_all_data),
                "total_symbols": len(binance_all_data) + len(okx_all_data),
                "data_type_stats": {
                    "binance": binance_stats,
                    "okx": okx_stats
                }
            }
        }
        
        if show_all:
            response_data['data'] = {
                "binance": binance_all_data,
                "okx": okx_all_data
            }
        else:
            response_data['sample'] = {
                "binance": _get_sample_data(binance_all_data, sample_size, show_types),
                "okx": _get_sample_data(okx_all_data, sample_size, show_types)
            }
            
            # 动态提示
            hints = []
            hints.append("如需查看全部数据，请添加参数 ?show_all=true")
            if not show_types:
                hints.append("如需查看所有数据类型，请添加参数 ?show_types=true")
            hints.append(f"当前显示抽样数量: {sample_size} (可调整: ?sample=5)")
            
            response_data['hint'] = " | ".join(hints)
        
        return web.json_response(response_data)
        
    except Exception as e:
        logger.error(f"获取WebSocket数据失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "timestamp": datetime.datetime.now().isoformat()
        }, status=500)


async def get_symbol_detail(request: web.Request) -> web.Response:
    """
    【调试接口】查看指定交易对的详细数据
    地址：GET /api/debug/symbol/{exchange}/{symbol}
    增强版：显示所有数据类型
    """
    try:
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.match_info.get('symbol', '').upper()
        show_all_types = request.query.get('show_all_types', '').lower() == 'true'
        
        if exchange not in ['binance', 'okx']:
            return web.json_response({
                "success": False,
                "error": f"不支持的交易所: {exchange}"
            }, status=400)
        
        # 获取指定交易对数据（所有数据类型）
        data = await data_store.get_market_data(exchange, symbol, get_latest=False)
        
        if not data:
            return web.json_response({
                "success": False,
                "error": f"未找到数据: {exchange} {symbol}",
                "hint": "可能是: 1. 交易对名称错误 2. 该交易对未被订阅 3. 数据尚未到达"
            }, status=404)
        
        # 计算数据年龄
        for data_type, data_content in data.items():
            if isinstance(data_content, dict) and 'timestamp' in data_content:
                timestamp = data_content['timestamp']
                age_seconds = _calculate_data_age(timestamp)
                data_content['age_seconds'] = age_seconds
        
        response = {
            "success": True,
            "exchange": exchange,
            "symbol": symbol,
            "data_types_count": len(data),
            "data_types": list(data.keys()),
            "timestamp": datetime.datetime.now().isoformat()
        }
        
        if show_all_types or len(data) <= 3:
            # 显示所有数据类型
            response['data'] = data
        else:
            # 默认只显示最新数据
            if 'latest' in data and data['latest'] in data:
                latest_type = data['latest']
                response['data'] = {latest_type: data[latest_type]}
                response['hint'] = f"当前显示最新数据类型: {latest_type}，如需查看所有类型请添加参数 ?show_all_types=true"
            else:
                response['data'] = data
        
        return web.json_response(response)
        
    except Exception as e:
        logger.error(f"获取交易对数据失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)


async def get_websocket_status(request: web.Request) -> web.Response:
    """
    【调试接口】查看WebSocket连接池状态
    地址：GET /api/debug/websocket_status
    """
    try:
        # 获取连接状态
        connection_status = await data_store.get_connection_status()
        
        # 获取数据存储统计
        data_stats = data_store.get_market_data_stats()
        
        # 统计信息
        stats = {
            "total_exchanges": len(connection_status),
            "exchanges": list(connection_status.keys()),
            "data_statistics": data_stats
        }
        
        return web.json_response({
            "success": True,
            "timestamp": datetime.datetime.now().isoformat(),
            "stats": stats,
            "connection_status": connection_status
        })
        
    except Exception as e:
        logger.error(f"获取WebSocket状态失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500)


async def get_funding_rates(request: web.Request) -> web.Response:
    """
    【调试接口】获取资金费率数据（修复版：从market_data读取）
    地址：GET /api/debug/funding_rates
    """
    try:
        # 获取查询参数
        query = request.query
        sort_by = query.get('sort_by', 'rate')
        
        # ✅ 从market_data获取资金费率数据
        funding_data = await data_store.get_market_data(exchange="binance", data_type="funding_settlement")
        
        # 转换为旧格式兼容
        binance_data = {
            "count": len(funding_data),
            "data": {}
        }
        
        for symbol, data_dict in funding_data.items():
            funding_info = data_dict.get('funding_settlement', {})
            if funding_info:
                binance_data["data"][symbol] = {
                    "funding_rate": funding_info.get('funding_rate', 0),
                    "funding_time": funding_info.get('funding_time'),
                    "symbol": symbol,
                    "age_seconds": float('inf')
                }
        
        funding_rates = {"binance": binance_data}
        
        # 如果有排序要求
        if sort_by and binance_data["data"]:
            sorted_items = sorted(
                binance_data["data"].items(),
                key=lambda x: _get_sort_key(x[1], sort_by)
            )
            binance_data["data"] = dict(sorted_items)
        
        return web.json_response({
            "success": True,
            "timestamp": datetime.datetime.now().isoformat(),
            "funding_rates": funding_rates,
            "summary": {
                "total_exchanges": 1,
                "total_symbols": binance_data["count"],
                "exchanges": ["binance"]
            }
        })
        
    except Exception as e:
        logger.error(f"获取资金费率数据失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "timestamp": datetime.datetime.now().isoformat()
        }, status=500)


def setup_debug_routes(app: web.Application):
    """设置调试接口路由"""
    app.router.add_get('/api/debug/all_websocket_data', get_all_websocket_data)
    app.router.add_get('/api/debug/symbol/{exchange}/{symbol}', get_symbol_detail)
    app.router.add_get('/api/debug/websocket_status', get_websocket_status)
    app.router.add_get('/api/debug/funding_rates', get_funding_rates)
    
    logger.info("✅ 调试路由已加载: /api/debug/all_websocket_data, /api/debug/symbol/*, /api/debug/websocket_status, /api/debug/funding_rates")
