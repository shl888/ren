"""
shared_data 路由模块 - 公开市场数据查询接口
从 debug.py 迁移而来，完全兼容原有参数
"""
from aiohttp import web
import datetime
import logging
import asyncio
from typing import Dict, Any

from .data_store import data_store

logger = logging.getLogger(__name__)


# ============ 辅助函数（保持同步，但会在线程池中调用）============
def _calculate_data_age(timestamp_str: str) -> float:
    """计算数据年龄（秒）"""
    if not timestamp_str:
        return float('inf')
    
    try:
        if 'T' in timestamp_str:
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
    """统计数据类型数量（同步函数，在线程池中执行）"""
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
    """获取抽样数据（同步函数，在线程池中执行）"""
    if not exchange_data:
        return {}
    
    sample = {}
    count = 0
    
    for symbol, data_dict in exchange_data.items():
        if count >= sample_size:
            break
            
        if not show_types and isinstance(data_dict, dict) and 'latest' in data_dict:
            latest_type = data_dict['latest']
            if latest_type in data_dict and latest_type != 'latest':
                sample[symbol] = data_dict[latest_type]
                count += 1
        else:
            sample[symbol] = data_dict
            count += 1
    
    return sample


# ============ 路由处理函数 ============

async def get_all_public_data(request: web.Request) -> web.Response:
    """
    获取所有公开市场数据
    原路径：/api/debug/all_websocket_data
    新路径：/api/public/data/all
    参数：?show_all=true&show_types=true&sample=5
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
        
        # ✅ [蚂蚁基因修复] 在线程池中执行同步函数
        loop = asyncio.get_event_loop()
        
        # 统计不同类型的数据量
        binance_stats, okx_stats = await asyncio.gather(
            loop.run_in_executor(None, _count_data_types, binance_all_data),
            loop.run_in_executor(None, _count_data_types, okx_all_data)
        )
        
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
            # ✅ [蚂蚁基因修复] 在线程池中执行抽样函数
            binance_sample, okx_sample = await asyncio.gather(
                loop.run_in_executor(None, _get_sample_data, binance_all_data, sample_size, show_types),
                loop.run_in_executor(None, _get_sample_data, okx_all_data, sample_size, show_types)
            )
            
            response_data['sample'] = {
                "binance": binance_sample,
                "okx": okx_sample
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
        logger.error(f"获取公开数据失败: {e}")
        return web.json_response({
            "success": False,
            "error": str(e),
            "timestamp": datetime.datetime.now().isoformat()
        }, status=500)


async def get_public_symbol_data(request: web.Request) -> web.Response:
    """
    查看指定交易对的公开数据
    原路径：/api/debug/symbol/{exchange}/{symbol}
    新路径：/api/public/data/{exchange}/{symbol}
    参数：?show_all_types=true
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
        
        # ✅ [蚂蚁基因修复] 获取事件循环
        loop = asyncio.get_event_loop()
        
        # 计算数据年龄 - 在线程池中执行同步函数
        for data_type, data_content in data.items():
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            if isinstance(data_content, dict) and 'timestamp' in data_content:
                timestamp = data_content['timestamp']
                # ✅ [蚂蚁基因修复] 在线程池中执行年龄计算
                age_seconds = await loop.run_in_executor(None, _calculate_data_age, timestamp)
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


def setup_public_data_routes(app: web.Application):
    """注册公开数据路由"""
    app.router.add_get('/api/public/data/all', get_all_public_data)
    app.router.add_get('/api/public/data/{exchange}/{symbol}', get_public_symbol_data)
    
    logger.info("=" * 60)
    logger.info("✅ 公开数据路由已注册:")
    logger.info("   GET /api/public/data/all")
    logger.info("   GET /api/public/data/{exchange}/{symbol}")
    logger.info("=" * 60)