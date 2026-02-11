"""
私人数据处理模块路由 - 仿制大脑格式，但完全独立（极简版）
只提供数据查看功能，不包含状态和清空功能
"""
import logging
import glob
import json
import os
from datetime import datetime
from aiohttp import web
from collections import defaultdict

logger = logging.getLogger(__name__)

class PrivateDataProcessingRoutes:
    """私人数据处理路由处理器 - 独立单例模式"""
    
    def __init__(self):
        # 使用全局单例
        from private_data_processing.manager import get_processor
        self.processor = get_processor()
        logger.info("✅ [私人数据处理路由] 初始化完成（独立单例模式）")
    
    async def api_root(self, request):
        """API根路径 - 显示所有可用端点"""
        api_docs = {
            "service": "私人数据处理模块API",
            "version": "1.1.0",
            "module_type": "独立数据处理模块（全局单例）",
            "data_source": "硬接收私人连接池推送数据",
            "function": "接收、分类、缓存币安订单数据；其他数据保持原样",
            "endpoints": {
                "/api/private_data_processing/": "API文档（本页）",
                "/api/private_data_processing/health": "健康检查",
                "/api/private_data_processing/data/private": "查看所有私人数据概览",
                "/api/private_data_processing/data/private/{exchange}": "按交易所查看私人数据",
                "/api/private_data_processing/data/private/{exchange}/{data_type}": "【即将废弃】查看特定私人数据详情",
                "/api/private_data_processing/data/private/binance/orders": "【新】查看币安所有订单分类数据（8种事件）"
            },
            "current_time": datetime.now().isoformat(),
            "note": "独立模块，与大脑数据完全分离，只处理私人连接池推送的数据"
        }
        return web.json_response(api_docs)
    
    async def health(self, request):
        """健康检查 - 只检查本模块"""
        try:
            processor = self.processor
            cache_dir = "binance/order_update"
            cache_exists = os.path.exists(cache_dir)
            cache_files = len(glob.glob(f"{cache_dir}/*.json")) if cache_exists else 0
            
            return web.json_response({
                "status": "healthy",
                "service": "private_data_processing_api",
                "timestamp": datetime.now().isoformat(),
                "module": "running",
                "storage_initialized": hasattr(processor, 'memory_store'),
                "data_types_count": len(processor.memory_store.get('private_data', {})) if hasattr(processor, 'memory_store') else 0,
                "cache_status": {
                    "directory_exists": cache_exists,
                    "cache_files_count": cache_files
                },
                "note": "私人数据处理API运行正常，币安订单分类缓存已启用"
            })
        except Exception as e:
            logger.error(f"[私人数据处理路由] 健康检查失败: {e}")
            return web.json_response({
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_all_private_data(self, request):
        """查看所有私人数据（概览）"""
        try:
            data = await self.processor.get_all_data()
            return web.json_response(data)
        except Exception as e:
            logger.error(f"[私人数据处理路由] 获取所有私人数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_private_data_by_exchange(self, request):
        """按交易所查看私人数据"""
        try:
            exchange = request.match_info.get('exchange', '').lower()
            if not exchange:
                return web.json_response({
                    "error": "需要指定交易所参数",
                    "timestamp": datetime.now().isoformat()
                }, status=400)
            
            data = await self.processor.get_data_by_exchange(exchange)
            return web.json_response(data)
        except Exception as e:
            logger.error(f"[私人数据处理路由] 按交易所获取私人数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_private_data_detail(self, request):
        """查看特定私人数据详情 - 即将废弃"""
        try:
            exchange = request.match_info.get('exchange', '').lower()
            data_type = request.match_info.get('data_type', '').lower()
            
            if not exchange or not data_type:
                return web.json_response({
                    "error": "需要指定交易所和数据类型参数",
                    "timestamp": datetime.now().isoformat()
                }, status=400)
            
            # 🚨 币安订单更新老接口 - 返回引导信息
            if exchange == 'binance' and data_type == 'order_update':
                return web.json_response({
                    "exchange": "binance",
                    "data_type": "order_update",
                    "message": "该接口已废弃，币安订单数据已按8种事件分类缓存",
                    "new_endpoint": "/api/private_data_processing/data/private/binance/orders",
                    "timestamp": datetime.now().isoformat()
                }, status=410)
            
            # 其他数据走老逻辑
            data = await self.processor.get_data_detail(exchange, data_type)
            
            if "error" in data:
                return web.json_response(data, status=404)
            return web.json_response(data)
            
        except Exception as e:
            logger.error(f"[私人数据处理路由] 获取私人数据详情失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)
    
    async def get_binance_orders(self, request):
        """【新】获取币安所有订单分类数据（直接从文件读）"""
        try:
            cache_dir = "binance/order_update"
            
            # 检查缓存目录是否存在
            if not os.path.exists(cache_dir):
                return web.json_response({
                    "exchange": "binance",
                    "timestamp": datetime.now().isoformat(),
                    "data": {},
                    "note": "暂无币安订单分类缓存数据"
                })
            
            # 8种事件类型
            categories = [
                '01_开仓', '02_设止损', '03_取消止损',
                '04_设止盈', '05_取消止盈', '06_触发止损',
                '07_触发止盈', '08_主动平仓'
            ]
            
            # 初始化结果结构
            result = {cat: defaultdict(list) for cat in categories}
            
            # 读取所有缓存文件
            pattern = f"{cache_dir}/*.json"
            for file_path in glob.glob(pattern):
                filename = os.path.basename(file_path)
                
                # 从文件名解析 合约_分类
                if '_' not in filename:
                    continue
                symbol, cat_with_ext = filename.split('_', 1)
                category = cat_with_ext.replace('.json', '')
                
                if category not in categories:
                    continue
                
                # 读取该文件所有记录
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            record = json.loads(line.strip())
                            # 只保留必要字段
                            result[category][symbol].append({
                                'timestamp': record.get('timestamp'),
                                'received_at': record.get('received_at'),
                                'data': record.get('data', {}).get('o', {}),
                                'order_id': record.get('data', {}).get('o', {}).get('i'),
                                'client_id': record.get('data', {}).get('o', {}).get('c')
                            })
                        except:
                            continue
            
            # 转换为普通dict，按时间倒序
            output = {}
            for cat in categories:
                output[cat] = {}
                for symbol, records in result[cat].items():
                    records.sort(key=lambda x: x['timestamp'] or '', reverse=True)
                    output[cat][symbol] = records[:20]  # 最多20条
            
            # 统计总条数
            total_count = sum(
                len(records) 
                for cat in output.values() 
                for records in cat.values()
            )
            
            return web.json_response({
                "exchange": "binance",
                "timestamp": datetime.now().isoformat(),
                "total_events": total_count,
                "cache_files_count": len(glob.glob(pattern)),
                "data": output,
                "note": "币安订单8种事件分类缓存数据，按合约分组"
            })
            
        except Exception as e:
            logger.error(f"[私人数据处理路由] 获取币安订单数据失败: {e}")
            return web.json_response({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }, status=500)


def setup_routes(app):
    """注册路由"""
    routes = PrivateDataProcessingRoutes()
    
    # 基础路由
    app.router.add_get('/api/private_data_processing/', routes.api_root)
    app.router.add_get('/api/private_data_processing/health', routes.health)
    
    # 数据查看路由
    app.router.add_get('/api/private_data_processing/data/private', routes.get_all_private_data)
    app.router.add_get('/api/private_data_processing/data/private/{exchange}', routes.get_private_data_by_exchange)
    app.router.add_get('/api/private_data_processing/data/private/{exchange}/{data_type}', routes.get_private_data_detail)
    
    # 🟢【新增】币安订单分类数据接口
    app.router.add_get('/api/private_data_processing/data/private/binance/orders', routes.get_binance_orders)
    
    logger.info("✅ [私人数据处理路由] 路由注册完成（含币安订单分类接口）")