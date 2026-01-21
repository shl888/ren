"""
数据管理器 - 负责数据接收、存储和推送
"""
import asyncio
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class DataManager:
    def __init__(self, brain):
        self.brain = brain
        
        # 数据状态跟踪
        self.last_market_time = None
        self.last_market_count = 0
        self.last_account_time = None
        self.last_trade_time = None
        
        # 内存存储
        self.memory_store = {
            'market_data': {},
            'private_data': {},
            'encrypted_keys': {},
            'env_apis': self._load_apis_from_env(),
            'exchange_tokens': {}
        }
    
    def _load_apis_from_env(self):
        """从环境变量加载API凭证"""
        apis = {
            'binance': {
                'api_key': os.getenv('BINANCE_API_KEY'),
                'api_secret': os.getenv('BINANCE_API_SECRET'),
            },
            'okx': {
                'api_key': os.getenv('OKX_API_KEY'),
                'api_secret': os.getenv('OKX_API_SECRET'),
                'passphrase': os.getenv('OKX_passphrase', ''),
            }
        }
        
        # 验证凭证是否存在
        for exchange, creds in apis.items():
            if not creds['api_key'] or not creds['api_secret']:
                logger.warning(f"⚠️【智能大脑】环境变量中{exchange}的API凭证不完整")
        
        logger.info(f"✅【智能大脑】已从环境变量加载API凭证")
        return apis
    
    # ==================== HTTP API处理器 ====================
    
    async def handle_api_root(self, request):
        """API根路径"""
        from aiohttp import web
        api_docs = {
            "service": "智能大脑数据管理器API",
            "version": "1.0.0",
            "endpoints": {
                "/api/brain/health": "健康检查",
                "/api/brain/data": "查看所有存储数据",
                "/api/brain/data/market": "查看市场数据",
                "/api/brain/data/private": "查看私人数据",
                "/api/brain/status": "查看数据状态",
                "/api/brain/data/clear": "清空数据（谨慎使用）"
            },
            "current_time": datetime.now().isoformat()
        }
        return web.json_response(api_docs)
    
    async def handle_health(self, request):
        """健康检查"""
        from aiohttp import web
        return web.json_response({
            "status": "healthy",
            "service": "data_manager",
            "timestamp": datetime.now().isoformat(),
            "memory_store_stats": {
                "market_data_count": len(self.memory_store['market_data']),
                "private_data_count": len(self.memory_store['private_data']),
                "encrypted_keys_count": len(self.memory_store['encrypted_keys']),
                "exchange_tokens_count": len(self.memory_store['exchange_tokens'])
            }
        })
    
    async def handle_get_all_data(self, request):
        """查看所有存储数据（概览）"""
        from aiohttp import web
        response = {
            "timestamp": datetime.now().isoformat(),
            "market_data": {
                "count": len(self.memory_store['market_data']),
                "keys": list(self.memory_store['market_data'].keys()),
                "last_update": self._format_time_diff(self.last_market_time) if self.last_market_time else "从未更新"
            },
            "private_data": {
                "count": len(self.memory_store['private_data']),
                "keys": list(self.memory_store['private_data'].keys()),
                "last_account_update": self._format_time_diff(self.last_account_time) if self.last_account_time else "从未更新",
                "last_trade_update": self._format_time_diff(self.last_trade_time) if self.last_trade_time else "从未更新"
            },
            "encrypted_keys": {
                "count": len(self.memory_store['encrypted_keys']),
                "keys": list(self.memory_store['encrypted_keys'].keys())
            },
            "exchange_tokens": {
                "count": len(self.memory_store['exchange_tokens']),
                "keys": list(self.memory_store['exchange_tokens'].keys())
            }
        }
        return web.json_response(response)
    
    async def handle_get_market_data(self, request):
        """查看所有市场数据"""
        from aiohttp import web
        formatted_market_data = {}
        for key, data in self.memory_store['market_data'].items():
            formatted_market_data[key] = {
                "symbol": data.get('symbol'),
                "data_type": data.get('data_type'),
                "count": data.get('count', 0),
                "received_at": data.get('received_at'),
                "raw_data_sample": data.get('raw_data')[:1] if isinstance(data.get('raw_data'), list) and len(data.get('raw_data')) > 0 else data.get('raw_data')
            }
        
        response = {
            "timestamp": datetime.now().isoformat(),
            "total_count": len(self.memory_store['market_data']),
            "market_data": formatted_market_data,
            "stats": {
                "last_update": self._format_time_diff(self.last_market_time) if self.last_market_time else "从未更新",
                "last_count": self.last_market_count
            }
        }
        return web.json_response(response)
    
    async def handle_get_market_data_by_exchange(self, request):
        """按交易所查看市场数据"""
        from aiohttp import web
        exchange = request.match_info.get('exchange', '').lower()
        
        exchange_data = {}
        for key, data in self.memory_store['market_data'].items():
            if exchange in key.lower():
                exchange_data[key] = {
                    "symbol": data.get('symbol'),
                    "data_type": data.get('data_type'),
                    "count": data.get('count', 0),
                    "received_at": data.get('received_at'),
                    "raw_data": data.get('raw_data')
                }
        
        response = {
            "exchange": exchange,
            "timestamp": datetime.now().isoformat(),
            "count": len(exchange_data),
            "data": exchange_data
        }
        return web.json_response(response)
    
    async def handle_get_market_data_detail(self, request):
        """查看特定市场数据详情"""
        from aiohttp import web
        exchange = request.match_info.get('exchange', '').lower()
        symbol = request.match_info.get('symbol', '').upper()
        key = f"market_{symbol}"
        
        if key in self.memory_store['market_data']:
            data = self.memory_store['market_data'][key]
            response = {
                "key": key,
                "exchange": exchange,
                "symbol": symbol,
                "data": data,
                "timestamp": datetime.now().isoformat()
            }
            return web.json_response(response)
        else:
            return web.json_response({
                "error": f"未找到数据: {key}",
                "available_keys": list(self.memory_store['market_data'].keys())
            }, status=404)
    
    async def handle_get_private_data(self, request):
        """查看所有私人数据"""
        from aiohttp import web
        formatted_private_data = {}
        for key, data in self.memory_store['private_data'].items():
            formatted_private_data[key] = {
                "exchange": data.get('exchange'),
                "data_type": data.get('data_type'),
                "received_at": data.get('received_at'),
                "raw_data_keys": list(data.get('raw_data', {}).keys()) if isinstance(data.get('raw_data'), dict) else type(data.get('raw_data')).__name__
            }
        
        response = {
            "timestamp": datetime.now().isoformat(),
            "total_count": len(self.memory_store['private_data']),
            "private_data": formatted_private_data,
            "stats": {
                "last_account_update": self._format_time_diff(self.last_account_time) if self.last_account_time else "从未更新",
                "last_trade_update": self._format_time_diff(self.last_trade_time) if self.last_trade_time else "从未更新"
            }
        }
        return web.json_response(response)
    
    async def handle_get_private_data_by_exchange(self, request):
        """按交易所查看私人数据"""
        from aiohttp import web
        exchange = request.match_info.get('exchange', '').lower()
        
        exchange_data = {}
        for key, data in self.memory_store['private_data'].items():
            if key.startswith(f"{exchange}_"):
                exchange_data[key] = {
                    "exchange": data.get('exchange'),
                    "data_type": data.get('data_type'),
                    "received_at": data.get('received_at'),
                    "raw_data": data.get('raw_data')
                }
        
        response = {
            "exchange": exchange,
            "timestamp": datetime.now().isoformat(),
            "count": len(exchange_data),
            "data": exchange_data
        }
        return web.json_response(response)
    
    async def handle_get_private_data_detail(self, request):
        """查看特定私人数据详情"""
        from aiohttp import web
        exchange = request.match_info.get('exchange', '').lower()
        data_type = request.match_info.get('data_type', '').lower()
        key = f"{exchange}_{data_type}"
        
        if key in self.memory_store['private_data']:
            data = self.memory_store['private_data'][key]
            response = {
                "key": key,
                "exchange": exchange,
                "data_type": data_type,
                "data": data,
                "timestamp": datetime.now().isoformat()
            }
            return web.json_response(response)
        else:
            return web.json_response({
                "error": f"未找到数据: {key}",
                "available_keys": list(self.memory_store['private_data'].keys())
            }, status=404)
    
    async def handle_get_apis(self, request):
        """查看API凭证状态（隐藏敏感信息）"""
        from aiohttp import web
        safe_apis = {}
        for exchange, creds in self.memory_store['env_apis'].items():
            safe_apis[exchange] = {
                "api_key_exists": bool(creds.get('api_key')),
                "api_secret_exists": bool(creds.get('api_secret')),
                "passphrase_exists": bool(creds.get('passphrase', '')),
                "api_key_preview": creds.get('api_key', '')[:8] + "..." if creds.get('api_key') else None
            }
        
        response = {
            "timestamp": datetime.now().isoformat(),
            "apis": safe_apis,
            "warning": "敏感信息已隐藏，只显示存在性和预览"
        }
        return web.json_response(response)
    
    async def handle_get_status(self, request):
        """查看数据状态"""
        from aiohttp import web
        status = {
            "market_data": {
                "last_update": self._format_time_diff(self.last_market_time) if self.last_market_time else "从未更新",
                "last_count": self.last_market_count,
                "stored_count": len(self.memory_store['market_data'])
            },
            "private_data": {
                "account": {
                    "last_update": self._format_time_diff(self.last_account_time) if self.last_account_time else "从未更新",
                    "stored_count": len([k for k in self.memory_store['private_data'].keys() if 'account' in k])
                },
                "trade": {
                    "last_update": self._format_time_diff(self.last_trade_time) if self.last_trade_time else "从未更新",
                    "stored_count": len([k for k in self.memory_store['private_data'].keys() if 'order' in k or 'trade' in k])
                },
                "position": {
                    "stored_count": len([k for k in self.memory_store['private_data'].keys() if 'position' in k])
                }
            },
            "frontend_connection": {
                "enabled": self.brain.frontend_relay is not None,
                "stats": self.brain.frontend_relay.get_stats_summary() if self.brain.frontend_relay else {}
            },
            "timestamp": datetime.now().isoformat()
        }
        return web.json_response(status)
    
    async def handle_clear_data(self, request):
        """清空所有数据"""
        from aiohttp import web
        try:
            # 记录清空前状态
            before_stats = {
                "market_data_count": len(self.memory_store['market_data']),
                "private_data_count": len(self.memory_store['private_data'])
            }
            
            # 清空数据
            self.memory_store['market_data'].clear()
            self.memory_store['private_data'].clear()
            
            # 重置状态
            self.last_market_time = None
            self.last_market_count = 0
            self.last_account_time = None
            self.last_trade_time = None
            
            logger.warning(f"⚠️【智能大脑】通过API清空所有数据: {before_stats}")
            
            return web.json_response({
                "success": True,
                "message": "所有数据已清空",
                "before_stats": before_stats,
                "after_stats": {
                    "market_data_count": 0,
                    "private_data_count": 0
                },
                "timestamp": datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"❌【智能大脑】清空数据失败: {e}")
            return web.json_response({
                "success": False,
                "error": str(e)
            }, status=500)
    
    async def handle_clear_data_type(self, request):
        """清空特定类型数据"""
        from aiohttp import web
        data_type = request.match_info.get('data_type', '').lower()
        
        try:
            if data_type == 'market':
                before_count = len(self.memory_store['market_data'])
                self.memory_store['market_data'].clear()
                self.last_market_time = None
                self.last_market_count = 0
                message = f"清空市场数据，共{before_count}条"
                
            elif data_type == 'private':
                before_count = len(self.memory_store['private_data'])
                self.memory_store['private_data'].clear()
                self.last_account_time = None
                self.last_trade_time = None
                message = f"清空私人数据，共{before_count}条"
                
            else:
                return web.json_response({
                    "success": False,
                    "error": f"不支持的数据类型: {data_type}",
                    "supported_types": ["market", "private"]
                }, status=400)
            
            logger.warning(f"⚠️【智能大脑】通过API清空{data_type}数据")
            
            return web.json_response({
                "success": True,
                "message": message,
                "data_type": data_type,
                "before_count": before_count,
                "timestamp": datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"❌【智能大脑】清空{data_type}数据失败: {e}")
            return web.json_response({
                "success": False,
                "error": str(e)
            }, status=500)
    
    # ==================== 核心数据处理方法 ====================
    
    async def receive_market_data(self, processed_data):
        """
        接收市场数据处理后的数据
        并推送到前端
        """
        try:
            if isinstance(processed_data, list):
                self.last_market_count = len(processed_data)
                
                if logger.isEnabledFor(logging.DEBUG):
                    if processed_data and len(processed_data) > 0:
                        symbol = processed_data[0].get('symbol', 'unknown')
                        logger.debug(f"📣【智能大脑】收到批量数据: {len(processed_data)}条, 第一个合约: {symbol}")
            else:
                logger.warning(f"⚠️【智能大脑】 收到非列表类型市场数据: {type(processed_data)}")
                self.last_market_count = 1
            
            self.last_market_time = datetime.now()
            
            # ✅【新增】存储市场数据到memory_store
            await self.store_market_data(processed_data)
            
            # 推送到前端
            if self.brain.frontend_relay:
                try:
                    await self.brain.frontend_relay.broadcast_market_data(processed_data)
                    if isinstance(processed_data, list) and len(processed_data) > 0:
                        logger.debug(f"✅【智能大脑】 已推送市场数据到前端: {len(processed_data)}条")
                except Exception as e:
                    logger.error(f"️❌【智能大脑】推送市场数据到前端失败: {e}")
            
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收数据错误: {e}")
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据
        先存储，再推送到前端
        """
        try:
            data_type = private_data.get('data_type', 'unknown')
            exchange = private_data.get('exchange', 'unknown')
            
            now = datetime.now()
            
            # ✅【步骤1】先存储私人数据
            storage_key = f"{exchange}_{data_type}"
            stored_data = {
                'raw_data': private_data,
                'exchange': exchange,
                'data_type': data_type,
                'received_at': now.isoformat(),
                'timestamp': private_data.get('timestamp', now.isoformat())
            }
            self.memory_store['private_data'][storage_key] = stored_data
            
            # ✅【步骤2】记录日志
            if data_type == 'account_update' or data_type == 'account':
                self.last_account_time = now
                logger.info(f"💰【智能大脑】 收到账户私人数据: {exchange}")
            elif data_type == 'order_update' or data_type == 'trade':
                self.last_trade_time = now
                logger.info(f"📝【智能大脑】 收到交易私人数据: {exchange}")
            elif data_type == 'position_update':
                self.last_account_time = now
                logger.info(f"📊【智能大脑】 收到持仓私人数据: {exchange}")
            else:
                self.last_account_time = now
                logger.info(f"⚠️【智能大脑】 收到未知类型私人数据: {exchange}.{data_type}")
            
            # ✅【步骤3】后推送到前端
            if self.brain.frontend_relay:
                try:
                    # 这里推送的是存储后的数据（可以包含处理结果）
                    await self.brain.frontend_relay.broadcast_private_data({
                        'type': 'private_data',
                        'exchange': exchange,
                        'data_type': data_type,
                        'data': private_data,  # 原始数据或处理后的数据
                        'stored_at': now.isoformat(),
                        'has_stored': True  # 标记已存储
                    })
                    logger.debug(f"✅【智能大脑】 已推送私人数据到前端: {exchange}.{data_type}")
                except Exception as e:
                    logger.error(f"❌【智能大脑】推送私人数据到前端失败: {e}")
                
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收私人数据错误: {e}")
    
    def _format_time_diff(self, last_time):
        if not last_time:
            return "⚠️【智能大脑】从未收到"
        
        now = datetime.now()
        diff = now - last_time
        
        if diff.total_seconds() < 60:
            return f"{int(diff.total_seconds())}秒前"
        elif diff.total_seconds() < 3600:
            return f"{int(diff.total_seconds() / 60)}分钟前"
        else:
            return f"{int(diff.total_seconds() / 3600)}小时前"
    
    async def _log_data_status(self):
        """定期记录数据状态"""
        while self.brain.running:
            try:
                await asyncio.sleep(60)
                
                market_count = self.last_market_count
                market_time = self._format_time_diff(self.last_market_time)
                
                if self.last_account_time:
                    account_status = f"✅【智能大脑】已更新，{self._format_time_diff(self.last_account_time)}"
                else:
                    account_status = "⚠️【智能大脑】从未收到"
                    
                if self.last_trade_time:
                    trade_status = f"✅【智能大脑】已更新，{self._format_time_diff(self.last_trade_time)}"
                else:
                    trade_status = "❌【智能大脑】从未收到"
                
                # 前端连接状态
                if self.brain.frontend_relay:
                    frontend_stats = self.brain.frontend_relay.get_stats_summary()
                    frontend_clients = frontend_stats.get('clients_connected', 0)
                    frontend_status = f"✅【智能大脑】已连接 {frontend_clients} 个客户端"
                else:
                    frontend_status = "⚠️【智能大脑】未启用"
                    frontend_clients = 0
                
                status_msg = f"""【智能大脑】【大脑数据状态】
成品数据，{market_count}条，已更新。{market_time}
私人数据-账户：{account_status}
私人数据-交易：{trade_status}
前端连接：{frontend_status}"""
                
                logger.info(status_msg)
                
                # 推送系统状态到前端
                if self.brain.frontend_relay and frontend_clients > 0:
                    try:
                        system_status = {
                            'market_data': {
                                'count': market_count,
                                'last_update': market_time
                            },
                            'private_data': {
                                'account': account_status,
                                'trade': trade_status
                            },
                            'frontend': {
                                'clients': frontend_clients,
                                'messages_sent': frontend_stats.get('messages_broadcast', 0)
                            },
                            'timestamp': datetime.now().isoformat()
                        }
                        await self.brain.frontend_relay.broadcast_system_status(system_status)
                    except Exception as e:
                        logger.debug(f"❌【智能大脑】推送系统状态失败: {e}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【智能大脑】状态日志错误: {e}")
                await asyncio.sleep(10)
    
    async def store_market_data(self, data):
        """存储市场数据到内存 - 每个symbol独立覆盖存储"""
        try:
            if not data:
                return
                
            if isinstance(data, list) and len(data) > 0:
                # ✅ 遍历列表，每个symbol独立存储
                for item in data:
                    symbol = item.get('symbol', 'unknown')
                    if not symbol or symbol == 'unknown':
                        logger.warning(f"⚠️【智能大脑】跳过无symbol的数据: {item}")
                        continue
                    
                    storage_key = f"market_{symbol}"
                    
                    stored_data = {
                        'raw_data': item,  # 单条数据
                        'received_at': datetime.now().isoformat(),
                        'count': 1,
                        'symbol': symbol,
                        'data_type': 'single'
                    }
                    
                    # ✅ 新数据覆盖旧数据
                    self.memory_store['market_data'][storage_key] = stored_data
                
                # ✅ 记录统计信息
                unique_symbols = len(set([i.get('symbol') for i in data if 'symbol' in i]))
                logger.info(f"✅【智能大脑】批量存储市场数据，共{len(data)}条，涉及{unique_symbols}个合约")
                
            elif isinstance(data, dict):
                # 单个数据对象（保留原有逻辑）
                symbol = data.get('symbol', 'single_data')
                storage_key = f"market_{symbol}"
                
                stored_data = {
                    'raw_data': data,
                    'received_at': datetime.now().isoformat(),
                    'count': 1,
                    'symbol': symbol,
                    'data_type': 'single'
                }
                
                self.memory_store['market_data'][storage_key] = stored_data
                logger.debug(f"✅【智能大脑】存储市场数据: {storage_key}")
                
            else:
                logger.warning(f"⚠️【智能大脑】无法存储未知类型的市场数据: {type(data)}")
                
        except Exception as e:
            logger.error(f"❌【智能大脑】存储市场数据失败: {e}")
    
    async def store_private_data(self, data):
        """存储私人数据到内存"""
        # 注意：这个方法现在被receive_private_data直接替代了
        # 保留这个空方法是为了接口兼容
        pass
    
    async def push_to_frontend(self, data_type, data):
        """推送数据到前端"""
        # 这个通用方法可能被更专门的推送方法替代
        # 保留这个空方法是为了接口兼容
        pass
