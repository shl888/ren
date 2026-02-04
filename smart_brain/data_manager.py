# data_manager.py
"""
数据管理器 - 简化存储版
只存储原始数据，不添加额外包装
"""
import asyncio
import logging
import os
import time  # 🔴 新增导入
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
        
        # 🔴【新增】批量存储日志控制
        self.last_batch_log_time = 0
        self.batch_log_interval = 60  # 60秒打印一次
        
        # 内存存储（简化结构）
        self.memory_store = {
            'market_data': {},
            'private_data': {},
            'env_apis': self._load_apis_from_env(),
            'exchange_tokens': {}  # 专门存储listenKey
        }
    
    # ==================== 接收步骤 ====================
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据（简化版）
        直接存储连接池传递的原始数据
        """
        try:
            exchange = private_data.get('exchange', 'unknown')
            data_type = private_data.get('data_type', 'unknown')
            
            logger.info(f"📨 【接收】DataManager收到{exchange}.{data_type}数据")
            
            now = datetime.now()
            storage_key = f"{exchange}_{data_type}"
            
            # ==================== 【简化存储逻辑】 ====================
            if data_type == 'listen_key':
                # 🎯 单独处理listenKey，存到 exchange_tokens
                listen_key = private_data.get('data', {}).get('listenKey')
                if listen_key:
                    self.memory_store['exchange_tokens'][exchange] = {
                        'key': listen_key,
                        'updated_at': now.isoformat(),
                        'source': 'http_module',
                        'exchange': exchange,
                        'data_type': 'listen_key'
                    }
                    logger.info(f"✅ 【保存】{exchange} listenKey已保存: {listen_key[:5]}...")
                    
                    # 通知连接池
                    if hasattr(self.brain, 'private_pool') and self.brain.private_pool:
                        asyncio.create_task(self._notify_listen_key_updated(exchange, listen_key))
                else:
                    logger.warning(f"⚠️ 收到空的listenKey: {exchange}")
                
            else:
                # 🎯 直接存储私人数据，不添加包装
                self.memory_store['private_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': data_type,
                    'data': private_data.get('data', {}),  # 直接存储原始数据
                    'timestamp': private_data.get('timestamp', now.isoformat()),
                    'received_at': now.isoformat()
                }
                
                logger.debug(f"✅ 【保存】{exchange}.{data_type}已直接保存")
            
            # 记录日志
            if data_type == 'account_update' or data_type == 'account':
                self.last_account_time = now
                logger.info(f"💰【智能大脑】 收到账户私人数据: {exchange}")
            elif data_type == 'order_update' or data_type == 'trade':
                self.last_trade_time = now
                logger.info(f"📝【智能大脑】 收到交易私人数据: {exchange}")
            elif data_type == 'position_update':
                self.last_account_time = now
                logger.info(f"📊【智能大脑】 收到持仓私人数据: {exchange}")
            elif data_type == 'listen_key':
                # 已经在上面记录了
                pass
            else:
                self.last_account_time = now
                logger.info(f"⚠️【智能大脑】 收到未知类型私人数据: {exchange}.{data_type}")
            
            # ✅ 推送到前端 - 推送简化后的数据
            if self.brain.frontend_relay:
                try:
                    await self.brain.frontend_relay.broadcast_private_data({
                        'type': 'private_data_stored',
                        'exchange': exchange,
                        'data_type': data_type,
                        'storage_key': storage_key,
                        'data': private_data.get('data', {}),  # 只推送原始数据
                        'received_at': now.isoformat(),
                        'timestamp': private_data.get('timestamp', now.isoformat())
                    })
                    logger.debug(f"✅【智能大脑】已推送私人数据到前端: {exchange}.{data_type}，简化格式")
                except Exception as e:
                    logger.error(f"❌【智能大脑】推送私人数据到前端失败: {e}")
                    
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收私人数据失败: {e}")
    
    async def receive_market_data(self, processed_data):
        """
        接收市场数据处理后的数据（保持原有逻辑）
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
            
            # ✅ 存储市场数据到memory_store
            stored_data = await self._store_market_data_and_get_result(processed_data)
            
            # ✅ 推送到前端 - 推送存储后的数据
            if self.brain.frontend_relay:
                try:
                    await self.brain.frontend_relay.broadcast_market_data({
                        'type': 'market_data_stored',
                        'storage_type': 'market_data',
                        'stored_data': stored_data,
                        'stored_at': datetime.now().isoformat(),
                        'count': len(stored_data) if isinstance(stored_data, dict) else 1
                    })
                    
                    if isinstance(processed_data, list) and len(processed_data) > 0:
                        logger.debug(f"✅【智能大脑】已推送市场数据到前端: {len(processed_data)}条")
                except Exception as e:
                    logger.error(f"️❌【智能大脑】推送市场数据到前端失败: {e}")
            
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收数据错误: {e}")
    
    async def _store_market_data_and_get_result(self, data):
        """存储市场数据并返回存储格式的结果"""
        try:
            if not data:
                return {}
                
            storage_results = {}
            
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
                    # 记录结果用于推送
                    storage_results[storage_key] = stored_data
                
                # ✅ 记录统计信息 - 🔴【修改】每分钟打印一次
                unique_symbols = len(set([i.get('symbol') for i in data if 'symbol' in i]))
                
                current_time = time.time()
                if current_time - self.last_batch_log_time >= self.batch_log_interval:
                    logger.info(f"✅【智能大脑】批量存储市场数据，共{len(data)}条，涉及{unique_symbols}个合约")
                    self.last_batch_log_time = current_time
                
                return storage_results
                
            elif isinstance(data, dict):
                # 单个数据对象
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
                storage_results[storage_key] = stored_data
                logger.debug(f"✅【智能大脑】存储市场数据: {storage_key}")
                return storage_results
                
            else:
                logger.warning(f"⚠️【智能大脑】无法存储未知类型的市场数据: {type(data)}")
                return {}
                
        except Exception as e:
            logger.error(f"❌【智能大脑】存储市场数据失败: {e}")
            return {}
    
    # ==================== 数据查询接口 ====================
    
    async def get_listen_key(self, exchange: str):
        """供连接池只读查询当前令牌"""
        token_info = self.memory_store['exchange_tokens'].get(exchange)
        if token_info:
            logger.debug(f"📤 【读取】{exchange} listenKey被读取")
            return token_info.get('key')
        else:
            logger.warning(f"⚠️ 【读取】{exchange} listenKey不存在")
            return None
    
    async def get_api_credentials(self, exchange: str):
        """供HTTP模块和连接池只读查询API凭证"""
        return self.memory_store['env_apis'].get(exchange)
    
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
    
    async def get_market_data_summary(self):
        """获取市场数据概览"""
        formatted_market_data = {}
        for key, data in self.memory_store['market_data'].items():
            formatted_market_data[key] = {
                "symbol": data.get('symbol'),
                "data_type": data.get('data_type'),
                "count": data.get('count', 0),
                "received_at": data.get('received_at'),
                "raw_data_sample": data.get('raw_data')[:1] if isinstance(data.get('raw_data'), list) and len(data.get('raw_data')) > 0 else data.get('raw_data')
            }
        
        return {
            "timestamp": datetime.now().isoformat(),
            "total_count": len(self.memory_store['market_data']),
            "market_data": formatted_market_data,
            "stats": {
                "last_update": self._format_time_diff(self.last_market_time) if self.last_market_time else "从未更新",
                "last_count": self.last_market_count
            }
        }
    
    async def get_private_data_summary(self):
        """获取私人数据概览（简化版）"""
        formatted_private_data = {}
        for key, data in self.memory_store['private_data'].items():
            formatted_private_data[key] = {
                "exchange": data.get('exchange'),
                "data_type": data.get('data_type'),
                "received_at": data.get('received_at'),
                "timestamp": data.get('timestamp'),
                "data_keys": list(data.get('data', {}).keys()) if isinstance(data.get('data'), dict) else type(data.get('data')).__name__
            }
        
        return {
            "timestamp": datetime.now().isoformat(),
            "total_count": len(self.memory_store['private_data']),
            "private_data": formatted_private_data,
            "stats": {
                "last_account_update": self._format_time_diff(self.last_account_time) if self.last_account_time else "从未更新",
                "last_trade_update": self._format_time_diff(self.last_trade_time) if self.last_trade_time else "从未更新"
            }
        }
    
    async def get_api_credentials_status(self):
        """获取API凭证状态（隐藏敏感信息）"""
        safe_apis = {}
        for exchange, creds in self.memory_store['env_apis'].items():
            safe_apis[exchange] = {
                "api_key_exists": bool(creds.get('api_key')),
                "api_secret_exists": bool(creds.get('api_secret')),
                "passphrase_exists": bool(creds.get('passphrase', '')),
                "api_key_preview": creds.get('api_key', '')[:5] + "..." if creds.get('api_key') else None
            }
        
        return {
            "timestamp": datetime.now().isoformat(),
            "apis": safe_apis,
            "warning": "敏感信息已隐藏，只显示存在性和预览"
        }
    
    async def get_system_status(self):
        """获取系统状态"""
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
            "timestamp": datetime.now().isoformat()
        }
        
        # 添加前端连接状态（如果有）
        if self.brain.frontend_relay:
            frontend_stats = self.brain.frontend_relay.get_stats_summary()
            status["frontend_connection"] = {
                "enabled": True,
                "stats": frontend_stats
            }
        else:
            status["frontend_connection"] = {
                "enabled": False,
                "stats": {}
            }
        
        return status
    
    async def clear_stored_data(self, data_type: str = None):
        """清空存储的数据"""
        try:
            before_stats = {
                "market_data_count": len(self.memory_store['market_data']),
                "private_data_count": len(self.memory_store['private_data']),
                "exchange_tokens_count": len(self.memory_store['exchange_tokens'])
            }
            
            if data_type == 'market':
                # 只清空市场数据
                self.memory_store['market_data'].clear()
                self.last_market_time = None
                self.last_market_count = 0
                message = f"清空市场数据，共{before_stats['market_data_count']}条"
                
            elif data_type == 'private':
                # 只清空私人数据
                self.memory_store['private_data'].clear()
                self.last_account_time = None
                self.last_trade_time = None
                message = f"清空私人数据，共{before_stats['private_data_count']}条"
                
            elif data_type == 'tokens':
                # 只清空令牌数据
                token_count = before_stats['exchange_tokens_count']
                self.memory_store['exchange_tokens'].clear()
                message = f"清空令牌数据，共{token_count}条"
                
            elif data_type is None:
                # 清空所有数据
                self.memory_store['market_data'].clear()
                self.memory_store['private_data'].clear()
                self.memory_store['exchange_tokens'].clear()
                self.last_market_time = None
                self.last_market_count = 0
                self.last_account_time = None
                self.last_trade_time = None
                message = f"清空所有数据，市场数据{before_stats['market_data_count']}条，私人数据{before_stats['private_data_count']}条，令牌{before_stats['exchange_tokens_count']}条"
                
            else:
                return {
                    "success": False,
                    "error": f"不支持的数据类型: {data_type}",
                    "supported_types": ["market", "private", "tokens"]
                }
            
            logger.warning(f"⚠️【智能大脑】清空{data_type or '所有'}数据")
            
            return {
                "success": True,
                "message": message,
                "data_type": data_type or "all",
                "before_stats": before_stats,
                "after_stats": {
                    "market_data_count": len(self.memory_store['market_data']),
                    "private_data_count": len(self.memory_store['private_data']),
                    "exchange_tokens_count": len(self.memory_store['exchange_tokens'])
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌【智能大脑】清空数据失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_market_data_by_exchange(self, exchange: str):
        """按交易所获取市场数据"""
        exchange_data = {}
        for key, data in self.memory_store['market_data'].items():
            if exchange.lower() in key.lower():
                exchange_data[key] = {
                    "symbol": data.get('symbol'),
                    "data_type": data.get('data_type'),
                    "count": data.get('count', 0),
                    "received_at": data.get('received_at'),
                    "raw_data": data.get('raw_data')
                }
        
        return {
            "exchange": exchange,
            "timestamp": datetime.now().isoformat(),
            "count": len(exchange_data),
            "data": exchange_data
        }
    
    async def get_market_data_detail(self, exchange: str, symbol: str):
        """获取特定市场数据详情"""
        key = f"market_{symbol.upper()}"
        
        if key in self.memory_store['market_data']:
            data = self.memory_store['market_data'][key]
            return {
                "key": key,
                "exchange": exchange,
                "symbol": symbol.upper(),
                "data": data,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return {
                "error": f"未找到数据: {key}",
                "available_keys": list(self.memory_store['market_data'].keys())
            }
    
    async def get_private_data_by_exchange(self, exchange: str):
        """按交易所获取私人数据（简化版）"""
        exchange_data = {}
        for key, data in self.memory_store['private_data'].items():
            if key.startswith(f"{exchange.lower()}_"):
                exchange_data[key] = {
                    "exchange": data.get('exchange'),
                    "data_type": data.get('data_type'),
                    "timestamp": data.get('timestamp'),
                    "received_at": data.get('received_at'),
                    "data": data.get('data')  # 直接返回原始数据
                }
        
        return {
            "exchange": exchange,
            "timestamp": datetime.now().isoformat(),
            "count": len(exchange_data),
            "data": exchange_data
        }
    
    async def get_private_data_detail(self, exchange: str, data_type: str):
        """获取特定私人数据详情（简化版）"""
        key = f"{exchange.lower()}_{data_type.lower()}"
        
        if key in self.memory_store['private_data']:
            data = self.memory_store['private_data'][key]
            return {
                "key": key,
                "exchange": exchange,
                "data_type": data_type,
                "timestamp": data.get('timestamp'),
                "received_at": data.get('received_at'),
                "data": data.get('data')  # 直接返回原始数据
            }
        else:
            return {
                "error": f"未找到数据: {key}",
                "available_keys": list(self.memory_store['private_data'].keys())
            }
    
    # ==================== 辅助方法 ====================
    
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
    
    async def _notify_listen_key_updated(self, exchange: str, listen_key: str):
        """通知连接池listenKey已更新"""
        try:
            # ✅ 真正调用连接池的监听方法
            if hasattr(self.brain, 'private_pool') and self.brain.private_pool:
                # 检查连接池是否有监听方法
                if hasattr(self.brain.private_pool, 'on_listen_key_updated'):
                    await self.brain.private_pool.on_listen_key_updated(exchange, listen_key)
                    logger.info(f"📢【智能大脑】已通知连接池{exchange} listenKey更新")
                else:
                    # 如果连接池没有监听方法，直接重新连接
                    logger.info(f"📢【智能大脑】连接池无监听方法，触发{exchange}重连")
                    await self._trigger_pool_reconnect(exchange)
        except Exception as e:
            logger.error(f"❌【智能大脑】通知连接池失败: {e}")
    
    async def _trigger_pool_reconnect(self, exchange: str):
        """触发连接池重新连接"""
        try:
            if exchange == 'binance' and hasattr(self.brain.private_pool, '_reconnect_exchange'):
                await self.brain.private_pool._reconnect_exchange('binance')
            elif exchange == 'okx' and hasattr(self.brain.private_pool, '_reconnect_exchange'):
                await self.brain.private_pool._reconnect_exchange('okx')
        except Exception as e:
            logger.error(f"❌【智能大脑】触发重连失败: {e}")