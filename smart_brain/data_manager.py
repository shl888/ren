"""
数据管理器 - 简化存储版
只存储原始数据，不添加额外包装
"""
import asyncio
import logging
import os
import time
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
        self.last_reference_time = None
        
        # 批量存储日志控制
        self.last_batch_log_time = 0
        self.batch_log_interval = 60
        
        # 内存存储（按来源分类）
        self.memory_store = {
            'market_data': {},      # 公开市场数据（实时行情、费率差）
            'user_data': {},        # 私人用户数据（币安+欧易）
            'reference_data': {},   # 公开参考数据（合约面值等）
            'env_apis': self._load_apis_from_env(),
            'exchange_tokens': {}   # 专门存储listenKey
        }
    
    # ==================== 接收步骤 ====================
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据（用户数据 + 参考数据）
        根据data_type自动分类存储
        """
        try:
            exchange = private_data.get('exchange', 'unknown')
            data_type = private_data.get('data_type', 'unknown')
            
            logger.debug(f"📨【智能大脑】DataManager收到{exchange}.{data_type}数据")
            
            now = datetime.now()
            
            # ===== 根据data_type分类存储 =====
            if data_type == 'contract_info':
                # 🎯 参考数据：合约面值等
                storage_key = f"{exchange}_{data_type}"
                self.memory_store['reference_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': data_type,
                    'data': private_data.get('data', {}),
                    'timestamp': private_data.get('timestamp', now.isoformat()),
                    'received_at': now.isoformat(),
                    'source': 'reference_task'
                }
                self.last_reference_time = now
                logger.debug(f"✅【智能大脑】参考数据 {exchange}.{data_type} 已保存")
                
                # ✅ 推送存储后的面值数据
                if self.brain.frontend_relay:
                    reference_data_to_push = self.memory_store.get('reference_data', {})
                    await self.brain.frontend_relay.broadcast_reference_data(reference_data_to_push)
                    logger.debug(f"📤【智能大脑】已推送面值数据，共{len(reference_data_to_push)}条")
                
            elif data_type == 'user_summary':
                # 🎯 用户数据：账户、持仓、订单等
                storage_key = f"{exchange}_user"
                self.memory_store['user_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': 'user_summary',
                    'data': private_data.get('data', {}),
                    'timestamp': private_data.get('timestamp', now.isoformat()),
                    'received_at': now.isoformat(),
                    'source': 'private_processor'
                }
                self.last_account_time = now
                logger.debug(f"✅【智能大脑】用户数据 {exchange} 已保存")
                
                # ✅ 推送存储后的私人数据
                if self.brain.frontend_relay:
                    user_data_to_push = self.memory_store.get('user_data', {})
                    await self.brain.frontend_relay.broadcast_private_data(user_data_to_push)
                    logger.debug(f"📤【智能大脑】已推送私人数据，共{len(user_data_to_push)}条")
                
            elif data_type == 'listen_key':
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
                    logger.info(f"✅【智能大脑】{exchange} listenKey已保存: {listen_key[:5]}...")
                    
                    # 通知连接池
                    if hasattr(self.brain, 'private_pool') and self.brain.private_pool:
                        asyncio.create_task(self._notify_listen_key_updated(exchange, listen_key))
                else:
                    logger.warning(f"⚠️【智能大脑】收到空的listenKey: {exchange}")
                
            else:
                # 🎯 未知类型，暂时存到user_data（带warning）
                logger.warning(f"⚠️ 未知数据类型 {data_type}，暂存到user_data")
                storage_key = f"{exchange}_{data_type}"
                self.memory_store['user_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': data_type,
                    'data': private_data.get('data', {}),
                    'timestamp': private_data.get('timestamp', now.isoformat()),
                    'received_at': now.isoformat()
                }
                
                # 推送未知类型数据
                if self.brain.frontend_relay:
                    user_data_to_push = self.memory_store.get('user_data', {})
                    await self.brain.frontend_relay.broadcast_private_data(user_data_to_push)
            
            # 记录日志
            if data_type in ['account_update', 'user_summary']:
                self.last_account_time = now
                logger.debug(f"💰【智能大脑】收到账户私人数据: {exchange}")
            elif data_type in ['order_update', 'trade']:
                self.last_trade_time = now
                logger.debug(f"📝【智能大脑】收到交易私人数据: {exchange}")
            elif data_type == 'position_update':
                self.last_account_time = now
                logger.debug(f"📊【智能大脑】收到持仓私人数据: {exchange}")
            elif data_type == 'contract_info':
                logger.debug(f"📋【智能大脑】收到参考数据: {exchange}.{data_type}")
                    
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收私人数据失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def receive_market_data(self, processed_data):
        """
        接收市场数据处理后的数据
        """
        try:
            if isinstance(processed_data, list):
                self.last_market_count = len(processed_data)
                
                if logger.isEnabledFor(logging.DEBUG):
                    if processed_data and len(processed_data) > 0:
                        symbol = processed_data[0].get('symbol', 'unknown')
                        logger.debug(f"📣【智能大脑】收到批量数据: {len(processed_data)}条, 第一个合约: {symbol}")
            else:
                logger.warning(f"⚠️【智能大脑】收到非列表类型市场数据: {type(processed_data)}")
                self.last_market_count = 1
            
            self.last_market_time = datetime.now()
            
            # ✅ 存储市场数据
            stored_data = await self._store_market_data_simplified(processed_data)
            
            # ✅ 推送存储后的市场数据
            if self.brain.frontend_relay and stored_data:
                market_data_to_push = self.memory_store.get('market_data', {})
                await self.brain.frontend_relay.broadcast_market_data(market_data_to_push)
                logger.debug(f"📤【智能大脑】已推送市场数据，共{len(market_data_to_push)}条")
            
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收数据错误: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _store_market_data_simplified(self, data):
        """存储市场数据为简化格式并返回结果"""
        try:
            if not data:
                return {}
                
            storage_results = {}
            
            if isinstance(data, list) and len(data) > 0:
                for item in data:
                    await asyncio.sleep(0)
                    symbol = item.get('symbol', 'unknown')
                    if not symbol or symbol == 'unknown':
                        continue
                    
                    simplified_data = self._create_simplified_market_data(item)
                    self.memory_store['market_data'][symbol] = simplified_data
                    storage_results[symbol] = simplified_data
                
                current_time = time.time()
                if current_time - self.last_batch_log_time >= self.batch_log_interval:
                    logger.debug(f"✅【智能大脑】批量存储市场数据，共{len(data)}条")
                    self.last_batch_log_time = current_time
                
                return storage_results
                
            elif isinstance(data, dict):
                symbol = data.get('symbol', 'unknown')
                simplified_data = self._create_simplified_market_data(data)
                self.memory_store['market_data'][symbol] = simplified_data
                storage_results[symbol] = simplified_data
                return storage_results
                
            return {}
                
        except Exception as e:
            logger.error(f"❌【智能大脑】存储市场数据失败: {e}")
            return {}
    
    def _create_simplified_market_data(self, raw_data):
        """创建简化格式的市场数据"""
        try:
            metadata = raw_data.get('metadata', {})
            
            return {
                'symbol': raw_data.get('symbol'),
                'trade_price_diff': raw_data.get('trade_price_diff'),
                'trade_price_diff_percent': raw_data.get('trade_price_diff_percent'),
                'rate_diff': raw_data.get('rate_diff'),
                'okx_trade_price': raw_data.get('okx_trade_price'),
                'okx_mark_price': raw_data.get('okx_mark_price'),
                'okx_price_to_mark_diff': raw_data.get('okx_price_to_mark_diff'),
                'okx_price_to_mark_diff_percent': raw_data.get('okx_price_to_mark_diff_percent'),
                'okx_funding_rate': raw_data.get('okx_funding_rate'),
                'okx_period_seconds': raw_data.get('okx_period_seconds'),
                'okx_countdown_seconds': raw_data.get('okx_countdown_seconds'),
                'okx_last_settlement': raw_data.get('okx_last_settlement'),
                'okx_current_settlement': raw_data.get('okx_current_settlement'),
                'okx_next_settlement': raw_data.get('okx_next_settlement'),
                'binance_trade_price': raw_data.get('binance_trade_price'),
                'binance_mark_price': raw_data.get('binance_mark_price'),
                'binance_price_to_mark_diff': raw_data.get('binance_price_to_mark_diff'),
                'binance_price_to_mark_diff_percent': raw_data.get('binance_price_to_mark_diff_percent'),
                'binance_funding_rate': raw_data.get('binance_funding_rate'),
                'binance_period_seconds': raw_data.get('binance_period_seconds'),
                'binance_countdown_seconds': raw_data.get('binance_countdown_seconds'),
                'binance_last_settlement': raw_data.get('binance_last_settlement'),
                'binance_current_settlement': raw_data.get('binance_current_settlement'),
                'binance_next_settlement': raw_data.get('binance_next_settlement'),
                'calculated_at': metadata.get('calculated_at', datetime.now().isoformat()),
                'source': metadata.get('source', 'step5_cross_calc')
            }
        except Exception as e:
            logger.error(f"❌【智能大脑】创建简化市场数据失败: {e}")
            return {
                'symbol': raw_data.get('symbol', 'unknown'),
                'trade_price_diff': None,
                'trade_price_diff_percent': None,
                'rate_diff': None,
                'okx_trade_price': None,
                'okx_mark_price': None,
                'okx_price_to_mark_diff': None,
                'okx_price_to_mark_diff_percent': None,
                'okx_funding_rate': None,
                'okx_period_seconds': None,
                'okx_countdown_seconds': None,
                'okx_last_settlement': None,
                'okx_current_settlement': None,
                'okx_next_settlement': None,
                'binance_trade_price': None,
                'binance_mark_price': None,
                'binance_price_to_mark_diff': None,
                'binance_price_to_mark_diff_percent': None,
                'binance_funding_rate': None,
                'binance_period_seconds': None,
                'binance_countdown_seconds': None,
                'binance_last_settlement': None,
                'binance_current_settlement': None,
                'binance_next_settlement': None,
                'calculated_at': datetime.now().isoformat(),
                'source': 'error'
            }
    
    # ==================== 数据查询接口（按来源）====================
    
    async def get_data_summary(self):
        """获取所有数据的大纲（按来源分类）"""
        try:
            sources = []
            
            # 1. 公开市场数据
            if self.memory_store['market_data']:
                sources.append({
                    "name": "public_market",
                    "description": "公开市场数据（实时行情、费率差）",
                    "item_count": len(self.memory_store['market_data']),
                    "endpoint": "/api/brain/data/public_market",
                    "last_update": self._format_time_iso(self.last_market_time)
                })
            
            # 2. 私人用户数据
            if self.memory_store['user_data']:
                sources.append({
                    "name": "private_user",
                    "description": "私人用户数据（账户、持仓、订单）",
                    "user_count": len(self.memory_store['user_data']),
                    "endpoint": "/api/brain/data/private_user",
                    "last_update": self._format_time_iso(self.last_account_time)
                })
            
            # 3. 参考数据（动态扫描所有合约数据）
            if self.memory_store['reference_data']:
                for key, data in self.memory_store['reference_data'].items():
                    if 'contract_info' in key:
                        exchange = data.get('exchange', 'unknown')
                        contract_data = data.get('data', {})
                        contract_count = len(contract_data.get('contracts', []))
                        
                        sources.append({
                            "name": f"{exchange}_contracts",
                            "description": f"{exchange.upper()}合约{'面值' if exchange == 'okx' else '精度'}数据",
                            "contract_count": contract_count,
                            "endpoint": f"/api/brain/data/{exchange}_contracts",
                            "last_update": data.get('timestamp')
                        })
            
            return {
                "timestamp": datetime.now().isoformat(),
                "source_count": len(sources),
                "sources": sources,
                "note": f"共{len(sources)}个数据来源，点击endpoint查看详情"
            }
        except Exception as e:
            logger.error(f"❌【智能大脑】获取数据大纲失败: {e}")
            return {"timestamp": datetime.now().isoformat(), "error": str(e), "sources": []}
    
    async def get_public_market_data(self):
        """获取公开市场数据详情"""
        try:
            return {
                "source": "public_market",
                "description": "公开市场数据（实时行情、费率差）",
                "timestamp": self._format_time_iso(self.last_market_time) or datetime.now().isoformat(),
                "count": len(self.memory_store['market_data']),
                "data": self.memory_store['market_data']
            }
        except Exception as e:
            logger.error(f"❌【智能大脑】获取公开市场数据失败: {e}")
            return {"error": str(e)}
    
    async def get_private_user_data(self):
        """获取私人用户数据详情"""
        try:
            user_data = {}
            for key, data in self.memory_store['user_data'].items():
                await asyncio.sleep(0)
                exchange = data.get('exchange')
                if exchange:
                    user_data[exchange] = data.get('data', {})
            
            return {
                "source": "private_user",
                "description": "私人用户数据（币安 + 欧易）",
                "timestamp": self._format_time_iso(self.last_account_time) or datetime.now().isoformat(),
                "count": len(self.memory_store['user_data']),
                "data": user_data
            }
        except Exception as e:
            logger.error(f"❌【智能大脑】获取私人用户数据失败: {e}")
            return {"error": str(e)}
    
    async def get_okx_contracts_data(self):
        """获取OKX合约面值数据详情"""
        try:
            contract_data = None
            for key, data in self.memory_store['reference_data'].items():
                await asyncio.sleep(0)
                if 'okx' in key and 'contract' in key:
                    contract_data = data.get('data', {})
                    break
            
            return {
                "source": "okx_contracts",
                "description": "OKX合约面值数据",
                "timestamp": self._format_time_iso(self.last_reference_time) or datetime.now().isoformat(),
                "count": len(contract_data.get('contracts', [])) if contract_data else 0,
                "data": contract_data.get('contracts', []) if contract_data else []
            }
        except Exception as e:
            logger.error(f"❌【智能大脑】获取OKX合约面值数据失败: {e}")
            return {"error": str(e)}
    
    async def get_binance_contracts_data(self):
        """获取币安合约精度数据详情"""
        try:
            contract_data = None
            for key, data in self.memory_store['reference_data'].items():
                await asyncio.sleep(0)
                if 'binance' in key and 'contract_info' in key:
                    contract_data = data.get('data', {})
                    break
            
            return {
                "source": "binance_contracts",
                "description": "币安合约精度数据",
                "timestamp": self._format_time_iso(self.last_reference_time) or datetime.now().isoformat(),
                "count": len(contract_data.get('contracts', [])) if contract_data else 0,
                "data": contract_data.get('contracts', []) if contract_data else []
            }
        except Exception as e:
            logger.error(f"❌【智能大脑】获取币安合约精度数据失败: {e}")
            return {"error": str(e)}
    
    async def get_api_credentials_status(self):
        """获取API凭证状态（隐藏敏感信息）"""
        try:
            safe_apis = {}
            for exchange, creds in self.memory_store['env_apis'].items():
                await asyncio.sleep(0)
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
        except Exception as e:
            logger.error(f"❌【智能大脑】获取API凭证状态失败: {e}")
            return {"error": str(e)}
    
    async def get_system_status(self):
        """获取系统状态"""
        status = {
            "market_data": {
                "last_update": self._format_time_diff(self.last_market_time) if self.last_market_time else "从未更新",
                "last_count": self.last_market_count,
                "stored_count": len(self.memory_store['market_data'])
            },
            "user_data": {
                "last_update": self._format_time_diff(self.last_account_time) if self.last_account_time else "从未更新",
                "stored_count": len(self.memory_store['user_data'])
            },
            "reference_data": {
                "last_update": self._format_time_diff(self.last_reference_time) if self.last_reference_time else "从未更新",
                "stored_count": len(self.memory_store['reference_data'])
            },
            "timestamp": datetime.now().isoformat()
        }
        
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
                "user_data_count": len(self.memory_store['user_data']),
                "reference_data_count": len(self.memory_store['reference_data']),
                "exchange_tokens_count": len(self.memory_store['exchange_tokens'])
            }
            
            if data_type == 'market':
                self.memory_store['market_data'].clear()
                self.last_market_time = None
                self.last_market_count = 0
                message = f"清空市场数据，共{before_stats['market_data_count']}条"
                
            elif data_type == 'user':
                self.memory_store['user_data'].clear()
                self.last_account_time = None
                self.last_trade_time = None
                message = f"清空用户数据，共{before_stats['user_data_count']}条"
                
            elif data_type == 'reference':
                self.memory_store['reference_data'].clear()
                self.last_reference_time = None
                message = f"清空参考数据，共{before_stats['reference_data_count']}条"
                
            elif data_type == 'tokens':
                token_count = before_stats['exchange_tokens_count']
                self.memory_store['exchange_tokens'].clear()
                message = f"清空令牌数据，共{token_count}条"
                
            elif data_type is None:
                self.memory_store['market_data'].clear()
                self.memory_store['user_data'].clear()
                self.memory_store['reference_data'].clear()
                self.memory_store['exchange_tokens'].clear()
                self.last_market_time = None
                self.last_market_count = 0
                self.last_account_time = None
                self.last_trade_time = None
                self.last_reference_time = None
                message = f"清空所有数据，市场数据{before_stats['market_data_count']}条，用户数据{before_stats['user_data_count']}条，参考数据{before_stats['reference_data_count']}条，令牌{before_stats['exchange_tokens_count']}条"
                
            else:
                return {
                    "success": False,
                    "error": f"不支持的数据类型: {data_type}",
                    "supported_types": ["market", "user", "reference", "tokens"]
                }
            
            logger.warning(f"⚠️【智能大脑】清空{data_type or '所有'}数据")
            
            return {
                "success": True,
                "message": message,
                "data_type": data_type or "all",
                "before_stats": before_stats,
                "after_stats": {
                    "market_data_count": len(self.memory_store['market_data']),
                    "user_data_count": len(self.memory_store['user_data']),
                    "reference_data_count": len(self.memory_store['reference_data']),
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
    
    # ==================== 原有接口 ====================
    
    async def get_listen_key(self, exchange: str):
        """供连接池只读查询当前令牌"""
        token_info = self.memory_store['exchange_tokens'].get(exchange)
        if token_info:
            logger.debug(f"📤【智能大脑】【读取】{exchange} listenKey被读取")
            return token_info.get('key')
        else:
            logger.warning(f"⚠️【智能大脑】【读取】{exchange} listenKey不存在")
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
        
        for exchange, creds in apis.items():
            if not creds['api_key'] or not creds['api_secret']:
                logger.warning(f"⚠️【智能大脑】环境变量中{exchange}的API凭证不完整")
        
        logger.info(f"✅【智能大脑】已从环境变量加载API凭证")
        return apis
    
    # ==================== 辅助方法 ====================
    
    def _format_time_iso(self, dt):
        """格式化时间为ISO格式"""
        return dt.isoformat() if dt else None
    
    def _format_time_diff(self, last_time):
        if not last_time:
            return "从未更新"
        
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
            await asyncio.sleep(0)
            try:
                await asyncio.sleep(60)
                
                market_count = len(self.memory_store['market_data'])
                market_time = self._format_time_diff(self.last_market_time)
                user_count = len(self.memory_store['user_data'])
                user_time = self._format_time_diff(self.last_account_time)
                ref_count = len(self.memory_store['reference_data'])
                ref_time = self._format_time_diff(self.last_reference_time)
                
                if self.brain.frontend_relay:
                    frontend_stats = self.brain.frontend_relay.get_stats_summary()
                    frontend_clients = frontend_stats.get('clients_connected', 0)
                    frontend_status = f"✅ 已连接 {frontend_clients} 个客户端"
                else:
                    frontend_status = "⚠️ 未启用"
                    frontend_clients = 0
                
                logger.info(f"""【智能大脑】数据状态
市场数据: {market_count}条, {market_time}
用户数据: {user_count}条, {user_time}
参考数据: {ref_count}条, {ref_time}
前端连接: {frontend_status}""")
                
                # 推送系统状态到前端
                if self.brain.frontend_relay and frontend_clients > 0:
                    try:
                        system_status = {
                            'market_data': {
                                'count': market_count,
                                'last_update': market_time
                            },
                            'user_data': {
                                'count': user_count,
                                'last_update': user_time
                            },
                            'reference_data': {
                                'count': ref_count,
                                'last_update': ref_time
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
            if hasattr(self.brain, 'private_pool') and self.brain.private_pool:
                if hasattr(self.brain.private_pool, 'on_listen_key_updated'):
                    await self.brain.private_pool.on_listen_key_updated(exchange, listen_key)
                    logger.info(f"📢【智能大脑】已通知连接池{exchange} listenKey更新")
                else:
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