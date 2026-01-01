"""
WebSocket连接池总管理器 - 角色互换版 + 资源管理 + 线程安全
"""
import asyncio
import logging
import sys
import os
import time
import threading  # 线程锁
from typing import Dict, Any, List, Optional
import ccxt.async_support as ccxt_async

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # brain_core目录
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store
from .exchange_pool import ExchangeWebSocketPool
from .config import EXCHANGE_CONFIGS
from .static_symbols import STATIC_SYMBOLS

logger = logging.getLogger(__name__)

# 线程安全的计数器
_counter_lock = threading.Lock()
_counter = 0

async def default_data_callback(data):
    """默认数据回调函数 - 线程安全版"""
    global _counter
    
    try:
        if not data:
            return
            
        exchange = data.get("exchange", "")
        symbol = data.get("symbol", "")
        
        if not exchange:
            logger.error(f"数据缺少exchange字段: {data}")
            return
        if not symbol:
            logger.error(f"数据缺少symbol字段: {data}")
            return
        
        await data_store.update_market_data(exchange, symbol, data)
        
        # 线程安全计数器
        with _counter_lock:
            _counter += 1
            if _counter % 100 == 0:
                logger.info(f"[数据回调] 已处理 {_counter} 条原始数据，最新: {exchange} {symbol}")
            
    except Exception as e:
        logger.error(f"数据回调函数错误: {e}，数据: {data}")

class WebSocketPoolManager:
    """WebSocket连接池管理器 - 资源安全版"""
    
    def __init__(self, data_callback=None):
        if data_callback:
            self.data_callback = data_callback
            logger.info(f"WebSocketPoolManager 使用自定义数据回调")
        else:
            self.data_callback = default_data_callback
            logger.info(f"WebSocketPoolManager 使用默认数据回调")
        
        self.exchange_pools = {}
        self.initialized = False
        self._initializing = False
        self._shutting_down = False
    
    async def initialize(self):
        """初始化所有交易所连接池 - 防重入版"""
        if self.initialized or self._initializing:
            logger.info("WebSocket连接池已在初始化或已初始化")
            return
        
        self._initializing = True
        logger.info(f"{'=' * 60}")
        logger.info("正在初始化WebSocket连接池管理器...")
        logger.info(f"{'=' * 60}")
        
        exchange_tasks = []
        for exchange_name in ["binance", "okx"]:
            if exchange_name in EXCHANGE_CONFIGS:
                task = asyncio.create_task(self._setup_exchange_pool(exchange_name))
                exchange_tasks.append(task)
        
        if exchange_tasks:
            await asyncio.gather(*exchange_tasks, return_exceptions=True)
        
        self.initialized = True
        self._initializing = False
        logger.info("✅ WebSocket连接池管理器初始化完成")
        logger.info(f"{'=' * 60}")
    
    async def _setup_exchange_pool(self, exchange_name: str):
        """设置单个交易所连接池"""
        try:
            logger.info(f"[{exchange_name}] 获取合约列表中...")
            symbols = await self._fetch_exchange_symbols(exchange_name)
            
            if not symbols:
                logger.warning(f"[{exchange_name}] API获取失败，使用静态合约列表")
                symbols = self._get_static_symbols(exchange_name)
            
            if not symbols:
                logger.error(f"[{exchange_name}] 无法获取任何合约，跳过该交易所")
                return
            
            logger.info(f"[{exchange_name}] 成功获取 {len(symbols)} 个合约")
            
            # 限制合约数量
            active_connections = EXCHANGE_CONFIGS[exchange_name].get("active_connections", 3)
            symbols_per_conn = EXCHANGE_CONFIGS[exchange_name].get("symbols_per_connection", 300)
            max_symbols = symbols_per_conn * active_connections
            
            if len(symbols) > max_symbols:
                logger.info(f"[{exchange_name}] 合约数量 {len(symbols)} > 限制 {max_symbols}，进行裁剪")
                symbols = symbols[:max_symbols]
            
            logger.info(f"[{exchange_name}] 初始化连接池...")
            pool = ExchangeWebSocketPool(exchange_name, self.data_callback)
            await pool.initialize(symbols)
            self.exchange_pools[exchange_name] = pool
            
            logger.info(f"✅ [{exchange_name}] 连接池初始化成功")
            
        except Exception as e:
            logger.error(f"[{exchange_name}] 设置失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _fetch_exchange_symbols(self, exchange_name: str) -> List[str]:
        """获取交易所的合约列表 - 资源泄露修复版"""
        symbols = []
        
        symbols = await self._fetch_symbols_via_api(exchange_name)
        if symbols:
            logger.info(f"✅ [{exchange_name}] 通过API成功获取 {len(symbols)} 个合约")
            return symbols
        
        logger.warning(f"[{exchange_name}] API获取失败，使用内置静态合约列表")
        symbols = self._get_static_symbols(exchange_name)
        logger.info(f"⚠️ [{exchange_name}] 使用静态合约列表，共 {len(symbols)} 个")
        return symbols
    
    async def _fetch_symbols_via_api(self, exchange_name: str) -> List[str]:
        """核心方法：通过API获取合约，确保100%资源释放"""
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            exchange = None
            try:
                config = self._get_exchange_config(exchange_name)
                exchange_class = getattr(ccxt_async, exchange_name)
                exchange = exchange_class(config)
                
                logger.info(f"[{exchange_name}] 正在加载市场数据... (尝试 {attempt}/{max_retries})")
                
                if exchange_name == "okx":
                    markets = await exchange.fetch_markets(params={'instType': 'SWAP'})
                    markets_dict = {}
                    for market in markets:
                        symbol = market.get('symbol', '')
                        if symbol:
                            markets_dict[symbol.upper()] = market
                    markets = markets_dict
                else:
                    markets = await exchange.load_markets()
                    markets = {k.upper(): v for k, v in markets.items()}
                
                logger.info(f"[{exchange_name}] 市场数据加载完成，共 {len(markets)} 个市场")
                
                filtered_symbols = self._filter_and_format_symbols(exchange_name, markets)
                
                if filtered_symbols:
                    symbol_groups = {}
                    for s in filtered_symbols:
                        prefix = s[:3]
                        symbol_groups.setdefault(prefix, 0)
                        symbol_groups[prefix] += 1
                    
                    top_groups = sorted(symbol_groups.items(), key=lambda x: x[1], reverse=True)[:5]
                    group_info = ", ".join([f"{g[0]}:{g[1]}" for g in top_groups])
                    logger.info(f"[{exchange_name}] 币种分组统计: {group_info}")
                
                return filtered_symbols
                
            except Exception as e:
                error_detail = str(e) if e else '未知错误'
                
                if attempt < max_retries:
                    wait_time = min(2 ** attempt, 30)
                    logger.warning(f'[{exchange_name}] 第{attempt}次尝试失败，{wait_time}秒后重试: {error_detail}')
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f'[{exchange_name}] 所有{max_retries}次尝试均失败: {error_detail}')
                    return []
            finally:
                # 🚨 核心修复：确保exchange被正确关闭
                if exchange:
                    try:
                        await exchange.close()
                        logger.debug(f"[{exchange_name}] exchange实例已关闭")
                    except Exception as e:
                        logger.debug(f"[{exchange_name}] 关闭exchange时出错: {e}")
    
    def _get_exchange_config(self, exchange_name: str) -> dict:
        """获取针对不同交易所优化的配置"""
        base_config = {
            'apiKey': '',
            'secret': '',
            'enableRateLimit': True,
            'timeout': 30000,
        }
        
        if exchange_name == "okx":
            base_config.update({
                'options': {
                    'defaultType': 'swap',
                    'fetchMarketDataRateLimit': 2000,
                }
            })
        elif exchange_name == "binance":
            base_config.update({
                'options': {
                    'defaultType': 'future',
                    'warnOnFetchOHLCVLimitArgument': False,
                }
            })
        
        return base_config
    
    def _filter_and_format_symbols(self, exchange_name: str, markets: dict) -> List[str]:
        """统一的合约筛选与格式化逻辑"""
        all_usdt_symbols = []
        logger.info(f"[{exchange_name}] 分析市场中...")
        
        for symbol, market in markets.items():
            try:
                symbol_upper = symbol.upper()
                
                if exchange_name == "binance":
                    is_perpetual = market.get('swap', False) or market.get('linear', False)
                    is_active = market.get('active', False)
                    is_usdt = '/USDT' in symbol_upper
                    
                    if is_perpetual and is_active and is_usdt:
                        parts = symbol_upper.split('/')
                        if len(parts) >= 2:
                            base_symbol = parts[0]
                            
                            if ':USDT' in base_symbol:
                                base_symbol = base_symbol.split(':')[0]
                            
                            clean_symbol = f"{base_symbol}USDT"
                            
                            if clean_symbol.endswith('USDTUSDT'):
                                clean_symbol = clean_symbol[:-4]
                            
                            all_usdt_symbols.append(clean_symbol)
                        
                elif exchange_name == "okx":
                    market_type = market.get('type', '').upper()
                    quote = market.get('quote', '').upper()
                    contract_type = market.get('contractType', '').upper()
                    
                    is_swap = market_type == 'SWAP' or market.get('swap', False) or 'SWAP' in symbol_upper
                    is_usdt_quote = quote == 'USDT' or '-USDT-' in symbol_upper
                    is_perpetual_contract = 'PERPETUAL' in contract_type or contract_type == '' or 'SWAP' in contract_type
                    
                    if is_swap and is_usdt_quote and is_perpetual_contract:
                        if '-USDT-SWAP' in symbol_upper:
                            clean_symbol = symbol.upper()
                        elif '/USDT:USDT' in symbol_upper:
                            clean_symbol = symbol.replace('/USDT:USDT', '-USDT-SWAP').upper()
                        else:
                            inst_id = market.get('info', {}).get('instId', '')
                            if inst_id and '-USDT-SWAP' in inst_id.upper():
                                clean_symbol = inst_id.upper()
                            else:
                                continue
                        
                        all_usdt_symbols.append(clean_symbol)
                
            except Exception as e:
                logger.debug(f"[{exchange_name}] 处理市场 {symbol} 时跳过: {e}")
                continue
        
        symbols = sorted(list(set(all_usdt_symbols)))
        
        if symbols:
            logger.info(f"✅ [{exchange_name}] 发现 {len(symbols)} 个USDT永续合约")
            logger.info(f"[{exchange_name}] 前10个合约示例: {symbols[:10]}")
        else:
            logger.warning(f"[{exchange_name}] 未找到USDT永续合约")
        
        return symbols
    
    def _get_static_symbols(self, exchange_name: str) -> List[str]:
        """备用方案：获取静态合约列表"""
        return STATIC_SYMBOLS.get(exchange_name, [])
    
    async def get_all_status(self) -> Dict[str, Any]:
        """获取所有交易所连接状态"""
        status = {}
        
        for exchange_name, pool in self.exchange_pools.items():
            try:
                pool_status = await pool.get_status()
                status[exchange_name] = pool_status
            except Exception as e:
                logger.error(f"[{exchange_name}] 获取状态错误: {e}")
                status[exchange_name] = {"error": str(e)}
        
        return status
    
    async def shutdown(self):
        """关闭所有连接池 - 防重入版"""
        if self._shutting_down:
            logger.info("连接池已在关闭中，跳过重复操作")
            return
        
        self._shutting_down = True
        logger.info("正在关闭所有WebSocket连接池...")
        
        for exchange_name, pool in self.exchange_pools.items():
            try:
                await pool.shutdown()
            except Exception as e:
                logger.error(f"[{exchange_name}] 关闭连接池错误: {e}")
        
        logger.info("✅ 所有WebSocket连接池已关闭")
