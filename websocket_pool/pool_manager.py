"""
WebSocket连接池总管理器 - 角色互换版 + 增强诊断
"""
import asyncio
import logging
import sys
import os
import time
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
from .static_symbols import STATIC_SYMBOLS  # 导入静态合约

logger = logging.getLogger(__name__)

# ============ 【修复：默认数据回调函数 - 支持原始数据】============
async def default_data_callback(data):
    """
    默认数据回调函数 - 将WebSocket接收的原始数据直接存入共享存储
    这是数据流的关键节点：WebSocket → 此函数 → data_store
    现在data包含完整的raw_data字段
    """
    try:
        # 验证数据有效性
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
        
        # ✅【关键修复】直接调用 data_store.update_market_data
        # 传递三个参数：exchange, symbol, data
        # 现在data包含完整的raw_data字段和原始数据
        await data_store.update_market_data(exchange, symbol, data)
        
        # 记录日志（每10000条记录一次，避免日志过多）
        default_data_callback.counter = getattr(default_data_callback, 'counter', 0) + 1
        if default_data_callback.counter % 10000 == 0:
            logger.info(f"[数据回调] 已处理 {default_data_callback.counter} 条原始数据，最新: {exchange} {symbol}")
            
    except TypeError as e:
        # 如果参数错误，记录详细错误信息
        logger.error(f"回调参数错误: {e}，数据格式可能不正确")
        logger.error(f"数据内容: exchange={data.get('exchange')}, symbol={data.get('symbol')}")
        logger.error(f"数据keys: {list(data.keys())}")
    except Exception as e:
        logger.error(f"数据回调函数错误: {e}，数据: {data}")

# ============ 【WebSocket连接池管理器类】============
class WebSocketPoolManager:
    """WebSocket连接池管理器"""
    
    def __init__(self, data_callback=None):  # ✅ 修改：参数改为可选
        """
        初始化连接池管理器
        
        参数:
            data_callback: 数据回调函数，如果为None则使用默认回调
        """
        # ✅【关键修改】优先使用传入的回调，如果没有则使用默认回调
        if data_callback:
            self.data_callback = data_callback
            logger.info(f"WebSocketPoolManager 使用自定义数据回调")
        else:
            # 使用我们修复的默认回调（支持原始数据）
            self.data_callback = default_data_callback
            logger.info(f"WebSocketPoolManager 使用默认数据回调（直接对接共享数据模块，支持原始数据）")
        
        self.exchange_pools = {}  # exchange_name -> ExchangeWebSocketPool
        self.initialized = False
        self._initializing = False  # ✅ 新增：初始化状态跟踪
        self._shutting_down = False  # ✅ 新增：关闭状态跟踪
        
    async def initialize(self):
        """初始化所有交易所连接池 - 防重入版"""
        if self.initialized or self._initializing:
            logger.info("WebSocket连接池已在初始化或已初始化")
            return
        
        self._initializing = True
        logger.info(f"{'=' * 60}")
        logger.info("正在初始化WebSocket连接池管理器...")
        logger.info(f"{'=' * 60}")
        
        # 获取所有交易所的合约（使用你的成功方法）
        exchange_tasks = []
        for exchange_name in ["binance", "okx"]:
            if exchange_name in EXCHANGE_CONFIGS:
                task = asyncio.create_task(self._setup_exchange_pool(exchange_name))
                exchange_tasks.append(task)
        
        # 等待所有交易所初始化完成
        if exchange_tasks:
            await asyncio.gather(*exchange_tasks, return_exceptions=True)
        
        self.initialized = True
        self._initializing = False
        logger.info("✅ WebSocket连接池管理器初始化完成")
        logger.info(f"{'=' * 60}")
    
    async def _setup_exchange_pool(self, exchange_name: str):
        """设置单个交易所连接池"""
        try:
            # 1. 获取合约列表
            logger.info(f"[{exchange_name}] 获取合约列表中...")
            symbols = await self._fetch_exchange_symbols(exchange_name)
            
            if not symbols:
                logger.warning(f"[{exchange_name}] API获取失败，使用静态合约列表")
                symbols = self._get_static_symbols(exchange_name)
            
            if not symbols:
                logger.error(f"[{exchange_name}] 无法获取任何合约，跳过该交易所")
                return
            
            logger.info(f"[{exchange_name}] 成功获取 {len(symbols)} 个合约")
            
            # 2. 限制合约数量（基于活跃连接数计算）
            active_connections = EXCHANGE_CONFIGS[exchange_name].get("active_connections", 3)
            symbols_per_conn = EXCHANGE_CONFIGS[exchange_name].get("symbols_per_connection", 300)
            max_symbols = symbols_per_conn * active_connections
            
            if len(symbols) > max_symbols:
                logger.info(f"[{exchange_name}] 合约数量 {len(symbols)} > 限制 {max_symbols}，进行裁剪")
                symbols = symbols[:max_symbols]
            
            # 3. 初始化连接池
            logger.info(f"[{exchange_name}] 初始化连接池...")
            pool = ExchangeWebSocketPool(exchange_name, self.data_callback)  # ✅ 这里使用正确的回调
            await pool.initialize(symbols)
            self.exchange_pools[exchange_name] = pool
            
            logger.info(f"✅ [{exchange_name}] 连接池初始化成功")
            
        except Exception as e:
            logger.error(f"[{exchange_name}] 设置失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _fetch_exchange_symbols(self, exchange_name: str) -> List[str]:
        """获取交易所的合约列表 - 增强稳健版"""
        symbols = []
        
        # 第1步: 尝试从API动态获取 (主路径)
        symbols = await self._fetch_symbols_via_api(exchange_name)
        if symbols:
            logger.info(f"✅ [{exchange_name}] 通过API成功获取 {len(symbols)} 个合约")
            return symbols
        
        # 第2步: API失败，使用项目内置的静态列表 (降级)
        logger.warning(f"[{exchange_name}] API获取失败，使用内置静态合约列表")
        symbols = self._get_static_symbols(exchange_name)
        logger.info(f"⚠️ [{exchange_name}] 使用静态合约列表，共 {len(symbols)} 个")
        return symbols
    
    async def _fetch_symbols_via_api(self, exchange_name: str) -> List[str]:
        """方法1: 通过交易所API动态获取 - 修复版"""
        exchange = None
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            try:
                # 针对不同交易所进行优化配置
                config = self._get_exchange_config(exchange_name)
                exchange_class = getattr(ccxt_async, exchange_name)
                exchange = exchange_class(config)
                
                logger.info(f"[{exchange_name}] 正在加载市场数据... (尝试 {attempt}/{max_retries})")
                
                # 关键区别：不同交易所使用不同方法
                if exchange_name == "okx":
                    # OKX需要使用特定参数获取SWAP合约
                    markets = await exchange.fetch_markets(params={'instType': 'SWAP'})
                    # OKX的fetch_markets返回的是列表，需要转换为统一的字典格式
                    markets_dict = {}
                    for market in markets:
                        symbol = market.get('symbol', '')
                        if symbol:
                            markets_dict[symbol.upper()] = market
                    markets = markets_dict
                else:
                    # 币安等其他交易所使用load_markets，返回的是字典
                    markets = await exchange.load_markets()
                    # 将键转为大写
                    markets = {k.upper(): v for k, v in markets.items()}
                
                logger.info(f"[{exchange_name}] 市场数据加载完成，共 {len(markets)} 个市场")
                
                # 处理并筛选合约
                filtered_symbols = self._filter_and_format_symbols(exchange_name, markets)
                
                if filtered_symbols:
                    # 打印分组统计
                    symbol_groups = {}
                    for s in filtered_symbols:
                        prefix = s[:3]
                        symbol_groups.setdefault(prefix, 0)
                        symbol_groups[prefix] += 1
                    
                    top_groups = sorted(symbol_groups.items(), key=lambda x: x[1], reverse=True)[:5]
                    group_info = ", ".join([f"{g[0]}:{g[1]}" for g in top_groups])
                    logger.info(f"[{exchange_name}] 币种分组统计: {group_info}")
                    
                    # 检查是否有重复USDT问题
                    duplicate_usdt_count = sum(1 for s in filtered_symbols if s.upper().endswith('USDTUSDT'))
                    if duplicate_usdt_count > 0:
                        logger.error(f"[{exchange_name}] ⚠️ 发现 {duplicate_usdt_count} 个重复USDT的合约!")
                        # 显示有问题的合约
                        problematic = [s for s in filtered_symbols if s.upper().endswith('USDTUSDT')][:5]
                        logger.error(f"有问题的合约示例: {problematic}")
                
                await exchange.close()
                return filtered_symbols
                
            except Exception as e:
                # 安全地记录真正的错误原因
                error_detail = str(e) if e and hasattr(e, '__str__') else '未知错误'
                
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # 指数退避
                    logger.warning(f'[{exchange_name}] 第{attempt}次尝试失败，{wait_time}秒后重试: {error_detail}')
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f'[{exchange_name}] 所有{max_retries}次尝试均失败: {error_detail}')
                    if exchange:
                        await exchange.close()
                    return []
    
    def _get_exchange_config(self, exchange_name: str) -> dict:
        """获取针对不同交易所优化的配置"""
        base_config = {
            'apiKey': '',  # 不需要API密钥获取合约列表
            'secret': '',
            'enableRateLimit': True,
            'timeout': 30000,  # 30秒超时
        }
        
        if exchange_name == "okx":
            # OKX专用配置
            base_config.update({
                'options': {
                    'defaultType': 'swap',
                    'fetchMarketDataRateLimit': 2000,  # 降低频率
                }
            })
        elif exchange_name == "binance":
            # 币安专用配置
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
                    # 币安合约转换 - 解决重复USDT
                    is_perpetual = market.get('swap', False) or market.get('linear', False)
                    is_active = market.get('active', False)
                    is_usdt = '/USDT' in symbol_upper
                    
                    if is_perpetual and is_active and is_usdt:
                        # 暴力提取基础币种名
                        # 格式可能是: BTC/USDT 或 BTC/USDT:USDT
                        parts = symbol_upper.split('/')
                        if len(parts) >= 2:
                            base_symbol = parts[0]  # BTC部分
                            
                            # 清理base_symbol中可能存在的:USDT
                            if ':USDT' in base_symbol:
                                base_symbol = base_symbol.split(':')[0]
                            
                            # 组成最终合约名
                            clean_symbol = f"{base_symbol}USDT"
                            
                            # 最终检查：确保没有重复USDT
                            if clean_symbol.endswith('USDTUSDT'):
                                clean_symbol = clean_symbol[:-4]  # 去掉一个USDT
                            
                            all_usdt_symbols.append(clean_symbol)
                            
                            # 调试：记录前几个合约的转换
                            if len(all_usdt_symbols) <= 3:
                                logger.info(f"币安合约转换示例: {symbol} → {clean_symbol}")
                        
                elif exchange_name == "okx":
                    # OKX合约转换 - 更稳健的判断
                    market_type = market.get('type', '').upper()
                    quote = market.get('quote', '').upper()
                    contract_type = market.get('contractType', '').upper()
                    
                    # 多种方式判断是否为USDT永续合约
                    is_swap = market_type == 'SWAP' or market.get('swap', False) or 'SWAP' in symbol_upper
                    is_usdt_quote = quote == 'USDT' or '-USDT-' in symbol_upper
                    is_perpetual_contract = 'PERPETUAL' in contract_type or contract_type == '' or 'SWAP' in contract_type
                    
                    if is_swap and is_usdt_quote and is_perpetual_contract:
                        # OKX保持 BTC-USDT-SWAP 格式
                        if '-USDT-SWAP' in symbol_upper:
                            clean_symbol = symbol.upper()  # 保持 BTC-USDT-SWAP 格式
                        elif '/USDT:USDT' in symbol_upper:
                            clean_symbol = symbol.replace('/USDT:USDT', '-USDT-SWAP').upper()
                        else:
                            # 尝试从info中获取标准ID
                            inst_id = market.get('info', {}).get('instId', '')
                            if inst_id and '-USDT-SWAP' in inst_id.upper():
                                clean_symbol = inst_id.upper()
                            else:
                                continue
                        
                        all_usdt_symbols.append(clean_symbol)
                        
                        # 调试：记录前几个合约的转换
                        if len(all_usdt_symbols) <= 3:
                            logger.info(f"OKX合约转换示例: {symbol} → {clean_symbol}")
                
            except Exception as e:
                logger.debug(f"[{exchange_name}] 处理市场 {symbol} 时跳过: {e}")
                continue
        
        # 去重排序
        symbols = sorted(list(set(all_usdt_symbols)))
        
        if symbols:
            logger.info(f"✅ [{exchange_name}] 发现 {len(symbols)} 个USDT永续合约")
            
            # 打印前10个合约验证格式
            logger.info(f"[{exchange_name}] 前10个合约示例: {symbols[:10]}")
        else:
            logger.warning(f"[{exchange_name}] 未找到USDT永续合约")
            # 打印一些市场信息帮助调试
            logger.info(f"[{exchange_name}] 市场样例 (前5个):")
            count = 0
            for symbol, market in list(markets.items())[:5]:
                market_type = market.get('type', 'unknown')
                quote = market.get('quote', 'unknown')
                active = market.get('active', False)
                logger.info(f"  {symbol}: type={market_type}, quote={quote}, active={active}")
                count += 1
        
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
        # ✅ 防重入检查
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
