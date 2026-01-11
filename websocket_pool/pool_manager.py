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

# ============ 【固定数据回调函数】============
async def default_data_callback(data):
    """默认数据回调函数 - 带阈值清零版"""
    try:
        if not data:
            logger.debug("[数据回调] 收到空数据")
            return
            
        exchange = data.get("exchange", "")
        symbol = data.get("symbol", "")
        data_type = data.get("data_type", "unknown")
        
        if not exchange:
            logger.warning(f"[数据回调] 数据缺少exchange字段")
            return
        if not symbol:
            logger.warning(f"[数据回调] 数据缺少symbol字段")
            return
        
        # 🚨 计数器初始化
        if not hasattr(default_data_callback, 'counter'):
            default_data_callback.counter = 0
            logger.info(f"🌎【数据回调初始化】计数器创建")
        
        # 🎯 关键：先增加计数
        default_data_callback.counter += 1
        current_count = default_data_callback.counter
        
        # 🎯 等于或超过300万就清零
        if current_count >= 3000000:
            default_data_callback.counter = 0
            current_count = 0
            logger.info(f"🫗【数据回调阈值重置】达到300万条，计数器清零重新开始")
        
        # 1. 第一条数据（重要） - 确认系统启动
        if current_count == 1:
            logger.info(f"🎉【数据回调第一条数据】{exchange} {symbol} ({data_type})")
        
        # 2. 每30000条记录一次数据流动
        if current_count % 30000 == 0:
            logger.info(f"✅【数据回调已接收】{current_count:,}条数据 - 最新: {exchange} {symbol}")
        
        # 3. 每300000条里程碑
        if current_count % 300000 == 0:
            logger.info(f"🏆【数据回调里程碑】{current_count:,} 条数据,已存储到data_store")
        
        # 🚨 关键：直接存储到data_store（不过大脑）
        await data_store.update_market_data(exchange, symbol, data)
            
    except Exception as e:
        logger.error(f"❌[数据回调] 存储失败: {e}")
        logger.error(f"❌[数据回调]失败数据: exchange={exchange}, symbol={symbol}")

# ============ 【WebSocket连接池管理器类】============
class WebSocketPoolManager:
    """WebSocket连接池管理器"""
    
    def __init__(self, admin_instance=None):  # ✅ 新增admin_instance参数
        """初始化连接池管理器 - 固定使用default_data_callback"""
        # 🚨 永远使用内部默认回调
        self.data_callback = default_data_callback
        self.admin_instance = admin_instance  # ✅ 保存管理员引用
        
        self.exchange_pools = {}  # exchange_name -> ExchangeWebSocketPool
        self.initialized = False
        self._initializing = False
        self._shutting_down = False
        self._common_symbols_cache = None  # ✅ 新增：双平台合约缓存
        self._last_symbols_update = 0  # ✅ 新增：上次更新时间
        
        logger.info("✅ WebSocketPoolManager 【连接池】初始化完成")
        logger.info("📊 数据流向: WebSocket → default_data_callback → data_store")
        if admin_instance:
            logger.info("☎️【连接池】 已设置管理员引用，支持直接重启请求")
        
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
                task = asyncio.create_task(self._setup_exchange_pool_optimized(exchange_name))
                exchange_tasks.append(task)
        
        # 等待所有交易所初始化完成
        if exchange_tasks:
            await asyncio.gather(*exchange_tasks, return_exceptions=True)
        
        self.initialized = True
        self._initializing = False
        logger.info("✅ WebSocket连接池管理器初始化完成")
        logger.info(f"{'=' * 60}")
    
    async def _setup_exchange_pool_optimized(self, exchange_name: str):
        """设置单个交易所连接池 - 优化版：只订阅双平台共有合约"""
        try:
            # 1. 获取双平台共有合约列表
            logger.info(f"[{exchange_name}] 🌎【连接池】获取双平台共有合约列表中...")
            common_symbols = await self._get_common_symbols()
            
            if not common_symbols or exchange_name not in common_symbols:
                logger.warning(f"[{exchange_name}] ❌【连接池】双平台过滤失败，使用单平台合约列表")
                return await self._setup_exchange_pool_fallback(exchange_name)
            
            symbols = common_symbols[exchange_name]
            
            if not symbols:
                logger.warning(f"[{exchange_name}] ❌【连接池】该交易所在双平台名单中没有合约")
                symbols = self._get_static_symbols(exchange_name)
            
            logger.info(f"[{exchange_name}] ✅✅✅【连接池】成功获取 {len(symbols)} 个双平台共有合约")
            
            # 2. 限制合约数量（基于活跃连接数计算）
            active_connections = EXCHANGE_CONFIGS[exchange_name].get("active_connections", 3)
            symbols_per_conn = EXCHANGE_CONFIGS[exchange_name].get("symbols_per_connection", 300)
            max_symbols = symbols_per_conn * active_connections
            
            if len(symbols) > max_symbols:
                logger.info(f"[{exchange_name}] 🤔【连接池】合约数量 {len(symbols)} > 限制 {max_symbols}，进行裁剪")
                symbols = symbols[:max_symbols]
            
            # 3. 初始化连接池
            logger.info(f"[{exchange_name}] 初始化连接池...")
            # ✅ 创建连接池时传入管理员引用
            pool = ExchangeWebSocketPool(exchange_name, self.data_callback, self.admin_instance)
            await pool.initialize(symbols)
            self.exchange_pools[exchange_name] = pool
            
            logger.info(f"✅ [{exchange_name}] 连接池初始化成功（双平台优化模式）")
            
        except Exception as e:
            logger.error(f"[{exchange_name}] ❌【连接池】设置失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _get_common_symbols(self, force_refresh: bool = False) -> Dict[str, List[str]]:
        """获取双平台共有合约 - 带缓存"""
        current_time = time.time()
        
        # 检查缓存是否有效（1小时有效期）
        if (not force_refresh and 
            self._common_symbols_cache is not None and 
            current_time - self._last_symbols_update < 3600):
            logger.info("📦【连接池】【双平台过滤】使用缓存的共有合约列表")
            return self._common_symbols_cache
        
        logger.info("🔄【连接池】【双平台过滤】开始计算双平台共有合约...")
        
        # 存储各交易所的原始合约列表
        all_symbols = {}
        
        try:
            # 1. 并行获取所有交易所的合约
            tasks = []
            for exchange_name in ["binance", "okx"]:
                task = asyncio.create_task(self._fetch_exchange_symbols_single(exchange_name))
                tasks.append((exchange_name, task))
            
            # 等待所有任务完成
            for exchange_name, task in tasks:
                try:
                    symbols = await task
                    all_symbols[exchange_name] = symbols
                    logger.info(f"✅【连接池】【双平台过滤】{exchange_name} 获取到 {len(symbols)} 个原始合约")
                except Exception as e:
                    logger.error(f"❌【连接池】【双平台过滤】{exchange_name} 获取合约失败: {e}")
                    # 降级到静态列表
                    all_symbols[exchange_name] = self._get_static_symbols(exchange_name)
            
            # 2. 计算双平台共有合约
            if "binance" in all_symbols and "okx" in all_symbols:
                # 标准化币安合约格式（去除可能的后缀）
                binance_standard = self._standardize_binance_symbols(all_symbols["binance"])
                okx_standard = self._standardize_okx_symbols(all_symbols["okx"])
                
                # 找出共有合约（基于标准化后的格式）
                binance_set = set(binance_standard.keys())
                okx_set = set(okx_standard.keys())
                
                common_base_symbols = binance_set.intersection(okx_set)
                
                if common_base_symbols:
                    logger.info(f"🎯【连接池】【双平台过滤】发现 {len(common_base_symbols)} 个双平台共有合约")
                    
                    # 为每个平台生成对应的合约名
                    result = {}
                    
                    # 币安：使用原始格式
                    binance_common = []
                    for base_symbol in common_base_symbols:
                        original_symbol = binance_standard[base_symbol]
                        binance_common.append(original_symbol)
                    
                    # OKX：使用原始格式
                    okx_common = []
                    for base_symbol in common_base_symbols:
                        original_symbol = okx_standard[base_symbol]
                        okx_common.append(original_symbol)
                    
                    result["binance"] = sorted(binance_common)
                    result["okx"] = sorted(okx_common)
                    
                    # 缓存结果
                    self._common_symbols_cache = result
                    self._last_symbols_update = current_time
                    
                    # 打印统计信息
                    logger.info(f"📊【连接池】【双平台过滤】币安双平台合约: {len(result['binance'])} 个")
                    logger.info(f"📊【连接池】【双平台过滤】OKX双平台合约: {len(result['okx'])} 个")
                    
                    # 打印前10个共有合约示例
                    sample_common = sorted(list(common_base_symbols))[:10]
                    logger.info(f"🔍【连接池】【双平台过滤】前10个共有合约: {sample_common}")
                    
                    return result
                else:
                    logger.error("❌❌❌【连接池】【双平台过滤】未找到任何双平台共有合约！")
            else:
                logger.error("❌❌❌【连接池】【双平台过滤】无法获取所有交易所合约列表")
        
        except Exception as e:
            logger.error(f"❌【连接池】【双平台过滤】计算共有合约失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # 失败时返回空
        return {}
    
    def _standardize_binance_symbols(self, symbols: List[str]) -> Dict[str, str]:
        """标准化币安合约格式 -> 基础币种名: 原始合约名"""
        standardized = {}
        for symbol in symbols:
            # 币安格式: BTCUSDT, ETHUSDT, 1000SHIBUSDT
            if symbol.endswith('USDT'):
                base_symbol = symbol[:-4]  # 去掉USDT
                standardized[base_symbol] = symbol
        return standardized
    
    def _standardize_okx_symbols(self, symbols: List[str]) -> Dict[str, str]:
        """标准化OKX合约格式 -> 基础币种名: 原始合约名"""
        standardized = {}
        for symbol in symbols:
            # OKX格式: BTC-USDT-SWAP, ETH-USDT-SWAP
            if '-USDT-SWAP' in symbol:
                # 提取基础币种: BTC-USDT-SWAP -> BTC
                parts = symbol.split('-')
                if len(parts) >= 1:
                    base_symbol = parts[0]  # BTC部分
                    standardized[base_symbol] = symbol
        return standardized
    
    async def _fetch_exchange_symbols_single(self, exchange_name: str) -> List[str]:
        """单独获取某个交易所的合约列表（不降级）"""
        try:
            # 使用原来的API获取方法
            symbols = await self._fetch_symbols_via_api(exchange_name)
            if not symbols:
                # 如果API失败，使用静态列表
                symbols = self._get_static_symbols(exchange_name)
            
            logger.debug(f"[{exchange_name}] 获取到 {len(symbols)} 个合约")
            return symbols
        except Exception as e:
            logger.error(f"[{exchange_name}] 获取合约失败: {e}")
            return []
    
    async def _setup_exchange_pool_fallback(self, exchange_name: str):
        """降级方案：使用原来的单平台逻辑"""
        logger.warning(f"[{exchange_name}] ⚠️ 使用单平台合约列表（降级模式）")
        
        symbols = await self._fetch_exchange_symbols(exchange_name)
        
        if not symbols:
            logger.warning(f"[{exchange_name}] ❌❌❌【连接池】API获取失败，使用静态合约列表")
            symbols = self._get_static_symbols(exchange_name)
        
        if not symbols:
            logger.error(f"[{exchange_name}] ❌❌❌【连接池】无法获取任何合约，跳过该交易所")
            return
        
        logger.info(f"[{exchange_name}] ⚠️【连接池】降级模式获取 {len(symbols)} 个合约")
        
        # 限制合约数量
        active_connections = EXCHANGE_CONFIGS[exchange_name].get("active_connections", 3)
        symbols_per_conn = EXCHANGE_CONFIGS[exchange_name].get("symbols_per_connection", 300)
        max_symbols = symbols_per_conn * active_connections
        
        if len(symbols) > max_symbols:
            logger.info(f"[{exchange_name}] 🤔【连接池】合约数量 {len(symbols)} > 限制 {max_symbols}，进行裁剪")
            symbols = symbols[:max_symbols]
        
        # 初始化连接池
        pool = ExchangeWebSocketPool(exchange_name, self.data_callback, self.admin_instance)
        await pool.initialize(symbols)
        self.exchange_pools[exchange_name] = pool
        
        logger.info(f"✅ [{exchange_name}] 连接池初始化成功（降级模式）")
    
    # ============ 以下为原始方法，保持不变 ============
    
    async def _setup_exchange_pool(self, exchange_name: str):
        """原始方法 - 保持兼容性"""
        return await self._setup_exchange_pool_optimized(exchange_name)
    
    async def _fetch_exchange_symbols(self, exchange_name: str) -> List[str]:
        """获取交易所的合约列表 - 增强稳健版"""
        symbols = []
        
        # 第1步: 尝试从API动态获取 (主路径)
        symbols = await self._fetch_symbols_via_api(exchange_name)
        if symbols:
            logger.info(f"✅✅✅ 【连接池】[{exchange_name}] 通过API成功获取 {len(symbols)} 个合约")
            return symbols
        
        # 第2步: API失败，使用项目内置的静态列表 (降级)
        logger.warning(f"❌❌❌【连接池】[{exchange_name}] API获取失败，使用内置静态合约列表")
        symbols = self._get_static_symbols(exchange_name)
        logger.info(f"⚠️【连接池】 [{exchange_name}] 使用静态合约列表，共 {len(symbols)} 个")
        return symbols
    
    async def _fetch_symbols_via_api(self, exchange_name: str) -> List[str]:
        """方法1: 通过交易所API动态获取 - 修复连接泄漏版"""
        exchange = None
        max_retries = 2
        last_error = None
        
        for attempt in range(1, max_retries + 1):
            exchange = None
            try:
                # 1. 创建交易所实例（带优化配置）
                exchange = self._create_exchange_instance(exchange_name)
                
                logger.info(f"[{exchange_name}] 🌎【连接池】正在加载市场数据... (尝试 {attempt}/{max_retries})")
                
                # 2. 获取市场数据（使用正确的API方法）
                markets = await self._fetch_markets_safe(exchange, exchange_name)
                
                if not markets:
                    logger.warning(f"[{exchange_name}] 获取市场数据失败，返回空")
                    # ✅ 确保即使失败也关闭连接
                    if exchange:
                        await self._safe_close_exchange(exchange, exchange_name)
                    continue
                
                # 3. 处理和筛选合约
                filtered_symbols = self._filter_and_format_symbols(exchange_name, markets)
                
                # 4. 正确关闭交易所实例
                if exchange:
                    await self._safe_close_exchange(exchange, exchange_name)
                
                if filtered_symbols:
                    logger.info(f"[{exchange_name}] ✅【连接池】成功获取 {len(filtered_symbols)} 个合约")
                    return filtered_symbols
                    
            except ccxt_async.RateLimitExceeded as e:
                last_error = f"频率限制: {e}"
                wait_time = 10 * attempt
                logger.warning(f'❌【连接池】[{exchange_name}] 频率限制，{wait_time}秒后重试')
                
                # ✅ 异常时也要关闭连接
                if exchange:
                    await self._safe_close_exchange(exchange, exchange_name)
                    
                await asyncio.sleep(wait_time)
                
            except ccxt_async.DDoSProtection as e:
                last_error = f"DDoS保护: {e}"
                wait_time = 15 * attempt
                logger.warning(f'❌【连接池】[{exchange_name}] DDoS保护触发，{wait_time}秒后重试')
                
                # ✅ 异常时也要关闭连接
                if exchange:
                    await self._safe_close_exchange(exchange, exchange_name)
                    
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                last_error = str(e)
                
                # ✅ 异常时也要关闭连接
                if exchange:
                    await self._safe_close_exchange(exchange, exchange_name)
                    
                if attempt < max_retries:
                    wait_time = 5 * attempt
                    logger.warning(f'❌【连接池】[{exchange_name}] 第{attempt}次尝试失败，{wait_time}秒后重试: {last_error}')
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f'❌【连接池】[{exchange_name}] 所有尝试均失败: {last_error}')
        
        logger.error(f'❌【连接池】[{exchange_name}] 所有尝试均失败，最后错误: {last_error}')
        return []
    
    async def _safe_close_exchange(self, exchange, exchange_name: str):
        """安全关闭交易所实例，防止连接泄漏"""
        try:
            if exchange and hasattr(exchange, 'close'):
                await exchange.close()
                logger.debug(f"[{exchange_name}] ✅ 交易所实例已正确关闭")
        except Exception as e:
            logger.warning(f"[{exchange_name}] ⚠️ 关闭交易所实例时出错: {e}")
    
    def _create_exchange_instance(self, exchange_name: str):
        """安全创建交易所实例 - 修正版"""
        exchange_class = getattr(ccxt_async, exchange_name)
        
        # 基础配置
        config = {
            'enableRateLimit': True,  # 🚀 关键：启用内置频率限制
            'timeout': 30000,         # 30秒超时
            'rateLimit': 2000,        # 降低频率限制，更保守
        }
        
        # 交易所特定配置 - ✅ 修正币安配置
        if exchange_name == "binance":
            config.update({
                'options': {
                    'defaultType': 'swap',  # ✅ 修正：使用'swap'获取永续合约
                    'defaultSubType': 'linear',  # ✅ 明确指定线性合约
                    'adjustedForTimeDifference': True,  # ✅ 修正拼写错误
                    'warnOnFetchOHLCVLimitArgument': False,
                    'recvWindow': 60000,  # ✅ 添加接收窗口
                    'cacheMarkets': True,  # ✅ 启用缓存减少API调用
                    'cacheTime': 1800,     # ✅ 30分钟缓存
                }
            })
        elif exchange_name == "okx":
            config.update({
                'options': {
                    'defaultType': 'swap',
                    'adjustedForTimeDifference': True,  # ✅ 统一参数名
                    'fetchMarketDataRateLimit': 3000,  # 降低频率
                }
            })
        
        return exchange_class(config)
    
    async def _fetch_markets_safe(self, exchange, exchange_name: str):
        """安全获取市场数据"""
        try:
            if exchange_name == "okx":
                # OKX: 使用fetch_markets获取SWAP合约
                markets = await exchange.fetch_markets(params={'instType': 'SWAP'})
                # 转换为统一的字典格式
                markets_dict = {}
                for market in markets:
                    symbol = market.get('symbol', '').upper()
                    if symbol:
                        markets_dict[symbol] = market
                return markets_dict
            else:
                # 币安等: 使用load_markets
                markets = await exchange.load_markets()
                # 转换为大写键
                return {k.upper(): v for k, v in markets.items()}
                
        except ccxt_async.NetworkError as e:
            logger.error(f"[{exchange_name}] 网络错误: {e}")
            return None
        except ccxt_async.ExchangeError as e:
            logger.error(f"[{exchange_name}] 交易所错误: {e}")
            return None
        except asyncio.TimeoutError as e:
            logger.error(f"[{exchange_name}] 超时错误: {e}")
            return None
        except Exception as e:
            logger.error(f"[{exchange_name}] 获取市场数据异常: {e}")
            return None
    
    def _filter_and_format_symbols(self, exchange_name: str, markets: dict) -> List[str]:
        """统一的合约筛选与格式化逻辑 - 支持1000开头合约"""
        all_usdt_symbols = []
        logger.info(f"🤔【连接池】[{exchange_name}] 分析市场中...")
        
        for symbol, market in markets.items():
            try:
                symbol_upper = symbol.upper()
                
                if exchange_name == "binance":
                    # 币安合约转换 - 保持完整的过滤条件
                    is_perpetual = market.get('swap', False) or market.get('linear', False) or market.get('future', False)
                    is_active = market.get('active', False)
                    is_usdt = '/USDT' in symbol_upper
                    
                    # ✅ 保持所有三个过滤条件
                    if is_perpetual and is_active and is_usdt:
                        # 🚨 改进：更健壮的合约名提取逻辑
                        # 处理格式: BTC/USDT, BTC/USDT:USDT, 1000SHIB/USDT:USDT
                        
                        # 1. 先去掉可能的:USDT后缀
                        clean_symbol = symbol_upper.replace(':USDT', '')
                        
                        # 2. 确保格式是 XXX/USDT
                        if '/USDT' in clean_symbol:
                            # 提取基础币种
                            base_part = clean_symbol.split('/USDT')[0]
                            
                            # 3. 如果base_part包含斜杠（异常情况），取最后一部分
                            if '/' in base_part:
                                base_part = base_part.split('/')[-1]
                            
                            # 4. 组成最终合约名
                            final_symbol = f"{base_part}USDT"
                            
                            # 5. 最终清理：确保没有重复USDT
                            if final_symbol.endswith('USDTUSDT'):
                                final_symbol = final_symbol[:-4]  # 去掉一个USDT
                            
                            # 6. 验证：确保不是空的和合理长度
                            if final_symbol and len(final_symbol) >= 4:
                                all_usdt_symbols.append(final_symbol)
                                
                                # 调试：记录前几个合约的转换
                                if len(all_usdt_symbols) <= 5:
                                    logger.info(f"🤔【连接池】币安合约转换: {symbol} → {final_symbol}")
                        
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
                            logger.info(f"🤔【连接池】OKX合约转换示例: {symbol} → {clean_symbol}")
                
            except Exception as e:
                logger.debug(f"🤔【连接池】[{exchange_name}] 处理市场 {symbol} 时跳过: {e}")
                continue
        
        # 去重排序
        symbols = sorted(list(set(all_usdt_symbols)))
        
        if symbols:
            logger.info(f"✅ 【连接池】[{exchange_name}] 发现 {len(symbols)} 个USDT永续合约")
            
            # 打印前10个合约验证格式
            logger.info(f"🔍【连接池】[{exchange_name}] 前10个合约示例: {symbols[:10]}")
            
            # 特别检查1000开头的合约
            thousand_symbols = [s for s in symbols if s.startswith('1000')]
            if thousand_symbols:
                logger.info(f"🔍【连接池】[{exchange_name}] 包含 {len(thousand_symbols)} 个1000开头合约: {thousand_symbols[:5]}...")
        else:
            logger.warning(f"⚠️⚠️⚠️⚠️⚠️【连接池】[{exchange_name}] 未找到USDT永续合约")
            # 打印一些市场信息帮助调试
            logger.info(f"🔍【连接池】[{exchange_name}] 市场样例 (前5个):")
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
                logger.error(f"❌【连接池】[{exchange_name}] 获取交易所连接状态错误: {e}")
                status[exchange_name] = {"error": str(e)}
        
        return status
    
    async def shutdown(self):
        """关闭所有连接池 - 防重入版"""
        # ✅ 防重入检查
        if self._shutting_down:
            logger.info("⚠️⚠️⚠️【连接池】连接池已在关闭中，跳过重复操作")
            return
        
        self._shutting_down = True
        logger.info("⚠️⚠️⚠️【连接池】正在关闭所有WebSocket连接池...")
        
        for exchange_name, pool in self.exchange_pools.items():
            try:
                await pool.shutdown()
            except Exception as e:
                logger.error(f"❌【连接池】[{exchange_name}] 关闭连接池错误: {e}")
        
        logger.info("✅ 【连接池】所有WebSocket连接池已关闭")
    
    # ============ 新增方法：双平台合约管理 ============
    
    async def refresh_common_symbols(self, force: bool = False):
        """手动刷新双平台共有合约列表"""
        logger.info("🔄【连接池】手动刷新双平台共有合约列表...")
        await self._get_common_symbols(force_refresh=force)
        logger.info("✅【连接池】双平台共有合约列表已刷新")
    
    def get_common_symbols_stats(self) -> Dict[str, Any]:
        """获取双平台合约统计信息"""
        if not self._common_symbols_cache:
            return {"status": "未计算", "binance_count": 0, "okx_count": 0}
        
        return {
            "status": "已计算",
            "binance_count": len(self._common_symbols_cache.get("binance", [])),
            "okx_count": len(self._common_symbols_cache.get("okx", [])),
            "last_update": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._last_symbols_update)),
            "cache_age_seconds": int(time.time() - self._last_symbols_update),
            "sample_symbols": {
                "binance": self._common_symbols_cache.get("binance", [])[:5],
                "okx": self._common_symbols_cache.get("okx", [])[:5],
            }
        }