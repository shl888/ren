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
            logger.info(f"🫗【数据回调阈值重置】达到500万条，计数器清零重新开始")
        
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
            logger.info(f"[{exchange_name}] 🌎【连接池】获取合约列表中...")
            symbols = await self._fetch_exchange_symbols(exchange_name)
            
            if not symbols:
                logger.warning(f"[{exchange_name}] ❌❌❌【连接池】API获取失败，使用静态合约列表")
                symbols = self._get_static_symbols(exchange_name)
            
            if not symbols:
                logger.error(f"[{exchange_name}] ❌❌❌【连接池】无法获取任何合约，跳过该交易所")
                return
            
            logger.info(f"[{exchange_name}] ✅✅✅【连接池】成功获取 {len(symbols)} 个合约")
            
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
            
            logger.info(f"✅ [{exchange_name}] 连接池初始化成功")
            
        except Exception as e:
            logger.error(f"[{exchange_name}] ❌【连接池】设置失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
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
        """方法1: 通过交易所API动态获取 - 修复版"""
        exchange = None
        max_retries = 2  # ✅ 修改为2：只尝试2次，快速降级到静态列表
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
                    continue
                
                # 3. 处理和筛选合约
                filtered_symbols = self._filter_and_format_symbols(exchange_name, markets)
                
                # 4. 正确关闭交易所实例
                if exchange:
                    try:
                        await exchange.close()
                    except Exception as e:
                        logger.debug(f"[{exchange_name}] 关闭交易所实例异常: {e}")
                
                if filtered_symbols:
                    logger.info(f"[{exchange_name}] ✅【连接池】成功获取 {len(filtered_symbols)} 个合约")
                    return filtered_symbols
                    
            except ccxt_async.RateLimitExceeded as e:
                last_error = f"频率限制: {e}"
                wait_time = 10 * attempt  # 指数退避，更长的等待时间
                logger.warning(f'❌【连接池】[{exchange_name}] 频率限制，{wait_time}秒后重试')
                await asyncio.sleep(wait_time)
                
            except ccxt_async.DDoSProtection as e:
                last_error = f"DDoS保护: {e}"
                wait_time = 15 * attempt  # 更长的等待时间
                logger.warning(f'❌【连接池】[{exchange_name}] DDoS保护触发，{wait_time}秒后重试')
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                last_error = str(e)
                if attempt < max_retries:
                    wait_time = 5 * attempt
                    logger.warning(f'❌【连接池】[{exchange_name}] 第{attempt}次尝试失败，{wait_time}秒后重试: {last_error}')
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f'❌【连接池】[{exchange_name}] 所有尝试均失败: {last_error}')
            finally:
                # 确保在异常时也尝试关闭
                if exchange:
                    try:
                        await exchange.close()
                    except:
                        pass
        
        logger.error(f'❌【连接池】[{exchange_name}] 所有尝试均失败，最后错误: {last_error}')
        return []
    
    def _create_exchange_instance(self, exchange_name: str):
        """安全创建交易所实例"""
        exchange_class = getattr(ccxt_async, exchange_name)
        
        # 基础配置
        config = {
            'enableRateLimit': True,  # 🚀 关键：启用内置频率限制
            'timeout': 30000,         # 30秒超时
            'rateLimit': 3000,        # ✅ 修改为3000ms：更保守的频率限制
        }
        
        # 交易所特定配置
        if exchange_name == "binance":
            config.update({
                'options': {
                    'defaultType': 'future',
                    'fetchMarkets': ['swap'],  # ✅ 新增：只获取永续合约
                    'warnOnFetchOHLCVLimitArgument': False,
                    'adjustForTimeDifference': True,
                }
            })
        elif exchange_name == "okx":
            config.update({
                'options': {
                    'defaultType': 'swap',
                    'fetchMarkets': ['swap'],  # ✅ 新增：显式限制只获取swap
                    'fetchMarketDataRateLimit': 3000,
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
        """统一的合约筛选与格式化逻辑"""
        all_usdt_symbols = []
        logger.info(f"🤔【连接池】[{exchange_name}] 分析市场中...")
        
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
                                logger.info(f"🤔【连接池】币安合约转换示例: {symbol} → {clean_symbol}")
                        
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
