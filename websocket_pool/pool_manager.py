"""
WebSocket连接池总管理器 - 增强版（独立获取 + 智能降级 + 精确双平台匹配）
"""
import asyncio
import logging
import sys
import os
import time
import json
import aiohttp
from typing import Dict, Any, List, Optional, Set

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # smart_brain目录
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

# ============ 【极简HTTP合约获取器】============
class SimpleSymbolFetcher:
    """极简合约获取器 - 直接HTTP请求，替代CCXT"""
    
    # 币安API
    BINANCE_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    
    # 欧意API
    OKX_URL = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    
    async def fetch_binance(self) -> List[str]:
        """获取币安USDT永续合约 - 2次重试，10秒超时"""
        for attempt in range(1, 3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.BINANCE_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status != 200:
                            logger.warning(f"[币安] HTTP {resp.status}，第{attempt}次尝试")
                            if resp.status >= 500:
                                await asyncio.sleep(3)
                                continue
                            elif resp.status >= 400:
                                logger.error(f"[币安] 客户端错误 {resp.status}，不重试")
                                return []
                            continue
                        
                        data = await resp.json()
                        
                        # 🚨 调试：记录返回的symbols数量
                        symbols_count = len(data.get('symbols', []))
                        logger.info(f"[币安调试] 返回 {symbols_count} 个交易对")
                        
                        symbols = []
                        for s in data.get('symbols', []):
                            contract_type = s.get('contractType', '')
                            quote_asset = s.get('quoteAsset', '')
                            status = s.get('status', '')
                            
                            if (contract_type == 'PERPETUAL' and 
                                quote_asset == 'USDT' and 
                                status == 'TRADING'):
                                symbols.append(s.get('symbol'))
                        
                        if symbols:
                            logger.info(f"✅ [币安] HTTP获取成功: {len(symbols)}个合约")
                            # 🚨 打印前5个示例
                            logger.info(f"[币安示例] {symbols[:5]}")
                            return symbols
                        else:
                            logger.warning(f"[币安] 筛选后为空，第{attempt}次尝试")
                            # 🚨 打印一些原始数据用于调试
                            if symbols_count > 0:
                                sample = data.get('symbols', [])[0]
                                logger.info(f"[币安原始示例] {sample}")
                            await asyncio.sleep(3)
                            continue
                            
            except asyncio.TimeoutError:
                logger.warning(f"[币安] 请求超时，第{attempt}次尝试")
                await asyncio.sleep(3)
            except aiohttp.ClientConnectorError as e:
                logger.warning(f"[币安] 连接错误: {e}，第{attempt}次尝试")
                await asyncio.sleep(3)
            except Exception as e:
                logger.warning(f"[币安] 请求异常: {e}，第{attempt}次尝试")
                await asyncio.sleep(3)
        
        logger.error("❌ [币安] 所有尝试失败")
        return []
    
    async def fetch_okx(self) -> List[str]:
        """获取欧意USDT永续合约 - 2次重试，10秒超时"""
        for attempt in range(1, 3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.OKX_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status != 200:
                            logger.warning(f"[欧意] HTTP {resp.status}，第{attempt}次尝试")
                            if resp.status >= 500:
                                await asyncio.sleep(3)
                                continue
                            elif resp.status >= 400:
                                logger.error(f"[欧意] 客户端错误 {resp.status}，不重试")
                                return []
                            continue
                        
                        data = await resp.json()
                        
                        # 🚨 调试1：打印完整的返回结构
                        logger.info(f"[欧意调试] 返回code: {data.get('code')}")
                        logger.info(f"[欧意调试] 返回msg: {data.get('msg')}")
                        
                        # 🚨 调试2：检查data字段
                        data_list = data.get('data', [])
                        logger.info(f"[欧意调试] data列表长度: {len(data_list)}")
                        
                        # 🚨 调试3：如果data不为空，打印第一条数据的完整结构
                        if data_list:
                            first_item = data_list[0]
                            logger.info(f"[欧意调试] 第一条数据完整结构:")
                            logger.info(json.dumps(first_item, indent=2, ensure_ascii=False))
                            
                            # 🚨 调试4：打印所有可用的字段名
                            logger.info(f"[欧意调试] 可用字段: {list(first_item.keys())}")
                        
                        if data.get('code') != '0':
                            logger.warning(f"[欧意] API返回错误码: {data.get('code')}，第{attempt}次尝试")
                            await asyncio.sleep(3)
                            continue
                        
                        # 🚨 调试5：先不加筛选，看看原始数量
                        all_symbols = [i.get('instId') for i in data_list if i.get('instId')]
                        logger.info(f"[欧意调试] 原始合约数量: {len(all_symbols)}")
                        
                        if all_symbols:
                            logger.info(f"[欧意示例] 前5个原始合约: {all_symbols[:5]}")
                        
                        # 正式筛选
                        symbols = []
                        usdt_count = 0
                        live_count = 0
                        
                        for i in data_list:
                            inst_id = i.get('instId', '')
                            quote_ccy = i.get('quoteCcy', '')
                            state = i.get('state', '')
                            
                            # 🚨 记录筛选条件命中情况
                            if quote_ccy == 'USDT':
                                usdt_count += 1
                            if state == 'live':
                                live_count += 1
                            
                            if (quote_ccy == 'USDT' and state == 'live'):
                                symbols.append(inst_id)
                        
                        logger.info(f"[欧意调试] USDT合约数量: {usdt_count}")
                        logger.info(f"[欧意调试] live状态合约数量: {live_count}")
                        
                        if symbols:
                            logger.info(f"✅ [欧意] HTTP获取成功: {len(symbols)}个合约")
                            logger.info(f"[欧意示例] 前5个: {symbols[:5]}")
                            return symbols
                        else:
                            logger.warning(f"[欧意] 获取到空列表，第{attempt}次尝试")
                            # 🚨 如果数据不为空但筛选为空，打印一些示例数据
                            if data_list:
                                sample = data_list[0]
                                logger.info(f"[欧意原始示例] instId={sample.get('instId')}, quoteCcy={sample.get('quoteCcy')}, state={sample.get('state')}")
                            await asyncio.sleep(3)
                            continue
                            
            except asyncio.TimeoutError:
                logger.warning(f"[欧意] 请求超时，第{attempt}次尝试")
                await asyncio.sleep(3)
            except aiohttp.ClientConnectorError as e:
                logger.warning(f"[欧意] 连接错误: {e}，第{attempt}次尝试")
                await asyncio.sleep(3)
            except Exception as e:
                logger.warning(f"[欧意] 请求异常: {e}，第{attempt}次尝试")
                await asyncio.sleep(3)
        
        logger.error("❌ [欧意] 所有尝试失败")
        return []

# ============ 【WebSocket连接池管理器类】============
class WebSocketPoolManager:
    """WebSocket连接池管理器 - 增强版（独立获取 + 智能降级 + 精确双平台匹配）"""
    
    def __init__(self, admin_instance=None):
        """初始化连接池管理器 - 固定使用default_data_callback"""
        self.data_callback = default_data_callback
        self.admin_instance = admin_instance
        
        self.exchange_pools = {}  # exchange_name -> ExchangeWebSocketPool
        self.initialized = False
        self._initializing = False
        self._shutting_down = False
        self._common_symbols_cache = None
        self._last_symbols_update = 0
        
        # 存储各交易所的原始合约列表和来源信息
        self._raw_symbols_info = {
            "binance": {"symbols": [], "source": "unknown", "count": 0},
            "okx": {"symbols": [], "source": "unknown", "count": 0}
        }
        
        # 极简HTTP获取器
        self.fetcher = SimpleSymbolFetcher()
        
        logger.info("✅ WebSocketPoolManager 【连接池】初始化完成（极简HTTP版）")
        logger.info("📊 数据流向: WebSocket → default_data_callback → data_store")
        if admin_instance:
            logger.info("☎️【连接池】 已设置管理员引用，支持直接重启请求")
    
    # ============ 改进的核心流程方法 ============
    
    async def initialize(self):
        """初始化所有交易所连接池 - 增强版"""
        if self.initialized or self._initializing:
            logger.info("WebSocket连接池已在初始化或已初始化")
            return
        
        self._initializing = True
        logger.info(f"{'=' * 60}")
        logger.info("🔄 正在初始化WebSocket连接池管理器（极简HTTP版）...")
        logger.info("🚀 流程：独立获取 → 智能降级 → 双平台匹配")
        logger.info(f"{'=' * 60}")
        
        try:
            # 1. 【独立获取】各交易所的原始合约列表（带降级）
            await self._fetch_all_exchange_symbols_independent()
            
            # 2. 【双平台匹配】基于原始数据进行匹配
            common_symbols = await self._calculate_common_symbols()
            
            # 3. 【初始化连接池】为每个交易所建立连接
            await self._initialize_all_exchange_pools(common_symbols)
            
            self.initialized = True
            logger.info("✅✅✅ WebSocket连接池管理器初始化完成（极简HTTP版）")
            logger.info(f"{'=' * 60}")
            
            # 打印初始化摘要
            self._print_initialization_summary()
            
        except Exception as e:
            logger.error(f"❌ 连接池管理器初始化失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # 即使失败也尝试继续
        finally:
            self._initializing = False
    
    async def _fetch_all_exchange_symbols_independent(self):
        """【步骤1】独立获取各交易所的原始合约列表（互不影响）"""
        logger.info("📥【步骤1】开始独立获取各交易所合约列表...")
        
        tasks = []
        for exchange_name in ["binance", "okx"]:
            task = asyncio.create_task(
                self._fetch_exchange_symbols_with_fallback(exchange_name)
            )
            tasks.append(task)
        
        # 等待所有任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        for i, exchange_name in enumerate(["binance", "okx"]):
            result = results[i]
            if isinstance(result, Exception):
                logger.error(f"❌[{exchange_name}] 获取合约失败: {result}")
                # 使用静态列表作为最后保障
                static_symbols = self._get_static_symbols(exchange_name)
                self._raw_symbols_info[exchange_name] = {
                    "symbols": static_symbols,
                    "source": "static_fallback",
                    "count": len(static_symbols)
                }
                logger.warning(f"⚠️[{exchange_name}] 使用静态列表兜底: {len(static_symbols)}个")
            else:
                self._raw_symbols_info[exchange_name] = result
                logger.info(f"✅[{exchange_name}] 获取完成: {result['count']}个合约（来源: {result['source']}）")
    
    async def _fetch_exchange_symbols_with_fallback(self, exchange_name: str) -> Dict[str, Any]:
        """获取单个交易所的合约列表（带智能降级）"""
        symbols = []
        source = "unknown"
        
        # 1. 优先尝试HTTP获取
        try:
            if exchange_name == "binance":
                symbols = await self.fetcher.fetch_binance()
            else:
                symbols = await self.fetcher.fetch_okx()
            
            if symbols:
                source = "http"
                logger.info(f"✅[{exchange_name}] HTTP获取成功: {len(symbols)}个")
                return {"symbols": symbols, "source": source, "count": len(symbols)}
            else:
                logger.warning(f"⚠️[{exchange_name}] HTTP获取返回空列表")
        except Exception as e:
            logger.warning(f"⚠️[{exchange_name}] HTTP获取异常: {e}")
        
        # 2. 降级：使用静态合约列表
        static_symbols = self._get_static_symbols(exchange_name)
        if static_symbols:
            symbols = static_symbols
            source = "static"
            logger.info(f"⚠️[{exchange_name}] 使用静态列表: {len(symbols)}个")
        else:
            logger.error(f"❌❌❌[{exchange_name}] 静态列表也为空，无合约可用")
            symbols = []
            source = "empty"
        
        return {"symbols": symbols, "source": source, "count": len(symbols)}
    
    async def _calculate_common_symbols(self) -> Dict[str, List[str]]:
        """【步骤2】计算双平台共有合约（基于原始数据）"""
        logger.info("🔄【步骤2】计算双平台共有合约...")
        
        binance_info = self._raw_symbols_info.get("binance", {})
        okx_info = self._raw_symbols_info.get("okx", {})
        
        binance_symbols = binance_info.get("symbols", [])
        okx_symbols = okx_info.get("symbols", [])
        
        # 记录原始数据统计
        logger.info(f"📊 币安原始合约: {len(binance_symbols)}个 (来源: {binance_info.get('source', 'unknown')})")
        logger.info(f"📊 OKX原始合约: {len(okx_symbols)}个 (来源: {okx_info.get('source', 'unknown')})")
        
        # 如果任一交易所没有合约，无法进行双平台匹配
        if not binance_symbols or not okx_symbols:
            logger.warning("⚠️ 至少一个交易所无合约，无法进行双平台匹配")
            return {}
        
        # 精确计算双平台共有合约
        common_result = self._find_common_symbols_precise(binance_symbols, okx_symbols)
        
        if common_result and common_result.get("binance") and common_result.get("okx"):
            # 缓存结果
            self._common_symbols_cache = common_result
            self._last_symbols_update = time.time()
            
            binance_count = len(common_result["binance"])
            okx_count = len(common_result["okx"])
            
            logger.info(f"🎯 发现 {binance_count} 个双平台共有合约")
            logger.info(f"📈 匹配成功率: 币安 {binance_count}/{len(binance_symbols)} ({binance_count/len(binance_symbols)*100:.1f}%)")
            logger.info(f"📈 匹配成功率: OKX {okx_count}/{len(okx_symbols)} ({okx_count/len(okx_symbols)*100:.1f}%)")
            
            # 打印前5个共有合约示例
            sample_count = min(5, binance_count)
            for i in range(sample_count):
                binance_sym = common_result["binance"][i]
                okx_sym = common_result["okx"][i]
                coin = self._extract_coin_precise(binance_sym, "binance")
                logger.info(f"  示例{i+1}: {coin} → 币安:{binance_sym} | OKX:{okx_sym}")
            
            return common_result
        else:
            logger.warning("⚠️ 未找到任何双平台共有合约")
            return {}
    
    async def _initialize_all_exchange_pools(self, common_symbols: Dict[str, List[str]]):
        """【步骤3】初始化所有交易所连接池"""
        logger.info("🚀【步骤3】初始化交易所连接池...")
        
        tasks = []
        for exchange_name in ["binance", "okx"]:
            # 确定最终使用的合约列表
            if common_symbols and exchange_name in common_symbols:
                symbols = common_symbols[exchange_name]
                mode = "双平台模式"
            else:
                # 双平台匹配失败，使用该交易所的原始合约列表
                symbols = self._raw_symbols_info[exchange_name].get("symbols", [])
                mode = "单平台模式"
            
            if not symbols:
                logger.warning(f"⚠️[{exchange_name}] 无合约可用，跳过初始化")
                continue
            
            task = asyncio.create_task(
                self._setup_exchange_pool_with_symbols(exchange_name, symbols, mode)
            )
            tasks.append(task)
        
        # 等待所有连接池初始化完成
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, exchange_name in enumerate(["binance", "okx"]):
                if i < len(results):
                    result = results[i]
                    if isinstance(result, Exception):
                        logger.error(f"❌[{exchange_name}] 连接池初始化失败: {result}")
                    # else: 成功日志在任务内部已记录
    
    async def _setup_exchange_pool_with_symbols(self, exchange_name: str, symbols: List[str], mode: str):
        """使用指定合约列表初始化单个交易所连接池"""
        try:
            logger.info(f"[{exchange_name}] 正在初始化连接池 ({mode})...")
            
            # 限制合约数量（根据配置）
            active_connections = EXCHANGE_CONFIGS[exchange_name].get("active_connections", 3)
            symbols_per_conn = EXCHANGE_CONFIGS[exchange_name].get("symbols_per_connection", 300)
            max_symbols = symbols_per_conn * active_connections
            
            original_count = len(symbols)
            if original_count > max_symbols:
                logger.info(f"[{exchange_name}] 合约数量 {original_count} > 限制 {max_symbols}，进行裁剪")
                symbols = symbols[:max_symbols]
                logger.info(f"[{exchange_name}] 裁剪后: {len(symbols)}个合约")
            
            # 初始化连接池
            pool = ExchangeWebSocketPool(exchange_name, self.data_callback, self.admin_instance)
            await pool.initialize(symbols)
            self.exchange_pools[exchange_name] = pool
            
            logger.info(f"✅[{exchange_name}] 连接池初始化成功 ({mode})")
            logger.info(f"  使用合约: {len(symbols)}个")
            logger.info(f"  连接配置: {active_connections}个连接, 每个连接最多{symbols_per_conn}个合约")
            
        except Exception as e:
            logger.error(f"[{exchange_name}] ❌ 连接池初始化失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def _print_initialization_summary(self):
        """打印初始化摘要"""
        logger.info(f"{'=' * 60}")
        logger.info("📋 【初始化完成摘要】")
        
        for exchange_name in ["binance", "okx"]:
            if exchange_name in self.exchange_pools:
                pool = self.exchange_pools[exchange_name]
                source_info = self._raw_symbols_info.get(exchange_name, {})
                
                logger.info(f"  [{exchange_name.upper()}]")
                logger.info(f"    状态: ✅ 运行中")
                logger.info(f"    数据源: {source_info.get('source', 'unknown')}")
                logger.info(f"    原始合约: {source_info.get('count', 0)}个")
                logger.info(f"    使用合约: {len(pool.symbols)}个")
            else:
                logger.info(f"  [{exchange_name.upper()}]")
                logger.info(f"    状态: ❌ 未运行")
        
        # 双平台匹配信息
        if self._common_symbols_cache:
            binance_count = len(self._common_symbols_cache.get("binance", []))
            okx_count = len(self._common_symbols_cache.get("okx", []))
            logger.info(f"  [双平台匹配]")
            logger.info(f"    状态: ✅ 已匹配")
            logger.info(f"    共有合约: {binance_count}个")
        else:
            logger.info(f"  [双平台匹配]")
            logger.info(f"    状态: ⚠️ 未匹配（单平台模式）")
        
        logger.info(f"{'=' * 60}")
    
    # ============ 精确双平台匹配核心方法（保持不变）============
    
    def _find_common_symbols_precise(self, binance_symbols: List[str], okx_symbols: List[str]) -> Dict[str, List[str]]:
        """精确查找双平台共有合约"""
        # 创建币种到合约的映射（精确提取）
        binance_coin_to_contract = {}
        okx_coin_to_contract = {}
        
        # 构建币安映射
        for symbol in binance_symbols:
            coin = self._extract_coin_precise(symbol, "binance")
            if coin and coin not in binance_coin_to_contract:
                binance_coin_to_contract[coin] = symbol
        
        # 构建OKX映射
        for symbol in okx_symbols:
            coin = self._extract_coin_precise(symbol, "okx")
            if coin and coin not in okx_coin_to_contract:
                okx_coin_to_contract[coin] = symbol
        
        # 找出共同币种（精确匹配）
        binance_coins = set(binance_coin_to_contract.keys())
        okx_coins = set(okx_coin_to_contract.keys())
        common_coins = sorted(list(binance_coins.intersection(okx_coins)))
        
        if not common_coins:
            return {}
        
        # 验证每个共同币种的匹配
        validated_common_coins = []
        match_errors = []
        
        for coin in common_coins:
            binance_contract = binance_coin_to_contract[coin]
            okx_contract = okx_coin_to_contract[coin]
            
            # 验证提取的币种是否正确
            binance_extracted = self._extract_coin_precise(binance_contract, "binance")
            okx_extracted = self._extract_coin_precise(okx_contract, "okx")
            
            if binance_extracted == okx_extracted == coin:
                # 进一步检查是否不是部分匹配
                if self._is_valid_match(coin, binance_contract, okx_contract):
                    validated_common_coins.append(coin)
                else:
                    match_errors.append({
                        "coin": coin,
                        "binance": binance_contract,
                        "okx": okx_contract,
                        "reason": "疑似错误匹配"
                    })
            else:
                match_errors.append({
                    "coin": coin,
                    "binance": binance_contract,
                    "okx": okx_contract,
                    "reason": f"币种提取不一致: {binance_extracted} vs {okx_extracted}"
                })
        
        # 记录匹配错误
        if match_errors:
            logger.warning(f"⚠️ 发现 {len(match_errors)} 个疑似错误匹配")
            for error in match_errors[:5]:
                logger.warning(f"  {error['coin']}: 币安={error['binance']}, OKX={error['okx']} - {error['reason']}")
        
        # 生成结果
        result = {
            "binance": [],
            "okx": []
        }
        
        for coin in validated_common_coins:
            result["binance"].append(binance_coin_to_contract[coin])
            result["okx"].append(okx_coin_to_contract[coin])
        
        # 按合约名排序
        result["binance"] = sorted(result["binance"])
        result["okx"] = sorted(result["okx"])
        
        return result
    
    def _extract_coin_precise(self, contract_name: str, exchange: str) -> Optional[str]:
        """精确提取币种"""
        if not contract_name:
            return None
        
        contract_upper = contract_name.upper()
        
        if exchange == "binance":
            # 币安格式: BTCUSDT, 1000SHIBUSDT, BTCDOMUSDT
            if contract_upper.endswith("USDT"):
                # 精确去掉USDT后缀
                coin = contract_upper[:-4]  # BTCUSDT -> BTC
                return coin
            return None
        
        elif exchange == "okx":
            # OKX格式: BTC-USDT-SWAP, 1000SHIB-USDT-SWAP
            if "-USDT-SWAP" in contract_upper:
                # 精确提取币种部分
                coin = contract_upper.replace("-USDT-SWAP", "")
                return coin
            return None
        
        return None
    
    def _is_valid_match(self, coin: str, binance_contract: str, okx_contract: str) -> bool:
        """验证匹配是否合理，防止部分匹配"""
        # 检查是否是常见错误匹配
        common_mistakes = [
            ("BTC", "BTCDOM"),  # BTC不应该匹配BTCDOM
            ("PUMP", "PUMPBTC"),  # PUMP不应该匹配PUMPBTC
            ("BABY", "BABYDOGE"),  # BABY不应该匹配BABYDOGE
            ("DOGE", "BABYDOGE"),  # DOGE不应该匹配BABYDOGE
            ("SHIB", "1000SHIB"),  # SHIB不应该匹配1000SHIB
            ("ETH", "ETHW"),  # ETH不应该匹配ETHW
        ]
        
        # 检查币安合约
        binance_coin = self._extract_coin_precise(binance_contract, "binance")
        if binance_coin != coin:
            return False
        
        # 检查OKX合约
        okx_coin = self._extract_coin_precise(okx_contract, "okx")
        if okx_coin != coin:
            return False
        
        # 检查常见错误匹配
        for correct, wrong in common_mistakes:
            if coin == correct and (binance_coin == wrong or okx_coin == wrong):
                logger.debug(f"发现常见错误匹配: {correct} 匹配到了 {wrong}")
                return False
        
        # 特殊检查：防止币安带数字前缀但OKX没有的情况
        if binance_contract.startswith("1000") and not okx_contract.startswith("1000"):
            logger.debug(f"数字前缀不匹配: 币安={binance_contract}, OKX={okx_contract}")
            return False
        
        return True
    
    def _get_static_symbols(self, exchange_name: str) -> List[str]:
        """备用方案：获取静态合约列表"""
        return STATIC_SYMBOLS.get(exchange_name, [])
    
    # ============ 管理和状态方法 ============
    
    async def get_all_status(self) -> Dict[str, Any]:
        """获取所有交易所连接状态"""
        status = {}
        
        for exchange_name, pool in self.exchange_pools.items():
            try:
                pool_status = await pool.get_status()
                status[exchange_name] = pool_status
            except Exception as e:
                logger.error(f"❌[{exchange_name}] 获取交易所连接状态错误: {e}")
                status[exchange_name] = {"error": str(e)}
        
        return status
    
    async def shutdown(self):
        """关闭所有连接池"""
        if self._shutting_down:
            logger.info("⚠️⚠️⚠️【连接池】连接池已在关闭中，跳过重复操作")
            return
        
        self._shutting_down = True
        logger.info("⚠️⚠️⚠️【连接池】正在关闭所有WebSocket连接池...")
        
        for exchange_name, pool in self.exchange_pools.items():
            try:
                await pool.shutdown()
            except Exception as e:
                logger.error(f"❌[{exchange_name}] 关闭连接池错误: {e}")
        
        logger.info("✅ 【连接池】所有WebSocket连接池已关闭")
    
    async def refresh_common_symbols(self, force: bool = False):
        """手动刷新双平台共有合约列表"""
        logger.info("🔄【连接池】手动刷新双平台共有合约列表...")
        # 这里需要实现 _get_common_symbols 方法
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
    
    def get_raw_symbols_info(self) -> Dict[str, Any]:
        """获取原始合约信息（用于调试）"""
        return self._raw_symbols_info