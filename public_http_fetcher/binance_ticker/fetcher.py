"""
币安24小时涨跌幅数据获取器
"""

import aiohttp
import asyncio
import logging
import time
from typing import Dict, Optional, Set

logger = logging.getLogger(__name__)


class BinanceTickerFetcher:
    """币安24小时涨跌幅数据获取器"""
    
    def __init__(self):
        self.base_url = "https://fapi.binance.com"
        self._valid_symbols: Optional[Set[str]] = None
        self._whitelist_updated_at: float = 0
        self._whitelist_ttl = 3600  # 白名单缓存1小时
    
    async def _fetch_valid_symbols(self) -> Set[str]:
        """
        获取当前活跃的 USDT 永续合约白名单（缓存1小时）
        
        通过 /fapi/v1/exchangeInfo 接口获取，只保留同时满足以下条件的合约：
        1. status == "TRADING"        （正在交易）
        2. quoteAsset == "USDT"       （U本位）
        3. contractType == "PERPETUAL"（永续合约）
        """
        now = time.time()
        
        # 缓存未过期，直接返回
        if self._valid_symbols and (now - self._whitelist_updated_at) < self._whitelist_ttl:
            logger.debug(f"📋【币安Ticker】使用缓存的白名单，共 {len(self._valid_symbols)} 个合约")
            return self._valid_symbols
        
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        logger.error(f"❌【币安Ticker】获取白名单失败: HTTP {response.status}")
                        return self._valid_symbols or set()
                    
                    data = await response.json()
                    symbols = set()
                    
                    for s in data.get('symbols', []):
                        if (s.get('status') == 'TRADING' and 
                            s.get('quoteAsset') == 'USDT' and 
                            s.get('contractType') == 'PERPETUAL'):
                            symbols.add(s.get('symbol'))
                    
                    self._valid_symbols = symbols
                    self._whitelist_updated_at = now
                    
                    logger.info(f"✅【币安Ticker】白名单已更新，共 {len(symbols)} 个有效USDT永续合约")
                    return symbols
                    
        except asyncio.TimeoutError:
            logger.error("❌【币安Ticker】获取白名单超时")
            return self._valid_symbols or set()
        except Exception as e:
            logger.error(f"❌【币安Ticker】获取白名单异常: {e}")
            return self._valid_symbols or set()
    
    async def fetch(self) -> Optional[Dict[str, Dict]]:
        """
        获取所有有效USDT永续合约的24小时涨跌幅数据
        """
        url = f"{self.base_url}/fapi/v1/ticker/24hr"
        
        try:
            async with aiohttp.ClientSession() as session:
                # 第一步：获取白名单（带缓存）
                valid_symbols = await self._fetch_valid_symbols()
                
                if not valid_symbols:
                    logger.warning("⚠️【币安Ticker】白名单为空，跳过本次获取")
                    return None
                
                # 第二步：获取全量行情数据
                async with session.get(url) as response:
                    if response.status != 200:
                        logger.error(f"❌【币安Ticker】请求失败: HTTP {response.status}")
                        return None
                    
                    raw_data = await response.json()
                    
                    # 第三步：用白名单过滤
                    result = {}
                    filtered_count = 0
                    
                    for item in raw_data:
                        symbol = item.get('symbol', '')
                        
                        if symbol not in valid_symbols:
                            filtered_count += 1
                            continue
                        
                        try:
                            result[symbol] = {
                                'symbol': symbol,
                                'priceChangePercent': float(item.get('priceChangePercent', 0)),
                                'lastPrice': float(item.get('lastPrice', 0)),
                                'volume': float(item.get('volume', 0))
                            }
                        except (ValueError, TypeError):
                            continue
                    
                    if filtered_count > 0:
                        logger.debug(f"🧹【币安Ticker】已过滤 {filtered_count} 个无效合约")
                    
                    logger.info(f"✅【币安Ticker】获取成功，共 {len(result)} 个有效USDT永续合约")
                    return result
                    
        except asyncio.TimeoutError:
            logger.error("❌【币安Ticker】请求超时")
            return None
        except Exception as e:
            logger.error(f"❌【币安Ticker】请求异常: {e}")
            return None