"""
币安24小时涨跌幅数据获取器
"""

import aiohttp
import asyncio
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class BinanceTickerFetcher:
    """币安24小时涨跌幅数据获取器"""
    
    def __init__(self):
        self.base_url = "https://fapi.binance.com"
        self.endpoint = "/fapi/v1/ticker/24hr"
    
    async def fetch(self) -> Optional[Dict[str, Dict]]:
        """
        获取所有USDT永续合约的24小时涨跌幅数据
        
        Returns:
            Dict: 以 symbol 为 key 的数据字典
            None: 请求失败
        """
        url = f"{self.base_url}{self.endpoint}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        logger.error(f"❌【币安Ticker】请求失败: HTTP {response.status}")
                        return None
                    
                    raw_data = await response.json()
                    return self._parse(raw_data)
                    
        except asyncio.TimeoutError:
            logger.error("❌【币安Ticker】请求超时")
            return None
        except Exception as e:
            logger.error(f"❌【币安Ticker】请求异常: {e}")
            return None
    
    def _parse(self, raw_data: list) -> Dict[str, Dict]:
        """
        解析原始数据，只保留USDT永续合约
        
        Returns:
            {
                'BTCUSDT': {
                    'symbol': 'BTCUSDT',
                    'priceChangePercent': 2.5,
                    'lastPrice': 50000.0,
                    'volume': 12345.6
                },
                ...
            }
        """
        result = {}
        
        for item in raw_data:
            symbol = item.get('symbol', '')
            
            # 只保留 USDT 后缀的永续合约
            if not symbol.endswith('USDT'):
                continue
            
            try:
                result[symbol] = {
                    'symbol': symbol,
                    'priceChangePercent': float(item.get('priceChangePercent', 0)),
                    'lastPrice': float(item.get('lastPrice', 0)),
                    'volume': float(item.get('volume', 0))
                }
            except (ValueError, TypeError) as e:
                logger.warning(f"⚠️【币安Ticker】解析 {symbol} 数据异常: {e}")
                continue
        
        logger.info(f"✅【币安Ticker】获取成功，共 {len(result)} 个USDT永续合约")
        return result