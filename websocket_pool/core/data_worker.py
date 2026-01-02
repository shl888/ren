"""
数据工作者 - 只做必要的事
"""
import json
import time
from typing import Set, Optional
from .worker_base import BaseWorker

class DataWorker(BaseWorker):
    """数据工作者 - 订阅全量数据"""
    
    def __init__(self, worker_id: str, exchange: str, ws_url: str, data_callback):
        super().__init__(worker_id, exchange, ws_url, data_callback, "data")
    
    async def _process_exchange_message(self, data: dict):
        """处理消息 - 只转发有效数据"""
        # 过滤控制消息
        if self._is_control_message(data):
            return
        
        # 构建标准数据
        processed = self._build_standard_data(data)
        if processed:
            processed["worker_id"] = self.worker_id
            processed["worker_type"] = "data"
            await self.data_callback(processed)
    
    def _is_control_message(self, data: dict) -> bool:
        """判断是否是控制消息"""
        if self.exchange == "binance":
            return "result" in data or "id" in data
        elif self.exchange == "okx":
            return "event" in data
        return False
    
    def _build_standard_data(self, data: dict) -> Optional[dict]:
        """构建标准数据"""
        try:
            if self.exchange == "binance":
                return self._process_binance_data(data)
            elif self.exchange == "okx":
                return self._process_okx_data(data)
        except Exception:
            pass
        return None
    
    def _process_binance_data(self, data: dict) -> dict:
        """处理币安数据"""
        event_type = data.get("e", "")
        symbol = data.get("s", "").upper()
        
        if event_type == "24hrTicker":
            data_type = "ticker"
        elif event_type == "markPriceUpdate":
            data_type = "mark_price"
        else:
            data_type = "unknown"
        
        return {
            "exchange": "binance",
            "symbol": symbol,
            "data_type": data_type,
            "raw_data": data,
            "timestamp": time.time()
        }
    
    def _process_okx_data(self, data: dict) -> Optional[dict]:
        """处理欧意数据"""
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        symbol = arg.get("instId", "")
        
        if not symbol or not data.get("data"):
            return None
        
        processed_symbol = symbol.replace('-USDT-SWAP', 'USDT')
        
        return {
            "exchange": "okx",
            "symbol": processed_symbol,
            "data_type": "funding_rate" if channel == "funding-rate" else "ticker",
            "channel": channel,
            "raw_data": data,
            "original_symbol": symbol,
            "timestamp": time.time()
        }
    
    async def subscribe(self, symbols: Set[str]) -> bool:
        """订阅合约"""
        if not symbols or not self._conn or not self._conn.is_connected:
            return False
        
        try:
            if self.exchange == "binance":
                success = await self._subscribe_binance(symbols)
            elif self.exchange == "okx":
                success = await self._subscribe_okx(symbols)
            else:
                return False
            
            if success:
                self._subscribed_symbols = symbols.copy()
                self._is_subscribed = True
            
            return success
        except Exception:
            return False
    
    async def _subscribe_binance(self, symbols: Set[str]) -> bool:
        """订阅币安"""
        streams = []
        for symbol in symbols:
            symbol_lower = symbol.lower()
            streams.append(f"{symbol_lower}@ticker")
            streams.append(f"{symbol_lower}@markPrice")
        
        # 简单分批
        batch_size = 50
        for i in range(0, len(streams), batch_size):
            batch = streams[i:i + batch_size]
            msg = {"method": "SUBSCRIBE", "params": batch, "id": 1}
            if not await self._conn.send(json.dumps(msg)):
                return False
            if i + batch_size < len(streams):
                await asyncio.sleep(1)
        
        return True
    
    async def _subscribe_okx(self, symbols: Set[str]) -> bool:
        """订阅欧意"""
        all_args = []
        for symbol in symbols:
            all_args.append({"channel": "tickers", "instId": symbol})
            all_args.append({"channel": "funding-rate", "instId": symbol})
        
        batch_size = 50
        for i in range(0, len(all_args), batch_size):
            batch = all_args[i:i + batch_size]
            msg = {"op": "subscribe", "args": batch}
            if not await self._conn.send(json.dumps(msg)):
                return False
            if i + batch_size < len(all_args):
                await asyncio.sleep(1)
        
        return True
    
    async def unsubscribe_all(self) -> bool:
        """取消订阅"""
        if not self._subscribed_symbols:
            return True
        
        try:
            self._subscribed_symbols.clear()
            self._is_subscribed = False
            return True
        except Exception:
            return False