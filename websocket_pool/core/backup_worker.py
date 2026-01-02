"""
备份工作者 - 最简单的实现
"""
import json
import time
from typing import Set
from .worker_base import BaseWorker

class BackupWorker(BaseWorker):
    """备份工作者 - 只保持心跳"""
    
    def __init__(self, worker_id: str, exchange: str, ws_url: str, data_callback):
        super().__init__(worker_id, exchange, ws_url, data_callback, "backup")
        self._is_ready = False
    
    async def _process_exchange_message(self, data: dict):
        """处理消息 - 只记录心跳"""
        pass  # 备份工作者不处理数据
    
    async def subscribe(self, symbols: Set[str]) -> bool:
        """订阅心跳"""
        if not self._conn or not self._conn.is_connected:
            return False
        
        try:
            # 备份工作者只订阅一个心跳合约
            symbol = list(symbols)[0] if symbols else "BTCUSDT"
            
            if self.exchange == "binance":
                success = await self._subscribe_binance(symbol)
            elif self.exchange == "okx":
                success = await self._subscribe_okx(symbol)
            else:
                return False
            
            if success:
                self._subscribed_symbols = {symbol}
                self._is_subscribed = True
                self._is_ready = True
            
            return success
        except Exception:
            return False
    
    async def _subscribe_binance(self, symbol: str) -> bool:
        """订阅币安心跳"""
        symbol_lower = symbol.lower()
        streams = [f"{symbol_lower}@ticker", f"{symbol_lower}@markPrice"]
        
        msg = {"method": "SUBSCRIBE", "params": streams, "id": 1}
        return await self._conn.send(json.dumps(msg))
    
    async def _subscribe_okx(self, symbol: str) -> bool:
        """订阅欧意心跳"""
        args = [
            {"channel": "tickers", "instId": symbol},
            {"channel": "funding-rate", "instId": symbol}
        ]
        
        msg = {"op": "subscribe", "args": args}
        return await self._conn.send(json.dumps(msg))
    
    async def unsubscribe_all(self) -> bool:
        """取消订阅"""
        self._subscribed_symbols.clear()
        self._is_subscribed = False
        self._is_ready = False
        return True
    
    async def takeover(self, data_symbols: Set[str]) -> bool:
        """
        接管数据工作者职责
        这是唯一复杂的方法，但逻辑必须清晰
        """
        # 1. 取消心跳
        await self.unsubscribe_all()
        
        # 2. 订阅全量数据（使用DataWorker的逻辑）
        success = await self._subscribe_full_data(data_symbols)
        if not success:
            # 失败时恢复心跳
            await self._restore_heartbeat()
            return False
        
        # 3. 更新状态
        self._subscribed_symbols = data_symbols.copy()
        self._is_subscribed = True
        
        # 4. 修改消息处理逻辑（现在需要处理数据）
        self._process_exchange_message = self._process_as_data_worker
        
        return True
    
    async def _subscribe_full_data(self, symbols: Set[str]) -> bool:
        """订阅全量数据（复制DataWorker的逻辑）"""
        if self.exchange == "binance":
            return await self._subscribe_binance_full(symbols)
        elif self.exchange == "okx":
            return await self._subscribe_okx_full(symbols)
        return False
    
    async def _subscribe_binance_full(self, symbols: Set[str]) -> bool:
        """订阅币安全量数据"""
        streams = []
        for symbol in symbols:
            symbol_lower = symbol.lower()
            streams.append(f"{symbol_lower}@ticker")
            streams.append(f"{symbol_lower}@markPrice")
        
        batch_size = 50
        for i in range(0, len(streams), batch_size):
            batch = streams[i:i + batch_size]
            msg = {"method": "SUBSCRIBE", "params": batch, "id": 1}
            if not await self._conn.send(json.dumps(msg)):
                return False
            if i + batch_size < len(streams):
                await asyncio.sleep(1)
        
        return True
    
    async def _subscribe_okx_full(self, symbols: Set[str]) -> bool:
        """订阅欧意全量数据"""
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
    
    async def _restore_heartbeat(self):
        """恢复心跳订阅"""
        if self.exchange == "binance":
            heartbeat = "BTCUSDT"
        elif self.exchange == "okx":
            heartbeat = "BTC-USDT-SWAP"
        else:
            return
        
        await self.subscribe({heartbeat})
    
    async def _process_as_data_worker(self, data: dict):
        """作为数据工作者处理消息（接管后使用）"""
        # 简化版的数据处理
        try:
            if self.exchange == "binance":
                symbol = data.get("s", "").upper()
                event_type = data.get("e", "")
                if symbol and event_type in ["24hrTicker", "markPriceUpdate"]:
                    processed = {
                        "exchange": "binance",
                        "symbol": symbol,
                        "data_type": "ticker" if event_type == "24hrTicker" else "mark_price",
                        "raw_data": data,
                        "worker_id": self.worker_id,
                        "worker_type": "data",  # 注意：现在是数据工作者
                        "timestamp": time.time()
                    }
                    await self.data_callback(processed)
            
            elif self.exchange == "okx":
                arg = data.get("arg", {})
                symbol = arg.get("instId", "")
                if symbol and data.get("data"):
                    processed_symbol = symbol.replace('-USDT-SWAP', 'USDT')
                    processed = {
                        "exchange": "okx",
                        "symbol": processed_symbol,
                        "data_type": "funding_rate" if arg.get("channel") == "funding-rate" else "ticker",
                        "raw_data": data,
                        "worker_id": self.worker_id,
                        "worker_type": "data",  # 注意：现在是数据工作者
                        "timestamp": time.time()
                    }
                    await self.data_callback(processed)
                    
        except Exception:
            pass
    
    @property
    def is_ready(self) -> bool:
        """是否准备好接管"""
        return self._is_ready