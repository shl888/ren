"""
工作者基类 - 最简版本
"""
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Optional, Set, Callable

logger = logging.getLogger(__name__)

class BaseWorker(ABC):
    """工作者基类 - 最简版本"""
    
    def __init__(self, worker_id: str, exchange: str, ws_url: str, data_callback: Callable, worker_type: str):
        self.worker_id = worker_id
        self.exchange = exchange
        self.ws_url = ws_url
        self.data_callback = data_callback
        self.worker_type = worker_type
        
        # 核心状态
        self._conn = None
        self._subscribed_symbols = set()
        self._is_subscribed = False
        self._last_active_time = time.time()
        
        logger.info(f"[{worker_id}] {worker_type}工作者创建")
    
    async def start(self, symbols: Optional[Set[str]] = None):
        """启动工作者"""
        logger.info(f"[{self.worker_id}] 正在启动")
        
        # 创建连接
        await self._create_connection()
        
        # 订阅
        if symbols:
            await self.subscribe(symbols)
    
    async def stop(self):
        """停止工作者"""
        logger.info(f"[{self.worker_id}] 正在停止")
        if self._conn:
            await self._conn.disconnect()
            self._conn = None
    
    async def _create_connection(self):
        """创建物理连接"""
        import uuid
        conn_id = f"conn_{self.exchange}_{self.worker_type}_{uuid.uuid4().hex[:8]}"
        
        from websocket_pool.core.connection import Connection
        self._conn = Connection(
            conn_id=conn_id,
            exchange=self.exchange,
            ws_url=self.ws_url,
            on_message=self._handle_raw_message
        )
        
        success = await self._conn.connect()
        if not success:
            logger.error(f"[{self.worker_id}] 连接创建失败")
            raise ConnectionError(f"无法建立连接: {self.worker_id}")
        
        logger.info(f"[{self.worker_id}] 连接创建成功")
    
    async def _handle_raw_message(self, raw_message: str):
        """处理原始消息"""
        self._last_active_time = time.time()
        
        try:
            import json
            data = json.loads(raw_message)
            await self._process_exchange_message(data)
        except Exception as e:
            logger.warning(f"[{self.worker_id}] 消息处理异常: {e}")
    
    @abstractmethod
    async def _process_exchange_message(self, data: dict):
        """处理交易所消息（子类实现）"""
        pass
    
    @abstractmethod
    async def subscribe(self, symbols: Set[str]) -> bool:
        """订阅合约（子类实现）"""
        pass
    
    @abstractmethod
    async def unsubscribe_all(self) -> bool:
        """取消所有订阅（子类实现）"""
        pass
    
    @property
    def is_connected(self) -> bool:
        """是否已连接"""
        return self._conn and self._conn.is_connected
    
    @property
    def is_subscribed(self) -> bool:
        """是否已订阅"""
        return self._is_subscribed
    
    async def get_status(self) -> dict:
        """获取状态"""
        conn_status = await self._conn.get_status() if self._conn else None
        
        return {
            "worker_id": self.worker_id,
            "worker_type": self.worker_type,
            "exchange": self.exchange,
            "connected": self.is_connected,
            "subscribed": self.is_subscribed,
            "symbols_count": len(self._subscribed_symbols),
            "last_active_age": time.time() - self._last_active_time,
            "connection": conn_status,
            "timestamp": time.time()
        }