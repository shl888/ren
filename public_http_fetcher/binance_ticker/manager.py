"""
币安24小时涨跌幅数据管理器
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

from .fetcher import BinanceTickerFetcher

logger = logging.getLogger(__name__)


class BinanceTickerManager:
    """币安24小时涨跌幅数据管理器"""
    
    def __init__(self, data_manager, update_interval: int = 60):
        """
        Args:
            data_manager: 大脑的数据管理器实例
            update_interval: 更新间隔（秒），默认60秒
        """
        self.data_manager = data_manager
        self.update_interval = update_interval
        self.fetcher = BinanceTickerFetcher()
        
        self._running = False
        self._task: Optional[asyncio.Task] = None
        
        logger.info(f"📊【币安Ticker管理器】初始化完成，更新间隔: {update_interval}秒")
    
    async def start(self):
        """启动定时更新任务"""
        if self._running:
            logger.warning("⚠️【币安Ticker管理器】已经在运行中")
            return
        
        self._running = True
        self._task = asyncio.create_task(self._update_loop())
        logger.info("🚀【币安Ticker管理器】已启动")
    
    async def stop(self):
        """停止定时更新任务"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("🛑【币安Ticker管理器】已停止")
    
    async def _update_loop(self):
        """定时更新循环"""
        while self._running:
            try:
                await self._update_once()
                await asyncio.sleep(self.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【币安Ticker管理器】更新循环异常: {e}")
                await asyncio.sleep(10)  # 出错后等10秒再试
    
    async def _update_once(self):
        """执行一次数据更新"""
        data = await self.fetcher.fetch()
        
        if data is None:
            return
        
        # 添加更新时间戳
        for symbol in data:
            data[symbol]['updated_at'] = datetime.now().isoformat()
        
        # 推送到 data_manager 的存储区
        await self._push_to_storage(data)
    
    async def _push_to_storage(self, data: dict):
        """
        推送数据到 data_manager 的 binance_ticker_24hr 存储区
        """
        if hasattr(self.data_manager, 'update_binance_ticker_24hr'):
            await self.data_manager.update_binance_ticker_24hr(data)
        else:
            # 如果 data_manager 还没有这个方法，直接操作 memory_store
            self.data_manager.memory_store['binance_ticker_24hr'] = data
            logger.debug(f"📤【币安Ticker管理器】数据已更新到存储区")
    
    async def fetch_once(self) -> dict:
        """手动获取一次数据（供测试用）"""
        return await self.fetcher.fetch()