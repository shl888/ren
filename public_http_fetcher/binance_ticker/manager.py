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
        logger.info("🚀【币安Ticker管理器】已启动，1分钟后开始首次获取")
    
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
        # 启动后先等1分钟，不立即请求
        logger.info("⏳【币安Ticker管理器】等待60秒后开始首次获取...")
        await asyncio.sleep(60)
        
        while self._running:
            try:
                # 尝试获取数据
                await self._update_once()
            except asyncio.CancelledError:
                break
            except Exception:
                # 任何异常都吞掉，打印日志，继续下一轮
                logger.error("❌【币安Ticker管理器】更新失败，跳过本次，等待下一轮", exc_info=True)
            
            # 等待下一轮（无论成功失败都等）
            if self._running:
                await asyncio.sleep(self.update_interval)
    
    async def _update_once(self):
        """执行一次数据更新"""
        # 加上超时控制，防止请求卡死
        try:
            data = await asyncio.wait_for(self.fetcher.fetch(), timeout=15.0)
        except asyncio.TimeoutError:
            logger.error("❌【币安Ticker管理器】请求超时（15秒），跳过本次")
            return
        except Exception as e:
            logger.error(f"❌【币安Ticker管理器】请求异常: {e}")
            return
        
        if data is None:
            logger.warning("⚠️【币安Ticker管理器】获取数据为空，跳过本次")
            return
        
        # 添加更新时间戳
        for symbol in data:
            data[symbol]['updated_at'] = datetime.now().isoformat()
        
        # 推送到 data_manager 的存储区
        await self._push_to_storage(data)
        logger.info(f"✅【币安Ticker管理器】数据已更新，共 {len(data)} 个合约")
    
    async def _push_to_storage(self, data: dict):
        """
        推送数据到 data_manager 的 binance_ticker_24hr 存储区
        """
        try:
            if hasattr(self.data_manager, 'update_binance_ticker_24hr'):
                await self.data_manager.update_binance_ticker_24hr(data)
            else:
                self.data_manager.memory_store['binance_ticker_24hr'] = data
        except Exception as e:
            logger.error(f"❌【币安Ticker管理器】推送数据到存储区失败: {e}")
    
    async def fetch_once(self) -> dict:
        """手动获取一次数据（供测试用）"""
        return await self.fetcher.fetch()