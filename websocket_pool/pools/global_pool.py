"""
全局连接池管理器
启动和管理所有交易所的连接池
"""
import asyncio
import logging
import time
from typing import Dict, Any, List, Optional

from ..config import EXCHANGE_CONFIGS
from .exchange_pool import ExchangePool, ExchangePoolConfig
from ..core.monitor import MonitorCenter

logger = logging.getLogger(__name__)

# 默认工作者配置
DEFAULT_WORKER_CONFIGS = {
    "binance": {
        "data_worker_count": 2,     # 2个数据工作者
        "backup_worker_count": 2,   # 2个备份工作者
        "max_symbols_per_worker": 300
    },
    "okx": {
        "data_worker_count": 1,     # 1个数据工作者
        "backup_worker_count": 1,   # 1个备份工作者
        "max_symbols_per_worker": 300
    }
}


class GlobalPoolManager:
    """
    全局连接池管理器
    管理所有交易所的连接池
    """
    
    def __init__(self, data_callback=None):
        """
        初始化全局管理器
        
        Args:
            data_callback: 数据回调函数
        """
        self.data_callback = data_callback
        
        # 交易所连接池
        self.exchange_pools: Dict[str, ExchangePool] = {}
        
        # 监控中心
        self.monitor_center = MonitorCenter(data_callback)
        
        # 状态
        self._running = False
        self._initialized = False
        
        logger.info("[GlobalPool] 全局连接池管理器初始化完成")
    
    async def initialize(self, all_symbols: Dict[str, List[str]]):
        """
        初始化所有交易所连接池
        
        Args:
            all_symbols: 交易所 -> 合约列表 的字典
        """
        if self._initialized:
            logger.warning("[GlobalPool] 已在初始化中或已初始化")
            return
        
        logger.info("=" * 60)
        logger.info("[GlobalPool] 正在初始化全局连接池...")
        logger.info("=" * 60)
        
        try:
            # 1. 初始化每个交易所的连接池
            for exchange, symbols in all_symbols.items():
                if exchange not in EXCHANGE_CONFIGS:
                    logger.warning(f"[GlobalPool] 跳过未配置的交易所: {exchange}")
                    continue
                
                logger.info(f"[GlobalPool] 初始化交易所: {exchange} - {len(symbols)} 个合约")
                await self._initialize_exchange_pool(exchange, symbols)
            
            # 2. 设置监控中心
            logger.info("[GlobalPool] 设置监控中心")
            await self._setup_monitor_center()
            
            # 3. 启动监控中心
            logger.info("[GlobalPool] 启动监控中心")
            await self.monitor_center.start()
            
            self._initialized = True
            self._running = True
            
            logger.info("=" * 60)
            logger.info("[GlobalPool] ✅ 全局连接池初始化完成")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"[GlobalPool] 初始化失败: {e}")
            await self.shutdown()
            raise
    
    async def _initialize_exchange_pool(self, exchange: str, symbols: List[str]):
        """
        初始化单个交易所连接池
        """
        # 获取配置
        exchange_config = EXCHANGE_CONFIGS.get(exchange, {})
        worker_config = DEFAULT_WORKER_CONFIGS.get(exchange, {})
        
        # 创建连接池配置
        pool_config = ExchangePoolConfig(
            exchange=exchange,
            ws_url=exchange_config.get("ws_public_url"),
            data_worker_count=worker_config.get("data_worker_count", 1),
            backup_worker_count=worker_config.get("backup_worker_count", 1),
            max_symbols_per_worker=worker_config.get("max_symbols_per_worker", 300)
        )
        
        if not pool_config.ws_url:
            logger.error(f"[GlobalPool] 交易所 {exchange} 未配置ws_url")
            return
        
        # 创建连接池
        pool = ExchangePool(pool_config, self.data_callback)
        
        # 初始化连接池
        await pool.initialize(symbols)
        
        # 保存
        self.exchange_pools[exchange] = pool
        
        logger.info(f"[GlobalPool] 交易所 {exchange} 初始化完成")
    
    async def _setup_monitor_center(self):
        """
        设置监控中心
        """
        # 为每个交易所的工作者配对注册到监控中心
        for exchange, pool in self.exchange_pools.items():
            worker_pairs = pool.get_worker_pairs()
            
            for data_worker, backup_worker, symbols in worker_pairs:
                self.monitor_center.add_worker_pair(
                    exchange=exchange,
                    data_worker=data_worker,
                    backup_worker=backup_worker,
                    symbols=symbols
                )
        
        logger.info(f"[GlobalPool] 监控中心已设置，共监控 {len(self.exchange_pools)} 个交易所")
    
    async def shutdown(self):
        """
        关闭全局连接池
        """
        if not self._running:
            return
        
        logger.info("[GlobalPool] 正在关闭全局连接池...")
        
        # 停止监控中心
        await self.monitor_center.stop()
        
        # 关闭所有交易所连接池
        shutdown_tasks = []
        for exchange, pool in self.exchange_pools.items():
            shutdown_tasks.append(pool.shutdown())
        
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        
        self.exchange_pools.clear()
        self._running = False
        self._initialized = False
        
        logger.info("[GlobalPool] ✅ 全局连接池已关闭")
    
    async def get_status(self) -> Dict[str, Any]:
        """
        获取全局状态
        """
        status = {
            "running": self._running,
            "initialized": self._initialized,
            "exchanges": {},
            "monitor_center": None,
            "timestamp": time.time()
        }
        
        # 各交易所状态
        for exchange, pool in self.exchange_pools.items():
            pool_status = await pool.get_status()
            status["exchanges"][exchange] = pool_status
        
        # 监控中心状态
        monitor_status = await self.monitor_center.get_status()
        status["monitor_center"] = monitor_status
        
        return status
    
    @property
    def is_running(self) -> bool:
        """
        是否在运行
        
        Returns:
            bool: 运行状态
        """
        return self._running
    
    @property
    def is_initialized(self) -> bool:
        """
        是否已初始化
        
        Returns:
            bool: 初始化状态
        """
        return self._initialized