"""
交易所连接池
管理特定交易所的所有工作者
"""
import asyncio
import logging
import time
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field

from ..core.data_worker import DataWorker
from ..core.backup_worker import BackupWorker, BackupWorker

logger = logging.getLogger(__name__)

@dataclass
class ExchangePoolConfig:
    """
    交易所连接池配置
    """
    exchange: str
    ws_url: str
    data_worker_count: int = 1      # 数据工作者数量
    backup_worker_count: int = 1    # 备份工作者数量
    max_symbols_per_worker: int = 300  # 每个工作者最大订阅数量


class ExchangePool:
    """
    交易所连接池
    管理特定交易所的所有工作者
    """
    
    def __init__(self, config: ExchangePoolConfig, data_callback=None):
        """
        初始化交易所连接池
        
        Args:
            config: 连接池配置
            data_callback: 数据回调函数
        """
        self.config = config
        self.data_callback = data_callback
        
        # 工作者存储
        self.data_workers: List[DataWorker] = []
        self.backup_workers: List[BackupWorker] = []
        
        # 合约分配
        self.worker_symbols: Dict[str, Set[str]] = {}  # worker_id -> symbols
        
        # 状态
        self._initialized = False
        self._initializing = False
        
        logger.info(f"[{config.exchange}] 交易所连接池创建")
    
    async def initialize(self, all_symbols: List[str]):
        """
        初始化连接池
        
        Args:
            all_symbols: 该交易所的所有合约列表
        """
        if self._initialized or self._initializing:
            return
        
        self._initializing = True
        logger.info(f"[{self.config.exchange}] 正在初始化，共 {len(all_symbols)} 个合约")
        
        try:
            # 1. 分配合约到数据工作者
            symbol_groups = self._distribute_symbols(all_symbols)
            
            # 2. 创建数据工作者
            logger.info(f"[{self.config.exchange}] 创建 {self.config.data_worker_count} 个数据工作者")
            for i in range(self.config.data_worker_count):
                await self._create_data_worker(i, symbol_groups[i] if i < len(symbol_groups) else set())
            
            # 3. 创建备份工作者
            logger.info(f"[{self.config.exchange}] 创建 {self.config.backup_worker_count} 个备份工作者")
            for i in range(self.config.backup_worker_count):
                await self._create_backup_worker(i)
            
            self._initialized = True
            logger.info(f"[{self.config.exchange}] 初始化完成")
            
        except Exception as e:
            logger.error(f"[{self.config.exchange}] 初始化失败: {e}")
            raise
        finally:
            self._initializing = False
    
    def _distribute_symbols(self, all_symbols: List[str]) -> List[Set[str]]:
        """
        分配合约到各个工作者
        """
        worker_count = self.config.data_worker_count
        symbols_per_worker = len(all_symbols) // worker_count
        remainder = len(all_symbols) % worker_count
        
        symbol_groups = []
        start_idx = 0
        
        for i in range(worker_count):
            # 计算该工作者的合约数量
            count = symbols_per_worker + (1 if i < remainder else 0)
            
            # 提取合约
            symbols = set(all_symbols[start_idx:start_idx + count])
            symbol_groups.append(symbols)
            
            start_idx += count
            
            logger.info(f"[{self.config.exchange}] 工作者 {i}: {len(symbols)} 个合约")
        
        return symbol_groups
    
    async def _create_data_worker(self, index: int, symbols: Set[str]):
        """
        创建数据工作者
        """
        worker_id = f"worker_{self.config.exchange}_data_{index}"
        
        worker = DataWorker(
            worker_id=worker_id,
            exchange=self.config.exchange,
            ws_url=self.config.ws_url,
            data_callback=self.data_callback
        )
        
        # 启动工作者
        await worker.start(symbols)
        
        # 保存
        self.data_workers.append(worker)
        self.worker_symbols[worker_id] = symbols.copy()
        
        logger.info(f"[{self.config.exchange}] 数据工作者创建成功: {worker_id} - {len(symbols)} 个合约")
    
    async def _create_backup_worker(self, index: int):
        """
        创建备份工作者
        """
        worker_id = f"worker_{self.config.exchange}_backup_{index}"
        
        worker = BackupWorker(
            worker_id=worker_id,
            exchange=self.config.exchange,
            ws_url=self.config.ws_url,
            data_callback=self.data_callback
        )
        
        # 启动工作者（订阅心跳）
        heartbeat_symbols = self._get_heartbeat_symbols()
        await worker.start(set(heartbeat_symbols))
        
        # 保存
        self.backup_workers.append(worker)
        
        logger.info(f"[{self.config.exchange}] 备份工作者创建成功: {worker_id}")
    
    def _get_heartbeat_symbols(self) -> List[str]:
        """
        获取心跳合约
        """
        if self.config.exchange == "binance":
            return ["BTCUSDT"]
        elif self.config.exchange == "okx":
            return ["BTC-USDT-SWAP"]
        return []
    
    def get_worker_pairs(self) -> List[tuple]:
        """
        获取工作者对（数据工作者 + 备份工作者）
        按照1:1配对
        """
        pairs = []
        min_count = min(len(self.data_workers), len(self.backup_workers))
        
        for i in range(min_count):
            data_worker = self.data_workers[i]
            backup_worker = self.backup_workers[i]
            symbols = self.worker_symbols.get(data_worker.worker_id, set())
            
            pairs.append((data_worker, backup_worker, symbols))
        
        return pairs
    
    async def shutdown(self):
        """
        关闭连接池
        """
        logger.info(f"[{self.config.exchange}] 正在关闭连接池")
        
        # 停止所有工作者
        stop_tasks = []
        for worker in self.data_workers + self.backup_workers:
            stop_tasks.append(worker.stop())
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        self.data_workers.clear()
        self.backup_workers.clear()
        self.worker_symbols.clear()
        self._initialized = False
        
        logger.info(f"[{self.config.exchange}] 连接池已关闭")
    
    async def get_status(self) -> dict:
        """
        获取连接池状态
        """
        status = {
            "exchange": self.config.exchange,
            "initialized": self._initialized,
            "data_workers": [],
            "backup_workers": [],
            "timestamp": time.time()
        }
        
        # 数据工作者状态
        for worker in self.data_workers:
            worker_status = await worker.get_status()
            status["data_workers"].append(worker_status)
        
        # 备份工作者状态
        for worker in self.backup_workers:
            worker_status = await worker.get_status()
            status["backup_workers"].append(worker_status)
        
        return status