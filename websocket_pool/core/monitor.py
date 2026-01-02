"""
监控中心
职责：监控所有工作者，执行故障转移，日志记录
特性：全局唯一，独立运行，永不中断
"""
import asyncio
import logging
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class WorkerPair:
    """
    工作者配对
    一个数据工作者 + 一个备份工作者
    """
    data_worker: any
    backup_worker: any
    symbols: Set[str] = field(default_factory=set)
    is_active: bool = True


class MonitorCenter:
    """
    监控中心
    全局唯一，监控所有交易所的所有工作者
    """
    
    def __init__(self, data_callback=None):
        """
        初始化监控中心
        
        Args:
            data_callback: 数据回调函数（用于传递给工作者）
        """
        self.data_callback = data_callback
        
        # 工作者管理
        self.worker_pairs: Dict[str, List[WorkerPair]] = {
            "binance": [],
            "okx": []
        }
        
        # 监控状态
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
        self._check_interval = 3  # 监控检查间隔（秒）
        
        # 故障转移锁
        self._failover_lock = asyncio.Lock()
        
        logger.info("[MonitorCenter] 监控中心初始化完成")
    
    async def start(self):
        """
        启动监控中心
        """
        if self._running:
            logger.warning("[MonitorCenter] 已在运行中")
            return
        
        self._running = True
        self._monitor_task = asyncio.create_task(
            self._monitor_loop(),
            name="monitor_center"
        )
        
        logger.info("[MonitorCenter] 监控中心已启动")
    
    async def stop(self):
        """
        停止监控中心
        """
        if not self._running:
            return
        
        self._running = False
        
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("[MonitorCenter] 监控中心已停止")
    
    async def _monitor_loop(self):
        """
        监控循环
        """
        logger.info("[MonitorCenter] 监控循环开始")
        
        while self._running:
            try:
                start_time = time.time()
                
                # 检查所有工作者对
                for exchange in ["binance", "okx"]:
                    for pair in self.worker_pairs[exchange]:
                        if pair.is_active:
                            await self._check_worker_pair(exchange, pair)
                
                # 计算执行时间
                elapsed = time.time() - start_time
                if elapsed < self._check_interval:
                    await asyncio.sleep(self._check_interval - elapsed)
                else:
                    logger.warning(f"[MonitorCenter] 监控检查耗时过长: {elapsed:.2f}s")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[MonitorCenter] 监控循环异常: {e}")
                await asyncio.sleep(self._check_interval)
        
        logger.info("[MonitorCenter] 监控循环结束")
    
    async def _check_worker_pair(self, exchange: str, pair: WorkerPair):
        """
        检查工作者对状态
        """
        # 检查数据工作者
        if pair.data_worker and not pair.data_worker.is_connected:
            logger.warning(f"[MonitorCenter] {exchange} 数据工作者断开: {pair.data_worker.worker_id}")
            await self._handle_data_worker_failure(exchange, pair)
        
        # 检查备份工作者
        if pair.backup_worker and not pair.backup_worker.is_connected:
            logger.warning(f"[MonitorCenter] {exchange} 备份工作者断开: {pair.backup_worker.worker_id}")
            await self._handle_backup_worker_failure(exchange, pair)
    
    async def _handle_data_worker_failure(self, exchange: str, pair: WorkerPair):
        """
        处理数据工作者故障
        原则：数据工作者断开 → 备份工作者接管 → 创建新的备份工作者
        """
        async with self._failover_lock:
            logger.info(f"[MonitorCenter] 开始处理数据工作者故障: {pair.data_worker.worker_id}")
            
            try:
                # 1. 备份工作者是否准备好接管？
                if not pair.backup_worker or not pair.backup_worker.is_ready_for_takeover:
                    logger.error(f"[MonitorCenter] 备份工作者未准备好接管: {pair.backup_worker.worker_id if pair.backup_worker else 'None'}")
                    
                    # 尝试重建备份工作者
                    await self._recreate_backup_worker(exchange, pair)
                    return
                
                # 2. 执行接管
                logger.info(f"[MonitorCenter] 备份工作者接管: {pair.backup_worker.worker_id}")
                success = await pair.backup_worker.takeover_data_worker(pair.symbols)
                
                if not success:
                    logger.error(f"[MonitorCenter] 接管失败")
                    # 尝试重建备份工作者
                    await self._recreate_backup_worker(exchange, pair)
                    return
                
                # 3. 交换角色
                old_data_worker = pair.data_worker
                pair.data_worker = pair.backup_worker
                
                # 4. 创建新的备份工作者
                new_backup = await self._create_new_backup_worker(
                    exchange,
                    old_data_worker.worker_id.replace("data", "backup_new")
                )
                pair.backup_worker = new_backup
                
                # 5. 停止原数据工作者（不重连！）
                await old_data_worker.stop()
                
                logger.info(f"[MonitorCenter] 故障转移完成:")
                logger.info(f"[MonitorCenter]   新数据工作者: {pair.data_worker.worker_id}")
                logger.info(f"[MonitorCenter]   新备份工作者: {pair.backup_worker.worker_id}")
                logger.info(f"[MonitorCenter]   原数据工作者: {old_data_worker.worker_id} 已停止")
                
            except Exception as e:
                logger.error(f"[MonitorCenter] 处理数据工作者故障异常: {e}")
    
    async def _handle_backup_worker_failure(self, exchange: str, pair: WorkerPair):
        """
        处理备份工作者故障
        原则：备份工作者断开 → 直接创建新的备份工作者
        """
        async with self._failover_lock:
            logger.info(f"[MonitorCenter] 开始处理备份工作者故障: {pair.backup_worker.worker_id}")
            
            try:
                # 直接创建新的备份工作者
                new_backup = await self._create_new_backup_worker(
                    exchange,
                    pair.backup_worker.worker_id.replace("backup", "backup_new")
                )
                
                # 停止原备份工作者
                await pair.backup_worker.stop()
                
                # 更新配对
                pair.backup_worker = new_backup
                
                logger.info(f"[MonitorCenter] 备份工作者已替换:")
                logger.info(f"[MonitorCenter]   新备份工作者: {new_backup.worker_id}")
                logger.info(f"[MonitorCenter]   原备份工作者: {pair.backup_worker.worker_id} 已停止")
                
            except Exception as e:
                logger.error(f"[MonitorCenter] 处理备份工作者故障异常: {e}")
    
    async def _recreate_backup_worker(self, exchange: str, pair: WorkerPair):
        """
        重建备份工作者
        """
        try:
            logger.info(f"[MonitorCenter] 重建备份工作者")
            
            new_backup = await self._create_new_backup_worker(
                exchange,
                f"worker_{exchange}_backup_recreated"
            )
            
            # 停止原备份工作者（如果存在）
            if pair.backup_worker:
                await pair.backup_worker.stop()
            
            pair.backup_worker = new_backup
            
            logger.info(f"[MonitorCenter] 备份工作者重建完成: {new_backup.worker_id}")
            
        except Exception as e:
            logger.error(f"[MonitorCenter] 重建备份工作者失败: {e}")
    
    async def _create_new_backup_worker(self, exchange: str, worker_id: str):
        """
        创建新的备份工作者
        """
        from .worker import BackupWorker
        
        # 需要从配置获取ws_url
        from ..config import EXCHANGE_CONFIGS
        
        ws_url = EXCHANGE_CONFIGS.get(exchange, {}).get("ws_public_url")
        if not ws_url:
            raise ValueError(f"交易所 {exchange} 未配置ws_url")
        
        # 创建备份工作者
        backup = BackupWorker(
            worker_id=worker_id,
            exchange=exchange,
            ws_url=ws_url,
            data_callback=self.data_callback
        )
        
        # 启动并订阅心跳
        heartbeat_symbols = self._get_heartbeat_symbols(exchange)
        await backup.start(set(heartbeat_symbols))
        
        return backup
    
    def _get_heartbeat_symbols(self, exchange: str) -> List[str]:
        """
        获取心跳合约
        """
        if exchange == "binance":
            return ["BTCUSDT"]
        elif exchange == "okx":
            return ["BTC-USDT-SWAP"]
        return []
    
    def add_worker_pair(self, exchange: str, data_worker: any, backup_worker: any, symbols: Set[str]):
        """
        添加工作者对到监控中心
        """
        pair = WorkerPair(
            data_worker=data_worker,
            backup_worker=backup_worker,
            symbols=symbols.copy(),
            is_active=True
        )
        
        self.worker_pairs[exchange].append(pair)
        logger.info(f"[MonitorCenter] 添加工作者对: {exchange} - {data_worker.worker_id} + {backup_worker.worker_id}")
    
    async def get_status(self) -> dict:
        """
        获取监控中心状态
        """
        status = {
            "running": self._running,
            "exchange_status": {},
            "timestamp": time.time()
        }
        
        for exchange, pairs in self.worker_pairs.items():
            exchange_status = {
                "pair_count": len(pairs),
                "pairs": []
            }
            
            for i, pair in enumerate(pairs):
                data_status = await pair.data_worker.get_status() if pair.data_worker else None
                backup_status = await pair.backup_worker.get_status() if pair.backup_worker else None
                
                exchange_status["pairs"].append({
                    "index": i,
                    "is_active": pair.is_active,
                    "symbols_count": len(pair.symbols),
                    "data_worker": data_status,
                    "backup_worker": backup_status
                })
            
            status["exchange_status"][exchange] = exchange_status
        
        return status