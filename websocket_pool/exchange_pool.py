"""
单个交易所的连接池管理 - 终极稳定版
设计原则：槽位固定，连接ID随角色变，冷却期绑定槽位
"""
import asyncio
import logging
import sys
import os
import time
from typing import Dict, Any, List, Optional
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store
from .connection import WebSocketConnection, ConnectionType
from .config import EXCHANGE_CONFIGS

logger = logging.getLogger(__name__)

class ExchangeWebSocketPool:
    """终极稳定版：槽位固定，连接ID动态"""
    
    def __init__(self, exchange: str, data_callback=None):
        self.exchange = exchange
        self.data_callback = data_callback or self._create_default_callback()
        self.config = EXCHANGE_CONFIGS.get(exchange, {})
        
        # 🚨 核心设计：槽位固定，连接对象可替换
        self.master_slots = []  # 每个元素是 {"index": 0, "connection": obj}
        self.warm_standby_slots = []  # 每个元素是 {"index": 0, "connection": obj}
        
        self.monitor_connection = None
        self.symbols = []
        self.symbol_groups = []
        
        # 🚨 冷却期绑定槽位索引
        self.slot_cooldown = {}  # {slot_index: 切换时间戳}
        
        self.health_check_task = None
        self.monitor_scheduler_task = None
        
        logger.info(f"[{self.exchange}] 终极稳定版连接池初始化完成")

    def _create_default_callback(self):
        async def default_callback(data):
            try:
                if "exchange" not in data or "symbol" not in data:
                    logger.warning(f"[{self.exchange}] 数据缺少必要字段: {data}")
                    return
                await data_store.update_market_data(data["exchange"], data["symbol"], data)
            except Exception as e:
                logger.error(f"[{self.exchange}] 数据存储失败: {e}")
        return default_callback
        
    async def initialize(self, symbols: List[str]):
        """初始化槽位，不是连接"""
        self.symbols = symbols
        
        symbols_per_master = self.config.get("symbols_per_master", 300)
        self.symbol_groups = [
            symbols[i:i + symbols_per_master]
            for i in range(0, len(symbols), symbols_per_master)
        ]
        
        masters_count = self.config.get("masters_count", 3)
        if len(self.symbol_groups) > masters_count:
            self._balance_symbol_groups(masters_count)
        
        logger.info(f"[{self.exchange}] 初始化 {len(self.symbol_groups)} 个主槽位 + 3个温备槽位")
        
        # 初始化主槽位
        await self._initialize_master_slots()
        
        # 初始化温备槽位
        await self._initialize_warm_slots()
        
        # 启动监控
        await self._initialize_monitor_scheduler()
        
        # 启动健康检查
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        
        logger.info(f"[{self.exchange}] 连接池初始化全部完成！")
    
    def _balance_symbol_groups(self, target_groups: int):
        """平衡合约分组"""
        avg_size = len(self.symbols) // target_groups
        remainder = len(self.symbols) % target_groups
        
        self.symbol_groups = []
        start = 0
        
        for i in range(target_groups):
            size = avg_size + (1 if i < remainder else 0)
            if start + size <= len(self.symbols):
                self.symbol_groups.append(self.symbols[start:start + size])
                start += size
        
        logger.info(f"[{self.exchange}] 合约重新平衡为 {len(self.symbol_groups)} 组")
    
    async def _initialize_master_slots(self):
        """初始化主槽位（槽位固定）"""
        ws_url = self.config.get("ws_public_url")
        
        for i, symbol_group in enumerate(self.symbol_groups):
            conn_id = f"{self.exchange}_master_{i}"
            
            connection = WebSocketConnection(
                exchange=self.exchange,
                ws_url=ws_url,
                connection_id=conn_id,
                connection_type=ConnectionType.MASTER,
                data_callback=self.data_callback,
                symbols=symbol_group
            )
            
            logger.info(f"[{conn_id}] 槽位{i}启动，订阅 {len(symbol_group)} 个合约")
            
            try:
                success = await asyncio.wait_for(connection.connect(), timeout=30)
                if success:
                    self.master_slots.append({
                        "index": i,
                        "connection": connection,
                        "cooldown_until": 0  # 🚨 冷却期记录
                    })
                    logger.info(f"[{conn_id}] 槽位{i}启动成功")
                else:
                    logger.error(f"[{conn_id}] 槽位{i}启动失败")
            except Exception as e:
                logger.error(f"[{conn_id}] 槽位{i}异常: {e}")
    
    async def _initialize_warm_slots(self):
        """初始化温备槽位"""
        ws_url = self.config.get("ws_public_url")
        warm_count = self.config.get("warm_standbys_count", 3)
        
        for i in range(warm_count):
            heartbeat_symbols = self._get_heartbeat_symbols()
            conn_id = f"{self.exchange}_warm_{i}"
            
            connection = WebSocketConnection(
                exchange=self.exchange,
                ws_url=ws_url,
                connection_id=conn_id,
                connection_type=ConnectionType.WARM_STANDBY,
                data_callback=self.data_callback,
                symbols=heartbeat_symbols
            )
            
            logger.info(f"[{conn_id}] 温备槽位{i}启动")
            
            try:
                success = await asyncio.wait_for(connection.connect(), timeout=30)
                if success:
                    self.warm_standby_slots.append({
                        "index": i,
                        "connection": connection
                    })
                    logger.info(f"[{conn_id}] 温备槽位{i}启动成功")
                else:
                    logger.error(f"[{conn_id}] 温备槽位{i}启动失败")
            except Exception as e:
                logger.error(f"[{conn_id}] 温备槽位{i}异常: {e}")
    
    def _get_heartbeat_symbols(self):
        if self.exchange == "binance":
            return ["BTCUSDT"]
        elif self.exchange == "okx":
            return ["BTC-USDT-SWAP"]
        return []
    
    async def _initialize_monitor_scheduler(self):
        """初始化监控调度器"""
        ws_url = self.config.get("ws_public_url")
        
        if not self.config.get("monitor_enabled", True):
            logger.warning(f"[{self.exchange}] 监控调度器被配置禁用")
            return
        
        conn_id = f"{self.exchange}_monitor"
        
        self.monitor_connection = WebSocketConnection(
            exchange=self.exchange,
            ws_url=ws_url,
            connection_id=conn_id,
            connection_type=ConnectionType.MONITOR,
            data_callback=self.data_callback,
            symbols=[]
        )
        
        success = await asyncio.wait_for(self.monitor_connection.connect(), timeout=30)
        if success:
            logger.info(f"[{conn_id}] 监控连接建立成功")
            self.monitor_scheduler_task = asyncio.create_task(self._monitor_scheduling_loop())
    
    async def _monitor_scheduling_loop(self):
        """监控循环：检查槽位，不是连接"""
        logger.info(f"[{self.exchange}_monitor] 开始监控调度循环，每3秒检查一次")
        
        while True:
            try:
                # 检查每个主槽位
                for slot in self.master_slots:
                    i = slot["index"]
                    master_conn = slot["connection"]
                    
                    # 🚨 冷却期检查（30秒内不重复切换）
                    if time.time() < slot.get("cooldown_until", 0):
                        continue
                    
                    if not master_conn.connected:
                        logger.warning(f"[监控调度] [{self.exchange}] 主槽位{i} ({master_conn.connection_id}) 断开")
                        await self._handle_slot_failure(i, slot)
                
                # 温备自动重连（不触发转移）
                for slot in self.warm_standby_slots:
                    warm_conn = slot["connection"]
                    if not warm_conn.connected:
                        logger.info(f"[监控调度] [{self.exchange}] 温备槽位自动重连: {warm_conn.connection_id}")
                        await warm_conn.connect()
                
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"[监控调度] [{self.exchange}] 循环错误: {e}")
                await asyncio.sleep(3)
    
    async def _handle_slot_failure(self, slot_index: int, slot: dict):
        """处理槽位故障：换连接，不换槽位"""
        logger.info(f"[监控调度] [{self.exchange}] 处理主槽位{slot_index}故障")
        
        # 找最佳温备
        best_warm_slot = None
        for warm_slot in self.warm_standby_slots:
            if warm_slot["connection"].connected and not warm_slot["connection"].is_active:
                best_warm_slot = warm_slot
                break
        
        if not best_warm_slot:
            logger.warning(f"[监控调度] [{self.exchange}] 无可用温备，尝试重连原主连接")
            await slot["connection"].connect()
            return
        
        # 执行转移
        success = await self._execute_slot_failover(slot, best_warm_slot)
        
        if success:
            # 🚨 设置槽位冷却期（30秒）
            slot["cooldown_until"] = time.time() + 30
            logger.info(f"[监控调度] [{self.exchange}] 槽位{slot_index}冷却期设置至30秒后")
        else:
            # 转移失败，重连原主
            logger.warning(f"[监控调度] [{self.exchange}] 转移失败，重连原主连接")
            await slot["connection"].connect()
    
    async def _execute_slot_failover(self, master_slot: dict, warm_slot: dict):
        """执行槽位转移：温备→主，原主→温备"""
        master_conn = master_slot["connection"]
        warm_conn = warm_slot["connection"]
        
        logger.info(f"[监控调度] [{self.exchange}] 转移: {master_conn.connection_id} -> {warm_conn.connection_id}")
        
        try:
            # 1. 原主降级
            if master_conn.connected and master_conn.subscribed:
                await master_conn._unsubscribe()
            
            # 2. 温备升级为主（ID也要变）
            master_symbols = self.symbol_groups[master_slot["index"]]
            await warm_conn.switch_role(ConnectionType.MASTER, master_symbols)
            
            # 🚨 核心：温备ID改为槽位ID
            old_warm_id = warm_conn.connection_id
            warm_conn.connection_id = f"{self.exchange}_master_{master_slot['index']}"
            logger.info(f"[监控调度] [{self.exchange}] ID变更: {old_warm_id} -> {warm_conn.connection_id}")
            
            # 3. 原主降级为温备
            await master_conn.disconnect()
            await asyncio.sleep(1)
            
            if await master_conn.connect():
                heartbeat_symbols = self._get_heartbeat_symbols()
                await master_conn.switch_role(ConnectionType.WARM_STANDBY, heartbeat_symbols)
                # 原主ID改为温备ID
                master_conn.connection_id = f"{self.exchange}_warm_{warm_slot['index']}"
            
            logger.info(f"[监控调度] [{self.exchange}] 槽位转移完成")
            return True
            
        except Exception as e:
            logger.error(f"[监控调度] [{self.exchange}] 转移失败: {e}")
            return False

    async def get_status(self) -> Dict[str, Any]:
        """获取槽位状态"""
        status = {
            "exchange": self.exchange,
            "timestamp": datetime.now().isoformat(),
            "master_slots": [],
            "warm_standby_slots": [],
            "monitor": None
        }
        
        for slot in self.master_slots:
            status["master_slots"].append({
                "index": slot["index"],
                "connection_id": slot["connection"].connection_id,
                "health": await slot["connection"].check_health(),
                "cooldown_until": slot.get("cooldown_until", 0)
            })
        
        for slot in self.warm_standby_slots:
            status["warm_standby_slots"].append({
                "index": slot["index"],
                "connection_id": slot["connection"].connection_id,
                "health": await slot["connection"].check_health()
            })
        
        if self.monitor_connection:
            status["monitor"] = await self.monitor_connection.check_health()
        
        return status

    async def shutdown(self):
        """关闭所有槽位"""
        logger.info(f"[{self.exchange}] 正在关闭所有槽位...")
        
        if self.monitor_scheduler_task:
            self.monitor_scheduler_task.cancel()
        if self.health_check_task:
            self.health_check_task.cancel()
        
        all_connections = [slot["connection"] for slot in self.master_slots + self.warm_standby_slots]
        if self.monitor_connection:
            all_connections.append(self.monitor_connection)
        
        await asyncio.gather(*[conn.disconnect() for conn in all_connections], return_exceptions=True)
        
        logger.info(f"[{self.exchange}] 所有槽位已关闭")
