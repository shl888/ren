"""
单个交易所的连接池管理 - 监控调度版
修复：并发初始化 + 强制后置检查 + 退避重连 + 重启锁 + 状态同步
"""
import asyncio
import logging
import sys
import os
from typing import Dict, Any, List, Optional, Set
from datetime import datetime

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # brain_core目录
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store
from .connection import WebSocketConnection, ConnectionType
from .config import EXCHANGE_CONFIGS

logger = logging.getLogger(__name__)

class ExchangeWebSocketPool:
    """单个交易所的WebSocket连接池 - 监控调度版"""
    
    def __init__(self, exchange: str, data_callback=None):
        self.exchange = exchange
        # 使用传入的回调，如果没有则创建默认回调
        if data_callback:
            self.data_callback = data_callback
        else:
            self.data_callback = self._create_default_callback()
            
        self.config = EXCHANGE_CONFIGS.get(exchange, {})
        
        # 连接池
        self.master_connections = []
        self.warm_standby_connections = []
        self.monitor_connection = None
        
        # 状态
        self.symbols = []  # ✅初始化为空列表
        self.symbol_groups = []
        
        # 任务
        self.health_check_task = None
        self.monitor_scheduler_task = None
        
        # 🚨【关键】重启锁：防止重复重启
        self.restarting_connections: Set[str] = set()
        
        logger.info(f"[{self.exchange}] ExchangeWebSocketPool 初始化完成")

    def _create_default_callback(self):
        """创建默认回调函数，直接对接共享数据模块"""
        async def default_callback(data):
            try:
                if "exchange" not in data or "symbol" not in data:
                    logger.warning(f"[{self.exchange}] 数据缺少必要字段: {data}")
                    return
                    
                await data_store.update_market_data(
                    data["exchange"],
                    data["symbol"],
                    data
                )
                    
            except Exception as e:
                logger.error(f"[{self.exchange}] 数据存储失败: {e}")
        
        return default_callback
        
    async def initialize(self, symbols: List[str]):
        """🚀 并发初始化 + 修复OKX单连接过载"""
        self.symbols = symbols  # ✅存储原始合约列表
        
        # 🚨【关键修复】使用正确的配置名
        symbols_per_connection = self.config.get("symbols_per_connection", 300)
        
        # 🚨【关键修复】针对OKX确保不超过单连接上限
        if self.exchange == "okx" and symbols_per_connection > 600:
            # 每个合约2个频道，1200频道上限 → 600合约上限
            old_limit = symbols_per_connection
            symbols_per_connection = 600
            logger.warning(f"[{self.exchange}] symbols_per_connection从{old_limit}调整为{symbols_per_connection}（OKX单连接1200频道限制）")
        
        self.symbol_groups = [
            symbols[i:i + symbols_per_connection]
            for i in range(0, len(symbols), symbols_per_connection)
        ]
        
        # 🚨【关键修复】确保不超过active_connections限制
        active_connections = self.config.get("active_connections", 3)
        if len(self.symbol_groups) > active_connections:
            logger.warning(f"[{self.exchange}] 分组数{len(self.symbol_groups)}超过active_connections={active_connections}，强制重新平衡")
            self._balance_symbol_groups(active_connections)
        
        # 🚨 恢复原始关键日志（显示分组详情）
        logger.info(f"[{self.exchange}] 初始化连接池，共 {len(symbols)} 个合约，分为 {len(self.symbol_groups)} 组")
        
        # 🚀 并发执行所有初始化任务
        init_tasks = [
            ("主连接", self._initialize_masters()),
            ("温备连接", self._initialize_warm_standbys()),
            ("监控调度器", self._initialize_monitor_scheduler()),
        ]
        
        # 🚨 为每个任务添加开始日志
        for name, _ in init_tasks:
            logger.info(f"[{self.exchange}] 开始初始化 {name}...")
        
        results = await asyncio.gather(
            *[task[1] for task in init_tasks], 
            return_exceptions=True
        )
        
        # 🚨 为每个任务添加完成日志
        for (name, _), result in zip(init_tasks, results):
            if isinstance(result, Exception):
                logger.error(f"[{self.exchange}] ❌ {name}初始化失败: {result}")
            else:
                logger.info(f"[{self.exchange}] ✅ {name}初始化完成")
        
        # 🚨 强制后置检查：确保监控调度器必须运行
        await self._enforce_monitor_scheduler()
        
        # 启动健康检查
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        logger.info(f"[{self.exchange}] 健康检查已启动")
        
        logger.info(f"[{self.exchange}] 连接池初始化全部完成！")
    
    async def _enforce_monitor_scheduler(self):
        """强制确保监控调度器运行"""
        # 检查监控连接是否存在且正常
        if not self.monitor_connection or not self.monitor_connection.connected:
            logger.warning(f"[{self.exchange}] ⚠️ 监控连接异常，尝试紧急恢复...")
            await self._initialize_monitor_scheduler()
        
        # 检查调度循环是否运行
        if not self.monitor_scheduler_task or self.monitor_scheduler_task.done():
            logger.warning(f"[{self.exchange}] ⚠️ 调度循环未运行，强制启动...")
            self.monitor_scheduler_task = asyncio.create_task(
                self._monitor_scheduling_loop()
            )
            logger.info(f"[{self.exchange}_monitor] 🚀 监控调度循环已强制启动")

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
    
    async def _initialize_masters(self):
        """初始化主连接 - 恢复详细日志"""
        ws_url = self.config.get("ws_public_url")
        
        # 🚨 恢复原始日志：显示分组详情
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
            
            # 🚨 恢复原始日志：显示每个主连接的合约数
            logger.info(f"[{conn_id}] 主连接启动，订阅 {len(symbol_group)} 个合约")
            
            try:
                success = await asyncio.wait_for(connection.connect(), timeout=30)
                if success:
                    self.master_connections.append(connection)
                    logger.info(f"[{conn_id}] 主连接启动成功")
                else:
                    logger.error(f"[{conn_id}] 主连接启动失败")
            except Exception as e:
                logger.error(f"[{conn_id}] 主连接异常: {e}")
        
        logger.info(f"[{self.exchange}] 主连接初始化完成: {len(self.master_connections)} 个")
    
    async def _initialize_warm_standbys(self):
        """初始化温备连接 - 恢复详细日志"""
        ws_url = self.config.get("ws_public_url")
        warm_standbys_count = self.config.get("warm_standbys_count", 3)
        
        for i in range(warm_standbys_count):
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
            
            logger.info(f"[{conn_id}] 温备连接启动（将延迟订阅心跳）")
            
            try:
                success = await asyncio.wait_for(connection.connect(), timeout=30)
                if success:
                    self.warm_standby_connections.append(connection)
                    logger.info(f"[{conn_id}] 温备连接启动成功")
                else:
                    logger.error(f"[{conn_id}] 温备连接启动失败")
            except asyncio.TimeoutError:
                logger.error(f"[{conn_id}] 温备连接超时30秒，强制跳过")
            except Exception as e:
                logger.error(f"[{conn_id}] 温备连接异常: {e}")
        
        logger.info(f"[{self.exchange}] 温备连接初始化完成: {len(self.warm_standby_connections)} 个")
    
    def _get_heartbeat_symbols(self):
        """获取温备心跳合约列表"""
        if self.exchange == "binance":
            return ["BTCUSDT"]
        elif self.exchange == "okx":
            return ["BTC-USDT-SWAP"]
        return []
    
    async def _initialize_monitor_scheduler(self):
        """初始化监控调度器 - 恢复详细日志"""
        ws_url = self.config.get("ws_public_url")
        
        if not self.config.get("monitor_enabled", True):
            logger.warning(f"[{self.exchange}] 监控调度器被配置禁用")
            return
        
        if not ws_url:
            logger.error(f"[{self.exchange}] WebSocket URL配置缺失")
            return
        
        conn_id = f"{self.exchange}_monitor"
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"[{conn_id}] 正在建立监控连接（第{attempt}次）")
                
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
                    
                    self.monitor_scheduler_task = asyncio.create_task(
                        self._monitor_scheduling_loop()
                    )
                    logger.info(f"[{conn_id}] 监控调度循环已启动")
                    return True
                    
            except asyncio.TimeoutError:
                logger.error(f"[{conn_id}] 监控连接超时（{attempt}/{max_retries}）")
            except Exception as e:
                logger.error(f"[{conn_id}] 监控连接异常（{attempt}/{max_retries}）: {e}")
            
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)
        
        logger.error(f"[{conn_id}] 监控调度器在{max_retries}次尝试后仍失败")
        return False
    
    async def _monitor_scheduling_loop(self):
        """监控调度循环 - 放宽阈值+重启锁+首次等待"""
        logger.info(f"[{self.exchange}_monitor] 开始监控调度循环，每15秒检查一次")
        
        # 🚨【关键】首次运行时，等待30秒让连接稳定
        await asyncio.sleep(30)
        
        # 跟踪重连次数用于退避
        reconnect_attempts = {}
        for conn in self.master_connections + self.warm_standby_connections:
            reconnect_attempts[conn.connection_id] = 0
        
        while True:
            try:
                # 🚨【关键】每次检查前强制更新状态
                await self._report_status_to_data_store()
                
                # 1. 检查主连接 - 阈值改为40秒，更加保守
                for i, master_conn in enumerate(self.master_connections):
                    # 🚨跳过正在重启的连接
                    if master_conn.connection_id in self.restarting_connections:
                        continue
                    
                    # 强制更新状态
                    health = await master_conn.check_health()
                    last_msg_ago = health.get("last_message_seconds_ago", 999)
                    connected = health.get("connected", False)
                    
                    # 🚨调试日志
                    if i == 0 and self.exchange == "okx":
                        logger.debug(f"[监控调度] [{self.exchange}] 主连接0状态: connected={connected}, last_msg={last_msg_ago:.1f}s, symbols={health.get('symbols_count')}")
                    
                    # 🚨只有当40秒无消息 AND connected=False才认为真的断开
                    if last_msg_ago > 40 and not connected:
                        logger.warning(f"[监控调度] [{self.exchange}] 主连接{i} {master_conn.connection_id} 已断开（{last_msg_ago:.1f}秒无消息, connected={connected}）")
                        
                        attempts = reconnect_attempts[master_conn.connection_id]
                        wait_time = min(2 ** (attempts + 3), 60)  # 指数退避
                        
                        # 创建重启任务但不阻塞
                        asyncio.create_task(self._restart_master_connection_with_delay(
                            i, wait_time
                        ))
                        reconnect_attempts[master_conn.connection_id] += 1
                        
                    elif last_msg_ago > 30:
                        logger.info(f"[监控调度] [{self.exchange}] 主连接{i} {master_conn.connection_id} 正常（{last_msg_ago:.1f}秒无消息）")
                        reconnect_attempts[master_conn.connection_id] = 0
            
                # 2. 检查温备连接 - 阈值也改为40秒
                for i, warm_conn in enumerate(self.warm_standby_connections):
                    health = await warm_conn.check_health()
                    last_msg_ago = health.get("last_message_seconds_ago", 999)
                    
                    if not health.get("connected", False) or last_msg_ago > 40:
                        logger.info(f"[监控调度] [{self.exchange}] 温备连接{i}重连中...")
                        await warm_conn.connect()
            
                await asyncio.sleep(15)  # 检查间隔改为15秒
                
            except Exception as e:
                logger.error(f"[监控调度] [{self.exchange}] 调度循环错误: {e}")
                await asyncio.sleep(10)
    
    async def _restart_master_connection_with_delay(self, master_index: int, delay_seconds: int):
        """带延迟和锁的重启 - 防止重复重启"""
        conn_id = f"{self.exchange}_master_{master_index}"
        
        # 🚨【关键】加锁：标记为正在重启
        if conn_id in self.restarting_connections:
            logger.warning(f"[监控调度] [{self.exchange}] 主连接{master_index}正在重启中，跳过重复请求")
            return
        
        self.restarting_connections.add(conn_id)
        
        try:
            if delay_seconds > 0:
                logger.info(f"[监控调度] [{self.exchange}] 主连接{master_index}将在{delay_seconds}秒后重启")
                await asyncio.sleep(delay_seconds)
            
            await self._restart_master_connection(master_index)
            
        finally:
            # 🚨【关键】解锁：无论成功失败，都移除标记
            self.restarting_connections.discard(conn_id)
    
    async def _restart_master_connection(self, master_index: int):
        """彻底重启主连接 - 修复状态同步"""
        logger.error(f"[监控调度] [{self.exchange}] 正在重启主连接{master_index}")
        
        old_conn = self.master_connections[master_index]
        old_symbols = old_conn.symbols  # 🚨保存原有合约列表
        
        # 1. 清理旧连接
        try:
            await old_conn.disconnect()
        except:
            pass
        
        # 2. 创建新连接（使用相同ID保持日志清晰）
        ws_url = self.config.get("ws_public_url")
        
        # 🚨【关键】确保symbols不为空
        symbols = old_symbols if old_symbols and len(old_symbols) > 0 else \
                  (self.symbol_groups[master_index] if master_index < len(self.symbol_groups) else [])
        
        if not symbols:
            logger.error(f"[监控调度] [{self.exchange}] 主连接{master_index}合约列表为空，无法重启")
            return False
        
        new_conn = WebSocketConnection(
            exchange=self.exchange,
            ws_url=ws_url,
            connection_id=f"{self.exchange}_master_{master_index}",  # 保持相同ID
            connection_type=ConnectionType.MASTER,
            data_callback=self.data_callback,
            symbols=symbols
        )
        
        # 3. 尝试连接（带重试）
        max_retries = 3
        for attempt in range(max_retries):
            try:
                success = await asyncio.wait_for(new_conn.connect(), timeout=60)
                if success and new_conn.connected and new_conn.subscribed:
                    # 🚨【关键】确保重启后更新connections列表
                    self.master_connections[master_index] = new_conn
                    logger.info(f"[监控调度] [{self.exchange}] 主连接{master_index}重启成功，合约数：{len(symbols)}")
                    return True
                else:
                    logger.warning(f"[监控调度] [{self.exchange}] 主连接{master_index}重启失败，尝试{attempt+1}/{max_retries}")
                    logger.warning(f"[监控调度] [{self.exchange}] 状态: connected={new_conn.connected}, subscribed={new_conn.subscribed}")
            except Exception as e:
                logger.error(f"[监控调度] [{self.exchange}] 重启异常: {e}")
                import traceback
                logger.error(traceback.format_exc())
            
            if attempt < max_retries - 1:
                await asyncio.sleep(10 * (attempt + 1))
        
        logger.error(f"[监控调度] [{self.exchange}] 主连接{master_index}重启失败，已放弃")
        return False
    
    async def _select_best_standby_from_pool(self):
        """从共享池选择最佳温备"""
        available_standbys = [
            conn for conn in self.warm_standby_connections 
            if conn.connected and not conn.is_active
        ]
        
        if not available_standbys:
            logger.warning(f"[监控调度] [{self.exchange}] 温备池无可用连接")
            return None
        
        selected_standby = min(
            available_standbys,
            key=lambda conn: (
                conn.last_message_seconds_ago or 999,
                conn.reconnect_count,
                len(conn.symbols)
            )
        )
        
        logger.info(f"[监控调度] [{self.exchange}] 选择最佳温备: {selected_standby.connection_id} (当前角色: {selected_standby.connection_type})")
        return selected_standby
    
    async def _monitor_handle_master_failure(self, master_index: int, failed_master):
        """监控处理主连接故障"""
        logger.info(f"[监控调度] [{self.exchange}] 处理主连接{master_index}故障")
        
        standby_conn = await self._select_best_standby_from_pool()
        
        if not standby_conn:
            logger.warning(f"[监控调度] [{self.exchange}] 无可用温备，尝试重连原主连接")
            await failed_master.connect()
            return
        
        logger.info(f"[监控调度] [{self.exchange}] 决策：执行故障转移")
        success = await self._monitor_execute_failover(master_index, failed_master, standby_conn)
        
        if not success:
            logger.warning(f"[监控调度] [{self.exchange}] 故障转移失败，重连原主连接")
            await failed_master.connect()
    
    async def _monitor_execute_failover(self, master_index: int, old_master, new_master):
        """监控执行故障转移"""
        logger.info(f"[监控调度] [{self.exchange}] 故障转移: {old_master.connection_id} (类型: {old_master.connection_type}) -> {new_master.connection_id} (类型: {new_master.connection_type})")
        
        try:
            # 1. 原主连接降级
            logger.info(f"[监控调度] [{self.exchange}] 步骤1: 原主连接取消订阅")
            if old_master.connected and old_master.subscribed:
                await old_master._unsubscribe()
            
            old_master.symbols = []
            
            # 2. 温备升级为主
            logger.info(f"[监控调度] [{self.exchange}] 步骤2: 温备升级为主")
            master_symbols = self.symbol_groups[master_index] if master_index < len(self.symbol_groups) else []
            
            success = await new_master.switch_role(ConnectionType.MASTER, master_symbols)
            if not success:
                logger.error(f"[监控调度] [{self.exchange}] 温备切换角色失败")
                return False
            
            # 3. 更新连接池结构
            if new_master in self.warm_standby_connections:
                self.warm_standby_connections.remove(new_master)
            
            self.master_connections[master_index] = new_master
            
            # 4. 原主连接重连为温备
            logger.info(f"[监控调度] [{self.exchange}] 步骤3: 原主连接重连为温备")
            await old_master.disconnect()
            await asyncio.sleep(1)
            
            if await old_master.connect():
                heartbeat_symbols = self._get_heartbeat_symbols()
                await old_master.switch_role(ConnectionType.WARM_STANDBY, heartbeat_symbols)
                
                if old_master not in self.warm_standby_connections:
                    self.warm_standby_connections.append(old_master)
                
                logger.info(f"[监控调度] [{self.exchange}] 原主连接已降级为温备")
            
            # 🚨【关键修复】明确记录新状态
            logger.info(f"[监控调度] [{self.exchange}] 故障转移完成 - 新主连接: {new_master.connection_id} (类型: {new_master.connection_type})")
            logger.info(f"[监控调度] [{self.exchange}] 原主连接已降级: {old_master.connection_id} (类型: {old_master.connection_type})")
            
            await self._report_failover_to_data_store(master_index, old_master.connection_id, new_master.connection_id)
            
            return True
            
        except Exception as e:
            logger.error(f"[监控调度] [{self.exchange}] 故障转移执行失败: {e}")
            return False
    
    async def _report_status_to_data_store(self):
        """报告状态到共享存储 - 强制同步"""
        try:
            status_report = {
                "exchange": self.exchange,
                "timestamp": datetime.now().isoformat(),
                "masters": [],
                "warm_standbys": [],
                "monitor": None,
                "pool_mode": "shared_pool"
            }
            
            # 🚨【关键】先强制检查所有主连接状态
            for conn in self.master_connections:
                # 在检查前强制更新状态
                await conn.check_health()
                status_report["masters"].append(await conn.check_health())
            
            # 再检查温备
            for conn in self.warm_standby_connections:
                await conn.check_health()
                status_report["warm_standbys"].append(await conn.check_health())
            
            # 检查监控
            if self.monitor_connection:
                await self.monitor_connection.check_health()
                status_report["monitor"] = await self.monitor_connection.check_health()
            
            # 🚨统一key名称，确保与pool_manager一致
            status_report["total_symbols"] = len(self.symbols)
            status_report["total_data_types"] = len(self.symbols) * 2  # 每个合约2个频道
            
            await data_store.update_connection_status(
                self.exchange, 
                "websocket_pool", 
                status_report
            )
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 报告状态失败: {e}")
    
    async def _report_failover_to_data_store(self, master_index: int, old_master_id: str, new_master_id: str):
        """报告故障转移到共享存储"""
        try:
            failover_record = {
                "exchange": self.exchange,
                "master_index": master_index,
                "old_master": old_master_id,
                "new_master": new_master_id,
                "timestamp": datetime.now().isoformat(),
                "type": "failover",
                "pool_mode": "shared_pool"
            }
            
            await data_store.update_connection_status(
                self.exchange,
                "failover_history",
                failover_record
            )
            
            logger.info(f"[监控调度] [{self.exchange}] 故障转移记录已保存")
            
        except Exception as e:
            logger.error(f"[监控调度] [{self.exchange}] 保存故障转移记录失败: {e}")
    
    async def _health_check_loop(self):
        """健康检查循环"""
        while True:
            try:
                masters_connected = sum(1 for c in self.master_connections if c.connected)
                warm_connected = sum(1 for c in self.warm_standby_connections if c.connected)
                
                if masters_connected < len(self.master_connections):
                    logger.info(f"[健康检查] [{self.exchange}] {masters_connected}/{len(self.master_connections)} 个主连接活跃")
                
                if warm_connected < len(self.warm_standby_connections):
                    logger.info(f"[健康检查] [{self.exchange}] {warm_connected}/{len(self.warm_standby_connections)} 个温备连接活跃")
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"[健康检查] [{self.exchange}] 错误: {e}")
                await asyncio.sleep(30)
    
    async def get_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        return await self._report_status_to_data_store()
    
    async def shutdown(self):
        """关闭连接池"""
        logger.info(f"[{self.exchange}] 正在关闭连接池...")
        
        if self.health_check_task:
            self.health_check_task.cancel()
        if self.monitor_scheduler_task:
            self.monitor_scheduler_task.cancel()
        
        tasks = []
        for conn in self.master_connections:
            tasks.append(conn.disconnect())
        for conn in self.warm_standby_connections:
            tasks.append(conn.disconnect())
        if self.monitor_connection:
            tasks.append(self.monitor_connection.disconnect())
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"[{self.exchange}] 连接池已关闭")
