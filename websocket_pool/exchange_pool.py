"""
单个交易所的连接池管理 - 监控调度版
修复：并发初始化 + 强制后置检查 + 完整日志恢复 + 退避重连 + 软健康检查
新增：接管逻辑7层安全防护
"""
import asyncio
import logging
import sys
import os
import time
from typing import Dict, Any, List
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
    
    def __init__(self, exchange: str, data_callback):
        self.exchange = exchange
        # 🚨 直接使用传入的回调（从pool_manager传入的default_data_callback）
        self.data_callback = data_callback
            
        self.config = EXCHANGE_CONFIGS.get(exchange, {})
        
        # 连接池
        self.master_connections = []
        self.warm_standby_connections = []
        self.monitor_connection = None
        
        # 状态
        self.symbols = []
        self.symbol_groups = []
        
        # 任务
        self.health_check_task = None
        self.monitor_scheduler_task = None
        
        logger.info(f"[{self.exchange}] ExchangeWebSocketPool 初始化完成")

    async def initialize(self, symbols: List[str]):
        """🚀 并发初始化 + 修复OKX单连接过载"""
        self.symbols = symbols
        
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
                data_callback=self.data_callback,  # 🚨 使用内部回调
                symbols=symbol_group
            )
            
            # 🚨【修复】使用连接的 log_with_role 方法
            connection.log_with_role("info", f"主连接启动，订阅 {len(symbol_group)} 个合约")
            
            try:
                success = await asyncio.wait_for(connection.connect(), timeout=30)
                if success:
                    self.master_connections.append(connection)
                    # 🚨【修复】使用连接的 log_with_role 方法
                    connection.log_with_role("info", "主连接启动成功")
                else:
                    # 🚨【修复】使用连接的 log_with_role 方法
                    connection.log_with_role("error", "主连接启动失败")
            except Exception as e:
                # 🚨【修复】使用连接的 log_with_role 方法
                connection.log_with_role("error", f"主连接异常: {e}")
        
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
                data_callback=self.data_callback,  # 🚨 使用内部回调
                symbols=heartbeat_symbols
            )
            
            # 🚨【修复】使用连接的 log_with_role 方法
            connection.log_with_role("info", "温备连接启动（将延迟订阅心跳）")
            
            try:
                success = await asyncio.wait_for(connection.connect(), timeout=30)
                if success:
                    self.warm_standby_connections.append(connection)
                    # 🚨【修复】使用连接的 log_with_role 方法
                    connection.log_with_role("info", "温备连接启动成功")
                else:
                    # 🚨【修复】使用连接的 log_with_role 方法
                    connection.log_with_role("error", "温备连接启动失败")
            except asyncio.TimeoutError:
                # 🚨【修复】使用连接的 log_with_role 方法
                connection.log_with_role("error", "温备连接超时30秒，强制跳过")
            except Exception as e:
                # 🚨【修复】使用连接的 log_with_role 方法
                connection.log_with_role("error", f"温备连接异常: {e}")
        
        # 🚨【修复】保留原来的汇总日志，但添加角色信息
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
                    data_callback=self.data_callback,  # 🚨 使用内部回调
                    symbols=[]
                )
                
                success = await asyncio.wait_for(self.monitor_connection.connect(), timeout=30)
                
                if success:
                    # 🚨【修复】使用连接的 log_with_role 方法
                    self.monitor_connection.log_with_role("info", "监控连接建立成功")
                    
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
        """监控调度循环 - 🚨【关键修复】简化接管触发逻辑"""
        logger.info(f"[{self.exchange}_monitor] 开始监控调度循环，每3秒检查一次")
        
        # 跟踪每个主连接的连续失败次数
        master_failures = {}
        
        while True:
            try:
                # 1. 监控主连接（简化健康检查）
                for i, master_conn in enumerate(self.master_connections):
                    # 🚨【简化健康检查】30秒内收到消息就算健康
                    is_healthy = (
                        master_conn.connected and 
                        master_conn.subscribed and
                        master_conn.last_message_seconds_ago < 30  # 30秒超时
                    )
                    
                    if not is_healthy:
                        # 记录失败次数
                        conn_id = master_conn.connection_id
                        current_failures = master_failures.get(conn_id, 0) + 1
                        master_failures[conn_id] = current_failures
                        
                        # 🚨 使用角色日志
                        master_conn.log_with_role("warning", f"第{current_failures}次健康检查失败")
                        
                        # 🚨【关键】连续2次失败才触发接管（防止误判）
                        if current_failures >= 2:
                            master_conn.log_with_role("critical", f"连续2次失败，触发接管!")
                            await self._simple_takeover(i)
                            # 接管后重置失败计数
                            master_failures[conn_id] = 0
                    else:
                        # 健康时重置失败计数
                        master_failures[master_conn.connection_id] = 0
                
                # 2. 监控温备连接（只检查连接状态）
                for i, warm_conn in enumerate(self.warm_standby_connections):
                    # 温备连接只检查是否连接，不检查消息时间（因为可能还在延迟订阅）
                    if not warm_conn.connected:
                        warm_conn.log_with_role("warning", "连接断开，尝试重连")
                        await warm_conn.connect()
                
                # 3. 定期报告状态
                await self._report_status_to_data_store()
                
                await asyncio.sleep(3)  # 3秒检查一次
                
            except Exception as e:
                logger.error(f"[监控调度] [{self.exchange}] 调度循环错误: {e}")
                await asyncio.sleep(5)

    async def _simple_takeover(self, master_index: int):
        """🚨【关键修复】简单接管：温备变主连接，主连接变温备 - 安全加固版"""
        logger.critical(f"[接管] [{self.exchange}] 开始接管主连接{master_index}")
        
        try:
            # 🚨【安全加固1】参数类型验证
            if not isinstance(master_index, int):
                logger.error(f"[接管] [{self.exchange}] 无效的主连接索引类型: {type(master_index)}")
                return False
                
            # 1. 检查温备池是否为空（双重检查）
            if not self.warm_standby_connections:
                logger.error(f"[接管] [{self.exchange}] 温备池为空，无法接管")
                return False
            
            # 🚨【安全加固2】检查主连接索引有效性
            if master_index < 0 or master_index >= len(self.master_connections):
                logger.error(f"[接管] [{self.exchange}] 无效的主连接索引: {master_index} (有效范围: 0-{len(self.master_connections)-1})")
                return False
            
            old_master = self.master_connections[master_index]
            
            # 🚨【安全加固3】验证原主连接
            if old_master is None:
                logger.critical(f"[接管] [{self.exchange}] ❌ 原主连接为空")
                return False
                
            # 显示原主连接的当前角色
            old_master.log_with_role("warning", "检测到故障，即将被接管")
            
            # 2. 从温备池取第一个温备（带异常捕获）
            try:
                # 🚨【关键修复】安全获取温备连接
                new_master = self.warm_standby_connections.pop(0)
            except IndexError as e:
                logger.critical(f"[接管] [{self.exchange}] ❌ 温备池弹出失败: {e}")
                logger.critical(f"[接管] [{self.exchange}] 当前温备池大小: {len(self.warm_standby_connections)}")
                return False
            
            # 🚨【安全加固4】验证获取的连接是否有效
            if new_master is None:
                logger.critical(f"[接管] [{self.exchange}] ❌ 获取到空的温备连接")
                return False
            
            # 🚨【安全加固5】记录当前池状态（用于故障恢复）
            pool_state_before = {
                "master_count": len(self.master_connections),
                "warm_count": len(self.warm_standby_connections),
                "old_master_id": old_master.connection_id,
                "new_master_id": new_master.connection_id
            }
            
            logger.info(f"[接管] [{self.exchange}] 接管前池状态: {pool_state_before}")
            
            # 3. 温备升级为主连接
            # 先取消温备的心跳订阅（如果有）
            if new_master.subscribed:
                new_master.log_with_role("info", "取消心跳订阅")
                await new_master._unsubscribe()
                await asyncio.sleep(1)  # 给交易所一点时间处理
            
            # 温备订阅主连接的合约
            master_symbols = self.symbol_groups[master_index] if master_index < len(self.symbol_groups) else old_master.symbols
            
            new_master.log_with_role("info", f"升级为主连接，订阅{len(master_symbols)}个合约")
            success = await new_master.switch_role(ConnectionType.MASTER, master_symbols)
            
            if not success:
                new_master.log_with_role("error", "升级失败，放回温备池")
                # 🚨【安全加固6】失败时恢复原状
                self.warm_standby_connections.insert(0, new_master)
                logger.warning(f"[接管] [{self.exchange}] 升级失败，已恢复温备池")
                return False
            
            # 4. 原主连接降级为温备
            old_master.log_with_role("info", "降级为温备")
            
            # 取消原主连接的订阅
            if old_master.connected and old_master.subscribed:
                old_master.log_with_role("info", "取消主连接订阅")
                await old_master._unsubscribe()
                await asyncio.sleep(1)
            
            # 原主连接重置为温备身份
            old_master.connection_type = ConnectionType.WARM_STANDBY
            old_master.symbols = self._get_heartbeat_symbols()
            
            # 5. 交换位置
            self.master_connections[master_index] = new_master
            self.warm_standby_connections.append(old_master)  # 放到尾部
            
            # 🚨 关键日志：显示池子状态
            logger.info(f"[接管] [{self.exchange}] 接管后温备池状态:")
            for i, conn in enumerate(self.warm_standby_connections):
                if conn is not None:
                    role_char = conn.role_display.get(conn.connection_type, "?")
                    position = "头" if i == 0 else "尾" if i == len(self.warm_standby_connections)-1 else "中"
                    logger.info(f"  位置{i}({position}): {conn.connection_id}({role_char})")
                else:
                    logger.warning(f"  位置{i}: ❌ 空连接!")
            
            # 6. 原主连接重新连接（作为温备）
            if not old_master.connected:
                old_master.log_with_role("info", "重新连接为温备")
                reconnect_success = await old_master.connect()
                if not reconnect_success:
                    old_master.log_with_role("warning", "温备重连失败，但仍在池中")
            
            # 🚨 最终状态汇总
            logger.critical(f"[接管] [{self.exchange}] ✅ 接管成功！")
            new_master.log_with_role("info", "现在担任主连接")
            old_master.log_with_role("info", f"现在担任温备，在池尾位置{len(self.warm_standby_connections)-1}")
            
            # 记录故障转移
            await self._report_failover_to_data_store(master_index, old_master.connection_id, new_master.connection_id)
            
            return True
            
        except Exception as e:
            logger.critical(f"[接管] [{self.exchange}] ❌ 接管过程未知异常: {e}")
            import traceback
            logger.critical(traceback.format_exc())
            
            # 🚨【安全加固7】发生异常时，尽可能恢复原状
            try:
                # 如果已经取了new_master但后续失败，尝试放回
                if 'new_master' in locals() and new_master is not None:
                    if new_master not in self.warm_standby_connections:
                        self.warm_standby_connections.insert(0, new_master)
                        logger.warning(f"[接管] [{self.exchange}] 异常恢复: 已将{new_master.connection_id}放回温备池")
            except:
                pass
                
            return False

    async def _restart_master_connection(self, master_index: int):
        """🚨【保留但不再使用】彻底重启主连接 - 只用于初始化"""
        logger.warning(f"[{self.exchange}] 重启主连接{master_index}（仅用于初始化）")
        
        old_conn = self.master_connections[master_index]
        
        # 清理旧连接
        try:
            await old_conn.disconnect()
        except:
            pass
        
        # 创建新连接（使用相同ID保持日志清晰）
        ws_url = self.config.get("ws_public_url")
        symbols = self.symbol_groups[master_index] if master_index < len(self.symbol_groups) else []
        
        new_conn = WebSocketConnection(
            exchange=self.exchange,
            ws_url=ws_url,
            connection_id=f"{self.exchange}_master_{master_index}",
            connection_type=ConnectionType.MASTER,
            data_callback=self.data_callback,
            symbols=symbols
        )
        
        # 尝试连接
        max_retries = 3
        for attempt in range(max_retries):
            try:
                success = await asyncio.wait_for(new_conn.connect(), timeout=60)
                if success and new_conn.connected and new_conn.subscribed:
                    self.master_connections[master_index] = new_conn
                    logger.info(f"[{self.exchange}] 主连接{master_index}重启成功")
                    return
                else:
                    logger.warning(f"[{self.exchange}] 主连接{master_index}重启失败，尝试{attempt+1}/{max_retries}")
            except Exception as e:
                logger.error(f"[{self.exchange}] 重启异常: {e}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(10 * (attempt + 1))
        
        logger.error(f"[{self.exchange}] 主连接{master_index}重启失败，已放弃")
    
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
        """报告状态到共享存储"""
        try:
            status_report = {
                "exchange": self.exchange,
                "timestamp": datetime.now().isoformat(),
                "masters": [],
                "warm_standbys": [],
                "monitor": None,
                "pool_mode": "shared_pool"
            }
            
            for conn in self.master_connections:
                status_report["masters"].append(await conn.check_health())
            
            for conn in self.warm_standby_connections:
                status_report["warm_standbys"].append(await conn.check_health())
            
            if self.monitor_connection:
                status_report["monitor"] = await self.monitor_connection.check_health()
            
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
        """健康检查循环 - 显示角色信息"""
        while True:
            try:
                # 🚨 显示主连接状态（带角色）
                for i, master in enumerate(self.master_connections):
                    role_char = master.role_display.get(master.connection_type, "?")
                    status = "✅" if master.connected else "❌"
                    logger.debug(f"[健康检查] 主连接{i}: {master.connection_id}({role_char}) {status}")
                
                # 🚨 显示温备连接状态（带角色）
                for i, warm in enumerate(self.warm_standby_connections):
                    role_char = warm.role_display.get(warm.connection_type, "?")
                    status = "✅" if warm.connected else "❌"
                    pos = "头" if i == 0 else "中" if i < len(self.warm_standby_connections)-1 else "尾"
                    logger.debug(f"[健康检查] 温备{i}({pos}): {warm.connection_id}({role_char}) {status}")
                
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