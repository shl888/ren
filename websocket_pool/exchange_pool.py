"""
单个交易所的连接池管理 - 监控调度版
完整7层安全防护 + 监控循环修复
"""
import asyncio
import logging
import sys
import os
import time
from datetime import datetime
from typing import Dict, Any, List

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
    """单个交易所的WebSocket连接池 - 完整7层防护"""
    
    def __init__(self, exchange: str, data_callback):
        self.exchange = exchange
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
        
        # 监控循环性能统计
        self.monitor_loop_count = 0
        self.monitor_slow_loops = 0
        self.monitor_last_perf_report = time.time()
        
        # 🚨【恢复】接管失败记录
        self.takeover_failures = []
        self.max_takeover_failures = 10
        
        logger.info(f"[{self.exchange}] ExchangeWebSocketPool 初始化完成")

    async def initialize(self, symbols: List[str]):
        """🚀 并发初始化"""
        self.symbols = symbols
        
        symbols_per_connection = self.config.get("symbols_per_connection", 300)
        
        if self.exchange == "okx" and symbols_per_connection > 600:
            old_limit = symbols_per_connection
            symbols_per_connection = 600
            logger.warning(f"[{self.exchange}] symbols_per_connection从{old_limit}调整为{symbols_per_connection}（OKX单连接1200频道限制）")
        
        self.symbol_groups = [
            symbols[i:i + symbols_per_connection]
            for i in range(0, len(symbols), symbols_per_connection)
        ]
        
        active_connections = self.config.get("active_connections", 3)
        if len(self.symbol_groups) > active_connections:
            logger.warning(f"[{self.exchange}] 分组数{len(self.symbol_groups)}超过active_connections={active_connections}，强制重新平衡")
            self._balance_symbol_groups(active_connections)
        
        logger.info(f"[{self.exchange}] 初始化连接池，共 {len(symbols)} 个合约，分为 {len(self.symbol_groups)} 组")
        
        init_tasks = [
            ("主连接", self._initialize_masters()),
            ("温备连接", self._initialize_warm_standbys()),
            ("监控调度器", self._initialize_monitor_scheduler()),
        ]
        
        for name, _ in init_tasks:
            logger.info(f"[{self.exchange}] 开始初始化 {name}...")
        
        results = await asyncio.gather(
            *[task[1] for task in init_tasks], 
            return_exceptions=True
        )
        
        for (name, _), result in zip(init_tasks, results):
            if isinstance(result, Exception):
                logger.error(f"[{self.exchange}] ❌ {name}初始化失败: {result}")
            else:
                logger.info(f"[{self.exchange}] ✅ {name}初始化完成")
        
        await self._enforce_monitor_scheduler()
        
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        logger.info(f"[{self.exchange}] 健康检查已启动")
        
        logger.info(f"[{self.exchange}] 连接池初始化全部完成！")

    async def _enforce_monitor_scheduler(self):
        """强制确保监控调度器运行"""
        if not self.monitor_connection or not self.monitor_connection.connected:
            logger.warning(f"[{self.exchange}] ⚠️【监控调度】 监控连接异常，尝试紧急恢复...")
            await self._initialize_monitor_scheduler()
        
        if not self.monitor_scheduler_task or self.monitor_scheduler_task.done():
            logger.warning(f"[{self.exchange}] ⚠️【监控调度】 调度循环未运行，强制启动...")
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
        """初始化主连接"""
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
            
            connection.log_with_role("info", f"主连接启动，订阅 {len(symbol_group)} 个合约")
            
            try:
                success = await asyncio.wait_for(connection.connect(), timeout=30)
                if success:
                    self.master_connections.append(connection)
                    connection.log_with_role("info", "主连接启动成功")
                else:
                    connection.log_with_role("error", "主连接启动失败")
            except Exception as e:
                connection.log_with_role("error", f"主连接异常: {e}")
        
        logger.info(f"[{self.exchange}] 主连接初始化完成: {len(self.master_connections)} 个")
    
    async def _initialize_warm_standbys(self):
        """初始化温备连接"""
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
            
            connection.log_with_role("info", "温备连接启动（将延迟订阅心跳）")
            
            try:
                success = await asyncio.wait_for(connection.connect(), timeout=30)
                if success:
                    self.warm_standby_connections.append(connection)
                    connection.log_with_role("info", "温备连接启动成功")
                else:
                    connection.log_with_role("error", "温备连接启动失败")
            except asyncio.TimeoutError:
                connection.log_with_role("error", "温备连接超时30秒，强制跳过")
            except Exception as e:
                connection.log_with_role("error", f"温备连接异常: {e}")
        
        logger.info(f"[{self.exchange}] 温备连接初始化完成: {len(self.warm_standby_connections)} 个")
    
    def _get_heartbeat_symbols(self):
        """获取温备心跳合约列表"""
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
        
        if not ws_url:
            logger.error(f"[{self.exchange}] WebSocket URL配置缺失")
            return
        
        conn_id = f"{self.exchange}_monitor"
        max_retries = 3
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"[{conn_id}] 【监控调度】正在建立监控连接（第{attempt}次）")
                
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
                    self.monitor_connection.log_with_role("info", "监控连接建立成功")
                    self.monitor_scheduler_task = asyncio.create_task(
                        self._monitor_scheduling_loop()
                    )
                    logger.info(f"[{conn_id}] 监控调度循环已启动")
                    return True
                    
            except asyncio.TimeoutError:
                logger.error(f"[{conn_id}] 【监控调度】监控连接超时（{attempt}/{max_retries}）")
            except Exception as e:
                logger.error(f"[{conn_id}] 【监控调度】监控连接异常（{attempt}/{max_retries}）: {e}")
            
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)
        
        logger.error(f"[{conn_id}] 监控调度器在{max_retries}次尝试后仍失败")
        return False
    
    async def _monitor_scheduling_loop(self):
        """监控调度循环 - 修复阻塞问题"""
        logger.info(f"[{self.exchange}_monitor] 开始监控调度循环，每3秒检查一次")
        
        last_report_time = time.time()
        report_interval = 30
        
        master_failures = {}
        
        loop_count = 0
        last_perf_report = time.time()
        
        while True:
            loop_count += 1
            self.monitor_loop_count += 1
            
            try:
                loop_start_time = time.time()
                
                try:
                    await asyncio.wait_for(
                        self._execute_monitor_cycle(master_failures),
                        timeout=2.5
                    )
                except asyncio.TimeoutError:
                    logger.warning(f"[{self.exchange}_monitor] ⚠️【监控调度】 监控周期超时2.5秒！")
                    self.monitor_slow_loops += 1
                
                current_time = time.time()
                if current_time - last_report_time >= report_interval:
                    logger.info(f"[{self.exchange}_monitor] 👀 【监控调度】监控运行中，未发现异常，持续检查连接状态...")
                    last_report_time = current_time
                
                # 🚨【新增】使用性能统计数据
                if current_time - last_perf_report >= 60:
                    elapsed = current_time - last_perf_report
                    loop_rate = loop_count / elapsed if elapsed > 0 else 0
                    logger.info(f"[{self.exchange}_monitor] 📈 【监控调度】性能: {loop_count}次循环, "
                               f"速率: {loop_rate:.1f}次/秒, "
                               f"慢循环: {self.monitor_slow_loops}次")
                    loop_count = 0
                    last_perf_report = current_time
                    self.monitor_slow_loops = 0
                
                loop_duration = time.time() - loop_start_time
                if loop_duration < 3:
                    await asyncio.sleep(3 - loop_duration)
                else:
                    logger.warning(f"[{self.exchange}_monitor] ⏱️ 【监控调度】循环耗时过长: {loop_duration:.2f}秒")
                    
            except Exception as e:
                logger.error(f"[监控调度] [{self.exchange}] 调度循环错误: {e}")
                await asyncio.sleep(5)
    
    async def _execute_monitor_cycle(self, master_failures):
        """执行一个监控周期 - 🚨【适配新的性能监控方法】"""
        for i, master_conn in enumerate(self.master_connections):
            # 🚨【新增】获取完整的健康状态（包含性能指标）
            health_status = await master_conn.check_health()
            
            is_healthy = (
                master_conn.connected and 
                master_conn.subscribed and
                master_conn.last_message_seconds_ago < 60  # 放宽到60秒
            )
            
            if not is_healthy:
                conn_id = master_conn.connection_id
                current_failures = master_failures.get(conn_id, 0) + 1
                master_failures[conn_id] = current_failures
                
                # 🚨【新增】显示性能指标
                message_rate = health_status.get("message_rate", 0.0)
                total_messages = health_status.get("total_messages", 0)
                uptime_seconds = health_status.get("uptime_seconds", 0.0)
                
                logger.warning(f"[{self.exchange}_monitor] ❌ 【监控调度】主连接{i}健康检查失败 "
                              f"(连接: {master_conn.connected}, "
                              f"订阅: {master_conn.subscribed}, "
                              f"最后消息: {master_conn.last_message_seconds_ago:.1f}秒前, "
                              f"速率: {message_rate:.1f}消息/秒, "
                              f"总消息: {total_messages}, "
                              f"运行: {uptime_seconds:.0f}秒) "
                              f"第{current_failures}次失败")
                
                if current_failures >= 3:
                    logger.critical(f"[{self.exchange}_monitor] 🔥 【监控调度】主连接{i}连续3次失败，触发接管!")
                    master_conn.log_with_role("critical", f"连续3次失败，触发接管!")
                    
                    # 🚨 异步执行接管，记录结果但不阻塞
                    takeover_task = asyncio.create_task(self._simple_takeover(i))
                    takeover_task.add_done_callback(
                        lambda task: logger.debug(f"[{self.exchange}_monitor] 【监控调度】接管任务完成: {task.result()}")
                    )
                    
                    master_failures[conn_id] = 0
            else:
                master_failures[master_conn.connection_id] = 0
                
                # 🚨【新增】即使健康也定期报告性能（每5次循环报告一次）
                if i == 0 and self.monitor_loop_count % 5 == 0:
                    message_rate = health_status.get("message_rate", 0.0)
                    total_messages = health_status.get("total_messages", 0)
                    uptime_seconds = health_status.get("uptime_seconds", 0.0)
                    
                    logger.debug(f"[{self.exchange}_monitor] 📊 【监控调度】主连接{i}性能: "
                                 f"速率{message_rate:.1f}消息/秒, "
                                 f"总消息{total_messages}, "
                                 f"运行{uptime_seconds:.0f}秒")
        
        for i, warm_conn in enumerate(self.warm_standby_connections):
            if not warm_conn.connected:
                warm_conn.log_with_role("warning", "连接断开，尝试重连")
                asyncio.create_task(warm_conn.connect())
        
        asyncio.create_task(self._report_status_to_data_store_async())
    
    async def _report_status_to_data_store_async(self):
        """异步报告状态"""
        try:
            await asyncio.wait_for(
                self._report_status_to_data_store(),
                timeout=2.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"[{self.exchange}_monitor] ⏱️ 【监控调度】状态报告超时2秒")
        except Exception as e:
            logger.error(f"[{self.exchange}_monitor] ❌ 【监控调度】状态报告失败: {e}")

    async def _simple_takeover(self, master_index: int):
        """🚨【完整7层防护】简单接管：温备变主连接，主连接变温备"""
        takeover_start = time.time()
        
        # 🚨【第0层】醒目开始标记
        logger.critical(f"【监控调度】 {'🔥' * 50}")
        logger.critical(f"🔥 [{self.exchange}] 【监控调度】检测到故障，开始接管主连接{master_index}!")
        logger.critical(f"🔥 【监控调度】时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.critical(f"【监控调度】 {'🔥' * 50}")
        
        try:
            # 🚨【第1层】参数类型验证
            if not isinstance(master_index, int):
                logger.error(f"【监控调度】[接管] [{self.exchange}] 无效的主连接索引类型: {type(master_index)}")
                return False
            
            # 🚨【第2层】检查温备池是否为空（双重检查）
            if not self.warm_standby_connections:
                logger.error(f"【监控调度】[接管] [{self.exchange}] 温备池为空，无法接管")
                # 记录失败
                self._record_takeover_failure("温备池为空")
                return False
            
            # 🚨【第3层】检查主连接索引有效性
            if master_index < 0 or master_index >= len(self.master_connections):
                logger.error(f"【监控调度】[接管] [{self.exchange}] 无效的主连接索引: {master_index} (有效范围: 0-{len(self.master_connections)-1})")
                self._record_takeover_failure(f"无效索引: {master_index}")
                return False
            
            old_master = self.master_connections[master_index]
            
            # 🚨【第4层】验证原主连接
            if old_master is None:
                logger.critical(f"【监控调度】[接管] [{self.exchange}] ❌ 原主连接为空")
                self._record_takeover_failure("原主连接为空")
                return False
            
            old_master.log_with_role("warning", "检测到故障，即将被接管")
            
            # 🚨【第5层】从温备池取第一个温备（带异常捕获）
            try:
                new_master = self.warm_standby_connections.pop(0)
            except IndexError as e:
                logger.critical(f"【监控调度】[接管] [{self.exchange}] ❌ 温备池弹出失败: {e}")
                logger.critical(f"【监控调度】[接管] [{self.exchange}] 当前温备池大小: {len(self.warm_standby_connections)}")
                self._record_takeover_failure(f"温备池弹出失败: {e}")
                return False
            
            # 🚨【第6层】验证获取的连接是否有效
            if new_master is None:
                logger.critical(f"【监控调度】[接管] [{self.exchange}] ❌ 获取到空的温备连接")
                self._record_takeover_failure("获取到空的温备连接")
                # 尝试放回原温备
                if old_master not in self.warm_standby_connections:
                    self.warm_standby_connections.append(old_master)
                return False
            
            # 🚨【安全记录】当前池状态（用于故障恢复）
            pool_state_before = {
                "master_count": len(self.master_connections),
                "warm_count": len(self.warm_standby_connections),
                "old_master_id": old_master.connection_id,
                "new_master_id": new_master.connection_id,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"【监控调度】[接管] [{self.exchange}] 接管前池状态: {pool_state_before}")
            
            # 3. 温备升级为主连接
            # 先取消温备的心跳订阅（如果有）
            if new_master.subscribed:
                new_master.log_with_role("info", "取消心跳订阅")
                try:
                    await asyncio.wait_for(new_master._unsubscribe(), timeout=5)
                    await asyncio.sleep(1)
                except asyncio.TimeoutError:
                    logger.warning(f"【监控调度】[接管] [{self.exchange}] 取消订阅超时，继续执行")
            
            # 温备订阅主连接的合约
            master_symbols = self.symbol_groups[master_index] if master_index < len(self.symbol_groups) else old_master.symbols
            
            new_master.log_with_role("info", f"升级为主连接，订阅{len(master_symbols)}个合约")
            try:
                success = await asyncio.wait_for(
                    new_master.switch_role(ConnectionType.MASTER, master_symbols),
                    timeout=30
                )
            except asyncio.TimeoutError:
                logger.error(f"【监控调度】[接管] [{self.exchange}] 切换角色超时30秒")
                success = False
            
            if not success:
                new_master.log_with_role("error", "升级失败，放回温备池")
                # 🚨【安全回退】失败时恢复原状
                self.warm_standby_connections.insert(0, new_master)
                logger.warning(f"【监控调度】[接管] [{self.exchange}] 升级失败，已恢复温备池")
                self._record_takeover_failure("升级失败")
                return False
            
            # 4. 原主连接降级为温备
            old_master.log_with_role("info", "降级为温备")
            
            # 取消原主连接的订阅
            if old_master.connected and old_master.subscribed:
                old_master.log_with_role("info", "取消主连接订阅")
                try:
                    await asyncio.wait_for(old_master._unsubscribe(), timeout=5)
                    await asyncio.sleep(1)
                except asyncio.TimeoutError:
                    logger.warning(f"【监控调度】[接管] [{self.exchange}] 原主连接取消订阅超时")
            
            old_master.connection_type = ConnectionType.WARM_STANDBY
            old_master.symbols = self._get_heartbeat_symbols()
            
            # 5. 交换位置
            self.master_connections[master_index] = new_master
            self.warm_standby_connections.append(old_master)  # 放到尾部
            
            # 🚨 关键日志：显示池子状态
            logger.info(f"【监控调度】[接管] [{self.exchange}] 接管后温备池状态:")
            for i, conn in enumerate(self.warm_standby_connections):
                if conn is not None:
                    role_char = conn.role_display.get(conn.connection_type, "?")
                    position = "头" if i == 0 else "尾" if i == len(self.warm_standby_connections)-1 else "中"
                    logger.info(f"  【监控调度】位置{i}({position}): {conn.connection_id}({role_char})")
                else:
                    logger.warning(f"  【监控调度】位置{i}: ❌ 空连接!")
            
            # 6. 原主连接重新连接（作为温备）
            if not old_master.connected:
                old_master.log_with_role("info", "重新连接为温备")
                try:
                    reconnect_success = await asyncio.wait_for(old_master.connect(), timeout=30)
                    if not reconnect_success:
                        old_master.log_with_role("warning", "温备重连失败，但仍在池中")
                except asyncio.TimeoutError:
                    old_master.log_with_role("error", "温备重连超时")
            
            # 🚨 最终状态汇总
            takeover_duration = time.time() - takeover_start
            logger.critical(f"【监控调度】[接管] [{self.exchange}] ✅ 接管成功！耗时: {takeover_duration:.2f}秒")
            new_master.log_with_role("info", "现在担任主连接")
            old_master.log_with_role("info", f"现在担任温备，在池尾位置{len(self.warm_standby_connections)-1}")
            
            # 记录故障转移
            await self._report_failover_to_data_store(master_index, old_master.connection_id, new_master.connection_id)
            
            # 🚨【成功清除失败记录】
            self.takeover_failures = []
            
            return True
            
        except Exception as e:
            logger.critical(f"【监控调度】[接管] [{self.exchange}] ❌ 接管过程未知异常: {e}")
            import traceback
            logger.critical(traceback.format_exc())
            
            # 🚨【第7层】发生异常时，尽可能恢复原状
            self._record_takeover_failure(f"异常: {str(e)}")
            
            try:
                # 如果已经取了new_master但后续失败，尝试放回
                if 'new_master' in locals() and new_master is not None:
                    if new_master not in self.warm_standby_connections:
                        self.warm_standby_connections.insert(0, new_master)
                        logger.warning(f"【监控调度】[接管] [{self.exchange}] 异常恢复: 已将{new_master.connection_id}放回温备池")
                
                # 确保原主连接还在主连接列表中
                if 'old_master' in locals() and old_master is not None:
                    if old_master not in self.master_connections:
                        # 尝试恢复原主连接
                        if master_index < len(self.master_connections):
                            if self.master_connections[master_index] is None:
                                self.master_connections[master_index] = old_master
                                logger.warning(f"【监控调度】[接管] [{self.exchange}] 异常恢复: 已恢复原主连接到位置{master_index}")
            except Exception as recovery_error:
                logger.error(f"【监控调度】[接管] [{self.exchange}] 异常恢复失败: {recovery_error}")
            
            # 🚨【检查失败次数过多】
            if len(self.takeover_failures) >= self.max_takeover_failures:
                logger.critical(f"【监控调度】[接管] [{self.exchange}] ⚠️⚠️⚠️ 接管失败次数过多({len(self.takeover_failures)}次)，建议人工干预!")
                
            return False
    
    def _record_takeover_failure(self, reason: str):
        """🚨【新增】记录接管失败"""
        failure_record = {
            "exchange": self.exchange,
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
            "failure_count": len(self.takeover_failures) + 1
        }
        
        self.takeover_failures.append(failure_record)
        
        # 保持最多记录数
        if len(self.takeover_failures) > self.max_takeover_failures:
            self.takeover_failures = self.takeover_failures[-self.max_takeover_failures:]
        
        logger.warning(f"【监控调度】[接管] [{self.exchange}] 接管失败记录: {reason} (总失败: {len(self.takeover_failures)}次)")

    async def _report_status_to_data_store(self):
        """报告状态到共享存储 - 包含接管失败记录和性能指标"""
        try:
            status_report = {
                "exchange": self.exchange,
                "timestamp": datetime.now().isoformat(),
                "masters": [],
                "warm_standbys": [],
                "monitor": None,
                "pool_mode": "shared_pool",
                "monitor_stats": {
                    "loop_count": self.monitor_loop_count,
                    "slow_loops": self.monitor_slow_loops,
                    "last_update": datetime.now().isoformat()
                },
                # 🚨【新增】接管失败统计
                "takeover_stats": {
                    "failure_count": len(self.takeover_failures),
                    "recent_failures": self.takeover_failures[-5:] if self.takeover_failures else []
                }
            }
            
            # 🚨【新增】收集性能统计汇总
            performance_summary = {
                "total_messages": 0,
                "avg_message_rate": 0.0,
                "min_message_rate": float('inf'),
                "max_message_rate": 0.0,
                "connected_count": 0
            }
            
            # 主连接状态
            for conn in self.master_connections:
                health_data = await conn.check_health()
                status_report["masters"].append(health_data)
                
                # 🚨【新增】汇总性能指标
                message_rate = health_data.get("message_rate", 0.0)
                total_messages = health_data.get("total_messages", 0)
                
                if conn.connected:
                    performance_summary["connected_count"] += 1
                    performance_summary["total_messages"] += total_messages
                    performance_summary["avg_message_rate"] += message_rate
                    performance_summary["min_message_rate"] = min(performance_summary["min_message_rate"], message_rate)
                    performance_summary["max_message_rate"] = max(performance_summary["max_message_rate"], message_rate)
            
            # 温备连接状态
            for conn in self.warm_standby_connections:
                health_data = await conn.check_health()
                status_report["warm_standbys"].append(health_data)
            
            # 计算平均消息速率
            if performance_summary["connected_count"] > 0:
                performance_summary["avg_message_rate"] = (
                    performance_summary["avg_message_rate"] / performance_summary["connected_count"]
                )
            else:
                performance_summary["min_message_rate"] = 0.0
            
            # 🚨【新增】添加性能汇总到状态报告
            status_report["performance_summary"] = performance_summary
            
            if self.monitor_connection:
                status_report["monitor"] = await self.monitor_connection.check_health()
            
            await data_store.update_connection_status(
                self.exchange, 
                "websocket_pool", 
                status_report
            )
            
            # 🚨【新增】在控制台也显示性能摘要（每隔一段时间）
            current_time = time.time()
            if current_time - getattr(self, '_last_perf_display', 0) > 30:
                logger.info(f"[{self.exchange}] 📊 【监控调度】性能摘要: "
                           f"连接{performance_summary['connected_count']}/{len(self.master_connections)}, "
                           f"消息速率{performance_summary['avg_message_rate']:.1f}/秒, "
                           f"范围[{performance_summary['min_message_rate']:.1f}-{performance_summary['max_message_rate']:.1f}], "
                           f"总消息{performance_summary['total_messages']}")
                self._last_perf_display = current_time
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 【监控调度】报告状态失败: {e}")
    
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
                "pool_mode": "shared_pool",
                "takeover_failure_count": len(self.takeover_failures)  # 🚨 包含失败统计
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
        """健康检查循环 - 🚨【适配性能监控】"""
        while True:
            try:
                for i, master in enumerate(self.master_connections):
                    health_status = await master.check_health()
                    role_char = master.role_display.get(master.connection_type, "?")
                    status = "✅" if master.connected else "❌"
                    message_rate = health_status.get("message_rate", 0.0)
                    total_messages = health_status.get("total_messages", 0)
                    
                    logger.debug(f"【监控调度】[健康检查] 主连接{i}: {master.connection_id}({role_char}) {status} "
                                f"速率{message_rate:.1f}消息/秒 总消息{total_messages}")
                
                for i, warm in enumerate(self.warm_standby_connections):
                    health_status = await warm.check_health()
                    role_char = warm.role_display.get(warm.connection_type, "?")
                    status = "✅" if warm.connected else "❌"
                    pos = "头" if i == 0 else "中" if i < len(self.warm_standby_connections)-1 else "尾"
                    message_rate = health_status.get("message_rate", 0.0)
                    total_messages = health_status.get("total_messages", 0)
                    
                    logger.debug(f"【监控调度】[健康检查] 温备{i}({pos}): {warm.connection_id}({role_char}) {status} "
                                f"速率{message_rate:.1f}消息/秒 总消息{total_messages}")
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"【监控调度】[健康检查] [{self.exchange}] 错误: {e}")
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