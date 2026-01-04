"""
单个交易所的连接池管理 - 监控调度版
🚨【日志增强版】仅增强日志，不改变业务逻辑
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
        
        logger.info(f"[{self.exchange}] 🚀 ExchangeWebSocketPool 初始化完成")

    async def initialize(self, symbols: List[str]):
        """🚨【日志增强】并发初始化 + 修复OKX单连接过载"""
        self.symbols = symbols
        
        # 🚨【关键修复】使用正确的配置名
        symbols_per_connection = self.config.get("symbols_per_connection", 300)
        
        # 🚨【关键修复】针对OKX确保不超过单连接上限
        if self.exchange == "okx" and symbols_per_connection > 600:
            old_limit = symbols_per_connection
            symbols_per_connection = 600
            logger.warning(f"[{self.exchange}] ⚠️【连接池】 symbols_per_connection从{old_limit}调整为{symbols_per_connection}（OKX单连接1200频道限制）")
        
        self.symbol_groups = [
            symbols[i:i + symbols_per_connection]
            for i in range(0, len(symbols), symbols_per_connection)
        ]
        
        # 🚨【关键修复】确保不超过active_connections限制
        active_connections = self.config.get("active_connections", 3)
        if len(self.symbol_groups) > active_connections:
            logger.warning(f"[{self.exchange}] ⚠️ 【连接池】分组数{len(self.symbol_groups)}超过active_connections={active_connections}，强制重新平衡")
            self._balance_symbol_groups(active_connections)
        
        # 🚨【增强】显示分组详情
        logger.info(f"[{self.exchange}] 📊 初始化连接池，共 {len(symbols)} 个合约，分为 {len(self.symbol_groups)} 组")
        for i, group in enumerate(self.symbol_groups):
            logger.info(f"[{self.exchange}]  分组{i}: {len(group)}个合约")
        
        # 🚀 并发执行所有初始化任务
        init_tasks = [
            ("主连接", self._initialize_masters()),
            ("温备连接", self._initialize_warm_standbys()),
            ("监控调度器", self._initialize_monitor_scheduler()),
        ]
        
        # 🚨 为每个任务添加开始日志
        for name, _ in init_tasks:
            logger.info(f"[{self.exchange}] 🔄 【连接池模块任务日志】开始初始化 {name}...")
        
        results = await asyncio.gather(
            *[task[1] for task in init_tasks], 
            return_exceptions=True
        )
        
        # 🚨 为每个任务添加完成日志
        for (name, _), result in zip(init_tasks, results):
            if isinstance(result, Exception):
                logger.error(f"[{self.exchange}] ❌【连接池模块任务日志】 {name}初始化失败: {result}")
            else:
                logger.info(f"[{self.exchange}] ✅【连接池模块任务日志】 {name}初始化完成")
        
        # 🚨 强制后置检查：确保监控调度器必须运行
        await self._enforce_monitor_scheduler()
        
        # 启动健康检查
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        logger.info(f"[{self.exchange}] 💓【监控调度】 健康检查已启动")
        
        logger.info(f"[{self.exchange}] 🎉 连接池初始化全部完成！")

    async def _enforce_monitor_scheduler(self):
        """🚨【日志增强】强制确保监控调度器运行"""
        # 检查监控连接是否存在且正常
        if not self.monitor_connection or not self.monitor_connection.connected:
            logger.warning(f"[{self.exchange}] ⚠️【监控调度】 监控连接异常，尝试紧急恢复...")
            await self._initialize_monitor_scheduler()
        
        # 检查调度循环是否运行
        if not self.monitor_scheduler_task or self.monitor_scheduler_task.done():
            logger.warning(f"[{self.exchange}] ⚠️【监控调度】 调度循环未运行，强制启动...")
            self.monitor_scheduler_task = asyncio.create_task(
                self._monitor_scheduling_loop()
            )
            logger.info(f"[{self.exchange}_monitor] 🚀 监控调度循环已强制启动")
        else:
            logger.info(f"[{self.exchange}_monitor] ✅ 监控调度循环运行中")

    def _balance_symbol_groups(self, target_groups: int):
        """平衡合约分组 - 🚨【日志增强】"""
        logger.info(f"[{self.exchange}] ⚖️【连接池】 开始平衡合约分组，目标: {target_groups}组")
        avg_size = len(self.symbols) // target_groups
        remainder = len(self.symbols) % target_groups
        
        self.symbol_groups = []
        start = 0
        
        for i in range(target_groups):
            size = avg_size + (1 if i < remainder else 0)
            if start + size <= len(self.symbols):
                self.symbol_groups.append(self.symbols[start:start + size])
                start += size
        
        logger.info(f"[{self.exchange}] ✅ 【连接池】合约重新平衡为 {len(self.symbol_groups)} 组")
        for i, group in enumerate(self.symbol_groups):
            logger.info(f"[{self.exchange}]   分组{i}: {len(group)}个合约")
    
    async def _initialize_masters(self):
        """🚨【日志增强】初始化主连接 - 恢复详细日志"""
        ws_url = self.config.get("ws_public_url")
        
        logger.info(f"[{self.exchange}] 🔄 【连接池】开始初始化主连接...")
        
        for i, symbol_group in enumerate(self.symbol_groups):
            conn_id = f"{self.exchange}_master_{i}"
            logger.info(f"[{self.exchange}] 🚀 【连接池】创建主连接{i}: {conn_id}，合约数: {len(symbol_group)}")
            
            connection = WebSocketConnection(
                exchange=self.exchange,
                ws_url=ws_url,
                connection_id=conn_id,
                connection_type=ConnectionType.MASTER,
                data_callback=self.data_callback,
                symbols=symbol_group
            )
            
            # 🚨【修复】使用连接的 log_with_role 方法
            connection.log_with_role("info", f"【连接池】主连接启动，订阅 {len(symbol_group)} 个合约")
            
            try:
                success = await asyncio.wait_for(connection.connect(), timeout=30)
                if success:
                    self.master_connections.append(connection)
                    connection.log_with_role("success", "主连接启动成功")
                    logger.info(f"[{self.exchange}] ✅ 【连接池】主连接{i}启动成功")
                else:
                    connection.log_with_role("error", "主连接启动失败")
                    logger.error(f"[{self.exchange}] ❌【连接池】 主连接{i}启动失败")
            except Exception as e:
                connection.log_with_role("error", f"主连接异常: {e}")
                logger.error(f"[{self.exchange}] ❌ 主【连接池】连接{i}异常: {e}")
        
        logger.info(f"[{self.exchange}] ✅ 【连接池】主连接初始化完成: {len(self.master_connections)} 个")
    
    async def _initialize_warm_standbys(self):
        """🚨【日志增强】初始化温备连接 - 恢复详细日志"""
        ws_url = self.config.get("ws_public_url")
        warm_standbys_count = self.config.get("warm_standbys_count", 3)
        
        logger.info(f"[{self.exchange}] 🔄 开始初始化温备连接，数量: {warm_standbys_count}")
        
        for i in range(warm_standbys_count):
            heartbeat_symbols = self._get_heartbeat_symbols()
            
            conn_id = f"{self.exchange}_warm_{i}"
            logger.info(f"[{self.exchange}] 🚀 创建温备连接{i}: {conn_id}")
            
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
                    connection.log_with_role("success", "温备连接启动成功")
                    logger.info(f"[{self.exchange}] ✅ 温备连接{i}启动成功")
                else:
                    connection.log_with_role("error", "温备连接启动失败")
                    logger.error(f"[{self.exchange}] ❌ 温备连接{i}启动失败")
            except asyncio.TimeoutError:
                connection.log_with_role("error", "温备连接超时30秒，强制跳过")
                logger.error(f"[{self.exchange}] ⏰ 温备连接{i}超时")
            except Exception as e:
                connection.log_with_role("error", f"温备连接异常: {e}")
                logger.error(f"[{self.exchange}] ❌ 温备连接{i}异常: {e}")
        
        logger.info(f"[{self.exchange}] ✅ 温备连接初始化完成: {len(self.warm_standby_connections)} 个")
    
    def _get_heartbeat_symbols(self):
        """获取温备心跳合约列表"""
        if self.exchange == "binance":
            return ["BTCUSDT"]
        elif self.exchange == "okx":
            return ["BTC-USDT-SWAP"]
        return []
    
    async def _initialize_monitor_scheduler(self):
        """🚨【日志增强】初始化监控调度器 - 恢复详细日志"""
        ws_url = self.config.get("ws_public_url")
        
        if not self.config.get("monitor_enabled", True):
            logger.warning(f"[{self.exchange}] ⚠️ 监控调度器被配置禁用")
            return False
        
        if not ws_url:
            logger.error(f"[{self.exchange}] ❌ 【监控调度】WebSocket URL配置缺失")
            return False
        
        conn_id = f"{self.exchange}_monitor"
        max_retries = 3
        
        logger.info(f"[{self.exchange}] 🔄 开始初始化监控调度器...")
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"[{self.exchange}] 🚀【监控调度】 正在建立监控连接（第{attempt}次尝试）")
                
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
                    # 🚨 强制打印监控连接成功
                    print(f"\n✅✅✅【强制打印】监控连接成功建立: {self.exchange}")
                    print(f"✅✅✅ 监控连接ID: {conn_id}")
                    print(f"✅✅✅ 连接状态: {self.monitor_connection.connected}\n")
                    
                    self.monitor_connection.log_with_role("success", "监控连接建立成功")
                    logger.info(f"[{self.exchange}] ✅【监控调度】 监控连接建立成功")
                    
                    self.monitor_scheduler_task = asyncio.create_task(
                        self._monitor_scheduling_loop()
                    )
                    
                    # 🚨 强制确认调度任务已启动
                    print(f"\n🚀🚀🚀【强制打印】监控调度循环已启动: {self.exchange}")
                    print(f"🚀🚀🚀 调度任务: {self.monitor_scheduler_task}")
                    print(f"🚀🚀🚀 任务状态: {'运行中' if not self.monitor_scheduler_task.done() else '已完成'}\n")
                    
                    logger.info(f"[{self.exchange}] ✅ 监控调度循环已启动")
                    return True
                else:
                    logger.warning(f"[{self.exchange}] ⚠️【监控调度】 监控连接建立失败（{attempt}/{max_retries}）")
                    
            except asyncio.TimeoutError:
                logger.error(f"[{self.exchange}] ⏰【监控调度】 监控连接超时（{attempt}/{max_retries}）")
            except Exception as e:
                logger.error(f"[{self.exchange}] ❌【监控调度】 监控连接异常（{attempt}/{max_retries}）: {e}")
            
            if attempt < max_retries:
                wait_time = 2 ** attempt
                logger.info(f"[{self.exchange}] ⏳【监控调度】 等待{wait_time}秒后重试...")
                await asyncio.sleep(wait_time)
        
        logger.error(f"[{self.exchange}] ❌ 监控调度器在{max_retries}次尝试后仍失败")
        return False
    
    async def _monitor_scheduling_loop(self):
        """🚨【日志增强】监控调度循环 - 详细状态显示"""
        logger.info(f"[{self.exchange}_monitor] 🚀 开始监控调度循环，每3秒检查一次")
        
        # 跟踪每个主连接的连续失败次数
        master_failures = {}
        
        while True:
            try:
                current_time = datetime.now().strftime("%H:%M:%S")
                logger.debug(f"[{self.exchange}_monitor] 🔍【监控调度】 监控检查开始 {current_time}")
                
                # 1. 详细监控主连接
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
                        
                        # 🚨 详细记录失败状态
                        health_status = {
                            "连接状态": "✅" if master_conn.connected else "❌",
                            "订阅状态": "✅" if master_conn.subscribed else "❌",
                            "最后消息": f"{master_conn.last_message_seconds_ago:.1f}秒前",
                            "角色": master_conn.role_display.get(master_conn.connection_type, "?"),
                            "连续失败": f"{current_failures}次"
                        }
                        
                        logger.warning(f"[{self.exchange}_monitor] ⚠️【监控调度】 主连接{i}({conn_id})异常: {health_status}")
                        
                        # 🚨【关键】连续2次失败才触发接管（防止误判）
                        if current_failures >= 2:
                            logger.critical(f"[{self.exchange}_monitor] 🚨【监控调度】 主连接{i}连续{current_failures}次失败，触发接管!")
                            await self._simple_takeover(i)
                            # 接管后重置失败计数
                            master_failures[conn_id] = 0
                    else:
                        # 健康时重置失败计数
                        master_failures[master_conn.connection_id] = 0
                
                # 2. 监控温备连接（只检查连接状态）
                for i, warm_conn in enumerate(self.warm_standby_connections):
                    # 温备连接只检查是否连接，不检查消息时间
                    if not warm_conn.connected:
                        logger.warning(f"[{self.exchange}_monitor] ⚠️【监控调度】 温备连接{i}({warm_conn.connection_id})断开，尝试重连")
                        warm_conn.log_with_role("warning", "监控检测到断开，尝试重连")
                        await warm_conn.connect()
                
                # 3. 定期报告状态
                await self._report_status_to_data_store()
                
                await asyncio.sleep(3)  # 3秒检查一次
                
            except Exception as e:
                logger.error(f"[{self.exchange}_monitor] ❌【监控调度】 调度循环错误: {e}")
                await asyncio.sleep(5)

    async def _simple_takeover(self, master_index: int):
        """🚨【日志增强】简单接管：温备变主连接，主连接变温备"""
        takeover_start = datetime.now()
        logger.critical(f"[{self.exchange}_monitor] 🚨🚨🚨【监控调度】 开始接管主连接{master_index}，时间: {takeover_start.strftime('%H:%M:%S')}")
        
        try:
            # 🚨 显示当前池状态
            logger.info(f"[{self.exchange}_monitor] 📊 【监控调度】当前状态: 主连接数={len(self.master_connections)}, 温备数={len(self.warm_standby_connections)}")
            
            # 1. 检查温备池是否为空
            if not self.warm_standby_connections:
                logger.critical(f"[{self.exchange}_monitor] ❌【监控调度】 温备池为空，无法接管")
                return False
            
            # 2. 检查主连接索引有效性
            if master_index < 0 or master_index >= len(self.master_connections):
                logger.critical(f"[{self.exchange}_monitor] ❌【监控调度】 无效的主连接索引: {master_index}")
                return False
            
            old_master = self.master_connections[master_index]
            
            # 🚨 详细记录原主连接状态
            old_master_status = {
                "ID": old_master.connection_id,
                "角色": old_master.role_display.get(old_master.connection_type, "?"),
                "连接状态": "✅" if old_master.connected else "❌",
                "订阅状态": "✅" if old_master.subscribed else "❌",
                "最后消息": f"{old_master.last_message_seconds_ago:.1f}秒前",
                "合约数": len(old_master.symbols)
            }
            logger.info(f"[{self.exchange}_monitor] 📋【监控调度】 原主连接详情: {old_master_status}")
            
            old_master.log_with_role("warning", "监控检测到故障，即将被接管")
            
            # 3. 从温备池取第一个温备
            logger.info(f"[{self.exchange}_monitor] 🔄【监控调度】 从温备池获取最佳温备...")
            try:
                new_master = self.warm_standby_connections.pop(0)
                logger.info(f"[{self.exchange}_monitor] ✅【监控调度】 获取到温备: {new_master.connection_id}")
            except IndexError as e:
                logger.critical(f"[{self.exchange}_monitor] ❌【监控调度】 温备池弹出失败: {e}")
                return False
            
            # 4. 温备升级为主连接
            logger.info(f"[{self.exchange}_monitor] 🔄【监控调度】 温备升级为主连接...")
            
            # 先取消温备的心跳订阅（如果有）
            if new_master.subscribed:
                new_master.log_with_role("info", "取消心跳订阅")
                await new_master._unsubscribe()
                await asyncio.sleep(1)
            
            # 温备订阅主连接的合约
            master_symbols = self.symbol_groups[master_index] if master_index < len(self.symbol_groups) else old_master.symbols
            
            new_master.log_with_role("info", f"升级为主连接，订阅{len(master_symbols)}个合约")
            success = await new_master.switch_role(ConnectionType.MASTER, master_symbols)
            
            if not success:
                new_master.log_with_role("error", "升级失败")
                logger.critical(f"[{self.exchange}_monitor] ❌【监控调度】 温备升级失败")
                # 失败时恢复原状
                self.warm_standby_connections.insert(0, new_master)
                logger.warning(f"[{self.exchange}_monitor] ⚠️【监控调度】 已恢复温备池")
                return False
            
            logger.info(f"[{self.exchange}_monitor] ✅【监控调度】 温备升级成功")
            
            # 5. 原主连接降级为温备
            logger.info(f"[{self.exchange}_monitor] 🔄【监控调度】 原主连接降级为温备...")
            old_master.log_with_role("info", "降级为温备")
            
            # 取消原主连接的订阅
            if old_master.connected and old_master.subscribed:
                old_master.log_with_role("info", "取消主连接订阅")
                await old_master._unsubscribe()
                await asyncio.sleep(1)
            
            # 原主连接重置为温备身份
            old_master.connection_type = ConnectionType.WARM_STANDBY
            old_master.symbols = self._get_heartbeat_symbols()
            
            # 6. 交换位置
            self.master_connections[master_index] = new_master
            self.warm_standby_connections.append(old_master)  # 放到尾部
            
            logger.info(f"[{self.exchange}_monitor] ✅【监控调度】 连接池更新完成")
            
            # 🚨 显示更新后的温备池状态
            logger.info(f"[{self.exchange}_monitor] 📊【监控调度】 接管后温备池状态 ({len(self.warm_standby_connections)}个):")
            for i, conn in enumerate(self.warm_standby_connections):
                if conn is not None:
                    role_char = conn.role_display.get(conn.connection_type, "?")
                    status = "✅" if conn.connected else "❌"
                    position = "头" if i == 0 else "尾" if i == len(self.warm_standby_connections)-1 else "中"
                    logger.info(f"  【监控调度】位置{i}({position}): {conn.connection_id}({role_char}) {status}")
            
            # 7. 原主连接重新连接（作为温备）
            if not old_master.connected:
                logger.info(f"[{self.exchange}_monitor] 🔄【监控调度】 原主连接重新连接为温备...")
                old_master.log_with_role("info", "重新连接为温备")
                reconnect_success = await old_master.connect()
                if not reconnect_success:
                    old_master.log_with_role("warning", "温备重连失败")
                    logger.warning(f"[{self.exchange}_monitor] ⚠️【监控调度】 原主连接重连失败")
            
            # 🚨 最终状态汇总
            takeover_end = datetime.now()
            duration = (takeover_end - takeover_start).total_seconds()
            
            logger.critical(f"[{self.exchange}_monitor] 🎉【监控调度】 接管成功！耗时: {duration:.2f}秒")
            new_master.log_with_role("success", "现在担任主连接")
            old_master.log_with_role("info", f"现在担任温备")
            
            # 记录故障转移
            await self._report_failover_to_data_store(master_index, old_master.connection_id, new_master.connection_id)
            
            return True
            
        except Exception as e:
            logger.critical(f"[{self.exchange}_monitor] ❌【监控调度】 接管过程异常: {e}")
            return False

    async def _restart_master_connection(self, master_index: int):
        """🚨【日志增强】彻底重启主连接 - 只用于初始化"""
        logger.warning(f"[{self.exchange}] 🔄 【监控调度】重启主连接{master_index}")
        
        old_conn = self.master_connections[master_index]
        
        # 清理旧连接
        try:
            logger.info(f"[{self.exchange}] 【监控调度】🛑 清理旧连接...")
            await old_conn.disconnect()
        except Exception as e:
            logger.warning(f"[{self.exchange}] ⚠️ 【监控调度】清理旧连接异常: {e}")
        
        # 创建新连接
        ws_url = self.config.get("ws_public_url")
        symbols = self.symbol_groups[master_index] if master_index < len(self.symbol_groups) else []
        
        logger.info(f"[{self.exchange}] 🚀【监控调度】 创建新连接，合约数: {len(symbols)}")
        
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
                logger.info(f"[{self.exchange}] 🔄【监控调度】 尝试连接（{attempt+1}/{max_retries}）...")
                success = await asyncio.wait_for(new_conn.connect(), timeout=60)
                if success and new_conn.connected and new_conn.subscribed:
                    self.master_connections[master_index] = new_conn
                    logger.info(f"[{self.exchange}] ✅【监控调度】 主连接{master_index}重启成功")
                    return
                else:
                    logger.warning(f"[{self.exchange}] ⚠️【监控调度】 主连接{master_index}重启失败")
            except Exception as e:
                logger.error(f"[{self.exchange}] ❌ 【监控调度】重启异常: {e}")
            
            if attempt < max_retries - 1:
                wait_time = 10 * (attempt + 1)
                logger.info(f"[{self.exchange}] ⏳【监控调度】 等待{wait_time}秒后重试...")
                await asyncio.sleep(wait_time)
        
        logger.error(f"[{self.exchange}] ❌ 【监控调度】主连接{master_index}重启失败，已放弃")
    
    async def _report_status_to_data_store(self):
        """🚨【日志增强】报告状态到共享存储"""
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
            
            logger.debug(f"[{self.exchange}] 📤 【监控调度】状态报告已发送到data_store")
            
        except Exception as e:
            logger.error(f"[{self.exchange}] ❌ 【监控调度】报告状态失败: {e}")
    
    async def _report_failover_to_data_store(self, master_index: int, old_master_id: str, new_master_id: str):
        """🚨【监控调度】【日志增强】报告故障转移到共享存储"""
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
            
            logger.info(f"[{self.exchange}_monitor] 📤【监控调度】 故障转移记录已保存到data_store")
            
        except Exception as e:
            logger.error(f"[{self.exchange}_monitor] ❌【监控调度】 保存故障转移记录失败: {e}")
    
    async def _health_check_loop(self):
        """🚨【监控调度】【日志增强】健康检查循环 - 显示详细角色信息"""
        logger.info(f"[{self.exchange}] 💓 【监控调度】健康检查循环启动")
        
        while True:
            try:
                current_time = datetime.now().strftime("%H:%M:%S")
                logger.debug(f"[{self.exchange}] 🔍 【监控调度】健康检查开始 {current_time}")
                
                # 🚨 显示主连接状态（带角色）
                healthy_masters = 0
                for i, master in enumerate(self.master_connections):
                    role_char = master.role_display.get(master.connection_type, "?")
                    status = "✅" if master.connected else "❌"
                    last_msg = f"{master.last_message_seconds_ago:.1f}秒前"
                    
                    if master.connected and master.subscribed:
                        healthy_masters += 1
                    
                    logger.debug(f"[{self.exchange}] 【监控调度】主连接{i}: {master.connection_id}({role_char}) {status} 最后消息: {last_msg}")
                
                # 🚨 显示温备连接状态（带角色）
                healthy_warm = 0
                for i, warm in enumerate(self.warm_standby_connections):
                    role_char = warm.role_display.get(warm.connection_type, "?")
                    status = "✅" if warm.connected else "❌"
                    pos = "头" if i == 0 else "中" if i < len(self.warm_standby_connections)-1 else "尾"
                    
                    if warm.connected:
                        healthy_warm += 1
                    
                    logger.debug(f"[{self.exchange}] 【监控调度】温备{i}({pos}): {warm.connection_id}({role_char}) {status}")
                
                # 显示监控连接状态
                if self.monitor_connection:
                    monitor_status = "✅" if self.monitor_connection.connected else "❌"
                    logger.debug(f"[{self.exchange}] 【监控调度】监控: {self.monitor_connection.connection_id} {monitor_status}")
                
                # 汇总状态
                logger.info(f"[{self.exchange}] 📊 【监控调度】健康汇总: 主连接{healthy_masters}/{len(self.master_connections)}，温备{healthy_warm}/{len(self.warm_standby_connections)}")
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"[{self.exchange}] ❌ 【监控调度】健康检查错误: {e}")
                await asyncio.sleep(30)
    
    async def get_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        return await self._report_status_to_data_store()
    
    async def shutdown(self):
        """🚨【日志增强】关闭连接池"""
        logger.info(f"[{self.exchange}] 🛑 正在关闭连接池...")
        
        # 停止任务
        if self.health_check_task:
            self.health_check_task.cancel()
            logger.debug(f"[{self.exchange}] 💓 【监控调度】健康检查任务已取消")
        
        if self.monitor_scheduler_task:
            self.monitor_scheduler_task.cancel()
            logger.debug(f"[{self.exchange}] 🔍 【监控调度】监控调度任务已取消")
        
        # 断开所有连接
        disconnect_tasks = []
        
        for conn in self.master_connections:
            disconnect_tasks.append(conn.disconnect())
            logger.debug(f"[{self.exchange}] 🛑 【监控调度】主连接 {conn.connection_id} 断开中...")
        
        for conn in self.warm_standby_connections:
            disconnect_tasks.append(conn.disconnect())
            logger.debug(f"[{self.exchange}] 🛑 【监控调度】温备连接 {conn.connection_id} 断开中...")
        
        if self.monitor_connection:
            disconnect_tasks.append(self.monitor_connection.disconnect())
            logger.debug(f"[{self.exchange}] 🛑 【监控调度】监控连接断开中...")
        
        if disconnect_tasks:
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)
        
        logger.info(f"[{self.exchange}] ✅ 连接池已关闭")
