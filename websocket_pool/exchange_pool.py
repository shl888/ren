"""
单个交易所的连接池管理 - 修复版 + 详细日志
增加安全防护和重启条件
"""
import asyncio
import logging
import sys
import os
from typing import Dict, Any, List, Set
from datetime import datetime

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store
from .connection import WebSocketConnection, ConnectionType
from .config import EXCHANGE_CONFIGS

logger = logging.getLogger(__name__)

class ExchangeWebSocketPool:
    """单个交易所的WebSocket连接池 - 修复版 + 详细日志"""
    
    def __init__(self, exchange: str, data_callback, admin_instance=None):
        self.exchange = exchange
        self.data_callback = data_callback
        self.admin_instance = admin_instance  # 🚨 新增：直接引用管理员实例
        self.config = EXCHANGE_CONFIGS.get(exchange, {})
        
        # 连接池
        self.master_connections = []
        self.warm_standby_connections = []
        
        # 状态
        self.symbols = []
        self.symbol_groups = []
        
        # 任务
        self.internal_monitor_task = None
        
        # 🚨【修复1】自管理状态
        self.self_managed = True
        self.need_restart = False
        self.failed_connections_track = set()  # 记录失败过的连接ID
        self.takeover_attempts = 0  # 接管尝试次数
        self.takeover_success_count = 0  # 接管成功次数
        
        logger.info(f"[{self.exchange}] ExchangeWebSocketPool 初始化完成")

    async def initialize(self, symbols: List[str]):
        """初始化连接池"""
        self.symbols = symbols
        
        # 分组配置
        symbols_per_connection = self.config.get("symbols_per_connection", 300)
        if self.exchange == "okx" and symbols_per_connection > 600:
            symbols_per_connection = 600
        
        # ✅ [蚂蚁基因修复] 将列表推导式改为分批处理，避免阻塞
        self.symbol_groups = []
        for i in range(0, len(symbols), symbols_per_connection):
            await asyncio.sleep(0)  # 每个迭代让出CPU
            self.symbol_groups.append(symbols[i:i + symbols_per_connection])
        
        # 检查分组数
        active_connections = self.config.get("active_connections", 3)
        if len(self.symbol_groups) > active_connections:
            self._balance_symbol_groups(active_connections)
        
        logger.info(f"[{self.exchange}] 🌎【连接池】连接池初始化，{len(symbols)}个合约分为{len(self.symbol_groups)}组")
        
        # 并发初始化
        tasks = [self._initialize_masters(), self._initialize_warm_standbys()]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # 启动监控
        self.internal_monitor_task = asyncio.create_task(self._internal_monitoring_loop())
        
        logger.info(f"[{self.exchange}] ✅【连接池】连接池初始化完成！")

    def _balance_symbol_groups(self, target_groups: int):
        """平衡合约分组（同步方法，但被异步调用，循环量小）"""
        avg_size = len(self.symbols) // target_groups
        remainder = len(self.symbols) % target_groups
        
        self.symbol_groups = []
        start = 0
        
        for i in range(target_groups):
            # 这个循环最多 target_groups 次，通常很小，保持同步
            size = avg_size + (1 if i < remainder else 0)
            if start + size <= len(self.symbols):
                self.symbol_groups.append(self.symbols[start:start + size])
                start += size

    async def _initialize_masters(self):
        """初始化主连接"""
        ws_url = self.config.get("ws_public_url")
        
        for i, symbol_group in enumerate(self.symbol_groups):
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            conn_id = f"{self.exchange}_master_{i}"
            connection = WebSocketConnection(
                exchange=self.exchange,
                ws_url=ws_url,
                connection_id=conn_id,
                connection_type=ConnectionType.MASTER,
                data_callback=self.data_callback,
                symbols=symbol_group
            )
            
            connection.log_with_role("info", f"✅【连接池】主连接启动，订阅{len(symbol_group)}个合约")
            
            try:
                success = await connection.connect()
                if success:
                    self.master_connections.append(connection)
                else:
                    connection.log_with_role("error", "❌【连接池】主连接启动失败")
            except Exception as e:
                connection.log_with_role("error", f"❌【连接池】主连接异常: {e}")
        
        logger.info(f"[{self.exchange}] ✅【连接池】主连接: {len(self.master_connections)}个")

    async def _initialize_warm_standbys(self):
        """初始化温备连接"""
        ws_url = self.config.get("ws_public_url")
        warm_standbys_count = self.config.get("warm_standbys_count", 3)
        
        for i in range(warm_standbys_count):
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            conn_id = f"{self.exchange}_warm_{i}"
            connection = WebSocketConnection(
                exchange=self.exchange,
                ws_url=ws_url,
                connection_id=conn_id,
                connection_type=ConnectionType.WARM_STANDBY,
                data_callback=self.data_callback,
                symbols=self._get_heartbeat_symbols()
            )
            
            connection.log_with_role("info", "✅【连接池】温备连接启动")
            
            try:
                success = await connection.connect()
                if success:
                    self.warm_standby_connections.append(connection)
                else:
                    connection.log_with_role("error", "❌【连接池】温备连接启动失败")
            except Exception as e:
                connection.log_with_role("error", f"❌【连接池】温备连接异常: {e}")
        
        logger.info(f"[{self.exchange}] ✅【连接池】温备连接: {len(self.warm_standby_connections)}个")

    def _get_heartbeat_symbols(self):
        """获取心跳合约"""
        if self.exchange == "binance":
            return ["BTCUSDT"]
        elif self.exchange == "okx":
            return ["BTC-USDT-SWAP"]
        return []

    async def _internal_monitoring_loop(self):
        """内部监控循环 - 重构优化版（每3秒检查，每300秒报告）"""
        logger.info(f"[{self.exchange}] 🚀【连接池】启动内部监控（每3秒检查，每300秒报告）")
        
        master_failures = {}  # 主连接失败计数
        loop_count = 0
        
        while True:
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU
            loop_count += 1
            try:
                # ==== 第1部分：健康监控（每3秒执行） ====
                
                # 1. 检查所有主连接
                for i, master_conn in enumerate(self.master_connections):
                    await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                    # 🚨【修复2】更严格的健康检查
                    is_healthy = (
                        master_conn.connected and 
                        master_conn.subscribed and
                        master_conn.last_message_seconds_ago < 30 and
                        master_conn.last_message_seconds_ago > 0  # 必须收到过消息
                    )
                    
                    if not is_healthy:
                        conn_id = master_conn.connection_id
                        current_failures = master_failures.get(conn_id, 0) + 1
                        master_failures[conn_id] = current_failures
                        
                        # 异常立即记录
                        master_conn.log_with_role("warning", f"❌【连接池】内部监控第{current_failures}次健康检查失败")
                        
                        # 🚨【修复3】连续2次失败才触发
                        if current_failures >= 2:
                            master_conn.log_with_role("critical", "⚠️【连接池】[内部监控]主连接连续断开2次，触发接管!")
                            takeover_success = await self._execute_takeover(i)
                            
                            if takeover_success:
                                # 接管成功，重置该位置计数器
                                master_failures[conn_id] = 0
                                self.takeover_success_count += 1
                            else:
                                # 接管失败，记录到失败集合
                                self.failed_connections_track.add(conn_id)
                    else:
                        # 健康时重置
                        master_failures[master_conn.connection_id] = 0
                
                # 2. 检查温备连接
                for warm_conn in self.warm_standby_connections:
                    await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                    if not warm_conn.connected:
                        warm_conn.log_with_role("warning", "❌【连接池】[内部监控]温备连接断开，尝试重连")
                        await warm_conn.connect()
                    
                    # 🚨【新增修复】检查温备连接是否缺少心跳合约
                    elif (warm_conn.connected and 
                          warm_conn.connection_type == ConnectionType.WARM_STANDBY and
                          not warm_conn.symbols):
                        warm_conn.log_with_role("warning", "⚠️【连接池】[内部监控]温备连接缺少心跳合约，正在修复...")
                        warm_conn.symbols = self._get_heartbeat_symbols()
                        if warm_conn.delayed_subscribe_task:
                            warm_conn.delayed_subscribe_task.cancel()
                        delay = warm_conn._get_delay_for_warm_standby()
                        warm_conn.delayed_subscribe_task = asyncio.create_task(
                            warm_conn._delayed_subscribe(delay)
                        )
                        warm_conn.log_with_role("info", f"【连接池】[内部监控]将在{delay}秒后订阅心跳")
                
                # ==== 第2部分：日志和报告（频率控制） ====
                
                # 判断是否到达300秒间隔（100次循环 × 3秒）
                should_report_detailed = (loop_count % 100 == 0)
                
                # 3. 运行状态日志（每300秒）
                if should_report_detailed:
                    logger.info(f"[{self.exchange}] ✅【连接池】内部监控运行中，已检查{loop_count}次")
                
                # 4. 状态更新（每3秒更新data_store，每300秒打印详细日志）
                if should_report_detailed:
                    # 详细报告（含日志）
                    await self._report_status_to_data_store()
                else:
                    # 静默更新（只更新data_store，不打印详细日志）
                    await self._update_data_store_quietly()
                
                # 等待下次循环
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"【连接池】[内部监控] [{self.exchange}] 错误: {e}")
                await asyncio.sleep(5)

    async def _update_data_store_quietly(self):
        """静默更新data_store - 只更新数据，不打印日志"""
        try:
            status_report = {
                "exchange": self.exchange,
                "timestamp": datetime.now().isoformat(),
                "masters": [],
                "warm_standbys": [],
                "self_managed": True,
                "need_restart": self.need_restart,
                "failed_connections_count": len(self.failed_connections_track),
                "takeover_attempts": self.takeover_attempts,
                "takeover_success_count": self.takeover_success_count
            }
            
            for conn in self.master_connections:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                status_report["masters"].append(await conn.check_health())
            
            for conn in self.warm_standby_connections:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                status_report["warm_standbys"].append(await conn.check_health())
            
            await data_store.update_connection_status(
                self.exchange, 
                "websocket_pool", 
                status_report
            )
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 静默更新data_store失败: {e}")

    async def _execute_takeover(self, master_index: int):
        """执行接管 - 详细日志版"""
        logger.critical(f"[{self.exchange}] ⚠️【触发接管】准备接管主连接{master_index}")
        
        self.takeover_attempts += 1
        
        try:
            # 🚨【安全防护1】参数验证
            if not isinstance(master_index, int):
                logger.error(f"❌【触发接管】 无效的主连接索引类型: {type(master_index)}")
                return False
                
            if master_index < 0 or master_index >= len(self.master_connections):
                logger.error(f"❌【触发接管】 无效的主连接索引: {master_index}")
                return False
            
            # 🚨【安全防护2】检查温备池
            if not self.warm_standby_connections:
                logger.error(f"❌【触发接管】 温备池为空")
                await self._check_and_request_restart("温备池为空")
                return False
            
            # 🚨【修复4】选择可用的温备
            b_standby = None
            standby_index = -1
            
            for i, standby in enumerate(self.warm_standby_connections):
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                if standby.connected:
                    b_standby = standby
                    standby_index = i
                    break
            
            # 如果没找到连接的温备，尝试重连第一个
            if not b_standby and self.warm_standby_connections:
                first_standby = self.warm_standby_connections[0]
                first_standby.log_with_role("info", "⚠️【触发接管】温备断开，尝试重连")
                if await first_standby.connect():
                    b_standby = first_standby
                    standby_index = 0
            
            if not b_standby:
                logger.error(f"❌【触发接管】 温备池无可用连接")
                await self._check_and_request_restart("温备池无可用连接")
                return False
            
            # 取出选中的温备
            b_standby = self.warm_standby_connections.pop(standby_index)
            
            # 获取原主连
            a_master = self.master_connections[master_index]
            
            # 🚨【安全防护3】验证连接
            if a_master is None:
                logger.critical(f" ❌ 【触发接管】原主连接为空")
                self.warm_standby_connections.insert(0, b_standby)
                return False
            
            if b_standby is None:
                logger.critical(f"❌ 【触发接管】温备连接为空")
                return False
            
            # 记录原主连接的合约
            a_symbols = a_master.symbols.copy()
            
            # 🎯 详细日志：记录原主连接和温备连接的状态 - 统一为内部监控格式
            report_id = "【触发接管】【内部监控】接管前状态"
            master_status = "✅" if a_master.connected else "❌"
            standby_status = "✅" if b_standby.connected else "❌"
            
            logger.info(f"[{self.exchange}] {report_id} | 原主连接 | ID:{a_master.connection_id} | 状态:{master_status}{a_master.connection_type} | 订阅数量:{len(a_symbols)}个合约")
            logger.info(f"[{self.exchange}] {report_id} | 候选温备 | ID:{b_standby.connection_id} | 状态:{standby_status}{b_standby.connection_type} | 当前合约:{b_standby.symbols}")
            
            # 步骤1: 温备连接，接管，原主连接的合约
            logger.info(f"[{self.exchange}] 🔄 【触发接管】步骤1: {b_standby.connection_id}开始接管{a_master.connection_id}的{len(a_symbols)}个合约")
            takeover_success = await b_standby.switch_role(ConnectionType.MASTER, a_symbols)
            
            if not takeover_success:
                logger.error(f"【触发接管】 {b_standby.connection_id}温备接管失败")
                # 🚨【安全防护4】失败恢复
                self.warm_standby_connections.insert(0, b_standby)
                self.failed_connections_track.add(b_standby.connection_id)
                return False
            
            logger.info(f"[{self.exchange}] ✅ 【触发接管】{b_standby.connection_id}接管成功，成为新主连接")
            
            # 步骤2: 原主连接清空合约
            logger.info(f"[{self.exchange}] 🔄 【触发接管】步骤2: {a_master.connection_id}清空原有{len(a_master.symbols)}个合约")
            a_master.symbols = []
            a_master.subscribed = False
            
            # 步骤3: 原主连接切换为温备
            logger.info(f"[{self.exchange}] 🔄 【触发接管】步骤3: {a_master.connection_id}切换为温备角色")
            success = await a_master.switch_role(ConnectionType.WARM_STANDBY)
            
            if success:
                logger.info(f"[{self.exchange}] ✅ 【触发接管】{a_master.connection_id}已成功切换为温备")
            else:
                a_master.log_with_role("error", "切换为温备失败，降级处理")
                # 降级：至少设置type和心跳合约
                a_master.connection_type = ConnectionType.WARM_STANDBY
                a_master.is_active = False
                a_master.symbols = self._get_heartbeat_symbols()
                a_master.log_with_role("info", "已手动设置心跳合约")
            
            # 步骤4: 原主连接进入温备池
            logger.info(f"[{self.exchange}] 🔄 【触发接管】步骤4: {a_master.connection_id}进入温备池尾部")
            self.warm_standby_connections.append(a_master)
            a_master.log_with_role("info", f"已进入温备池，位置{len(self.warm_standby_connections)-1}")
            
            # 步骤5: 更新主连接列表
            logger.info(f"[{self.exchange}] 🔄 【触发接管】步骤5: {b_standby.connection_id}替换为主连接列表位置{master_index}")
            self.master_connections[master_index] = b_standby
            b_standby.log_with_role("info", "现在担任主连接")
            
            logger.info(f"[{self.exchange}] 🎉【触发接管】【接管完成】 {a_master.connection_id}(主→备) ↔ {b_standby.connection_id}(备→主)")
            
            # 🚨【安全防护5】记录接管
            await self._report_failover_to_data_store(master_index, a_master.connection_id, b_standby.connection_id)
            
            logger.critical(f"【触发接管】 [{self.exchange}] ✅ 接管完成！")
            
            # 重置接管尝试计数
            self.takeover_attempts = 0
            
            return True
            
        except Exception as e:
            logger.critical(f"【触发接管】 [{self.exchange}] ❌ 接管异常: {e}")
            import traceback
            logger.critical(traceback.format_exc())
            return False

    async def _check_and_request_restart(self, reason: str):
        """检查并请求重启 - 详细日志版"""
        logger.info(f"[{self.exchange}] 🔍 检查重启条件:")
        
        total_connections = len(self.master_connections) + len(self.warm_standby_connections)
        
        # 条件1：接管尝试次数过多
        logger.info(f"[{self.exchange}]   ⚠️【连接池】【内部监控】条件1-接管次数: {self.takeover_attempts}/{total_connections*2}")
        if self.takeover_attempts >= total_connections * 2:
            logger.critical(f"[{self.exchange}] 🆘 ⚠️【连接池】【内部监控】触发重启条件1: 接管尝试{self.takeover_attempts}次 ≥ 限制{total_connections*2}次")
            self.need_restart = True
        
        # 条件2：所有连接都失败过
        all_connection_ids = set()
        for conn in self.master_connections:
            all_connection_ids.add(conn.connection_id)
        for conn in self.warm_standby_connections:
            all_connection_ids.add(conn.connection_id)
        
        logger.info(f"[{self.exchange}]   ⚠️【连接池】【内部监控】条件2-失败记录: {len(self.failed_connections_track)}/{len(all_connection_ids)}")
        if self.failed_connections_track:
            logger.info(f"[{self.exchange}]   ⚠️【连接池】【内部监控】已失败连接: {list(self.failed_connections_track)}")
        
        if self.failed_connections_track.issuperset(all_connection_ids) and all_connection_ids:
            logger.critical(f"[{self.exchange}] 🆘 ⚠️【连接池】【内部监控】触发重启条件2: 所有{len(all_connection_ids)}个连接都失败过")
            self.need_restart = True
        
        # 如果需要重启，直接通知管理员
        if self.need_restart:
            logger.critical(f"[{self.exchange}] 🆘☎️【连接池】【内部监控】 发送重启请求给管理员，原因: {reason}")
            await self._notify_admin_restart_needed(f"接管监控触发: {reason}")

    async def _notify_admin_restart_needed(self, reason: str):
        """✅ 直接通知管理员需要重启 - 新增方法"""
        try:
            logger.critical(f"[{self.exchange}] 🆘☎️【连接池】【内部监控】 直接请求管理员重启！原因: {reason}")
            
            # ✅ 直接调用管理员的方法
            if self.admin_instance:
                await self.admin_instance.handle_restart_request(self.exchange, reason)
            else:
                logger.error(f"[{self.exchange}]🆘❌【连接池】【内部监控】 无法通知管理员：admin_instance未设置")
                
        except Exception as e:
            logger.error(f"[{self.exchange}] 🆘❌【连接池】【内部监控】发送重启请求失败: {e}")

    async def _report_status_to_data_store(self):
        """报告状态到共享存储 - 详细日志版"""
        try:
            # 🎯 详细状态报告日志 - 统一为单行格式，便于搜索
            report_id = "[内部监控]详细连接状态"
            
            # 主连接状态 - 单行格式
            for i, master in enumerate(self.master_connections):
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                status_icon = "✅" if master.connected else "❌"
                subscribed_icon = "📡" if master.subscribed else "📭"
                last_msg = f"{master.last_message_seconds_ago:.1f}秒前"
                
                logger.info(f"[{self.exchange}] {report_id} | 主连接{i} | ID:{master.connection_id} | 状态:{status_icon}{master.connection_type} | 订阅:{subscribed_icon}{len(master.symbols)}个合约 | 最后消息:{last_msg}")
            
            # 温备连接状态 - 单行格式
            for i, standby in enumerate(self.warm_standby_connections):
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                status_icon = "✅" if standby.connected else "❌"
                has_symbols = "📝" if standby.symbols else "📭"
                
                logger.info(f"[{self.exchange}] {report_id} | 温备连接{i} | ID:{standby.connection_id} | 状态:{status_icon}{standby.connection_type} | 合约:{has_symbols}{len(standby.symbols)}个")
            
            # 统计信息 - 单行格式
            restart_status = '🆘 是' if self.need_restart else '✅ 否'
            logger.info(f"[{self.exchange}] 📊 [内部监控]统计信息: 接管尝试:{self.takeover_attempts}次, 接管成功:{self.takeover_success_count}次, 连接失败:{len(self.failed_connections_track)}个, 需要重启:{restart_status}")
            
            # 🚨 更新data_store
            status_report = {
                "exchange": self.exchange,
                "timestamp": datetime.now().isoformat(),
                "masters": [],
                "warm_standbys": [],
                "self_managed": True,
                "need_restart": self.need_restart,
                "failed_connections_count": len(self.failed_connections_track),
                "takeover_attempts": self.takeover_attempts,
                "takeover_success_count": self.takeover_success_count
            }
            
            for conn in self.master_connections:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                status_report["masters"].append(await conn.check_health())
            
            for conn in self.warm_standby_connections:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                status_report["warm_standbys"].append(await conn.check_health())
            
            await data_store.update_connection_status(
                self.exchange, 
                "websocket_pool", 
                status_report
            )
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 详细状态报告失败: {e}")

    async def _report_failover_to_data_store(self, master_index: int, old_master_id: str, new_master_id: str):
        """报告故障转移"""
        try:
            failover_record = {
                "exchange": self.exchange,
                "master_index": master_index,
                "old_master": old_master_id,
                "new_master": new_master_id,
                "timestamp": datetime.now().isoformat(),
                "type": "failover",
                "self_managed": True
            }
            
            await data_store.update_connection_status(
                self.exchange,
                "failover_history",
                failover_record
            )
            
        except Exception as e:
            logger.error(f"[内部监控] 保存故障转移记录失败: {e}")

    async def get_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        try:
            status_report = {
                "exchange": self.exchange,
                "timestamp": datetime.now().isoformat(),
                "masters": [],
                "warm_standbys": [],
                "self_managed": True,
                "need_restart": self.need_restart,
                "failed_connections_count": len(self.failed_connections_track),
                "takeover_attempts": self.takeover_attempts,
                "takeover_success_count": self.takeover_success_count
            }
            
            for conn in self.master_connections:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                status_report["masters"].append(await conn.check_health())
            
            for conn in self.warm_standby_connections:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
                status_report["warm_standbys"].append(await conn.check_health())
            
            return status_report
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 获取状态失败: {e}")
            return {"error": str(e)}

    async def shutdown(self):
        """关闭连接池"""
        logger.info(f"[{self.exchange}] ⚠️【连接池】正在关闭连接池...")
        if self.internal_monitor_task:
            self.internal_monitor_task.cancel()
        
        tasks = []
        for conn in self.master_connections:
            tasks.append(conn.disconnect())
        for conn in self.warm_standby_connections:
            tasks.append(conn.disconnect())
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"[{self.exchange}] ❌【连接池】连接池已关闭")