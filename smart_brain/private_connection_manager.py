"""
私人连接管理器 - 大脑的指挥官
负责：1. 接收大脑初始资源 2. 调度连接池 3. 监控连接状态 4. 处理连接事件
按照新设计：由大脑提供初始资源，管理器负责维护连接循环
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Callable

logger = logging.getLogger(__name__)

class PrivateConnectionManager:
    """私人连接系统的指挥官 - 重构版"""
    
    def __init__(self, brain):
        self.brain = brain
        self.private_pool = None  # 私人连接池实例
        self.running = False
        
        # 状态管理
        self.initialized = False  # 是否已接收大脑初始资源
        self.maintenance_tasks = {}  # 维护任务
        self.health_check_tasks = {}  # 健康检查任务
        
        # 连接状态跟踪
        self.connection_status = {
            'binance': {
                'status': 'disconnected',  # disconnected, connecting, connected, failed
                'last_connect_attempt': None,
                'last_success_time': None,
                'failure_count': 0,
                'is_maintaining': False
            },
            'okx': {
                'status': 'disconnected',
                'last_connect_attempt': None,
                'last_success_time': None,
                'failure_count': 0,
                'is_maintaining': False
            }
        }
        
        # 临时存储（仅用于当前连接尝试）
        self.current_attempt_resources = {
            'binance': {'token': None, 'apis': None},
            'okx': {'apis': None}
        }
        
        logger.info("🧠 [私人连接管理器] 初始化完成（等待大脑提供资源）")
    
    async def initialize_with_resources(self, binance_token: Optional[str], 
                                       binance_apis: Dict[str, str], 
                                       okx_apis: Dict[str, str]) -> bool:
        """
        由大脑调用，提供初始资源进行初始化
        这是管理器启动的唯一入口
        """
        logger.info("🧠 [私人连接管理器] 正在接收大脑初始资源...")
        
        try:
            # 验证资源
            if not binance_apis:
                logger.warning("⚠️ 大脑未提供币安API，币安连接将不可用")
            
            if not okx_apis:
                logger.warning("⚠️ 大脑未提供欧意API，欧意连接将不可用")
            
            # 1. 保存当前连接尝试的资源
            self.current_attempt_resources['binance']['token'] = binance_token
            self.current_attempt_resources['binance']['apis'] = binance_apis.copy() if binance_apis else None
            self.current_attempt_resources['okx']['apis'] = okx_apis.copy() if okx_apis else None
            
            # 2. 创建私人连接池实例
            try:
                # 动态导入，避免循环依赖
                from private_ws_pool import PrivateWebSocketPool
                self.private_pool = PrivateWebSocketPool(
                    status_callback=self._handle_pool_status,  # ✅ 状态回调
                    data_callback=self.brain.data_manager.receive_private_data
                )
                logger.info("✅ [私人连接管理器] 私人连接池实例已创建")
            except ImportError as e:
                logger.error(f"❌ [私人连接管理器] 无法导入私人连接池模块: {e}")
                return False
            except Exception as e:
                logger.error(f"❌ [私人连接管理器] 创建连接池失败: {e}")
                return False
            
            # 3. 标记为已初始化
            self.initialized = True
            self.running = True
            
            logger.info("✅ [私人连接管理器] 初始化完成，等待启动命令")
            return True
            
        except Exception as e:
            logger.error(f"❌ [私人连接管理器] 初始化失败: {e}")
            return False
    
    async def start_all_connections(self):
        """
        启动所有交易所的私人连接
        由大脑在适当时候调用
        """
        if not self.initialized:
            logger.error("❌ [私人连接管理器] 未初始化，无法启动连接")
            return False
        
        logger.info("🚀 [私人连接管理器] 正在启动所有私人连接...")
        
        # 启动维护循环
        await self._start_maintenance_loops()
        
        logger.info("🎯 [私人连接管理器] 连接维护已启动")
        return True
    
    async def _start_maintenance_loops(self):
        """启动所有交易所的连接维护循环"""
        try:
            # 启动币安维护循环
            if self.current_attempt_resources['binance']['apis']:
                self.maintenance_tasks['binance'] = asyncio.create_task(
                    self._maintain_binance_connection()
                )
                logger.info("🔄 币安连接维护循环已启动")
            else:
                logger.warning("⚠️ 没有币安API，跳过币安连接维护")
            
            # 启动欧意维护循环
            if self.current_attempt_resources['okx']['apis']:
                self.maintenance_tasks['okx'] = asyncio.create_task(
                    self._maintain_okx_connection()
                )
                logger.info("🔄 欧意连接维护循环已启动")
            else:
                logger.warning("⚠️ 没有欧意API，跳过欧意连接维护")
            
            # 启动健康检查
            self.health_check_tasks['monitor'] = asyncio.create_task(
                self._monitor_connections_health()
            )
            
        except Exception as e:
            logger.error(f"❌ 启动维护循环失败: {e}")
    
    async def _maintain_binance_connection(self):
        """
        币安连接维护循环
        无限循环，直到连接成功或管理器关闭
        """
        MAX_LOOP_ATTEMPTS = 10  # 最大循环次数（防止无限死循环）
        COOL_DOWN_SECONDS = 30  # 失败后冷却时间
        
        loop_count = 0
        
        while self.running and self.connection_status['binance']['is_maintaining']:
            try:
                loop_count += 1
                logger.info(f"🔁 币安连接维护循环 (第{loop_count}次)")
                
                # 1. 向大脑请求最新资源
                logger.info("📥 向大脑请求币安资源...")
                resources = await self.brain.data_manager.provide_resources_for_connection('binance')
                
                if not resources or not resources.get('token'):
                    logger.error("❌ 无法获取币安资源，等待后重试...")
                    await asyncio.sleep(COOL_DOWN_SECONDS)
                    continue
                
                # 2. 更新当前尝试的资源
                self.current_attempt_resources['binance']['token'] = resources['token']
                self.current_attempt_resources['binance']['apis'] = resources['apis']
                
                # 3. 下发连接指令给连接池（带内部重试）
                self.connection_status['binance']['status'] = 'connecting'
                self.connection_status['binance']['last_connect_attempt'] = datetime.now().isoformat()
                
                logger.info(f"📤 下发币安连接指令（令牌: {resources['token'][:15]}...）")
                
                success = await self.private_pool.establish_binance_connection(
                    listen_key=resources['token'],
                    credentials=resources['apis'],
                    max_retries=3,      # 连接池内部重试3次
                    retry_delay=2       # 每次重试间隔2秒
                )
                
                # 4. 处理连接结果
                if success:
                    self.connection_status['binance']['status'] = 'connected'
                    self.connection_status['binance']['last_success_time'] = datetime.now().isoformat()
                    self.connection_status['binance']['failure_count'] = 0
                    
                    logger.info("✅ 币安连接维护成功，等待可能的断开...")
                    
                    # 等待直到连接断开或管理器停止
                    while (self.running and 
                           self.connection_status['binance']['status'] == 'connected'):
                        await asyncio.sleep(5)
                    
                    logger.warning("⚠️ 币安连接已断开，重新开始维护循环...")
                    
                else:
                    # 连接失败
                    self.connection_status['binance']['status'] = 'failed'
                    self.connection_status['binance']['failure_count'] += 1
                    
                    logger.error(f"❌ 币安连接失败（失败次数: {self.connection_status['binance']['failure_count']}）")
                    
                    # 清理当前资源
                    self.current_attempt_resources['binance']['token'] = None
                    self.current_attempt_resources['binance']['apis'] = None
                    
                    # 冷却一下再试
                    if self.running:
                        logger.info(f"⏸️ 冷却{COOL_DOWN_SECONDS}秒后重试...")
                        await asyncio.sleep(COOL_DOWN_SECONDS)
                
                # 检查是否超过最大循环次数
                if loop_count >= MAX_LOOP_ATTEMPTS:
                    logger.warning(f"⚠️ 币安连接已达到最大循环次数({MAX_LOOP_ATTEMPTS})，暂停维护")
                    self.connection_status['binance']['is_maintaining'] = False
                    break
                
            except asyncio.CancelledError:
                logger.info("币安连接维护循环被取消")
                break
            except Exception as e:
                logger.error(f"❌ 币安连接维护循环异常: {e}")
                await asyncio.sleep(COOL_DOWN_SECONDS)
        
        logger.info("币安连接维护循环结束")
    
    async def _maintain_okx_connection(self):
        """
        欧意连接维护循环
        类似币安，但没有令牌概念
        """
        MAX_LOOP_ATTEMPTS = 10
        COOL_DOWN_SECONDS = 30
        
        loop_count = 0
        self.connection_status['okx']['is_maintaining'] = True
        
        while self.running and self.connection_status['okx']['is_maintaining']:
            try:
                loop_count += 1
                logger.info(f"🔁 欧意连接维护循环 (第{loop_count}次)")
                
                # 1. 向大脑请求最新资源
                logger.info("📥 向大脑请求欧意资源...")
                resources = await self.brain.data_manager.provide_resources_for_connection('okx')
                
                if not resources or not resources.get('apis'):
                    logger.error("❌ 无法获取欧意资源，等待后重试...")
                    await asyncio.sleep(COOL_DOWN_SECONDS)
                    continue
                
                # 2. 更新当前尝试的资源
                self.current_attempt_resources['okx']['apis'] = resources['apis']
                
                # 3. 下发连接指令给连接池
                self.connection_status['okx']['status'] = 'connecting'
                self.connection_status['okx']['last_connect_attempt'] = datetime.now().isoformat()
                
                apis = resources['apis']
                logger.info(f"📤 下发欧意连接指令（API Key: {apis.get('api_key', '')[:8]}...）")
                
                success = await self.private_pool.establish_okx_connection(
                    api_key=apis['api_key'],
                    api_secret=apis['api_secret'],
                    passphrase=apis.get('passphrase', ''),
                    max_retries=3,
                    retry_delay=2
                )
                
                # 4. 处理连接结果
                if success:
                    self.connection_status['okx']['status'] = 'connected'
                    self.connection_status['okx']['last_success_time'] = datetime.now().isoformat()
                    self.connection_status['okx']['failure_count'] = 0
                    
                    logger.info("✅ 欧意连接维护成功，等待可能的断开...")
                    
                    # 等待直到连接断开
                    while (self.running and 
                           self.connection_status['okx']['status'] == 'connected'):
                        await asyncio.sleep(5)
                    
                    logger.warning("⚠️ 欧意连接已断开，重新开始维护循环...")
                    
                else:
                    # 连接失败
                    self.connection_status['okx']['status'] = 'failed'
                    self.connection_status['okx']['failure_count'] += 1
                    
                    logger.error(f"❌ 欧意连接失败（失败次数: {self.connection_status['okx']['failure_count']}）")
                    
                    # 清理当前资源
                    self.current_attempt_resources['okx']['apis'] = None
                    
                    # 冷却一下再试
                    if self.running:
                        logger.info(f"⏸️ 冷却{COOL_DOWN_SECONDS}秒后重试...")
                        await asyncio.sleep(COOL_DOWN_SECONDS)
                
                # 检查是否超过最大循环次数
                if loop_count >= MAX_LOOP_ATTEMPTS:
                    logger.warning(f"⚠️ 欧意连接已达到最大循环次数({MAX_LOOP_ATTEMPTS})，暂停维护")
                    self.connection_status['okx']['is_maintaining'] = False
                    break
                
            except asyncio.CancelledError:
                logger.info("欧意连接维护循环被取消")
                break
            except Exception as e:
                logger.error(f"❌ 欧意连接维护循环异常: {e}")
                await asyncio.sleep(COOL_DOWN_SECONDS)
        
        logger.info("欧意连接维护循环结束")
    
    async def _monitor_connections_health(self):
        """监控连接健康状态"""
        logger.info("🏥 启动连接健康监控...")
        
        while self.running:
            try:
                await asyncio.sleep(30)  # 每30秒检查一次
                
                # 检查币安连接
                if (self.connection_status['binance']['status'] == 'connected' and
                    self.private_pool and hasattr(self.private_pool, 'get_connection_status')):
                    
                    pool_status = self.private_pool.get_connection_status()
                    binance_connected = pool_status.get('connections', {}).get('binance', {}).get('connected', False)
                    
                    if not binance_connected:
                        logger.warning("⚠️ 健康监控：币安连接状态异常，标记为断开")
                        self.connection_status['binance']['status'] = 'disconnected'
                
                # 检查欧意连接
                if (self.connection_status['okx']['status'] == 'connected' and
                    self.private_pool and hasattr(self.private_pool, 'get_connection_status')):
                    
                    pool_status = self.private_pool.get_connection_status()
                    okx_connected = pool_status.get('connections', {}).get('okx', {}).get('connected', False)
                    
                    if not okx_connected:
                        logger.warning("⚠️ 健康监控：欧意连接状态异常，标记为断开")
                        self.connection_status['okx']['status'] = 'disconnected'
                
                # 记录状态
                if logger.isEnabledFor(logging.DEBUG):
                    status_msg = (
                        f"连接状态 - 币安: {self.connection_status['binance']['status']}, "
                        f"欧意: {self.connection_status['okx']['status']}"
                    )
                    logger.debug(status_msg)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ 健康监控异常: {e}")
                await asyncio.sleep(10)
        
        logger.info("连接健康监控结束")
    
    async def _handle_pool_status(self, status_data: Dict[str, Any]):
        """
        处理连接池上报的状态事件
        这是连接池 → 大脑的核心回调
        """
        try:
            exchange = status_data.get('exchange')
            event = status_data.get('event')
            timestamp = status_data.get('timestamp', datetime.now().isoformat())
            
            logger.info(f"📡 [私人连接管理器] 收到{exchange}状态事件: {event}")
            
            # 记录API使用
            await self.brain.data_manager.record_api_usage(exchange, f"status_{event}")
            
            # 根据事件类型处理
            if event == 'connection_closed':
                # 连接断开
                logger.warning(f"⚠️ [私人连接管理器] {exchange}连接断开")
                self.connection_status[exchange]['status'] = 'disconnected'
                
                # 如果正在维护中，标记需要重新连接
                if self.connection_status[exchange]['is_maintaining']:
                    logger.info(f"🔄 {exchange}连接断开，维护循环会处理重连")
                
            elif event == 'connection_established':
                logger.info(f"✅ [私人连接管理器] {exchange}私人连接已建立")
                self.connection_status[exchange]['status'] = 'connected'
                self.connection_status[exchange]['last_success_time'] = datetime.now().isoformat()
                self.connection_status[exchange]['failure_count'] = 0
                
            elif event == 'connection_failed':
                logger.error(f"❌ [私人连接管理器] {exchange}连接失败")
                self.connection_status[exchange]['status'] = 'failed'
                self.connection_status[exchange]['failure_count'] += 1
                
            elif event == 'error':
                logger.error(f"🚨 [私人连接管理器] {exchange}报告错误: {status_data.get('message')}")
                
            elif event == 'listenkey_expired':
                logger.warning(f"🔄 [私人连接管理器] {exchange}令牌过期，需要刷新")
                # 令牌过期，连接池会断开，维护循环会处理
                
            # 可以添加更多事件处理逻辑...
            
        except Exception as e:
            logger.error(f"❌ [私人连接管理器] 处理状态事件失败: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """获取连接管理器状态"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'initialized': self.initialized,
            'running': self.running,
            'connections': self.connection_status,
            'current_resources': {
                'binance': {
                    'has_token': bool(self.current_attempt_resources['binance']['token']),
                    'has_apis': bool(self.current_attempt_resources['binance']['apis'])
                },
                'okx': {
                    'has_apis': bool(self.current_attempt_resources['okx']['apis'])
                }
            },
            'maintenance_tasks': {
                'binance': 'running' if self.connection_status['binance']['is_maintaining'] else 'stopped',
                'okx': 'running' if self.connection_status['okx']['is_maintaining'] else 'stopped'
            }
        }
        
        # 添加连接池状态（如果可用）
        if self.private_pool and hasattr(self.private_pool, 'get_status'):
            try:
                pool_status = self.private_pool.get_status()
                status['pool_status'] = pool_status
            except:
                status['pool_status'] = 'unavailable'
        
        return status
    
    async def shutdown(self):
        """关闭所有连接和任务"""
        logger.info("🛑 [私人连接管理器] 正在关闭...")
        self.running = False
        
        # 1. 停止维护循环
        for exchange, task in self.maintenance_tasks.items():
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                logger.info(f"✅ {exchange}维护循环已停止")
        
        # 2. 停止健康监控
        for task_name, task in self.health_check_tasks.items():
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # 3. 关闭连接池
        if self.private_pool:
            await self.private_pool.shutdown()
        
        # 4. 清理资源
        self.current_attempt_resources['binance'] = {'token': None, 'apis': None}
        self.current_attempt_resources['okx'] = {'apis': None}
        
        # 5. 更新状态
        self.connection_status['binance']['status'] = 'disconnected'
        self.connection_status['binance']['is_maintaining'] = False
        self.connection_status['okx']['status'] = 'disconnected'
        self.connection_status['okx']['is_maintaining'] = False
        
        logger.info("✅ [私人连接管理器] 已关闭")
    
    async def restart_connection(self, exchange: str):
        """手动重启指定交易所的连接"""
        if exchange not in ['binance', 'okx']:
            logger.error(f"❌ 不支持的交易所: {exchange}")
            return False
        
        logger.info(f"🔄 手动重启{exchange}连接...")
        
        # 停止当前维护任务（如果存在）
        if exchange in self.maintenance_tasks:
            task = self.maintenance_tasks[exchange]
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # 重置状态
        self.connection_status[exchange]['status'] = 'disconnected'
        self.connection_status[exchange]['is_maintaining'] = True
        
        # 重新启动维护循环
        if exchange == 'binance':
            self.maintenance_tasks['binance'] = asyncio.create_task(
                self._maintain_binance_connection()
            )
        elif exchange == 'okx':
            self.maintenance_tasks['okx'] = asyncio.create_task(
                self._maintain_okx_connection()
            )
        
        logger.info(f"✅ {exchange}连接重启已触发")
        return True