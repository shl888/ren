"""
私人WebSocket连接池管理器 - 重构版：增强自主管理能力
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Callable

# 导入我们刚刚创建的组件
from .connection import BinancePrivateConnection, OKXPrivateConnection
from .raw_data_cache import RawDataCache
from .data_formatter import PrivateDataFormatter

logger = logging.getLogger(__name__)

class PrivateWebSocketPool:
    """私人连接池 - 自主管理版"""
    
    def __init__(self, data_callback: Callable):
        """
        参数:
            data_callback: 数据回调函数 (连接池 → 大脑DataManager)
        """
        self.data_callback = data_callback
        
        # 组件初始化
        self.raw_data_cache = RawDataCache()
        self.data_formatter = PrivateDataFormatter()
        
        # 连接存储
        self.connections = {
            'binance': None,
            'okx': None
        }
        
        # 状态管理
        self.running = False
        self.brain_store = None  # 大脑存储接口
        self.reconnect_tasks = {}
        self.health_check_tasks = {}
        
        logger.info("🔗 [私人连接池] 初始化完成")
    
    async def start(self, brain_store):
        """启动连接池 - 自主启动"""
        logger.info("🚀 [私人连接池] 正在启动...")
        
        self.brain_store = brain_store
        self.running = True
        
        # 启动连接检查任务
        asyncio.create_task(self._connection_monitor_loop())
        
        # 立即尝试连接
        asyncio.create_task(self._try_connect_all())
        
        logger.info("✅ [私人连接池] 已启动，进入自主管理模式")
        return True
    
    async def _connection_monitor_loop(self):
        """连接监控循环"""
        while self.running:
            try:
                # 检查所有连接状态
                for exchange in ['binance', 'okx']:
                    connection = self.connections[exchange]
                    
                    if connection and not connection.connected:
                        logger.warning(f"🔁 [私人连接池] {exchange}连接断开，尝试重连...")
                        await self._reconnect_exchange(exchange)
                
                await asyncio.sleep(10)  # 每10秒检查一次
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [私人连接池] 监控循环异常: {e}")
                await asyncio.sleep(30)
    
    async def _try_connect_all(self):
        """尝试连接所有交易所"""
        tasks = []
        for exchange in ['binance', 'okx']:
            tasks.append(self._setup_exchange_connection(exchange))
        
        # 并发尝试连接
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if r is True)
        logger.info(f"🎯 [私人连接池] 连接尝试完成: {success_count}/{len(tasks)} 成功")
    
    async def _setup_exchange_connection(self, exchange: str) -> bool:
        """设置指定交易所的私人连接"""
        try:
            logger.info(f"🔗 [私人连接池] 正在设置 {exchange} 私人连接...")
            
            if exchange == 'binance':
                return await self._setup_binance_connection()
            elif exchange == 'okx':
                return await self._setup_okx_connection()
            else:
                logger.error(f"❌ [私人连接池] 不支持的交易所: {exchange}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [私人连接池] 设置{exchange}连接失败: {e}")
            return False
    
    async def _setup_binance_connection(self) -> bool:
        """设置币安私人连接"""
        try:
            if not self.brain_store:
                logger.error("❌ [私人连接池] 未设置大脑存储接口")
                return False
            
            # 1. 从大脑获取listenKey
            listen_key = await self.brain_store.get_listen_key('binance')
            if not listen_key:
                logger.warning("⚠️ [私人连接池] 币安listenKey不存在，等待中...")
                return False
            
            # 2. 获取API凭证（用于可能的重新获取）
            api_creds = await self.brain_store.get_api_credentials('binance')
            if not api_creds:
                logger.error("❌ [私人连接池] 币安API凭证不存在")
                return False
            
            # 3. 创建连接实例
            connection = BinancePrivateConnection(
                listen_key=listen_key,
                status_callback=self._handle_connection_status,
                data_callback=self._process_and_forward_data,
                raw_data_cache=self.raw_data_cache
            )
            
            # 4. 建立连接
            success = await connection.connect()
            if success:
                self.connections['binance'] = connection
                logger.info("✅ [私人连接池] 币安私人连接建立成功")
                
                # 启动健康检查
                self.health_check_tasks['binance'] = asyncio.create_task(
                    self._health_check_loop('binance')
                )
            else:
                logger.error("❌ [私人连接池] 币安私人连接建立失败")
                
                # 安排重连
                await self._schedule_reconnect('binance')
            
            return success
            
        except Exception as e:
            logger.error(f"❌ [私人连接池] 设置币安连接异常: {e}")
            
            # 安排重连
            await self._schedule_reconnect('binance')
            return False
    
    async def _setup_okx_connection(self) -> bool:
        """设置欧意私人连接"""
        try:
            if not self.brain_store:
                logger.error("❌ [私人连接池] 未设置大脑存储接口")
                return False
            
            # 1. 从大脑获取API凭证
            api_creds = await self.brain_store.get_api_credentials('okx')
            if not api_creds:
                logger.warning("⚠️ [私人连接池] 欧意API凭证不存在，等待中...")
                return False
            
            # 2. 创建连接实例
            connection = OKXPrivateConnection(
                api_key=api_creds['api_key'],
                api_secret=api_creds['api_secret'],
                passphrase=api_creds.get('passphrase', ''),
                status_callback=self._handle_connection_status,
                data_callback=self._process_and_forward_data,
                raw_data_cache=self.raw_data_cache
            )
            
            # 3. 建立连接
            success = await connection.connect()
            if success:
                self.connections['okx'] = connection
                logger.info("✅ [私人连接池] 欧意私人连接建立成功")
                
                # 启动健康检查
                self.health_check_tasks['okx'] = asyncio.create_task(
                    self._health_check_loop('okx')
                )
            else:
                logger.error("❌ [私人连接池] 欧意私人连接建立失败")
                
                # 安排重连
                await self._schedule_reconnect('okx')
            
            return success
            
        except Exception as e:
            logger.error(f"❌ [私人连接池] 设置欧意连接异常: {e}")
            
            # 安排重连
            await self._schedule_reconnect('okx')
            return False
    
    async def _health_check_loop(self, exchange: str):
        """健康检查循环"""
        while self.running and exchange in self.connections:
            try:
                connection = self.connections[exchange]
                if connection and connection.connected:
                    # 检查最后消息时间
                    if connection.last_message_time:
                        seconds_since_last = (datetime.now() - connection.last_message_time).total_seconds()
                        if seconds_since_last > 45:  # 45秒没收到消息认为有问题
                            logger.warning(f"⚠️ [私人连接池] {exchange} 45秒未收到消息，可能已断开")
                            connection.connected = False
                
                await asyncio.sleep(10)  # 每10秒检查一次
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [私人连接池] {exchange}健康检查异常: {e}")
                await asyncio.sleep(30)
    
    async def _schedule_reconnect(self, exchange: str, delay: int = 5):
        """安排重连"""
        if exchange in self.reconnect_tasks:
            # 已经有重连任务，取消旧的
            self.reconnect_tasks[exchange].cancel()
        
        async def reconnect_task():
            await asyncio.sleep(delay)
            if self.running:
                logger.info(f"🔁 [私人连接池] 执行{exchange}重连...")
                if exchange == 'binance':
                    await self._setup_binance_connection()
                elif exchange == 'okx':
                    await self._setup_okx_connection()
        
        self.reconnect_tasks[exchange] = asyncio.create_task(reconnect_task())
    
    async def _reconnect_exchange(self, exchange: str):
        """重连指定交易所"""
        logger.info(f"🔁 [私人连接池] 正在重连{exchange}...")
        
        # 断开现有连接
        if self.connections[exchange]:
            await self.connections[exchange].disconnect()
            self.connections[exchange] = None
        
        # 重新连接
        if exchange == 'binance':
            await self._setup_binance_connection()
        elif exchange == 'okx':
            await self._setup_okx_connection()
    
    async def _handle_connection_status(self, status_data: Dict[str, Any]):
        """处理连接状态事件"""
        try:
            exchange = status_data.get('exchange')
            event = status_data.get('event')
            
            logger.info(f"📡 [私人连接池] {exchange}状态事件: {event}")
            
            if event == 'connection_closed':
                # 连接断开，安排重连
                logger.warning(f"⚠️ [私人连接池] {exchange}连接断开")
                await self._schedule_reconnect(exchange)
                
            elif event == 'connection_established':
                logger.info(f"✅ [私人连接池] {exchange}私人连接已建立")
                
            elif event == 'listenkey_expired':
                logger.error(f"🚨 [私人连接池] {exchange} listenKey已过期")
                # listenKey过期，需要等待http模块更新
                # 这里可以断开连接，让重连逻辑处理
                if self.connections[exchange]:
                    await self.connections[exchange].disconnect()
                    self.connections[exchange] = None
                
        except Exception as e:
            logger.error(f"❌ [私人连接池] 处理状态事件失败: {e}")
    
    async def _process_and_forward_data(self, raw_formatted_data: Dict[str, Any]):
        """处理并转发数据（连接 → 格式化器 → 大脑）"""
        try:
            # 1. 进一步格式化数据
            formatted_data = await self.data_formatter.format(raw_formatted_data)
            
            # 2. 添加处理元数据
            formatted_data['processed_timestamp'] = datetime.now().isoformat()
            formatted_data['formatter_version'] = self.data_formatter.formatter_version
            
            # 3. 转发给大脑
            await self.data_callback(formatted_data)
            
            logger.debug(f"📨 [私人连接池] 已转发数据: {formatted_data['exchange']}.{formatted_data['data_type']}")
            
        except Exception as e:
            logger.error(f"❌ [私人连接池] 处理转发数据失败: {e}")
            # 即使格式化失败，也尝试转发原始数据
            try:
                raw_formatted_data['processing_error'] = str(e)
                await self.data_callback(raw_formatted_data)
            except:
                pass
    
    async def shutdown(self):
        """关闭所有连接和组件"""
        logger.info("🛑 [私人连接池] 正在关闭...")
        self.running = False
        
        # 取消所有任务
        for task in self.reconnect_tasks.values():
            task.cancel()
        
        for task in self.health_check_tasks.values():
            task.cancel()
        
        # 关闭所有连接
        shutdown_tasks = []
        for exchange, connection in self.connections.items():
            if connection:
                shutdown_tasks.append(connection.disconnect())
        
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        
        self.connections = {'binance': None, 'okx': None}
        logger.info("✅ [私人连接池] 已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'running': self.running,
            'connections': {},
            'components': {
                'raw_data_cache': 'active' if self.raw_data_cache else 'inactive',
                'data_formatter': self.data_formatter.get_status() if self.data_formatter else 'inactive'
            }
        }
        
        for exchange in ['binance', 'okx']:
            connection = self.connections[exchange]
            status['connections'][exchange] = {
                'connected': connection.connected if connection else False,
                'has_listen_key': self.brain_store is not None
            }
        
        return status