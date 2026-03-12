"""
私人WebSocket连接池管理器 - 双连接热备版
币安：依赖主动探测（3秒探测，2次失败）
欧意：依赖数据间隔健康检查（5秒阈值）
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from collections import deque
import time

from .connection import BinancePrivateConnection, OKXPrivateConnection

logger = logging.getLogger(__name__)

class MessageDeduplicator:
    """
    消息去重器 - 基于消息ID/序列号的滑动窗口
    双连接都会收到相同数据，谁先到就转发谁，重复的丢弃
    """
    def __init__(self, window_size=1000):
        self.window_size = window_size
        self.recent_ids = deque(maxlen=window_size)
        self.id_set = set()
        logger.debug(f"【私人连接池】[去重器] 初始化，窗口大小: {window_size}")
    
    def extract_unique_id(self, data: Dict[str, Any]) -> str:
        """从消息中提取唯一ID"""
        exchange = data.get('exchange')
        msg_data = data.get('data', {})
        
        if exchange == 'binance':
            event_type = msg_data.get('e', 'unknown')
            event_time = msg_data.get('E', 0)
            symbol = msg_data.get('s', '')
            
            if event_type == 'ORDER_TRADE_UPDATE':
                order_id = msg_data.get('o', {}).get('i', '')
                return f"binance:order:{order_id}:{event_time}"
            elif event_type == 'ACCOUNT_UPDATE':
                update_time = msg_data.get('u', event_time)
                return f"binance:account:{update_time}"
            else:
                return f"binance:{event_type}:{symbol}:{event_time}"
        
        elif exchange == 'okx':
            arg = msg_data.get('arg', {})
            channel = arg.get('channel', 'unknown')
            data_list = msg_data.get('data', [])
            
            if data_list and len(data_list) > 0:
                first_data = data_list[0]
                if 'uTime' in first_data:
                    return f"okx:{channel}:{first_data.get('uTime')}"
                elif 'cTime' in first_data:
                    return f"okx:{channel}:{first_data.get('cTime')}"
                elif 'instId' in first_data:
                    return f"okx:{channel}:{first_data.get('instId')}:{first_data.get('uTime', time.time())}"
            
            return f"okx:{channel}:{int(time.time()*1000)}"
        
        return f"{exchange}:{datetime.now().timestamp()}"
    
    def is_duplicate(self, data: Dict[str, Any]) -> bool:
        """检查是否重复消息"""
        try:
            msg_id = self.extract_unique_id(data)
            
            if msg_id in self.id_set:
                return True
            
            self.recent_ids.append(msg_id)
            self.id_set.add(msg_id)
            
            if len(self.id_set) > self.window_size * 1.5:
                self.id_set = set(self.recent_ids)
            
            return False
            
        except Exception as e:
            logger.error(f"【私人连接池】[去重器] 检查重复失败: {e}")
            return False


class ExchangeConnectionPair:
    """交易所连接对 - 管理同一交易所的两个连接"""
    
    def __init__(self, exchange: str, data_callback: Callable):
        self.exchange = exchange
        self.data_callback = data_callback
        
        self.conn_primary = None
        self.conn_secondary = None
        
        self.deduplicator = MessageDeduplicator()
        
        self.primary_healthy = False
        self.secondary_healthy = False
        
        self.total_messages = 0
        self.duplicate_messages = 0
        self.primary_messages = 0
        self.secondary_messages = 0
        self.last_switch_time = None
        
        logger.info(f"【私人连接池】[{exchange}连接对] 初始化完成")
    
    def update_health_status(self, conn_name: str, is_healthy: bool):
        """更新连接健康状态"""
        if conn_name == 'primary':
            self.primary_healthy = is_healthy
        else:
            self.secondary_healthy = is_healthy
        
        if not is_healthy:
            logger.warning(f"【私人连接池】[{self.exchange}] {conn_name}连接不健康")
    
    async def process_message(self, data: Dict[str, Any], source: str):
        """处理来自某个连接的消息"""
        try:
            self.total_messages += 1
            if source == 'primary':
                self.primary_messages += 1
            else:
                self.secondary_messages += 1
            
            if self.deduplicator.is_duplicate(data):
                self.duplicate_messages += 1
                logger.debug(f"【私人连接池】[{self.exchange}] 丢弃重复消息 from {source}")
                return
            
            await self.data_callback(data)
            
        except Exception as e:
            logger.error(f"【私人连接池】[{self.exchange}] 处理消息失败: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """获取连接对状态"""
        return {
            'primary_healthy': self.primary_healthy,
            'secondary_healthy': self.secondary_healthy,
            'total_messages': self.total_messages,
            'duplicate_messages': self.duplicate_messages,
            'duplicate_rate': f"{(self.duplicate_messages/self.total_messages*100):.1f}%" if self.total_messages > 0 else "0%",
            'primary_messages': self.primary_messages,
            'secondary_messages': self.secondary_messages,
            'dedup_window_size': self.deduplicator.window_size
        }


class PrivateWebSocketPool:
    """私人连接池 - 双连接热备版"""
    
    def __init__(self):
        self.data_callback = None
        
        self.connection_pairs = {
            'binance': None,
            'okx': None
        }
        
        self.connections = {
            'binance': None,
            'okx': None
        }
        
        self.running = False
        self.brain_store = None
        self.start_time = None
        self.reconnect_tasks = {}
        
        self.quality_stats = {
            'binance': {
                'total_attempts': 0,
                'success_attempts': 0,
                'consecutive_failures': 0,
                'last_success': None,
                'success_rate': 100.0,
                'last_error': None,
                'mode': 'dual_active_probe'  # 主动探测模式
            },
            'okx': {
                'total_attempts': 0,
                'success_attempts': 0,
                'consecutive_failures': 0,
                'last_success': None,
                'success_rate': 100.0,
                'last_error': None,
                'mode': 'dual_active_dataflow'  # 数据流模式
            }
        }
        
        self.push_stats = {
            'total_created': 0,
            'total_success': 0,
            'total_failed': 0,
            'last_push_time': None
        }
        
        self.health_monitor_task = None
        
        logger.info("🔗 [私人连接池] 双连接热备版初始化完成")
        logger.info("   ├─ 币安: 主动探测模式 (3秒探测, 2次失败重连)")
        logger.info("   └─ 欧意: 数据流模式 (5秒无数据重连)")
    
    async def start(self, brain_store):
        """启动连接池"""
        logger.info("🚀 [私人连接池] 正在启动双连接热备版...")
        
        self.brain_store = brain_store
        self.running = True
        self.start_time = datetime.now()
        
        self.health_monitor_task = asyncio.create_task(self._health_monitor_loop())
        asyncio.create_task(self._staggered_connect_all())
        
        logger.info("✅ [私人连接池] 已启动")
        return True
    
    async def _staggered_connect_all(self):
        """分批建立所有交易所的双连接"""
        logger.info("🔗 [私人连接池] 第一阶段：建立币安双连接")
        binance_success = await self._setup_binance_dual_connections()
        
        await asyncio.sleep(3)
        
        logger.info("🔗 [私人连接池] 第二阶段：建立欧意双连接")
        okx_success = await self._setup_okx_dual_connections()
        
        success_count = sum([binance_success, okx_success])
        logger.info(f"🎯 [私人连接池] 双连接建立完成: {success_count}/2 成功")
        
        if not binance_success:
            logger.info("🔁 [私人连接池] 币安双连接建立失败，10秒后重试")
            await self._schedule_reconnect('binance', 10, is_dual=True)
        
        if not okx_success:
            logger.info("🔁 [私人连接池] 欧意双连接建立失败，10秒后重试")
            await self._schedule_reconnect('okx', 10, is_dual=True)
    
    async def _setup_binance_dual_connections(self) -> bool:
        """建立币安双连接"""
        try:
            if not self.brain_store:
                logger.error("❌ [私人连接池] 未设置大脑存储接口")
                return False
            
            listen_key = await self.brain_store.get_listen_key('binance')
            if not listen_key:
                logger.warning("⚠️ [私人连接池] 币安listenKey不存在，等待中...")
                return False
            
            api_creds = await self.brain_store.get_api_credentials('binance')
            if not api_creds:
                logger.error("❌ [私人连接池] 币安API凭证不存在")
                return False
            
            if not self.connection_pairs['binance']:
                self.connection_pairs['binance'] = ExchangeConnectionPair(
                    'binance', 
                    self._process_and_forward_data
                )
            
            # 建立主连接
            logger.info("【私人连接池】[币安] 正在建立主连接...")
            primary_conn = BinancePrivateConnection(
                listen_key=listen_key,
                status_callback=self._handle_connection_status,
                data_callback=self._create_conn_callback('binance', 'primary'),
                raw_data_cache=None
            )
            
            primary_success = await primary_conn.connect()
            if not primary_success:
                logger.error("【私人连接池】[币安] 主连接建立失败")
                return False
            
            # 建立备连接
            logger.info("【私人连接池】[币安] 正在建立备连接...")
            secondary_conn = BinancePrivateConnection(
                listen_key=listen_key,
                status_callback=self._handle_connection_status,
                data_callback=self._create_conn_callback('binance', 'secondary'),
                raw_data_cache=None
            )
            
            secondary_success = await secondary_conn.connect()
            if not secondary_success:
                logger.warning("【私人连接池】[币安] 备连接建立失败，但主连接已成功")
            
            self.connection_pairs['binance'].conn_primary = primary_conn
            self.connection_pairs['binance'].conn_secondary = secondary_conn if secondary_success else None
            
            self.connection_pairs['binance'].update_health_status('primary', True)
            self.connection_pairs['binance'].update_health_status('secondary', secondary_success)
            
            self.connections['binance'] = primary_conn
            
            logger.info(f"✅ 【私人连接池】[币安] 双连接建立完成: 主={'✓' if primary_success else '✗'}, 备={'✓' if secondary_success else '✗'}")
            
            self._update_quality_stats('binance', True)
            return True
            
        except Exception as e:
            logger.error(f"❌ [【私人连接池】币安] 建立双连接异常: {e}")
            self.quality_stats['binance']['last_error'] = str(e)
            return False
    
    async def _setup_okx_dual_connections(self) -> bool:
        """建立欧意双连接"""
        try:
            if not self.brain_store:
                logger.error("❌ [私人连接池] 未设置大脑存储接口")
                return False
            
            api_creds = await self.brain_store.get_api_credentials('okx')
            if not api_creds:
                logger.warning("⚠️ [私人连接池] 欧意API凭证不存在，等待中...")
                return False
            
            if not self.connection_pairs['okx']:
                self.connection_pairs['okx'] = ExchangeConnectionPair(
                    'okx', 
                    self._process_and_forward_data
                )
            
            # 建立主连接
            logger.info("【私人连接池】[欧意] 正在建立主连接...")
            primary_conn = OKXPrivateConnection(
                api_key=api_creds['api_key'],
                api_secret=api_creds['api_secret'],
                passphrase=api_creds.get('passphrase', ''),
                status_callback=self._handle_connection_status,
                data_callback=self._create_conn_callback('okx', 'primary'),
                raw_data_cache=None
            )
            
            primary_success = await primary_conn.connect()
            if not primary_success:
                logger.error("【私人连接池】[欧意] 主连接建立失败")
                return False
            
            # 建立备连接
            logger.info("【私人连接池】[欧意] 正在建立备连接...")
            secondary_conn = OKXPrivateConnection(
                api_key=api_creds['api_key'],
                api_secret=api_creds['api_secret'],
                passphrase=api_creds.get('passphrase', ''),
                status_callback=self._handle_connection_status,
                data_callback=self._create_conn_callback('okx', 'secondary'),
                raw_data_cache=None
            )
            
            secondary_success = await secondary_conn.connect()
            if not secondary_success:
                logger.warning("【私人连接池】[欧意] 备连接建立失败，但主连接已成功")
            
            self.connection_pairs['okx'].conn_primary = primary_conn
            self.connection_pairs['okx'].conn_secondary = secondary_conn if secondary_success else None
            
            self.connection_pairs['okx'].update_health_status('primary', True)
            self.connection_pairs['okx'].update_health_status('secondary', secondary_success)
            
            self.connections['okx'] = primary_conn
            
            logger.info(f"✅ [【私人连接池】欧意] 双连接建立完成: 主={'✓' if primary_success else '✗'}, 备={'✓' if secondary_success else '✗'}")
            
            self._update_quality_stats('okx', True)
            return True
            
        except Exception as e:
            logger.error(f"❌ [【私人连接池】欧意] 建立双连接异常: {e}")
            self.quality_stats['okx']['last_error'] = str(e)
            return False
    
    def _create_conn_callback(self, exchange: str, conn_name: str):
        """创建连接专用的回调函数"""
        async def callback(data: Dict[str, Any]):
            try:
                pair = self.connection_pairs.get(exchange)
                if pair:
                    await pair.process_message(data, conn_name)
            except Exception as e:
                logger.error(f"【私人连接池】[{exchange}:{conn_name}] 回调处理失败: {e}")
        return callback
    
    async def _health_monitor_loop(self):
        """健康监控循环 - 根据交易所类型采用不同判断逻辑"""
        while self.running:
            try:
                for exchange, pair in self.connection_pairs.items():
                    if not pair:
                        continue
                    
                    # 检查主连接
                    if pair.conn_primary:
                        is_healthy = await self._check_connection_health(pair.conn_primary)
                        old_status = pair.primary_healthy
                        pair.update_health_status('primary', is_healthy)
                        
                        if old_status and not is_healthy:
                            logger.warning(f"【私人连接池】[{exchange}] 主连接变不健康，立即重建")
                            asyncio.create_task(self._rebuild_single_connection(
                                exchange, 'primary'
                            ))
                    
                    # 检查备连接
                    if pair.conn_secondary:
                        is_healthy = await self._check_connection_health(pair.conn_secondary)
                        old_status = pair.secondary_healthy
                        pair.update_health_status('secondary', is_healthy)
                        
                        if old_status and not is_healthy:
                            logger.warning(f"【私人连接池】[{exchange}] 备连接变不健康，立即重建")
                            asyncio.create_task(self._rebuild_single_connection(
                                exchange, 'secondary'
                            ))
                    
                    # 如果备连接不存在，尝试创建
                    if not pair.conn_secondary and pair.conn_primary:
                        logger.info(f"【私人连接池】[{exchange}] 备连接不存在，尝试创建")
                        asyncio.create_task(self._rebuild_single_connection(
                            exchange, 'secondary'
                        ))
                
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [【私人连接池】健康监控] 异常: {e}")
                await asyncio.sleep(10)
    
    async def _check_connection_health(self, connection) -> bool:
        """
        检查单个连接是否健康
        币安：只检查探测状态和WebSocket状态（不检查数据间隔）
        欧意：检查数据间隔（5秒）和WebSocket状态
        """
        try:
            if not connection:
                return False
            
            if not connection.connected:
                return False
            
            # ===== 币安：只依赖探测机制 =====
            if hasattr(connection, 'probe_interval'):  # 币安有探测属性
                # 1. 检查探测失败次数（由探测循环自己维护）
                if hasattr(connection, 'consecutive_probe_failures'):
                    if connection.consecutive_probe_failures >= connection.max_consecutive_failures:
                        logger.debug(f"【私人连接池】[币安] 探测失败{connection.consecutive_probe_failures}次")
                        return False
                
                # 2. 检查WebSocket底层状态
                if hasattr(connection, 'ws') and connection.ws:
                    if connection.ws.closed:
                        return False
                
                # ✅ 币安不检查数据间隔，完全依赖探测
                return True
            
            # ===== 欧意：检查数据间隔 =====
            else:  # 欧意
                # 1. 检查最后消息时间（5秒阈值）
                if connection.last_message_time:
                    seconds_since = (datetime.now() - connection.last_message_time).total_seconds()
                    if seconds_since > 5:  # 5秒无数据就不健康
                        logger.warning(f"【私人连接池】[欧意] {seconds_since:.0f}秒无数据")
                        return False
                else:
                    # 从未收到过消息
                    if connection.connection_established_time:
                        elapsed = (datetime.now() - connection.connection_established_time).total_seconds()
                        if elapsed > 8:  # 连接建立8秒还没收到数据
                            logger.warning(f"【私人连接池】[欧意] 连接建立{elapsed:.0f}秒未收数据")
                            return False
                
                # 2. 检查WebSocket底层状态
                if hasattr(connection, 'ws') and connection.ws:
                    if connection.ws.closed:
                        return False
                
                return True
            
        except Exception as e:
            logger.error(f"【私人连接池】健康检查异常: {e}")
            return False
    
    async def _rebuild_single_connection(self, exchange: str, conn_type: str):
        """重建单个连接"""
        try:
            logger.info(f"【私人连接池】[{exchange}] 开始重建{conn_type}连接")
            
            pair = self.connection_pairs.get(exchange)
            if not pair:
                logger.error(f"【私人连接池】[{exchange}] 连接对不存在")
                return
            
            new_conn = None
            if exchange == 'binance':
                listen_key = await self.brain_store.get_listen_key('binance')
                if not listen_key:
                    logger.error("【私人连接池】[币安] 无法获取listenKey")
                    return
                
                new_conn = BinancePrivateConnection(
                    listen_key=listen_key,
                    status_callback=self._handle_connection_status,
                    data_callback=self._create_conn_callback(exchange, conn_type),
                    raw_data_cache=None
                )
                
            elif exchange == 'okx':
                api_creds = await self.brain_store.get_api_credentials('okx')
                if not api_creds:
                    logger.error("【私人连接池】[欧意] 无法获取API凭证")
                    return
                
                new_conn = OKXPrivateConnection(
                    api_key=api_creds['api_key'],
                    api_secret=api_creds['api_secret'],
                    passphrase=api_creds.get('passphrase', ''),
                    status_callback=self._handle_connection_status,
                    data_callback=self._create_conn_callback(exchange, conn_type),
                    raw_data_cache=None
                )
            
            if not new_conn:
                return
            
            success = await new_conn.connect()
            if not success:
                logger.error(f"【私人连接池】[{exchange}] {conn_type}连接重建失败")
                await asyncio.sleep(10)
                asyncio.create_task(self._rebuild_single_connection(exchange, conn_type))
                return
            
            if conn_type == 'primary':
                if pair.conn_primary:
                    await pair.conn_primary.disconnect()
                pair.conn_primary = new_conn
            else:
                if pair.conn_secondary:
                    await pair.conn_secondary.disconnect()
                pair.conn_secondary = new_conn
            
            pair.update_health_status(conn_type, True)
            logger.info(f"✅ 【私人连接池】[{exchange}] {conn_type}连接重建成功")
            
        except Exception as e:
            logger.error(f"【私人连接池】[{exchange}] 重建{conn_type}连接异常: {e}")
            await asyncio.sleep(15)
            asyncio.create_task(self._rebuild_single_connection(exchange, conn_type))
    
    async def _schedule_reconnect(self, exchange: str, delay: int = 5, is_dual: bool = False):
        """安排重连"""
        if exchange in self.reconnect_tasks:
            try:
                self.reconnect_tasks[exchange].cancel()
            except:
                pass
        
        async def reconnect_task():
            try:
                await asyncio.sleep(delay)
                if self.running:
                    logger.info(f"🔁 [私人连接池] 执行{exchange}重连...")
                    
                    if is_dual:
                        if exchange == 'binance':
                            success = await self._setup_binance_dual_connections()
                        else:
                            success = await self._setup_okx_dual_connections()
                    else:
                        if exchange == 'binance':
                            success = await self._setup_binance_connection()
                        else:
                            success = await self._setup_okx_connection()
                    
                    self._update_quality_stats(exchange, success)
                    
                    if not success:
                        next_delay = min(delay * 2, 120)
                        await self._schedule_reconnect(exchange, next_delay, is_dual)
                        
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"❌ [私人连接池] 重连任务异常: {e}")
        
        self.reconnect_tasks[exchange] = asyncio.create_task(reconnect_task())
    
    def _update_quality_stats(self, exchange: str, success: bool):
        """更新连接质量统计"""
        stats = self.quality_stats[exchange]
        stats['total_attempts'] += 1
        
        if success:
            stats['success_attempts'] += 1
            stats['consecutive_failures'] = 0
            stats['last_success'] = datetime.now()
            stats['last_error'] = None
        else:
            stats['consecutive_failures'] += 1
        
        if stats['total_attempts'] > 0:
            stats['success_rate'] = (stats['success_attempts'] / stats['total_attempts']) * 100
    
    # 保留原有的单连接建立方法作为兼容
    async def _setup_binance_connection(self) -> bool:
        """单连接建立（兼容旧代码）"""
        try:
            if not self.brain_store:
                return False
            
            listen_key = await self.brain_store.get_listen_key('binance')
            if not listen_key:
                return False
            
            api_creds = await self.brain_store.get_api_credentials('binance')
            if not api_creds:
                return False
            
            connection = BinancePrivateConnection(
                listen_key=listen_key,
                status_callback=self._handle_connection_status,
                data_callback=self._process_and_forward_data,
                raw_data_cache=None
            )
            
            success = await connection.connect()
            if success:
                self.connections['binance'] = connection
            
            return success
            
        except Exception as e:
            logger.error(f"【私人连接池】单连接建立异常: {e}")
            return False
    
    async def _setup_okx_connection(self) -> bool:
        """单连接建立（兼容旧代码）"""
        try:
            if not self.brain_store:
                return False
            
            api_creds = await self.brain_store.get_api_credentials('okx')
            if not api_creds:
                return False
            
            connection = OKXPrivateConnection(
                api_key=api_creds['api_key'],
                api_secret=api_creds['api_secret'],
                passphrase=api_creds.get('passphrase', ''),
                status_callback=self._handle_connection_status,
                data_callback=self._process_and_forward_data,
                raw_data_cache=None
            )
            
            success = await connection.connect()
            if success:
                self.connections['okx'] = connection
            
            return success
            
        except Exception as e:
            logger.error(f"【私人连接池】单连接建立异常: {e}")
            return False
    
    async def on_listen_key_updated(self, exchange: str, listen_key: str):
        """监听listenKey更新事件"""
        try:
            logger.info(f"📢 [私人连接池] 收到{exchange} listenKey更新通知")
            
            if exchange == 'binance':
                logger.info(f"🔗 [私人连接池] 5秒后重建币安双连接...")
                await self._schedule_reconnect('binance', 5, is_dual=True)
            elif exchange == 'okx':
                logger.info(f"🔗 [私人连接池] listenKey更新，但OKX使用API key连接，跳过")
            else:
                logger.warning(f"⚠️ [私人连接池] 未知交易所: {exchange}")
                
        except Exception as e:
            logger.error(f"❌ [私人连接池] 处理listenKey更新失败: {e}")
    
    async def _handle_connection_status(self, status_data: Dict[str, Any]):
        """处理连接状态事件"""
        try:
            exchange = status_data.get('exchange')
            event = status_data.get('event')
            logger.info(f"📡 [私人连接池] {exchange}状态事件: {event}")
        except Exception as e:
            logger.error(f"❌ [私人连接池] 处理状态事件失败: {e}")
    
    async def _process_and_forward_data(self, raw_data: Dict[str, Any]):
        """处理并转发数据"""
        try:
            self.push_stats['total_created'] += 1
            self.push_stats['last_push_time'] = datetime.now().isoformat()
            asyncio.create_task(self._push_to_manager(raw_data.copy()))
        except Exception as e:
            logger.error(f"❌ [私人连接池] 创建推送任务失败: {e}")
            self.push_stats['total_failed'] += 1

    async def _push_to_manager(self, data: Dict[str, Any]):
        """实际推送数据到 manager"""
        try:
            from private_data_processing.manager import receive_private_data
            await receive_private_data(data)
            self.push_stats['total_success'] += 1
        except ImportError as e:
            logger.error(f"❌ [私人连接池] 无法导入manager模块: {e}")
            self.push_stats['total_failed'] += 1
        except Exception as e:
            logger.error(f"❌ [私人连接池] 推送数据失败: {e}")
            self.push_stats['total_failed'] += 1
    
    async def shutdown(self):
        """关闭所有连接"""
        logger.info("🛑 [私人连接池] 正在关闭...")
        self.running = False
        
        if self.health_monitor_task:
            self.health_monitor_task.cancel()
            try:
                await self.health_monitor_task
            except:
                pass
        
        for exchange, task in self.reconnect_tasks.items():
            if task:
                task.cancel()
        
        shutdown_tasks = []
        for exchange, pair in self.connection_pairs.items():
            if pair:
                if pair.conn_primary:
                    shutdown_tasks.append(
                        asyncio.wait_for(pair.conn_primary.disconnect(), timeout=5)
                    )
                if pair.conn_secondary:
                    shutdown_tasks.append(
                        asyncio.wait_for(pair.conn_secondary.disconnect(), timeout=5)
                    )
        
        if shutdown_tasks:
            try:
                await asyncio.gather(*shutdown_tasks, return_exceptions=True)
            except:
                pass
        
        self.connections = {'binance': None, 'okx': None}
        self.connection_pairs = {'binance': None, 'okx': None}
        
        logger.info(f"📊 [私人连接池] 推送统计: 创建{self.push_stats['total_created']}, "
                   f"成功{self.push_stats['total_success']}, 失败{self.push_stats['total_failed']}")
        logger.info("✅ [私人连接池] 已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'running': self.running,
            'uptime_seconds': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            'connections': {},
            'connection_pairs': {},
            'quality_stats': self.quality_stats,
            'push_stats': self.push_stats,
            'alerts': [],
            'exchange_modes': {
                'binance': '主动探测模式 (3秒探测, 2次失败重连)',
                'okx': '数据流模式 (5秒无数据重连)'
            },
            'data_destination': '私人数据处理模块（异步推送+去重）'
        }
        
        for exchange, pair in self.connection_pairs.items():
            if pair:
                pair_status = pair.get_status()
                status['connection_pairs'][exchange] = pair_status
                
                if not pair_status['primary_healthy']:
                    status['alerts'].append(f"【私人连接池】{exchange}主连接不健康")
                if pair.conn_secondary and not pair_status['secondary_healthy']:
                    status['alerts'].append(f"【私人连接池】{exchange}备连接不健康")
                if not pair.conn_secondary:
                    status['alerts'].append(f"【私人连接池】{exchange}备连接不存在")
            else:
                status['connection_pairs'][exchange] = {'error': 'not_initialized'}
                status['alerts'].append(f"【私人连接池】{exchange}连接对未初始化")
        
        for exchange in ['binance', 'okx']:
            connection = self.connections[exchange]
            if connection:
                status['connections'][exchange] = {
                    'connected': connection.connected,
                    'last_message_time': connection.last_message_time.isoformat() if connection.last_message_time else None,
                    'message_counter': connection.message_counter
                }
            else:
                status['connections'][exchange] = {'connected': False}
        
        return status