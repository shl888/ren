"""
私人WebSocket连接池管理器 - 双连接热备版
在原有成功连接基础上，增加双连接同时收数据，保证永不丢数据
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
        self.recent_ids = deque(maxlen=window_size)  # 滑动窗口，自动维护大小
        self.id_set = set()  # 用于快速查找
        logger.debug(f"[去重器] 初始化，窗口大小: {window_size}")
    
    def extract_unique_id(self, data: Dict[str, Any]) -> str:
        """
        从消息中提取唯一ID
        不同交易所的消息ID提取方式不同
        """
        exchange = data.get('exchange')
        msg_data = data.get('data', {})
        
        if exchange == 'binance':
            # 币安消息：使用事件类型+时间戳+交易对作为唯一ID
            event_type = msg_data.get('e', 'unknown')
            event_time = msg_data.get('E', 0)
            symbol = msg_data.get('s', '')
            
            # 如果是订单更新，用订单ID
            if event_type == 'ORDER_TRADE_UPDATE':
                order_id = msg_data.get('o', {}).get('i', '')
                return f"binance:order:{order_id}:{event_time}"
            # 如果是账户更新，用更新时间
            elif event_type == 'ACCOUNT_UPDATE':
                update_time = msg_data.get('u', event_time)
                return f"binance:account:{update_time}"
            else:
                return f"binance:{event_type}:{symbol}:{event_time}"
        
        elif exchange == 'okx':
            # 欧意消息：使用channel+时间戳+ID
            arg = msg_data.get('arg', {})
            channel = arg.get('channel', 'unknown')
            data_list = msg_data.get('data', [])
            
            if data_list and len(data_list) > 0:
                first_data = data_list[0]
                # 不同频道有不同的ID字段
                if 'uTime' in first_data:  # 更新时间
                    return f"okx:{channel}:{first_data.get('uTime')}"
                elif 'cTime' in first_data:  # 创建时间
                    return f"okx:{channel}:{first_data.get('cTime')}"
                elif 'instId' in first_data:  # 产品ID
                    return f"okx:{channel}:{first_data.get('instId')}:{first_data.get('uTime', time.time())}"
            
            # 兜底：使用当前时间戳（毫秒级）
            return f"okx:{channel}:{int(time.time()*1000)}"
        
        # 兜底方案
        return f"{exchange}:{datetime.now().timestamp()}"
    
    def is_duplicate(self, data: Dict[str, Any]) -> bool:
        """检查是否重复消息"""
        try:
            msg_id = self.extract_unique_id(data)
            
            # 检查是否在集合中
            if msg_id in self.id_set:
                return True
            
            # 新消息，加入窗口
            self.recent_ids.append(msg_id)
            self.id_set.add(msg_id)
            
            # 维护set大小（防止内存无限增长）
            if len(self.id_set) > self.window_size * 1.5:
                # 重建set，只保留窗口内的ID
                self.id_set = set(self.recent_ids)
            
            return False
            
        except Exception as e:
            logger.error(f"[去重器] 检查重复失败: {e}")
            return False  # 出错时假设不重复，宁可多处理也不丢数据


class ExchangeConnectionPair:
    """
    交易所连接对 - 管理同一交易所的两个连接
    核心思想：双连接同时收数据，去重后转发
    """
    def __init__(self, exchange: str, data_callback: Callable):
        self.exchange = exchange
        self.data_callback = data_callback  # 最终数据回调（已去重）
        
        # 两个连接实例
        self.conn_primary = None   # 主连接
        self.conn_secondary = None  # 备连接
        
        # 去重器
        self.deduplicator = MessageDeduplicator()
        
        # 连接状态
        self.primary_healthy = False
        self.secondary_healthy = False
        
        # 统计
        self.total_messages = 0
        self.duplicate_messages = 0
        self.primary_messages = 0
        self.secondary_messages = 0
        self.last_switch_time = None
        
        logger.info(f"[{exchange}连接对] 初始化完成")
    
    def update_health_status(self, conn_name: str, is_healthy: bool):
        """更新连接健康状态"""
        if conn_name == 'primary':
            self.primary_healthy = is_healthy
        else:
            self.secondary_healthy = is_healthy
        
        # 记录切换时间
        if not is_healthy:
            logger.warning(f"[{self.exchange}] {conn_name}连接不健康")
    
    async def process_message(self, data: Dict[str, Any], source: str):
        """
        处理来自某个连接的消息
        source: 'primary' 或 'secondary'
        """
        try:
            # 更新统计
            self.total_messages += 1
            if source == 'primary':
                self.primary_messages += 1
            else:
                self.secondary_messages += 1
            
            # 去重检查
            if self.deduplicator.is_duplicate(data):
                self.duplicate_messages += 1
                logger.debug(f"[{self.exchange}] 丢弃重复消息 from {source}")
                return
            
            # 非重复消息，转发给上层
            await self.data_callback(data)
            
        except Exception as e:
            logger.error(f"[{self.exchange}] 处理消息失败: {e}")
    
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
    """
    私人连接池 - 双连接热备版
    每个交易所维护两个同时收数据的连接，保证永不丢数据
    """
    
    def __init__(self):
        self.data_callback = None
        
        # 连接对存储（每个交易所一个连接对）
        self.connection_pairs = {
            'binance': None,
            'okx': None
        }
        
        # 原始的单个连接（为了兼容性，保留但不再主要使用）
        self.connections = {
            'binance': None,
            'okx': None
        }
        
        # 状态管理
        self.running = False
        self.brain_store = None
        self.start_time = None
        self.reconnect_tasks = {}
        
        # 连接质量统计
        self.quality_stats = {
            'binance': {
                'total_attempts': 0,
                'success_attempts': 0,
                'consecutive_failures': 0,
                'last_success': None,
                'success_rate': 100.0,
                'last_error': None,
                'mode': 'dual_active'  # 改为双活模式
            },
            'okx': {
                'total_attempts': 0,
                'success_attempts': 0,
                'consecutive_failures': 0,
                'last_success': None,
                'success_rate': 100.0,
                'last_error': None,
                'mode': 'dual_active'  # 改为双活模式
            }
        }
        
        # 推送统计
        self.push_stats = {
            'total_created': 0,
            'total_success': 0,
            'total_failed': 0,
            'last_push_time': None
        }
        
        # 健康监控任务
        self.health_monitor_task = None
        
        logger.info("🔗 [私人连接池] 双连接热备版初始化完成")
    
    async def start(self, brain_store):
        """启动连接池"""
        logger.info("🚀 [私人连接池] 正在启动双连接热备版...")
        
        self.brain_store = brain_store
        self.running = True
        self.start_time = datetime.now()
        
        # 启动健康监控循环（持续监控两个连接的健康状态）
        self.health_monitor_task = asyncio.create_task(self._health_monitor_loop())
        
        # 分批尝试建立双连接
        asyncio.create_task(self._staggered_connect_all())
        
        logger.info("✅ [私人连接池] 已启动，双连接热备模式运行中")
        return True
    
    async def _staggered_connect_all(self):
        """分批建立所有交易所的双连接"""
        # 先连接币安
        logger.info("🔗 [私人连接池] 第一阶段：建立币安双连接")
        binance_success = await self._setup_binance_dual_connections()
        
        # 等待3秒再连接欧意
        await asyncio.sleep(3)
        
        logger.info("🔗 [私人连接池] 第二阶段：建立欧意双连接")
        okx_success = await self._setup_okx_dual_connections()
        
        success_count = sum([binance_success, okx_success])
        logger.info(f"🎯 [私人连接池] 双连接建立完成: {success_count}/2 成功")
        
        # 失败的安排重连（会尝试重建双连接）
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
            
            # 获取listenKey（币安私人连接需要）
            listen_key = await self.brain_store.get_listen_key('binance')
            if not listen_key:
                logger.warning("⚠️ [私人连接池] 币安listenKey不存在，等待中...")
                return False
            
            # 获取API凭证
            api_creds = await self.brain_store.get_api_credentials('binance')
            if not api_creds:
                logger.error("❌ [私人连接池] 币安API凭证不存在")
                return False
            
            # 创建连接对（如果不存在）
            if not self.connection_pairs['binance']:
                self.connection_pairs['binance'] = ExchangeConnectionPair(
                    'binance', 
                    self._process_and_forward_data
                )
            
            # ===== 建立主连接 =====
            logger.info("[币安] 正在建立主连接...")
            primary_conn = BinancePrivateConnection(
                listen_key=listen_key,
                status_callback=self._handle_connection_status,
                data_callback=self._create_conn_callback('binance', 'primary'),
                raw_data_cache=None
            )
            
            primary_success = await primary_conn.connect()
            if not primary_success:
                logger.error("[币安] 主连接建立失败")
                return False
            
            # ===== 建立备连接（使用相同的listenKey）=====
            logger.info("[币安] 正在建立备连接...")
            secondary_conn = BinancePrivateConnection(
                listen_key=listen_key,  # 同一个listenKey
                status_callback=self._handle_connection_status,
                data_callback=self._create_conn_callback('binance', 'secondary'),
                raw_data_cache=None
            )
            
            secondary_success = await secondary_conn.connect()
            if not secondary_success:
                logger.warning("[币安] 备连接建立失败，但主连接已成功，继续运行")
                # 不返回False，因为主连接可用
            
            # 保存连接
            self.connection_pairs['binance'].conn_primary = primary_conn
            self.connection_pairs['binance'].conn_secondary = secondary_conn if secondary_success else None
            
            # 更新健康状态
            self.connection_pairs['binance'].update_health_status('primary', True)
            self.connection_pairs['binance'].update_health_status('secondary', secondary_success)
            
            # 为了兼容性，也设置原始的connections
            self.connections['binance'] = primary_conn
            
            logger.info(f"✅ [币安] 双连接建立完成: 主={'✓' if primary_success else '✗'}, 备={'✓' if secondary_success else '✗'}")
            
            # 更新统计
            self._update_quality_stats('binance', True)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ [币安] 建立双连接异常: {e}")
            self.quality_stats['binance']['last_error'] = str(e)
            return False
    
    async def _setup_okx_dual_connections(self) -> bool:
        """建立欧意双连接"""
        try:
            if not self.brain_store:
                logger.error("❌ [私人连接池] 未设置大脑存储接口")
                return False
            
            # 获取API凭证
            api_creds = await self.brain_store.get_api_credentials('okx')
            if not api_creds:
                logger.warning("⚠️ [私人连接池] 欧意API凭证不存在，等待中...")
                return False
            
            # 创建连接对（如果不存在）
            if not self.connection_pairs['okx']:
                self.connection_pairs['okx'] = ExchangeConnectionPair(
                    'okx', 
                    self._process_and_forward_data
                )
            
            # ===== 建立主连接 =====
            logger.info("[欧意] 正在建立主连接...")
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
                logger.error("[欧意] 主连接建立失败")
                return False
            
            # ===== 建立备连接 =====
            logger.info("[欧意] 正在建立备连接...")
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
                logger.warning("[欧意] 备连接建立失败，但主连接已成功，继续运行")
            
            # 保存连接
            self.connection_pairs['okx'].conn_primary = primary_conn
            self.connection_pairs['okx'].conn_secondary = secondary_conn if secondary_success else None
            
            # 更新健康状态
            self.connection_pairs['okx'].update_health_status('primary', True)
            self.connection_pairs['okx'].update_health_status('secondary', secondary_success)
            
            # 为了兼容性，也设置原始的connections
            self.connections['okx'] = primary_conn
            
            logger.info(f"✅ [欧意] 双连接建立完成: 主={'✓' if primary_success else '✗'}, 备={'✓' if secondary_success else '✗'}")
            
            # 更新统计
            self._update_quality_stats('okx', True)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ [欧意] 建立双连接异常: {e}")
            self.quality_stats['okx']['last_error'] = str(e)
            return False
    
    def _create_conn_callback(self, exchange: str, conn_name: str):
        """
        创建连接专用的回调函数
        每个连接有自己的回调，标识消息来源
        """
        async def callback(data: Dict[str, Any]):
            """连接收到数据时的回调"""
            try:
                # 通过连接对处理消息
                pair = self.connection_pairs.get(exchange)
                if pair:
                    await pair.process_message(data, conn_name)
            except Exception as e:
                logger.error(f"[{exchange}:{conn_name}] 回调处理失败: {e}")
        
        return callback
    
    async def _health_monitor_loop(self):
        """
        健康监控循环
        持续检查每个交易所的两个连接，发现不健康立即重建
        """
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
                        
                        # 如果从健康变为不健康，立即重建
                        if old_status and not is_healthy:
                            logger.warning(f"[{exchange}] 主连接变不健康，立即重建")
                            asyncio.create_task(self._rebuild_single_connection(
                                exchange, 'primary'
                            ))
                    
                    # 检查备连接
                    if pair.conn_secondary:
                        is_healthy = await self._check_connection_health(pair.conn_secondary)
                        old_status = pair.secondary_healthy
                        pair.update_health_status('secondary', is_healthy)
                        
                        # 如果从健康变为不健康，立即重建
                        if old_status and not is_healthy:
                            logger.warning(f"[{exchange}] 备连接变不健康，立即重建")
                            asyncio.create_task(self._rebuild_single_connection(
                                exchange, 'secondary'
                            ))
                    
                    # 如果备连接不存在，尝试创建
                    if not pair.conn_secondary and pair.conn_primary:
                        logger.info(f"[{exchange}] 备连接不存在，尝试创建")
                        asyncio.create_task(self._rebuild_single_connection(
                            exchange, 'secondary'
                        ))
                
                await asyncio.sleep(5)  # 每5秒检查一次
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [健康监控] 异常: {e}")
                await asyncio.sleep(10)
    
    async def _check_connection_health(self, connection) -> bool:
        """检查单个连接是否健康"""
        try:
            if not connection:
                return False
            
            if not connection.connected:
                return False
            
            # 检查最后消息时间（超过20秒无消息认为不健康）
            if connection.last_message_time:
                seconds_since = (datetime.now() - connection.last_message_time).total_seconds()
                if seconds_since > 20:
                    logger.debug(f"连接超过{seconds_since:.0f}秒无消息")
                    return False
            
            # 检查WebSocket底层状态
            if hasattr(connection, 'ws') and connection.ws:
                if connection.ws.closed:
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"健康检查异常: {e}")
            return False
    
    async def _rebuild_single_connection(self, exchange: str, conn_type: str):
        """
        重建单个连接
        conn_type: 'primary' 或 'secondary'
        """
        try:
            logger.info(f"[{exchange}] 开始重建{conn_type}连接")
            
            pair = self.connection_pairs.get(exchange)
            if not pair:
                logger.error(f"[{exchange}] 连接对不存在")
                return
            
            # 获取当前健康的连接作为参考
            healthy_conn = pair.conn_primary if conn_type == 'secondary' else pair.conn_secondary
            
            # 创建新连接
            new_conn = None
            if exchange == 'binance':
                # 获取listenKey
                listen_key = await self.brain_store.get_listen_key('binance')
                if not listen_key:
                    logger.error("[币安] 无法获取listenKey")
                    return
                
                new_conn = BinancePrivateConnection(
                    listen_key=listen_key,
                    status_callback=self._handle_connection_status,
                    data_callback=self._create_conn_callback(exchange, conn_type),
                    raw_data_cache=None
                )
                
            elif exchange == 'okx':
                # 获取API凭证
                api_creds = await self.brain_store.get_api_credentials('okx')
                if not api_creds:
                    logger.error("[欧意] 无法获取API凭证")
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
            
            # 建立连接
            success = await new_conn.connect()
            if not success:
                logger.error(f"[{exchange}] {conn_type}连接重建失败")
                # 稍后重试
                await asyncio.sleep(10)
                asyncio.create_task(self._rebuild_single_connection(exchange, conn_type))
                return
            
            # 替换旧连接
            if conn_type == 'primary':
                # 断开旧连接
                if pair.conn_primary:
                    await pair.conn_primary.disconnect()
                pair.conn_primary = new_conn
            else:
                if pair.conn_secondary:
                    await pair.conn_secondary.disconnect()
                pair.conn_secondary = new_conn
            
            # 更新健康状态
            pair.update_health_status(conn_type, True)
            
            logger.info(f"✅ [{exchange}] {conn_type}连接重建成功")
            
        except Exception as e:
            logger.error(f"[{exchange}] 重建{conn_type}连接异常: {e}")
            # 异常后重试
            await asyncio.sleep(15)
            asyncio.create_task(self._rebuild_single_connection(exchange, conn_type))
    
    async def _health_monitor_loop(self):
        """健康监控循环 - 完整版"""
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
                        
                        # 如果从健康变为不健康，立即重建
                        if old_status and not is_healthy:
                            logger.warning(f"[{exchange}] 主连接变不健康，立即重建")
                            asyncio.create_task(self._rebuild_single_connection(
                                exchange, 'primary'
                            ))
                    
                    # 检查备连接
                    if pair.conn_secondary:
                        is_healthy = await self._check_connection_health(pair.conn_secondary)
                        old_status = pair.secondary_healthy
                        pair.update_health_status('secondary', is_healthy)
                        
                        # 如果从健康变为不健康，立即重建
                        if old_status and not is_healthy:
                            logger.warning(f"[{exchange}] 备连接变不健康，立即重建")
                            asyncio.create_task(self._rebuild_single_connection(
                                exchange, 'secondary'
                            ))
                    
                    # 如果备连接不存在，尝试创建
                    if not pair.conn_secondary and pair.conn_primary:
                        logger.info(f"[{exchange}] 备连接不存在，尝试创建")
                        asyncio.create_task(self._rebuild_single_connection(
                            exchange, 'secondary'
                        ))
                
                await asyncio.sleep(5)  # 每5秒检查一次
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [健康监控] 异常: {e}")
                await asyncio.sleep(10)
    
    async def _check_connection_health(self, connection) -> bool:
        """检查单个连接是否健康"""
        try:
            if not connection:
                return False
            
            if not connection.connected:
                return False
            
            # 检查最后消息时间（超过20秒无消息认为不健康）
            if connection.last_message_time:
                seconds_since = (datetime.now() - connection.last_message_time).total_seconds()
                if seconds_since > 20:
                    logger.debug(f"连接超过{seconds_since:.0f}秒无消息")
                    return False
            
            # 检查WebSocket底层状态
            if hasattr(connection, 'ws') and connection.ws:
                if connection.ws.closed:
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"健康检查异常: {e}")
            return False
    
    async def _rebuild_single_connection(self, exchange: str, conn_type: str):
        """
        重建单个连接
        conn_type: 'primary' 或 'secondary'
        """
        try:
            logger.info(f"[{exchange}] 开始重建{conn_type}连接")
            
            pair = self.connection_pairs.get(exchange)
            if not pair:
                logger.error(f"[{exchange}] 连接对不存在")
                return
            
            # 获取当前健康的连接作为参考
            healthy_conn = pair.conn_primary if conn_type == 'secondary' else pair.conn_secondary
            
            # 创建新连接
            new_conn = None
            if exchange == 'binance':
                # 获取listenKey
                listen_key = await self.brain_store.get_listen_key('binance')
                if not listen_key:
                    logger.error("[币安] 无法获取listenKey")
                    return
                
                new_conn = BinancePrivateConnection(
                    listen_key=listen_key,
                    status_callback=self._handle_connection_status,
                    data_callback=self._create_conn_callback(exchange, conn_type),
                    raw_data_cache=None
                )
                
            elif exchange == 'okx':
                # 获取API凭证
                api_creds = await self.brain_store.get_api_credentials('okx')
                if not api_creds:
                    logger.error("[欧意] 无法获取API凭证")
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
            
            # 建立连接
            success = await new_conn.connect()
            if not success:
                logger.error(f"[{exchange}] {conn_type}连接重建失败")
                # 稍后重试
                await asyncio.sleep(10)
                asyncio.create_task(self._rebuild_single_connection(exchange, conn_type))
                return
            
            # 替换旧连接
            if conn_type == 'primary':
                # 断开旧连接
                if pair.conn_primary:
                    await pair.conn_primary.disconnect()
                pair.conn_primary = new_conn
            else:
                if pair.conn_secondary:
                    await pair.conn_secondary.disconnect()
                pair.conn_secondary = new_conn
            
            # 更新健康状态
            pair.update_health_status(conn_type, True)
            
            logger.info(f"✅ [{exchange}] {conn_type}连接重建成功")
            
        except Exception as e:
            logger.error(f"[{exchange}] 重建{conn_type}连接异常: {e}")
            # 异常后重试
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
                        # 重建双连接
                        if exchange == 'binance':
                            success = await self._setup_binance_dual_connections()
                        else:
                            success = await self._setup_okx_dual_connections()
                    else:
                        # 兼容旧的单连接重连
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
            logger.error(f"单连接建立异常: {e}")
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
            logger.error(f"单连接建立异常: {e}")
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
            
            # 注意：双连接模式下，单个连接的状态事件不影响整体
            # 健康监控会处理具体的连接重建
            
        except Exception as e:
            logger.error(f"❌ [私人连接池] 处理状态事件失败: {e}")
    
    async def _process_and_forward_data(self, raw_data: Dict[str, Any]):
        """
        处理并转发数据 - 异步非阻塞版本
        注意：这个函数会被单连接调用，双连接会通过各自的回调绕过它
        """
        try:
            # 更新统计
            self.push_stats['total_created'] += 1
            self.push_stats['last_push_time'] = datetime.now().isoformat()
            
            # 创建独立任务推送
            asyncio.create_task(self._push_to_manager(raw_data.copy()))
            
        except Exception as e:
            logger.error(f"❌ [连接池] 创建推送任务失败: {e}")
            self.push_stats['total_failed'] += 1

    async def _push_to_manager(self, data: Dict[str, Any]):
        """实际推送数据到 manager"""
        try:
            from private_data_processing.manager import receive_private_data
            await receive_private_data(data)
            self.push_stats['total_success'] += 1
        except ImportError as e:
            logger.error(f"❌ [连接池] 无法导入manager模块: {e}")
            self.push_stats['total_failed'] += 1
        except Exception as e:
            logger.error(f"❌ [连接池] 推送数据失败: {e}")
            self.push_stats['total_failed'] += 1
    
    async def shutdown(self):
        """关闭所有连接"""
        logger.info("🛑 [私人连接池] 正在关闭...")
        self.running = False
        
        # 取消健康监控
        if self.health_monitor_task:
            self.health_monitor_task.cancel()
            try:
                await self.health_monitor_task
            except:
                pass
        
        # 取消重连任务
        for exchange, task in self.reconnect_tasks.items():
            if task:
                task.cancel()
        
        # 关闭所有连接对中的连接
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
        
        # 打印统计
        logger.info(f"📊 [连接池] 推送统计: 创建{self.push_stats['total_created']}, "
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
                'binance': '双连接热备模式（主动探测）',
                'okx': '双连接热备模式（心跳）'
            },
            'data_destination': '私人数据处理模块（异步推送+去重）'
        }
        
        # 获取每个交易所连接对的详细状态
        for exchange, pair in self.connection_pairs.items():
            if pair:
                pair_status = pair.get_status()
                status['connection_pairs'][exchange] = pair_status
                
                # 添加告警
                if not pair_status['primary_healthy']:
                    status['alerts'].append(f"{exchange}主连接不健康")
                if pair.conn_secondary and not pair_status['secondary_healthy']:
                    status['alerts'].append(f"{exchange}备连接不健康")
                if not pair.conn_secondary:
                    status['alerts'].append(f"{exchange}备连接不存在")
            else:
                status['connection_pairs'][exchange] = {'error': 'not_initialized'}
                status['alerts'].append(f"{exchange}连接对未初始化")
        
        # 兼容旧的connections状态
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