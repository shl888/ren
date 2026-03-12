"""
私人WebSocket连接池管理器 - 双连接热备版
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional

from .connection import BinancePrivateConnection, OKXPrivateConnection

logger = logging.getLogger(__name__)


class PrivateWebSocketPool:
    """
    私人连接池 - 双连接热备架构
    """
    
    def __init__(self):
        # 双连接存储
        self.connections: Dict[str, Dict[str, Optional[Any]]] = {
            'binance': {'primary': None, 'backup': None},
            'okx': {'primary': None, 'backup': None}
        }
        
        self.active_connection: Dict[str, str] = {
            'binance': 'primary',
            'okx': 'primary'
        }
        
        self.quality_stats = {
            'binance': {
                'total_attempts': 0,
                'success_attempts': 0,
                'consecutive_failures': 0,
                'last_success': None,
                'success_rate': 100.0,
                'last_error': None
            },
            'okx': {
                'total_attempts': 0,
                'success_attempts': 0,
                'consecutive_failures': 0,
                'last_success': None,
                'success_rate': 100.0,
                'last_error': None
            }
        }
        
        # 数据去重缓存
        self.message_cache: Dict[str, float] = {}
        self.message_ttl = 5.0
        
        self.push_stats = {
            'total_received': 0,
            'total_deduplicated': 0,
            'total_duplicates': 0,
            'last_push_time': None
        }
        
        self.running = False
        self.brain_store = None
        self.start_time = None
        self.reconnect_tasks: Dict[str, asyncio.Task] = {}
        self.switch_lock = asyncio.Lock()
        
        logger.info("🔗 [连接池] 双连接热备版初始化完成")
    
    async def start(self, brain_store):
        """启动连接池"""
        logger.info("🚀 [连接池] 启动...")
        
        self.brain_store = brain_store
        self.running = True
        self.start_time = datetime.now()
        
        asyncio.create_task(self._connection_monitor_loop())
        asyncio.create_task(self._cache_cleanup_loop())
        asyncio.create_task(self._staggered_connect_all())
        
        logger.info("✅ [连接池] 已启动")
        return True
    
    async def _staggered_connect_all(self):
        """分批建立连接"""
        logger.info("🔗 [连接池] 阶段1：建立主连接")
        
        binance_primary_ok = await self._setup_connection('binance', 'primary')
        await asyncio.sleep(1)
        
        okx_primary_ok = await self._setup_connection('okx', 'primary')
        
        logger.info("🔗 [连接池] 阶段2：3秒后建立备连接")
        await asyncio.sleep(3)
        
        binance_backup_ok = await self._setup_connection('binance', 'backup')
        await asyncio.sleep(1)
        
        okx_backup_ok = await self._setup_connection('okx', 'backup')
        
        primary_ok = sum([binance_primary_ok, okx_primary_ok])
        backup_ok = sum([binance_backup_ok, okx_backup_ok])
        
        logger.info(f"🎯 [连接池] 连接建立完成: 主{primary_ok}/2, 备{backup_ok}/2")
        
        # 安排失败重连
        if not binance_primary_ok:
            await self._schedule_reconnect('binance', 'primary', 5)
        if not binance_backup_ok:
            await self._schedule_reconnect('binance', 'backup', 10)
        if not okx_primary_ok:
            await self._schedule_reconnect('okx', 'primary', 5)
        if not okx_backup_ok:
            await self._schedule_reconnect('okx', 'backup', 10)
    
    async def _setup_connection(self, exchange: str, conn_type: str) -> bool:
        """建立单个连接"""
        try:
            if not self.brain_store:
                logger.error("❌ [连接池] 未设置大脑存储接口")
                return False
            
            # 生成连接ID
            connection_id = f"{exchange}_private_{conn_type}_{int(time.time())}"
            
            if exchange == 'binance':
                listen_key = await self.brain_store.get_listen_key('binance')
                if not listen_key:
                    logger.warning(f"⚠️ [连接池] 币安listenKey不存在")
                    return False
                
                # 修复：通过kwargs传递所有参数
                connection = BinancePrivateConnection(
                    listen_key=listen_key,
                    connection_id=connection_id,
                    status_callback=self._handle_connection_status,
                    data_callback=self._deduplicated_data_callback,
                    raw_data_cache=None
                )
                
            elif exchange == 'okx':
                api_creds = await self.brain_store.get_api_credentials('okx')
                if not api_creds:
                    logger.warning(f"⚠️ [连接池] 欧意API凭证不存在")
                    return False
                
                # 修复：通过kwargs传递所有参数
                connection = OKXPrivateConnection(
                    api_key=api_creds['api_key'],
                    api_secret=api_creds['api_secret'],
                    passphrase=api_creds.get('passphrase', ''),
                    connection_id=connection_id,
                    status_callback=self._handle_connection_status,
                    data_callback=self._deduplicated_data_callback,
                    raw_data_cache=None
                )
            else:
                logger.error(f"❌ [连接池] 未知交易所: {exchange}")
                return False
            
            success = await connection.connect()
            
            if success:
                self.connections[exchange][conn_type] = connection
                
                if conn_type == 'primary' and not self.connections[exchange].get(self.active_connection[exchange]):
                    self.active_connection[exchange] = 'primary'
                
                logger.info(f"✅ [连接池] {exchange}.{conn_type} 连接成功 [{connection_id}]")
                return True
            else:
                logger.error(f"❌ [连接池] {exchange}.{conn_type} 连接失败")
                return False
                
        except Exception as e:
            logger.error(f"❌ [连接池] 设置{exchange}.{conn_type}连接异常: {e}")
            return False
    
    async def _connection_monitor_loop(self):
        """连接监控循环"""
        while self.running:
            try:
                for exchange in ['binance', 'okx']:
                    for conn_type in ['primary', 'backup']:
                        connection = self.connections[exchange].get(conn_type)
                        
                        if not connection:
                            if self.running:
                                delay = 5 if conn_type == 'primary' else 15
                                await self._schedule_reconnect(exchange, conn_type, delay)
                            continue
                        
                        if not connection.connected:
                            logger.warning(f"🔁 [连接池] 检测到{exchange}.{conn_type}断开")
                            
                            if self.active_connection.get(exchange) == conn_type:
                                await self._switch_to_backup(exchange)
                            
                            await self._schedule_reconnect(exchange, conn_type, 3)
                
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [连接池] 监控循环异常: {e}")
                await asyncio.sleep(10)
    
    async def _switch_to_backup(self, exchange: str):
        """故障切换"""
        async with self.switch_lock:
            current = self.active_connection.get(exchange)
            backup = 'backup' if current == 'primary' else 'primary'
            
            backup_conn = self.connections[exchange].get(backup)
            if backup_conn and backup_conn.connected:
                self.active_connection[exchange] = backup
                logger.info(f"🔄 [连接池] {exchange} 切换: {current} -> {backup}")
            else:
                logger.error(f"🚨 [连接池] {exchange} 双连接均不可用！")
    
    async def _schedule_reconnect(self, exchange: str, conn_type: str, delay: int):
        """安排重连"""
        task_key = f"{exchange}_{conn_type}"
        
        if task_key in self.reconnect_tasks:
            try:
                self.reconnect_tasks[task_key].cancel()
            except:
                pass
        
        async def reconnect_task():
            try:
                await asyncio.sleep(delay)
                
                if not self.running:
                    return
                
                existing = self.connections[exchange].get(conn_type)
                if existing and existing.connected:
                    return
                
                logger.info(f"🔁 [连接池] 执行{exchange}.{conn_type}重连...")
                
                if existing:
                    try:
                        await existing.disconnect()
                    except:
                        pass
                
                success = await self._setup_connection(exchange, conn_type)
                self._update_quality_stats(exchange, success)
                
                if success:
                    current_active = self.active_connection.get(exchange)
                    current_conn = self.connections[exchange].get(current_active)
                    if conn_type == 'primary' and (not current_conn or not current_conn.connected):
                        async with self.switch_lock:
                            self.active_connection[exchange] = 'primary'
                            logger.info(f"🔄 [连接池] {exchange} 主连接恢复，切回primary")
                else:
                    next_delay = min(delay * 2, 120)
                    logger.warning(f"🔁 [连接池] {exchange}.{conn_type}重连失败，{next_delay}秒后重试")
                    await self._schedule_reconnect(exchange, conn_type, next_delay)
                    
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"❌ [连接池] 重连任务异常: {e}")
        
        self.reconnect_tasks[task_key] = asyncio.create_task(reconnect_task())
    
    def _update_quality_stats(self, exchange: str, success: bool):
        """更新统计"""
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
        
        if stats['total_attempts'] >= 10 and stats['success_rate'] < 80:
            logger.warning(f"⚠️ [连接池] {exchange} 成功率低: {stats['success_rate']:.1f}%")
    
    async def _deduplicated_data_callback(self, data: Dict[str, Any]):
        """数据去重回调"""
        try:
            self.push_stats['total_received'] += 1
            
            msg_id = self._generate_message_id(data)
            now = time.time()
            
            if msg_id in self.message_cache:
                last_time = self.message_cache[msg_id]
                if now - last_time < self.message_ttl:
                    self.push_stats['total_duplicates'] += 1
                    logger.debug(f"[去重] 丢弃重复: {msg_id}")
                    return
            
            self.message_cache[msg_id] = now
            self.push_stats['total_deduplicated'] += 1
            self.push_stats['last_push_time'] = datetime.now().isoformat()
            
            asyncio.create_task(self._push_to_manager(data))
            
        except Exception as e:
            logger.error(f"❌ [连接池] 去重处理失败: {e}")
    
    def _generate_message_id(self, data: Dict[str, Any]) -> str:
        """生成消息指纹"""
        exchange = data.get('exchange', 'unknown')
        raw = data.get('data', {})
        
        if exchange == 'binance':
            event = raw.get('e', 'unknown')
            ts = raw.get('E', 0)
            order_id = raw.get('i', raw.get('t', 0))
            return f"bn_{event}_{ts}_{order_id}"
        elif exchange == 'okx':
            arg = raw.get('arg', {})
            channel = arg.get('channel', 'unknown')
            ts = raw.get('ts', 0)
            inst_id = arg.get('instId', '')
            return f"ok_{channel}_{ts}_{inst_id}"
        else:
            return f"uk_{hash(str(data))}"
    
    async def _push_to_manager(self, data: Dict[str, Any]):
        """推送到业务层"""
        try:
            from private_data_processing.manager import receive_private_data
            await receive_private_data(data)
            logger.debug(f"✅ [连接池] 推送: {data.get('exchange')}.{data.get('data_type')}")
        except ImportError as e:
            logger.error(f"❌ [连接池] 无法导入manager: {e}")
        except Exception as e:
            logger.error(f"❌ [连接池] 推送失败: {e}")
    
    async def _cache_cleanup_loop(self):
        """缓存清理"""
        while self.running:
            try:
                await asyncio.sleep(10)
                
                now = time.time()
                expired = [k for k, v in self.message_cache.items() if now - v > self.message_ttl]
                
                for k in expired:
                    del self.message_cache[k]
                
                if len(self.message_cache) > 1000:
                    sorted_items = sorted(self.message_cache.items(), key=lambda x: x[1])
                    for k, _ in sorted_items[:len(sorted_items)//2]:
                        del self.message_cache[k]
                    logger.warning(f"[连接池] 缓存强制清理: {len(self.message_cache)}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [连接池] 缓存清理异常: {e}")
    
    async def _handle_connection_status(self, status_data: Dict[str, Any]):
        """处理状态事件"""
        try:
            exchange = status_data.get('exchange')
            event = status_data.get('event')
            conn_id = status_data.get('connection_id', '')
            
            conn_type = 'primary' if 'primary' in conn_id else 'backup'
            
            logger.info(f"📡 [连接池] {exchange}.{conn_type} 状态: {event}")
            
            if event in ['connection_closed', 'error']:
                if self.active_connection.get(exchange) == conn_type:
                    await self._switch_to_backup(exchange)
                await self._schedule_reconnect(exchange, conn_type, 3)
            elif event == 'connection_established':
                logger.info(f"✅ [连接池] {exchange}.{conn_type} 已建立")
            elif event == 'connection_failed':
                logger.error(f"❌ [连接池] {exchange}.{conn_type} 失败")
                await self._schedule_reconnect(exchange, conn_type, 5)
                
        except Exception as e:
            logger.error(f"❌ [连接池] 处理状态失败: {e}")
    
    async def on_listen_key_updated(self, exchange: str, listen_key: str):
        """ListenKey更新处理"""
        try:
            logger.info(f"📢 [连接池] 收到{exchange} listenKey更新")
            
            if exchange == 'binance':
                logger.info(f"🔗 [连接池] 5秒后重建币安双连接...")
                
                for conn_type in ['primary', 'backup']:
                    conn = self.connections['binance'].get(conn_type)
                    if conn:
                        await conn.disconnect()
                        self.connections['binance'][conn_type] = None
                
                await asyncio.sleep(5)
                await self._setup_connection('binance', 'primary')
                await asyncio.sleep(2)
                await self._setup_connection('binance', 'backup')
            elif exchange == 'okx':
                logger.info(f"🔗 [连接池] OKX使用API key，无需处理listenKey")
                
        except Exception as e:
            logger.error(f"❌ [连接池] 处理listenKey更新失败: {e}")
    
    async def shutdown(self):
        """关闭连接池"""
        logger.info("🛑 [连接池] 正在关闭...")
        self.running = False
        
        for task in self.reconnect_tasks.values():
            if task:
                task.cancel()
        
        shutdown_tasks = []
        for exchange in ['binance', 'okx']:
            for conn_type in ['primary', 'backup']:
                conn = self.connections[exchange].get(conn_type)
                if conn:
                    shutdown_tasks.append(
                        asyncio.wait_for(conn.disconnect(), timeout=5)
                    )
        
        if shutdown_tasks:
            try:
                await asyncio.gather(*shutdown_tasks, return_exceptions=True)
            except:
                pass
        
        logger.info(
            f"📊 [连接池] 统计: "
            f"接收{self.push_stats['total_received']}, "
            f"去重后{self.push_stats['total_deduplicated']}, "
            f"重复{self.push_stats['total_duplicates']}"
        )
        logger.info("✅ [连接池] 已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取状态"""
        now = datetime.now()
        uptime = (now - self.start_time).total_seconds() if self.start_time else 0
        
        status = {
            'timestamp': now.isoformat(),
            'running': self.running,
            'uptime_seconds': uptime,
            'connections': {},
            'quality_stats': self.quality_stats,
            'push_stats': self.push_stats,
            'dedup_cache_size': len(self.message_cache),
            'active_connection': self.active_connection,
            'alerts': []
        }
        
        for exchange in ['binance', 'okx']:
            status['connections'][exchange] = {}
            
            for conn_type in ['primary', 'backup']:
                conn = self.connections[exchange].get(conn_type)
                
                if conn:
                    conn_info = {
                        'connected': conn.connected,
                        'is_active': self.active_connection.get(exchange) == conn_type,
                        'message_count': conn.message_counter,
                        'last_message': conn.last_message_time.isoformat() if conn.last_message_time else None,
                        'established': conn.connection_established_time.isoformat() if conn.connection_established_time else None
                    }
                    
                    if not conn.connected:
                        status['alerts'].append(f"{exchange}.{conn_type}断开")
                    
                    status['connections'][exchange][conn_type] = conn_info
                else:
                    status['connections'][exchange][conn_type] = {'connected': False, 'status': 'not_initialized'}
                    status['alerts'].append(f"{exchange}.{conn_type}未初始化")
        
        return status
