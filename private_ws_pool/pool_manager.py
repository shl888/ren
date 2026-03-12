"""
私人WebSocket连接池管理器
币安：双连接（内部主动探测自愈）
欧意：单连接（内部心跳自愈）
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Callable
from collections import deque
import time

from .connection import BinancePrivateConnection, OKXPrivateConnection

logger = logging.getLogger(__name__)

class MessageDeduplicator:
    """消息去重器 - 双连接去重"""
    def __init__(self, window_size=1000):
        self.window_size = window_size
        self.recent_ids = deque(maxlen=window_size)
        self.id_set = set()
    
    def extract_unique_id(self, data: Dict[str, Any]) -> str:
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
            return f"binance:{event_type}:{symbol}:{event_time}"
        
        elif exchange == 'okx':
            arg = msg_data.get('arg', {})
            channel = arg.get('channel', 'unknown')
            data_list = msg_data.get('data', [])
            if data_list:
                first = data_list[0]
                if 'uTime' in first:
                    return f"okx:{channel}:{first.get('uTime')}"
            return f"okx:{channel}:{int(time.time()*1000)}"
        
        return f"{exchange}:{datetime.now().timestamp()}"
    
    def is_duplicate(self, data: Dict[str, Any]) -> bool:
        try:
            msg_id = self.extract_unique_id(data)
            if msg_id in self.id_set:
                return True
            self.recent_ids.append(msg_id)
            self.id_set.add(msg_id)
            if len(self.id_set) > self.window_size * 1.5:
                self.id_set = set(self.recent_ids)
            return False
        except Exception:
            return False


class BinanceConnectionPair:
    """币安连接对 - 双连接热备"""
    def __init__(self, data_callback: Callable):
        self.conn_a = None
        self.conn_b = None
        self.deduplicator = MessageDeduplicator()
        self.total_messages = 0
        self.duplicate_messages = 0
        
    async def process_message(self, data: Dict[str, Any], source: str):
        """处理消息（带去重）"""
        self.total_messages += 1
        if self.deduplicator.is_duplicate(data):
            self.duplicate_messages += 1
            return
        await data_callback(data)


class PrivateWebSocketPool:
    """私人连接池 - 完全自愈版"""
    
    def __init__(self):
        self.binance_pair = None
        self.okx_conn = None
        
        self.running = False
        self.brain_store = None
        self.start_time = None
        
        self.push_stats = {
            'total_created': 0,
            'total_success': 0,
            'total_failed': 0,
        }
        
        logger.info("🔗 [私人连接池] 初始化完成")
        logger.info("   ├─ 币安: 双连接热备 (3秒主动探测, 内部自愈)")
        logger.info("   └─ 欧意: 单连接 (5秒ping-pong心跳, 内部自愈)")
    
    async def start(self, brain_store):
        """启动连接池"""
        logger.info("🚀 [私人连接池] 正在启动...")
        self.brain_store = brain_store
        self.running = True
        self.start_time = datetime.now()
        
        # 直接建立连接，不需要监控循环
        asyncio.create_task(self._connect_all())
        
        logger.info("✅ [私人连接池] 已启动")
        return True
    
    async def _connect_all(self):
        """建立所有连接"""
        # 币安双连接
        logger.info("🔗 [私人连接池] 建立币安双连接...")
        await self._setup_binance()
        
        await asyncio.sleep(3)
        
        # 欧意单连接
        logger.info("🔗 [私人连接池] 建立欧意单连接...")
        await self._setup_okx()
    
    async def _setup_binance(self):
        """建立币安双连接"""
        try:
            listen_key = await self.brain_store.get_listen_key('binance')
            if not listen_key:
                logger.error("❌ [币安] 无法获取listenKey")
                return False
            
            self.binance_pair = BinanceConnectionPair(self._process_and_forward_data)
            
            # 连接A
            conn_a = BinancePrivateConnection(
                listen_key=listen_key,
                status_callback=self._handle_status,
                data_callback=self._create_callback('binance', 'A'),
                raw_data_cache=None
            )
            success_a = await conn_a.connect()
            
            # 连接B
            conn_b = BinancePrivateConnection(
                listen_key=listen_key,
                status_callback=self._handle_status,
                data_callback=self._create_callback('binance', 'B'),
                raw_data_cache=None
            )
            success_b = await conn_b.connect()
            
            self.binance_pair.conn_a = conn_a if success_a else None
            self.binance_pair.conn_b = conn_b if success_b else None
            
            logger.info(f"✅ [币安] 双连接: A={'✓' if success_a else '✗'}, B={'✓' if success_b else '✗'}")
            return True
            
        except Exception as e:
            logger.error(f"❌ [币安] 建立连接失败: {e}")
            return False
    
    async def _setup_okx(self):
        """建立欧意单连接"""
        try:
            api_creds = await self.brain_store.get_api_credentials('okx')
            if not api_creds:
                logger.error("❌ [欧意] 无法获取API凭证")
                return False
            
            self.okx_conn = OKXPrivateConnection(
                api_key=api_creds['api_key'],
                api_secret=api_creds['api_secret'],
                passphrase=api_creds.get('passphrase', ''),
                status_callback=self._handle_status,
                data_callback=self._process_and_forward_data,
                raw_data_cache=None
            )
            
            success = await self.okx_conn.connect()
            logger.info(f"✅ [欧意] 连接: {'✓' if success else '✗'}")
            return success
            
        except Exception as e:
            logger.error(f"❌ [欧意] 建立连接失败: {e}")
            return False
    
    def _create_callback(self, exchange: str, conn_name: str):
        """创建币安连接回调"""
        async def callback(data: Dict[str, Any]):
            if self.binance_pair:
                await self.binance_pair.process_message(data, conn_name)
        return callback
    
    async def _handle_status(self, status_data: Dict[str, Any]):
        """处理状态事件 - 只记录，不干预"""
        exchange = status_data.get('exchange')
        event = status_data.get('event')
        logger.info(f"📡 [状态] {exchange}: {event}")
        
        # 连接自己会重连，我们只需要记录
        
    async def _process_and_forward_data(self, raw_data: Dict[str, Any]):
        """转发数据"""
        try:
            self.push_stats['total_created'] += 1
            asyncio.create_task(self._push_to_manager(raw_data.copy()))
        except Exception as e:
            logger.error(f"❌ 创建推送任务失败: {e}")
            self.push_stats['total_failed'] += 1

    async def _push_to_manager(self, data: Dict[str, Any]):
        """推送数据"""
        try:
            from private_data_processing.manager import receive_private_data
            await receive_private_data(data)
            self.push_stats['total_success'] += 1
        except Exception as e:
            logger.error(f"❌ 推送失败: {e}")
            self.push_stats['total_failed'] += 1
    
    async def shutdown(self):
        """关闭所有连接"""
        logger.info("🛑 [私人连接池] 正在关闭...")
        self.running = False
        
        tasks = []
        if self.binance_pair:
            if self.binance_pair.conn_a:
                tasks.append(self.binance_pair.conn_a.disconnect())
            if self.binance_pair.conn_b:
                tasks.append(self.binance_pair.conn_b.disconnect())
        if self.okx_conn:
            tasks.append(self.okx_conn.disconnect())
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("✅ [私人连接池] 已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取状态"""
        return {
            'binance': {
                'conn_a': self.binance_pair.conn_a.connected if self.binance_pair and self.binance_pair.conn_a else False,
                'conn_b': self.binance_pair.conn_b.connected if self.binance_pair and self.binance_pair.conn_b else False,
                'total_msgs': self.binance_pair.total_messages if self.binance_pair else 0,
                'duplicate_msgs': self.binance_pair.duplicate_messages if self.binance_pair else 0,
            },
            'okx': {
                'connected': self.okx_conn.connected if self.okx_conn else False,
                'authenticated': self.okx_conn.authenticated if self.okx_conn else False,
            },
            'push_stats': self.push_stats
        }