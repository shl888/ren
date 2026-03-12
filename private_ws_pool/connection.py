"""
私人WebSocket连接实现 - 99.99%稳定性版
币安：主动探测模式 + TCP保活 | 欧意：纯应用层心跳 + TCP保活
双连接热备架构：主备同时在线，无缝切换，数据去重
"""
import asyncio
import json
import logging
import time
import hmac
import hashlib
import base64
import socket
import ssl
from datetime import datetime
from typing import Dict, Any, Set, Optional
import websockets
from websockets.protocol import State

logger = logging.getLogger(__name__)


class PrivateWebSocketConnection:
    """
    私人WebSocket连接基类 - 单连接实例
    每个交易所运行2个实例（主+备），实现热备冗余
    """
    
    def __init__(self, exchange: str, connection_id: str,
                 status_callback, data_callback, raw_data_cache=None):
        """
        基类初始化
        
        Args:
            exchange: 交易所标识 'binance' 或 'okx'
            connection_id: 连接唯一标识，格式: {exchange}_private_{type}_{timestamp}
            status_callback: 状态回调函数
            data_callback: 数据回调函数
            raw_data_cache: 原始数据缓存（可选）
        """
        self.exchange = exchange
        self.connection_id = connection_id
        self.status_callback = status_callback
        self.data_callback = data_callback
        self.raw_data_cache = raw_data_cache
        
        # 连接状态
        self.ws = None
        self.connected = False
        self.subscribed = False
        self.last_message_time = None
        self.reconnect_count = 0
        
        # 稳定性参数
        self.continuous_failure_count = 0
        self.last_connect_success = None
        self.message_counter = 0
        self.connection_established_time = None
        self.first_message_received = False
        
        # 任务
        self.receive_task = None
        self.health_check_task = None
        self.heartbeat_task = None
        self.probe_task = None
        
        # 重连策略（指数退避）
        self.quick_retry_delays = [1, 2, 4]
        self.slow_retry_delays = [10, 20, 40]
        
        logger.debug(f"[私人连接] {connection_id} 初始化完成")
    
    async def connect(self):
        """建立连接（由子类实现）"""
        raise NotImplementedError
    
    async def disconnect(self):
        """优雅断开连接，清理所有资源"""
        try:
            self.connected = False
            self.subscribed = False
            
            # 取消所有任务（避免资源泄漏）
            tasks = [
                self.health_check_task, 
                self.heartbeat_task, 
                self.receive_task,
                self.probe_task
            ]
            for task in tasks:
                if task:
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass
            
            # 关闭WebSocket
            if self.ws:
                await self.ws.close()
                self.ws = None
            
            logger.info(f"[私人连接] {self.connection_id} 已断开")
            
        except Exception as e:
            logger.error(f"[私人连接] 断开连接失败: {e}")
    
    async def _report_status(self, event: str, extra_data: Dict[str, Any] = None):
        """上报状态给连接池（带连接ID区分主备）"""
        try:
            status = {
                'exchange': self.exchange,
                'connection_id': self.connection_id,
                'event': event,
                'timestamp': datetime.now().isoformat(),
                'continuous_failures': self.continuous_failure_count
            }
            if extra_data:
                status.update(extra_data)
            
            await self.status_callback(status)
            
        except Exception as e:
            logger.error(f"[私人连接] 上报状态失败: {e}")
    
    async def _save_raw_data(self, data_type: str, raw_data: Dict[str, Any]):
        """保存原始数据到缓存"""
        try:
            if self.raw_data_cache:
                await self.raw_data_cache.save(
                    exchange=self.exchange,
                    data_type=data_type,
                    raw_data=raw_data
                )
        except Exception as e:
            logger.error(f"[私人连接] 保存原始数据失败: {e}")
    
    async def _connect_with_retry(self, connect_func, max_quick_retries=3, max_slow_retries=2):
        """
        通用带重试的连接方法
        先快速重试3次（应对瞬时网络抖动），再慢速重试2次（应对持续性故障）
        """
        # 阶段1：快速重试
        for attempt in range(max_quick_retries):
            try:
                logger.info(f"[{self.connection_id}] 快速重试第{attempt + 1}次")
                await connect_func()
                return True
            except Exception as e:
                logger.warning(f"[{self.connection_id}] 快速重试失败: {type(e).__name__}: {str(e)[:50]}")
                if attempt == max_quick_retries - 1:
                    break
                wait_time = self.quick_retry_delays[attempt] if attempt < len(self.quick_retry_delays) else 4
                await asyncio.sleep(wait_time)
        
        # 阶段2：慢速重试
        for attempt in range(max_slow_retries):
            try:
                logger.info(f"[{self.connection_id}] 慢速重试第{attempt + 1}次")
                await connect_func()
                return True
            except Exception as e:
                logger.warning(f"[{self.connection_id}] 慢速重试失败: {type(e).__name__}")
                if attempt == max_slow_retries - 1:
                    break
                wait_time = self.slow_retry_delays[attempt] if attempt < len(self.slow_retry_delays) else 40
                await asyncio.sleep(wait_time)
        
        return False
    
    def _create_tcp_socket(self, use_tcp_keepalive: bool = True) -> socket.socket:
        """
        创建配置TCP保活的socket
        防止网络层静默断开（防火墙/NAT超时导致连接假死）
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        if use_tcp_keepalive:
            # 启用TCP Keepalive（内核层保活）
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            
            # Linux专用：精细控制保活参数
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 30)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                logger.debug(f"[{self.connection_id}] TCP保活已启用: 30s/10s/3次")
            except (AttributeError, OSError) as e:
                logger.warning(f"[{self.connection_id}] 精细TCP保活设置失败: {e}")
        
        return sock


class BinancePrivateConnection(PrivateWebSocketConnection):
    """
    币安私人连接 - 主动探测模式 + TCP保活
    币安私有流特点：
    - 无订阅概念，连接后自动推送所有用户数据
    - 无官方WebSocket心跳，需应用层主动探测
    - 24小时强制断开（服务器策略，不可避免）
    """
    
    def __init__(self, listen_key: str, **kwargs):
        """
        币安连接初始化
        
        Args:
            listen_key: 币安listenKey
            **kwargs: 包含connection_id, status_callback, data_callback等
        """
        # 从kwargs提取或生成connection_id
        connection_id = kwargs.get('connection_id', 'binance_private')
        
        # 调用父类初始化（只传必要参数，避免重复）
        super().__init__(
            exchange='binance',
            connection_id=connection_id,
            status_callback=kwargs.get('status_callback'),
            data_callback=kwargs.get('data_callback'),
            raw_data_cache=kwargs.get('raw_data_cache')
        )
        
        self.listen_key = listen_key
        
        # 主动探测参数（优化版：更快发现死亡）
        self.probe_interval = 15
        self.probe_timeout = 5
        self.max_consecutive_failures = 2
        
        # 探测状态
        self.probe_counter = 0
        self.probe_ids: Set[int] = set()
        self.probe_response_received = True
        self.consecutive_probe_failures = 0
        self.last_probe_sent = None
        self.waiting_for_probe = False
        
        # 服务器配置（主备）
        self.backup_servers = [
            f"wss://fstream.binance.com/ws/{listen_key}",
            f"wss://fstream.binancefuture.com/ws/{listen_key}",
        ]
        self.current_server_index = 0
        
        logger.info(f"[币安私人] {self.connection_id} 初始化（探测间隔{self.probe_interval}秒）")
    
    async def connect(self):
        """建立连接并启动主动探测"""
        try:
            logger.info(f"[币安私人] {self.connection_id} 正在连接...")
            
            self.continuous_failure_count += 1
            success = await self._try_multiple_servers()
            
            if success:
                self.continuous_failure_count = 0
                self.last_connect_success = datetime.now()
                self.connection_established_time = datetime.now()
                self.first_message_received = False
                self.consecutive_probe_failures = 0
                self.probe_ids.clear()
                
                # 启动主动探测任务
                self.probe_task = asyncio.create_task(self._active_probe_loop())
                
                logger.info(f"[币安私人] {self.connection_id} 连接成功")
                return True
            else:
                logger.error(f"[币安私人] {self.connection_id} 所有服务器连接失败")
                return False
                
        except Exception as e:
            logger.error(f"[币安私人] {self.connection_id} 连接异常: {e}")
            await self._report_status('connection_failed', {'error': str(e)})
            return False
    
    async def _try_multiple_servers(self):
        """轮询尝试多个服务器"""
        for server_index, server_url in enumerate(self.backup_servers):
            logger.info(f"[币安私人] {self.connection_id} 尝试服务器 {server_index + 1}/{len(self.backup_servers)}")
            self.current_server_index = server_index
            
            success = await self._connect_with_retry(
                lambda: self._connect_single_server(server_url)
            )
            
            if success:
                return True
            else:
                logger.warning(f"[币安私人] 服务器{server_index + 1}连接失败")
                await asyncio.sleep(2)
        
        return False
    
    async def _connect_single_server(self, url: str):
        """连接到单个服务器"""
        try:
            sock = self._create_tcp_socket(use_tcp_keepalive=True)
            
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            self.ws = await asyncio.wait_for(
                websockets.connect(
                    url,
                    sock=sock,
                    ssl=ssl_context,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=8,
                    max_size=5*1024*1024,
                ),
                timeout=20
            )
            
            self.connected = True
            self.last_message_time = datetime.now()
            self.first_message_received = False
            
            self.receive_task = asyncio.create_task(self._receive_messages())
            
            await self._report_status('connection_established')
            logger.info(f"[币安私人] {self.connection_id} 服务器连接成功")
            
        except Exception as e:
            logger.error(f"[币安私人] 连接服务器失败: {e}")
            raise
    
    async def _active_probe_loop(self):
        """主动探测循环"""
        while self.connected:
            try:
                await asyncio.sleep(self.probe_interval)
                
                if self.waiting_for_probe:
                    self.consecutive_probe_failures += 1
                    logger.warning(
                        f"[币安探测] {self.connection_id} 探测#{self.probe_counter}未响应，"
                        f"连续失败: {self.consecutive_probe_failures}"
                    )
                    
                    if self.consecutive_probe_failures >= self.max_consecutive_failures:
                        logger.error(
                            f"[币安探测] {self.connection_id} 连续{self.consecutive_probe_failures}次探测失败"
                        )
                        self.connected = False
                        break
                else:
                    if self.consecutive_probe_failures > 0:
                        logger.info(f"[币安探测] {self.connection_id} 探测恢复")
                    self.consecutive_probe_failures = 0
                
                self.probe_counter += 1
                probe_id = 99900 + (self.probe_counter % 100)
                
                probe_msg = {
                    "method": "LIST_SUBSCRIPTIONS",
                    "id": probe_id
                }
                
                logger.debug(f"[币安探测] {self.connection_id} 发送探测#{self.probe_counter}")
                self.last_probe_sent = datetime.now()
                self.waiting_for_probe = True
                self.probe_ids.add(probe_id)
                
                await self.ws.send(json.dumps(probe_msg))
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[币安探测] {self.connection_id} 发送失败: {e}")
                self.connected = False
                break
    
    async def _receive_messages(self):
        """接收消息"""
        try:
            async for message in self.ws:
                self.last_message_time = datetime.now()
                self.message_counter += 1
                
                if not self.first_message_received:
                    self.first_message_received = True
                    logger.info(f"[币安私人] {self.connection_id} 收到第一条消息")
                
                try:
                    data = json.loads(message)
                    
                    msg_id = data.get('id')
                    if msg_id and msg_id in self.probe_ids:
                        self.waiting_for_probe = False
                        self.probe_ids.discard(msg_id)
                        logger.debug(f"[币安探测] {self.connection_id} 收到响应")
                        continue
                    
                    await self._process_binance_message(data)
                    
                except json.JSONDecodeError:
                    logger.warning(f"[币安私人] {self.connection_id} 无法解析JSON")
                except Exception as e:
                    logger.error(f"[币安私人] {self.connection_id} 处理消息错误: {e}")
                    
        except websockets.ConnectionClosed as e:
            logger.warning(f"[币安私人] {self.connection_id} 连接关闭: {e.code}")
            await self._report_status('connection_closed', {
                'code': e.code,
                'reason': e.reason
            })
        except Exception as e:
            logger.error(f"[币安私人] {self.connection_id} 接收消息错误: {e}")
            await self._report_status('error', {'error': str(e)})
        finally:
            self.connected = False
            if self.probe_task:
                self.probe_task.cancel()
    
    async def _process_binance_message(self, data: Dict[str, Any]):
        """处理币安消息"""
        try:
            event_type = data.get('e', 'unknown')
            await self._save_raw_data(event_type, data)
            
            formatted_data = {
                'exchange': 'binance',
                'connection_id': self.connection_id,
                'data_type': event_type.lower(),
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            await self.data_callback(formatted_data)
            
        except Exception as e:
            logger.error(f"[币安私人] {self.connection_id} 传递失败: {e}")
    
    async def disconnect(self):
        """断开连接"""
        if self.probe_task:
            self.probe_task.cancel()
            try:
                await self.probe_task
            except (asyncio.CancelledError, Exception):
                pass
        await super().disconnect()


class OKXPrivateConnection(PrivateWebSocketConnection):
    """
    欧意私人连接 - 纯应用层心跳 + TCP保活
    """
    
    def __init__(self, api_key: str, api_secret: str, passphrase: str = '', **kwargs):
        """
        欧意连接初始化
        
        Args:
            api_key: API Key
            api_secret: API Secret
            passphrase: 密码短语
            **kwargs: 包含connection_id, status_callback, data_callback等
        """
        connection_id = kwargs.get('connection_id', 'okx_private')
        
        super().__init__(
            exchange='okx',
            connection_id=connection_id,
            status_callback=kwargs.get('status_callback'),
            data_callback=kwargs.get('data_callback'),
            raw_data_cache=kwargs.get('raw_data_cache')
        )
        
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        
        self.ws_url = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
        self.broker_id = "9999"
        self.backup_url = "wss://ws.okx.com:8443/ws/v5/private"
        
        self.authenticated = False
        self.heartbeat_interval = 25
        self.last_heartbeat_time = None
        self.no_message_threshold = 40
        
        logger.info(f"[欧意私人] {self.connection_id} 初始化（心跳间隔{self.heartbeat_interval}秒）")
    
    async def connect(self):
        """建立欧意连接"""
        try:
            logger.info(f"[欧意私人] {self.connection_id} 正在连接")
            
            self.continuous_failure_count += 1
            success = await self._triple_connect_flow()
            
            if success:
                self.continuous_failure_count = 0
                self.last_connect_success = datetime.now()
                self.connection_established_time = datetime.now()
                self.first_message_received = False
                logger.info(f"[欧意私人] {self.connection_id} 连接建立成功")
                return True
            else:
                logger.error(f"[欧意私人] {self.connection_id} 连接失败")
                return False
                
        except Exception as e:
            logger.error(f"[欧意私人] {self.connection_id} 连接异常: {e}")
            await self._report_status('connection_failed', {'error': str(e)})
            return False
    
    async def _triple_connect_flow(self):
        """三重保障连接流程"""
        connect_success = await self._connect_with_retry(self._connect_websocket)
        if not connect_success:
            return False
        
        auth_success = await self._authenticate_with_fallback()
        if not auth_success:
            await self.disconnect()
            return False
        
        self.authenticated = True
        
        subscribe_success = await self._smart_subscribe()
        if not subscribe_success:
            logger.warning(f"[欧意私人] {self.connection_id} 订阅部分失败")
        
        await self._start_maintenance_tasks()
        
        return True
    
    async def _connect_websocket(self):
        """连接WebSocket"""
        logger.debug(f"[欧意私人] {self.connection_id} 正在连接WebSocket...")
        
        try:
            await self._connect_single(self.ws_url)
        except Exception as e:
            logger.warning(f"[欧意私人] {self.connection_id} 主URL失败: {e}")
            logger.info(f"[欧意私人] {self.connection_id} 尝试备用URL")
            await self._connect_single(self.backup_url)
            logger.info(f"[欧意私人] {self.connection_id} 备用URL连接成功")
    
    async def _connect_single(self, url: str):
        """单个URL连接"""
        sock = self._create_tcp_socket(use_tcp_keepalive=True)
        
        self.ws = await asyncio.wait_for(
            websockets.connect(
                url,
                sock=sock,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                max_size=5*1024*1024,
            ),
            timeout=15
        )
        
        self.connected = True
        logger.info(f"[欧意私人] {self.connection_id} WebSocket连接成功")
    
    async def _authenticate_with_fallback(self):
        """双重认证保障"""
        try:
            if await self._authenticate():
                return True
        except Exception as e:
            logger.warning(f"[欧意私人] {self.connection_id} 主认证失败: {e}")
        
        await asyncio.sleep(1)
        logger.info(f"[欧意私人] {self.connection_id} 尝试备认证方案")
        
        try:
            return await self._authenticate_with_new_timestamp()
        except Exception as e:
            logger.error(f"[欧意私人] {self.connection_id} 备认证失败: {e}")
            return False
    
    async def _authenticate(self) -> bool:
        """使用当前时间戳认证"""
        timestamp = str(int(time.time()))
        return await self._authenticate_with_timestamp(timestamp)
    
    async def _authenticate_with_new_timestamp(self) -> bool:
        """使用新时间戳认证"""
        timestamp = str(int(time.time()) - 1)
        return await self._authenticate_with_timestamp(timestamp)
    
    async def _authenticate_with_timestamp(self, timestamp: str) -> bool:
        """执行认证"""
        try:
            message = timestamp + 'GET' + '/users/self/verify'
            
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
            
            signature_base64 = base64.b64encode(signature).decode('utf-8')
            
            auth_msg = {
                "op": "login",
                "args": [
                    {
                        "apiKey": self.api_key,
                        "passphrase": self.passphrase,
                        "timestamp": timestamp,
                        "sign": signature_base64
                    }
                ]
            }
            
            logger.debug(f"[欧意私人] {self.connection_id} 发送认证请求")
            await self.ws.send(json.dumps(auth_msg))
            
            response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            response_data = json.loads(response)
            
            if response_data.get('event') == 'login' and response_data.get('code') == '0':
                logger.info(f"[欧意私人] {self.connection_id} 认证成功")
                return True
            else:
                logger.error(f"[欧意私人] {self.connection_id} 认证失败: {response_data}")
                return False
                
        except Exception as e:
            logger.error(f"[欧意私人] {self.connection_id} 认证异常: {e}")
            return False
    
    async def _smart_subscribe(self) -> bool:
        """智能分批订阅"""
        try:
            primary_channels = [
                {"channel": "account", "brokerId": self.broker_id}
            ]
            await self.ws.send(json.dumps({
                "op": "subscribe",
                "args": primary_channels
            }))
            logger.info(f"[欧意私人] {self.connection_id} 已订阅账户频道")
            
            await asyncio.sleep(0.5)
            
            secondary_channels = [
                {"channel": "orders", "instType": "SWAP", "brokerId": self.broker_id},
                {"channel": "positions", "instType": "SWAP", "brokerId": self.broker_id}
            ]
            await self.ws.send(json.dumps({
                "op": "subscribe",
                "args": secondary_channels
            }))
            logger.info(f"[欧意私人] {self.connection_id} 已订阅订单/持仓频道")
            
            return True
            
        except Exception as e:
            logger.error(f"[欧意私人] {self.connection_id} 订阅失败: {e}")
            return False
    
    async def _start_maintenance_tasks(self):
        """启动维护任务"""
        self.receive_task = asyncio.create_task(self._receive_messages())
        self.heartbeat_task = asyncio.create_task(self._okx_heartbeat_loop())
        self.health_check_task = asyncio.create_task(self._passive_health_check())
        
        logger.info(f"[欧意私人] {self.connection_id} 维护任务已启动")
    
    async def _okx_heartbeat_loop(self):
        """OKX官方心跳"""
        while self.connected and self.authenticated:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                if self.ws.state != State.OPEN:
                    logger.error(f"[欧意私人] {self.connection_id} WebSocket未打开")
                    self.connected = False
                    break
                
                await self.ws.send(json.dumps({"op": "ping"}))
                self.last_heartbeat_time = datetime.now()
                logger.debug(f"[欧意私人] {self.connection_id} 心跳已发送")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[欧意私人] {self.connection_id} 心跳失败: {e}")
                self.connected = False
                break
    
    async def _passive_health_check(self):
        """被动健康检查"""
        while self.connected and self.authenticated:
            await asyncio.sleep(10)
            
            if self.last_message_time:
                elapsed = (datetime.now() - self.last_message_time).total_seconds()
                if elapsed > self.no_message_threshold:
                    logger.warning(f"[欧意私人] {self.connection_id} {elapsed:.0f}秒未收到消息")
    
    async def _receive_messages(self):
        """接收消息"""
        try:
            async for message in self.ws:
                self.last_message_time = datetime.now()
                self.message_counter += 1
                
                if not self.first_message_received:
                    self.first_message_received = True
                    logger.info(f"[欧意私人] {self.connection_id} 收到第一条消息")
                
                try:
                    data = json.loads(message)
                    await self._process_okx_message(data)
                except json.JSONDecodeError:
                    logger.warning(f"[欧意私人] {self.connection_id} 无法解析JSON")
                except Exception as e:
                    logger.error(f"[欧意私人] {self.connection_id} 处理消息错误: {e}")
                    
        except websockets.ConnectionClosed as e:
            logger.warning(f"[欧意私人] {self.connection_id} 连接关闭: {e.code}")
            await self._report_status('connection_closed', {
                'code': e.code,
                'reason': e.reason
            })
        except Exception as e:
            logger.error(f"[欧意私人] {self.connection_id} 接收消息错误: {e}")
            await self._report_status('error', {'error': str(e)})
        finally:
            self.connected = False
            self.authenticated = False
    
    async def _process_okx_message(self, data: Dict[str, Any]):
        """处理欧意消息"""
        if data.get('event'):
            event = data['event']
            if event == 'login':
                logger.debug(f"[欧意私人] {self.connection_id} 登录事件")
            elif event == 'subscribe':
                logger.debug(f"[欧意私人] {self.connection_id} 订阅事件")
            elif event == 'error':
                logger.error(f"[欧意私人] {self.connection_id} 错误事件: {data}")
            elif event == 'pong':
                logger.debug(f"[欧意私人] {self.connection_id} 收到pong")
            return
        
        arg = data.get('arg', {})
        channel = arg.get('channel', 'unknown')
        await self._save_raw_data(channel, data)
        
        formatted_data = {
            'exchange': 'okx',
            'connection_id': self.connection_id,
            'data_type': self._map_okx_channel_type(channel),
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        try:
            await self.data_callback(formatted_data)
        except Exception as e:
            logger.error(f"[欧意私人] {self.connection_id} 传递失败: {e}")
    
    def _map_okx_channel_type(self, channel: str) -> str:
        """映射频道类型"""
        mapping = {
            'account': 'account_update',
            'orders': 'order_update',
            'positions': 'position_update',
            'balance_and_position': 'account_position_update'
        }
        return mapping.get(channel, 'unknown')
