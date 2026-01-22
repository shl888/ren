"""
私人WebSocket连接实现 - 支持币安和欧意
"""
import asyncio
import json
import logging
import time
import hmac
import hashlib
import base64
from datetime import datetime
from typing import Dict, Any, Optional
import websockets
import ssl
import traceback

logger = logging.getLogger(__name__)

class PrivateWebSocketConnection:
    """私人WebSocket连接基类"""
    
    def __init__(self, exchange: str, connection_id: str,
                 status_callback, data_callback, raw_data_cache):
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
        
        # 任务
        self.receive_task = None
        self.health_check_task = None
        
        logger.debug(f"[私人连接] {connection_id} 初始化")
    
    async def connect(self):
        """建立连接（由子类实现）"""
        raise NotImplementedError
    
    async def disconnect(self):
        """断开连接"""
        try:
            self.connected = False
            self.subscribed = False
            
            if self.health_check_task:
                self.health_check_task.cancel()
            
            if self.ws:
                await self.ws.close()
                self.ws = None
            
            if self.receive_task:
                self.receive_task.cancel()
            
            logger.info(f"[私人连接] {self.connection_id} 已断开")
            
        except Exception as e:
            logger.error(f"[私人连接] 断开连接失败: {e}")
    
    async def _start_health_check(self):
        """启动健康检查（每5秒检查一次）"""
        while self.connected:
            await asyncio.sleep(5)
            
            # 检查是否收到消息
            if self.last_message_time:
                seconds_since_last = (datetime.now() - self.last_message_time).total_seconds()
                if seconds_since_last > 30:  # 30秒没收到消息认为有问题
                    logger.warning(f"[私人连接] {self.connection_id} 30秒未收到消息，可能已断开")
                    await self._report_status('health_check_failed', {
                        'seconds_since_last': seconds_since_last
                    })
                    self.connected = False
                    break
    
    async def _report_status(self, event: str, extra_data: Dict[str, Any] = None):
        """上报状态给大脑"""
        try:
            status = {
                'exchange': self.exchange,
                'connection_id': self.connection_id,
                'event': event,
                'timestamp': datetime.now().isoformat()
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


class BinancePrivateConnection(PrivateWebSocketConnection):
    """币安私人连接"""
    
    def __init__(self, listen_key: str, **kwargs):
        super().__init__('binance', 'binance_private', **kwargs)
        self.listen_key = listen_key
        
        # （币安实盘地址）
        # self.ws_url = f"wss://fstream.binance.com/ws/{listen_key}"
        
        # ✅ 币安测试网地址（官方推荐）
        self.ws_url = f"wss://testnet.binancefuture.com/ws/{listen_key}"
        
        # ⚠️ 备用地址（如果主域名失败，可降级使用）
        # self.ws_url = f"wss://fstream.binancefuture.com/ws/{listen_key}"
        
    async def connect(self):
        """建立币安私人连接"""
        try:
            logger.info(f"[币安私人] 正在连接: {self.ws_url[:50]}...")
            
            # ✅ 创建SSL上下文（测试网证书可能有问题）
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            # ✅ 带重试的连接
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.ws = await asyncio.wait_for(
                        websockets.connect(
                            self.ws_url,
                            ssl=ssl_context,
                            ping_interval=20,    # 每20秒自动ping
                            ping_timeout=10,     # 10秒内收不到pong认为断开
                            close_timeout=5,
                            # ✅ 添加必要的请求头
                            extra_headers={
                                "User-Agent": "Mozilla/5.0 (compatible; Python-Binance-Bot/1.0)"
                            }
                        ),
                        timeout=15  # 连接超时15秒
                    )
                    logger.info(f"[币安私人] 第{attempt + 1}次连接成功")
                    break
                except asyncio.TimeoutError:
                    logger.warning(f"[币安私人] 第{attempt + 1}次连接超时")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)  # 指数退避
                except Exception as e:
                    logger.error(f"[币安私人] 第{attempt + 1}次连接失败: {type(e).__name__}: {e}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)
            
            self.connected = True
            self.last_message_time = datetime.now()
            
            # 启动接收任务
            self.receive_task = asyncio.create_task(self._receive_messages())
            
            # ✅ 禁用应用层健康检查（币安是静默模式）
            logger.debug(f"[币安私人] 跳过应用层健康检查（交易所静默模式）")
            
            await self._report_status('connection_established')
            logger.info(f"[币安私人] 连接建立成功")
            
            return True
            
        except Exception as e:
            logger.error(f"[币安私人] 连接失败: {type(e).__name__}: {e}")
            logger.error(f"[币安私人] 详细错误追踪:\n{traceback.format_exc()}")
            await self._report_status('connection_failed', {
                'error': str(e),
                'error_type': type(e).__name__
            })
            return False
    
    async def _receive_messages(self):
        """接收币安私人消息"""
        try:
            async for message in self.ws:
                self.last_message_time = datetime.now()
                
                try:
                    data = json.loads(message)
                    await self._process_binance_message(data)
                except json.JSONDecodeError:
                    logger.warning(f"[币安私人] 无法解析JSON消息: {message[:100]}")
                except Exception as e:
                    logger.error(f"[币安私人] 处理消息错误: {e}")
                    
        except websockets.ConnectionClosed as e:
            logger.warning(f"[币安私人] 连接关闭: code={e.code}, reason={e.reason}")
            await self._report_status('connection_closed', {
                'code': e.code,
                'reason': e.reason
            })
        except Exception as e:
            logger.error(f"[币安私人] 接收消息错误: {e}")
            await self._report_status('error', {'error': str(e)})
        finally:
            self.connected = False
    
    async def _process_binance_message(self, data: Dict[str, Any]):
        """处理币安私人消息"""
        # 1. 保存原始数据到缓存
        event_type = data.get('e', 'unknown')
        await self._save_raw_data(event_type, data)
        
        # 2. 格式化处理
        formatted = {
            'exchange': 'binance',
            'data_type': self._map_binance_event_type(event_type),
            'timestamp': datetime.now().isoformat(),
            'raw_data': data,
            'standardized': {
                'event_type': event_type,
                'status': 'raw_data_only'
            }
        }
        
        # 3. ✅ 只传递给大脑，不在连接池推送
        try:
            await self.data_callback(formatted)
        except Exception as e:
            logger.error(f"[币安私人] 传递给大脑失败: {e}")
    
    def _map_binance_event_type(self, event_type: str) -> str:
        """映射币安事件类型到标准类型"""
        mapping = {
            'ACCOUNT_UPDATE': 'account_update',        # 账户余额/持仓更新
            'ORDER_TRADE_UPDATE': 'order_update',      # 订单状态更新
            'TRADE_LITE': 'trade_update',              # 2024-09新增：仅成交推送
            'listenKeyExpired': 'system_event',        # listenKey过期
            'MARGIN_CALL': 'risk_event',               # 保证金预警
            'balanceUpdate': 'balance_update',         # 兼容旧版余额更新
            'outboundAccountPosition': 'account_update',  # 兼容旧版账户
            'executionReport': 'order_update'          # 兼容旧版订单
        }
        return mapping.get(event_type, 'unknown')


class OKXPrivateConnection(PrivateWebSocketConnection):
    """欧意私人连接"""
    
    def __init__(self, api_key: str, api_secret: str, passphrase: str = '', **kwargs):
        super().__init__('okx', 'okx_private', **kwargs)
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        # 欧意真实交易地址
        # self.ws_url = "wss://ws.okx.com:8443/ws/v5/private"   
        
        # 欧意模拟交易地址
        self.ws_url = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
        
        # 欧意模拟交易时,需要添加这一行
        self.broker_id = "9999"
        
        self.authenticated = False
    
    async def connect(self):
        """建立欧意私人连接（包含认证）"""
        try:
            logger.info(f"[欧意私人] 正在连接: {self.ws_url}")
            
            self.ws = await websockets.connect(
                self.ws_url,
                ping_interval=3,
                ping_timeout=5,
                close_timeout=5
            )
            
            self.connected = True
            
            # 1. 首先进行认证
            auth_success = await self._authenticate()
            if not auth_success:
                logger.error("[欧意私人] 认证失败")
                await self.disconnect()
                return False
            
            self.authenticated = True
            
            # 2. 订阅频道
            subscribe_success = await self._subscribe_channels()
            if not subscribe_success:
                logger.warning("[欧意私人] 订阅频道失败，但连接已建立")
            
            # 3. 启动任务
            self.receive_task = asyncio.create_task(self._receive_messages())
            self.health_check_task = asyncio.create_task(self._start_health_check())
            
            await self._report_status('connection_established')
            logger.info(f"[欧意私人] 连接建立成功")
            
            return True
            
        except Exception as e:
            logger.error(f"[欧意私人] 连接失败: {e}")
            await self._report_status('connection_failed', {'error': str(e)})
            return False
    
    async def _authenticate(self) -> bool:
        """欧意WebSocket认证"""
        try:
            # ✅ 生成Unix时间戳（秒）
            timestamp = str(int(time.time()))
            
            # ✅ 正确的签名消息：timestamp + "GET" + "/users/self/verify"
            message = timestamp + 'GET' + '/users/self/verify'
            
            # ✅ 生成HMAC-SHA256签名
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
            
            # ✅ Base64编码
            signature_base64 = base64.b64encode(signature).decode('utf-8')
            
            # 构造认证消息
            auth_msg = {
                "op": "login",
                "args": [
                    {
                        "apiKey": self.api_key,
                        "passphrase": self.passphrase,
                        "timestamp": timestamp,  # ✅ 使用Unix时间戳
                        "sign": signature_base64
                    }
                ]
            }
            
            logger.debug(f"[欧意私人] 发送认证请求: timestamp={timestamp}")
            await self.ws.send(json.dumps(auth_msg))
            
            # 等待认证响应
            response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            response_data = json.loads(response)
            
            if response_data.get('event') == 'login' and response_data.get('code') == '0':
                logger.info("[欧意私人] 认证成功")
                return True
            else:
                logger.error(f"[欧意私人] 认证失败: {response_data}")
                return False
                
        except Exception as e:
            logger.error(f"[欧意私人] 认证异常: {e}")
            return False
    
    async def _subscribe_channels(self) -> bool:
        """订阅欧意私人频道"""
        try:
            # （真实交易）订阅账户、订单、持仓频道
            # subscribe_msg = {
            #     "op": "subscribe",
            #     "args": [
            #         {"channel": "account"},
            #         {"channel": "orders", "instType": "SWAP"},
            #         {"channel": "positions", "instType": "SWAP"}
            #     ]
            # }
            
            # （模拟交易）修改为带brokerId的订阅
            subscribe_msg = {
                "op": "subscribe",
                "args": [
                    {"channel": "account", "brokerId": self.broker_id},
                    {"channel": "orders", "instType": "SWAP", "brokerId": self.broker_id},
                    {"channel": "positions", "instType": "SWAP", "brokerId": self.broker_id}
                ]
            }
            
            await self.ws.send(json.dumps(subscribe_msg))
            logger.info("[欧意私人] 已发送订阅请求")
            
            # 这里可以等待订阅响应，但为了简单我们先返回成功
            return True
            
        except Exception as e:
            logger.error(f"[欧意私人] 订阅失败: {e}")
            return False
    
    async def _receive_messages(self):
        """接收欧意私人消息"""
        try:
            async for message in self.ws:
                self.last_message_time = datetime.now()
                
                try:
                    data = json.loads(message)
                    await self._process_okx_message(data)
                except json.JSONDecodeError:
                    logger.warning(f"[欧意私人] 无法解析JSON消息: {message[:100]}")
                except Exception as e:
                    logger.error(f"[欧意私人] 处理消息错误: {e}")
                    
        except websockets.ConnectionClosed as e:
            logger.warning(f"[欧意私人] 连接关闭: code={e.code}, reason={e.reason}")
            await self._report_status('connection_closed', {
                'code': e.code,
                'reason': e.reason
            })
        except Exception as e:
            logger.error(f"[欧意私人] 接收消息错误: {e}")
            await self._report_status('error', {'error': str(e)})
        finally:
            self.connected = False
            self.authenticated = False
    
    async def _process_okx_message(self, data: Dict[str, Any]):
        """处理欧意私人消息"""
        # 1. 检查是否是事件消息（如登录、订阅响应）
        if data.get('event'):
            event = data['event']
            if event == 'login':
                logger.debug(f"[欧意私人] 登录事件: {data.get('code')}")
            elif event == 'subscribe':
                logger.debug(f"[欧意私人] 订阅事件: {data.get('arg')}")
            elif event == 'error':
                logger.error(f"[欧意私人] 错误事件: {data}")
            return
        
        # 2. 保存原始数据
        arg = data.get('arg', {})
        channel = arg.get('channel', 'unknown')
        await self._save_raw_data(channel, data)
        
        # 3. 格式化处理
        formatted = {
            'exchange': 'okx',
            'data_type': self._map_okx_channel_type(channel),
            'timestamp': datetime.now().isoformat(),
            'raw_data': data,
            'standardized': {
                'channel': channel,
                'status': 'raw_data_only'
            }
        }
        
        # 4. ✅ 只传递给大脑，不在连接池推送
        try:
            await self.data_callback(formatted)
        except Exception as e:
            logger.error(f"[欧意私人] 传递给大脑失败: {e}")
    
    def _map_okx_channel_type(self, channel: str) -> str:
        """映射欧意频道到标准类型"""
        mapping = {
            'account': 'account_update',
            'orders': 'order_update',
            'positions': 'position_update',
            'balance_and_position': 'account_position_update'
        }
        return mapping.get(channel, 'unknown')
