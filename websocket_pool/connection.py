"""
单个WebSocket连接实现 - 集成心跳策略版
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, Callable, Optional
import websockets

# 导入心跳策略
from .heartbeat_strategy import create_heartbeat_strategy

logger = logging.getLogger(__name__)

class ConnectionType:
    MASTER = "master"
    WARM_STANDBY = "warm_standby"

class WebSocketConnection:
    """单个WebSocket连接 - 集成心跳策略"""
    
    def __init__(
        self,
        exchange: str,
        ws_url: str,
        connection_id: str,
        connection_type: str,
        data_callback: Callable,
        symbols: list = None
    ):
        self.exchange = exchange
        self.ws_url = ws_url
        self.connection_id = connection_id
        self.connection_type = connection_type
        self.data_callback = data_callback
        self.symbols = symbols or []
        
        # 连接状态
        self.ws = None
        self.connected = False
        self.last_message_time = None
        self.reconnect_count = 0
        self.subscribed = False
        self.is_active = False
        
        # 任务
        self.receive_task = None
        self.delayed_subscribe_task = None
        
        # 🎯 新增：心跳策略（币安时为None）
        self.heartbeat_strategy = create_heartbeat_strategy(exchange, self)
        
        # 角色显示
        self.role_display = {
            ConnectionType.MASTER: "主",
            ConnectionType.WARM_STANDBY: "备",
        }
        
        # 基础配置
        self.reconnect_interval = 3
        self.min_subscribe_interval = 2.5
        
        # 日志频率限制器
        self._json_decode_error_count = 0
        self._last_callback_error_log = None
        
        logger.debug(f"WebSocketConnection初始化: {connection_id}")
    
    def log_with_role(self, level: str, message: str):
        """带角色信息的日志"""
        role_char = self.role_display.get(self.connection_type, "?")
        full_name = f"{self.connection_id}({role_char})"
        
        log_method = getattr(logger, level, logger.info)
        log_method(f"[{full_name}] {message}")
    
    async def connect(self):
        """建立WebSocket连接"""
        try:
            self.log_with_role("info", f"🌎【连接池】正在建立连接 {self.ws_url}")
            
            # 重置状态
            self.subscribed = False
            self.is_active = False
            
            # 建立连接（禁用websockets库的自动ping）
            self.ws = await asyncio.wait_for(
                websockets.connect(
                    self.ws_url,
                    ping_interval=None,  # 禁用自动ping
                    ping_timeout=None,   # 禁用自动ping超时
                    close_timeout=5,
                    max_size=10 * 1024 * 1024  # 10MB
                ),
                timeout=30
            )
            
            self.connected = True
            self.last_message_time = datetime.now()
            self.reconnect_count = 0
            
            self.log_with_role("info", "✅【连接池】连接建立成功")
            
            # 🎯 启动心跳策略（如果有的话）
            if self.heartbeat_strategy:
                await self.heartbeat_strategy.start()
            
            # 根据角色处理订阅
            if self.connection_type == ConnectionType.MASTER and self.symbols:
                # 主连接立即订阅
                subscribe_success = await self._subscribe()
                if not subscribe_success:
                    self.log_with_role("error", "❌【连接池】主连接订阅失败")
                    self.connected = False
                    if self.heartbeat_strategy:
                        await self.heartbeat_strategy.stop()
                    return False
                
                self.is_active = True
                self.log_with_role("info", "✅【连接池】主连接已激活并订阅")
            
            elif self.connection_type == ConnectionType.WARM_STANDBY and self.symbols:
                # 温备延迟订阅心跳
                delay_seconds = self._get_delay_for_warm_standby()
                self.delayed_subscribe_task = asyncio.create_task(
                    self._delayed_subscribe(delay_seconds)
                )
                self.log_with_role("info", f"【连接池】将在 {delay_seconds} 秒后订阅心跳")
            
            # 启动接收任务
            self.receive_task = asyncio.create_task(self._receive_messages())
            
            return True
            
        except asyncio.TimeoutError:
            self.log_with_role("error", "⚠️【连接池】连接超时30秒")
            self.connected = False
            return False
        except Exception as e:
            self.log_with_role("error", f"❌【连接池】连接失败: {e}")
            self.connected = False
            return False
    
    def _get_delay_for_warm_standby(self):
        """获取延迟时间（错开订阅）"""
        try:
            parts = self.connection_id.split('_')
            if len(parts) >= 3:
                index = int(parts[-1])
                return 10 + (index * 5)  # 10秒、15秒、20秒
        except:
            pass
        return 10
    
    async def _delayed_subscribe(self, delay_seconds: int):
        """延迟订阅"""
        try:
            self.log_with_role("info", f"🌎【连接池】等待 {delay_seconds} 秒后订阅...")
            await asyncio.sleep(delay_seconds)
            
            if self.connected and not self.subscribed and self.symbols:
                self.log_with_role("info", "🌎【连接池】开始延迟订阅")
                await self._subscribe()
                self.log_with_role("info", "✅【连接池】延迟订阅完成")
            elif not self.connected:
                self.log_with_role("warning", "❌【连接池】连接已断开，取消延迟订阅")
            elif self.subscribed:
                self.log_with_role("info", "✅【连接池】已经订阅，跳过延迟订阅")
                
        except Exception as e:
            self.log_with_role("error", f"❌【连接池】延迟订阅失败: {e}")
    
    async def switch_role(self, new_role: str, new_symbols: list = None):
        """切换连接角色"""
        try:
            old_role_char = self.role_display.get(self.connection_type, "?")
            new_role_char = self.role_display.get(new_role, "?")
            self.log_with_role("info", f"⚠️【触发接管】角色切换: {old_role_char} → {new_role_char}")
            
            # 1. 取消当前订阅
            if self.connected and self.subscribed:
                self.log_with_role("info", "⚠️【触发接管】取消当前订阅")
                await self._unsubscribe()
                self.subscribed = False
                await asyncio.sleep(1)
            
            # 2. 更新角色
            old_role = self.connection_type
            self.connection_type = new_role
            
            # 3. 设置新合约
            if new_symbols is not None:
                self.symbols = new_symbols.copy()
            
            # 4. 根据新角色处理
            if new_role == ConnectionType.MASTER and self.symbols:
                self.log_with_role("info", f"⚠️【触发接管】主连接订阅{len(self.symbols)}个合约")
                success = await self._subscribe()
                if success:
                    self.subscribed = True
                    self.is_active = True
                    self.log_with_role("info", "✅【触发接管】主连接订阅成功")
                    return True
                else:
                    self.log_with_role("error", "❌【触发接管】主连接订阅失败")
                    self.connection_type = old_role
                    return False
            
            elif new_role == ConnectionType.WARM_STANDBY:
                self.is_active = False
                
                if not self.symbols:
                    if self.exchange == "binance":
                        self.symbols = ["BTCUSDT"]
                    elif self.exchange == "okx":
                        self.symbols = ["BTC-USDT-SWAP"]
                
                if self.connected and self.symbols:
                    delay_seconds = self._get_delay_for_warm_standby()
                    self.delayed_subscribe_task = asyncio.create_task(
                        self._delayed_subscribe(delay_seconds)
                    )
                    self.log_with_role("info", f"【触发接管】将在{delay_seconds}秒后订阅心跳")
                
                return True
            
            return True
                
        except Exception as e:
            self.log_with_role("error", f"❌【触发接管】角色切换失败: {e}")
            return False
    
    async def _subscribe(self):
        """订阅数据"""
        if not self.symbols:
            self.log_with_role("warning", "❌【连接池】没有合约可订阅")
            return False
        
        self.log_with_role("info", f"🌎【连接池】开始订阅 {len(self.symbols)} 个合约")
        
        if self.exchange == "binance":
            return await self._subscribe_binance()
        elif self.exchange == "okx":
            return await self._subscribe_okx()
        
        return False
    
    async def _subscribe_binance(self):
        """订阅币安数据"""
        try:
            streams = []
            
            for symbol in self.symbols:
                symbol_lower = symbol.lower()
                streams.append(f"{symbol_lower}@ticker")
                streams.append(f"{symbol_lower}@markPrice")
            
            batch_size = 50
            for i in range(0, len(streams), batch_size):
                batch = streams[i:i+batch_size]
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": batch,
                    "id": i // batch_size + 1
                }
                
                await self.ws.send(json.dumps(subscribe_msg))
                
                if i + batch_size < len(streams):
                    await asyncio.sleep(1.5)
            
            self.subscribed = True
            self.log_with_role("info", f"✅【连接池】币安订阅完成，共 {len(self.symbols)} 个合约")
            return True
            
        except Exception as e:
            self.log_with_role("error", f"❌【连接池】币安订阅失败: {e}")
            return False
    
    async def _subscribe_okx(self):
        """订阅欧意数据"""
        try:
            all_subscriptions = []
            for symbol in self.symbols:
                all_subscriptions.append({"channel": "tickers", "instId": symbol})
                all_subscriptions.append({"channel": "funding-rate", "instId": symbol})
            
            batch_size = 100
            
            for batch_idx in range(0, len(all_subscriptions), batch_size):
                batch = all_subscriptions[batch_idx:batch_idx+batch_size]
                subscribe_msg = {"op": "subscribe", "args": batch}
                
                await self.ws.send(json.dumps(subscribe_msg))
                
                if batch_idx + batch_size < len(all_subscriptions):
                    await asyncio.sleep(1.0)
            
            await asyncio.sleep(2)
            
            self.subscribed = True
            self.log_with_role("info", f"✅ 【连接池】OKX订阅成功！频道数:{len(all_subscriptions)}")
            return True
            
        except Exception as e:
            self.log_with_role("error", f"❌【连接池】OKX订阅失败: {e}")
            return False
    
    async def _unsubscribe(self):
        """取消订阅"""
        try:
            if not self.symbols:
                return
                
            self.log_with_role("info", f"✅【连接池】取消订阅 {len(self.symbols)} 个合约")
            
            if self.exchange == "binance":
                streams = []
                for symbol in self.symbols:
                    symbol_lower = symbol.lower()
                    streams.append(f"{symbol_lower}@ticker")
                    streams.append(f"{symbol_lower}@markPrice")
                
                batch_size = 100
                for i in range(0, len(streams), batch_size):
                    batch = streams[i:i+batch_size]
                    unsubscribe_msg = {
                        "method": "UNSUBSCRIBE",
                        "params": batch,
                        "id": 1
                    }
                    await self.ws.send(json.dumps(unsubscribe_msg))
                    await asyncio.sleep(1)
                
            elif self.exchange == "okx":
                batch_size = 100
                for i in range(0, len(self.symbols), batch_size):
                    batch = self.symbols[i:i+batch_size]
                    args = []
                    for symbol in batch:
                        args.append({"channel": "tickers", "instId": symbol})
                    
                    unsubscribe_msg = {
                        "op": "unsubscribe",
                        "args": args
                    }
                    await self.ws.send(json.dumps(unsubscribe_msg))
                    await asyncio.sleep(2)
            
        except Exception as e:
            self.log_with_role("error", f"❌【连接池】取消订阅失败: {e}")
    
    async def _receive_messages(self):
        """接收消息"""
        try:
            async for message in self.ws:
                # 更新时间戳
                self.last_message_time = datetime.now()
                
                if not message:
                    continue
                
                # 🎯 先让心跳策略处理消息（如果有策略且策略处理了消息）
                heartbeat_handled = False
                if self.heartbeat_strategy:
                    heartbeat_handled = await self.heartbeat_strategy.on_message_received(message)
                
                # 如果不是心跳消息，处理业务数据
                if not heartbeat_handled:
                    asyncio.create_task(self._process_message(message))
                
        except websockets.exceptions.ConnectionClosed as e:
            self.log_with_role("error", f"❌【连接池】连接关闭 - 代码: {e.code}, 原因: {e.reason}")
            await self._handle_disconnect()
            
        except Exception as e:
            self.log_with_role("error", f"接收消息错误: {e}")
            await self._handle_disconnect()
        
        finally:
            await self._handle_disconnect()
    
    async def _handle_disconnect(self):
        """处理断开连接"""
        self.connected = False
        self.subscribed = False
        self.is_active = False
        if self.heartbeat_strategy:
            await self.heartbeat_strategy.stop()
    
    async def _process_message(self, message):
        """处理业务消息"""
        try:
            data = json.loads(message)
            
            if self.exchange == "binance":
                await self._process_binance_message(data)
            elif self.exchange == "okx":
                await self._process_okx_message(data)
                
        except json.JSONDecodeError:
            self._json_decode_error_count += 1
            if self._json_decode_error_count <= 3 or self._json_decode_error_count % 10 == 0:
                self.log_with_role("warning", 
                    f"❌【连接池】无法解析JSON消息(第{self._json_decode_error_count}次)")
        except Exception as e:
            self.log_with_role("error", f"❌【连接池】处理消息错误: {e}")
    
    async def _process_binance_message(self, data):
        """处理币安消息"""
        if "result" in data or "id" in data:
            return
        
        event_type = data.get("e", "")
        
        if event_type == "24hrTicker":
            symbol = data.get("s", "").upper()
            if not symbol:
                return
            
            processed = {
                "exchange": "binance",
                "symbol": symbol,
                "data_type": "ticker",
                "event_type": event_type,
                "raw_data": data,
                "timestamp": datetime.now().isoformat()
            }
            
            try:
                await self.data_callback(processed)
            except Exception as e:
                current_time = datetime.now()
                if (self._last_callback_error_log is None or 
                    (current_time - self._last_callback_error_log).total_seconds() > 30):
                    self.log_with_role("warning", f"❌【连接池】数据回调失败: {e}")
                    self._last_callback_error_log = current_time
        
        elif event_type == "markPriceUpdate":
            symbol = data.get("s", "").upper()
            
            processed = {
                "exchange": "binance",
                "symbol": symbol,
                "data_type": "mark_price",
                "event_type": event_type,
                "raw_data": data,
                "timestamp": datetime.now().isoformat()
            }
            
            try:
                await self.data_callback(processed)
            except Exception as e:
                current_time = datetime.now()
                if (self._last_callback_error_log is None or 
                    (current_time - self._last_callback_error_log).total_seconds() > 30):
                    self.log_with_role("warning", f"❌【连接池】数据回调失败: {e}")
                    self._last_callback_error_log = current_time
    
    async def _process_okx_message(self, data):
        """处理欧意消息"""
        if data.get("event"):
            event_type = data.get("event")
            
            if event_type == "error":
                self.log_with_role("critical", f"🔥 ❌【连接池】OKX错误: {json.dumps(data)}")
                if "too many requests" in str(data).lower():
                    self.connected = False
                    return
            
            elif event_type == "subscribe":
                return
            
            return
        
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        symbol = arg.get("instId", "")
        
        try:
            if channel == "funding-rate":
                if data.get("data") and len(data["data"]) > 0:
                    processed_symbol = symbol.replace('-USDT-SWAP', 'USDT')
                    
                    processed = {
                        "exchange": "okx",
                        "symbol": processed_symbol,
                        "data_type": "funding_rate",
                        "channel": channel,
                        "raw_data": data,
                        "original_symbol": symbol,
                        "timestamp": datetime.now().isoformat()
                    }
                    await self.data_callback(processed)
                    
            elif channel == "tickers":
                if data.get("data") and len(data["data"]) > 0:
                    processed_symbol = symbol.replace('-USDT-SWAP', 'USDT')
                    
                    processed = {
                        "exchange": "okx",
                        "symbol": processed_symbol,
                        "data_type": "ticker",
                        "channel": channel,
                        "raw_data": data,
                        "original_symbol": symbol,
                        "timestamp": datetime.now().isoformat()
                    }
                    await self.data_callback(processed)
        
        except Exception as e:
            current_time = datetime.now()
            if (self._last_callback_error_log is None or 
                (current_time - self._last_callback_error_log).total_seconds() > 10):
                self.log_with_role("warning", f"❌【连接池】解析OKX数据失败: {e}")
                self._last_callback_error_log = current_time
    
    async def disconnect(self):
        """正常断开连接"""
        try:
            self.log_with_role("info", "正在断开连接...")
            
            if self.delayed_subscribe_task:
                self.delayed_subscribe_task.cancel()
            
            # 停止心跳策略（如果有的话）
            if self.heartbeat_strategy:
                await self.heartbeat_strategy.stop()
            
            if self.ws and self.connected:
                await self.ws.close()
                self.connected = False
            
            if self.receive_task:
                self.receive_task.cancel()
                
            self.subscribed = False
            self.is_active = False
            
            self.log_with_role("info", "✅ 连接已断开")
            
        except Exception as e:
            self.log_with_role("error", f"❌ 断开连接错误: {e}")
    
    async def _emergency_disconnect(self, reason: str):
        """紧急断开连接"""
        try:
            self.log_with_role("critical", f"🔥 执行紧急断开: {reason}")
            
            old_connected = self.connected
            self.connected = False
            self.subscribed = False
            self.is_active = False
            
            # 停止心跳策略（如果有的话）
            if self.heartbeat_strategy:
                await self.heartbeat_strategy.stop()
            
            if self.delayed_subscribe_task:
                self.delayed_subscribe_task.cancel()
                
            if self.ws and old_connected:
                try:
                    await asyncio.wait_for(self.ws.close(), timeout=3)
                except:
                    pass
                    
            if self.receive_task:
                self.receive_task.cancel()
                
            self.log_with_role("info", "✅ 紧急断开完成")
            
        except Exception as e:
            self.log_with_role("error", f"紧急断开异常: {e}")
    
    @property
    def last_message_seconds_ago(self) -> float:
        """距上次消息的时间"""
        if self.last_message_time:
            return (datetime.now() - self.last_message_time).total_seconds()
        return 999
    
    async def check_health(self) -> Dict[str, Any]:
        """检查健康状态"""
        now = datetime.now()
        last_msg_seconds = (now - self.last_message_time).total_seconds() if self.last_message_time else 999
        
        # 获取心跳策略状态（如果有的话）
        heartbeat_status = {}
        if self.heartbeat_strategy:
            heartbeat_status = self.heartbeat_strategy.get_status()
        else:
            heartbeat_status = {
                "strategy": "None",
                "note": f"{self.exchange}心跳由websockets库协议层自动处理",
                "timestamp": now.isoformat()
            }
        
        return {
            "connection_id": self.connection_id,
            "exchange": self.exchange,
            "type": self.connection_type,
            "connected": self.connected,
            "subscribed": self.subscribed,
            "is_active": self.is_active,
            "symbols_count": len(self.symbols),
            "last_message_seconds_ago": last_msg_seconds,
            "reconnect_count": self.reconnect_count,
            "heartbeat": heartbeat_status,
            "timestamp": now.isoformat()
        }
        