"""
单个WebSocket连接实现 - 支持角色互换
支持自动重连、数据解析、状态管理
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Callable
import websockets
import aiohttp
import time  # 确保导入了time

# 🚨 新增导入 - 合约收集器
try:
    from .symbol_collector import add_symbol_from_websocket
    SYMBOL_COLLECTOR_AVAILABLE = True
except ImportError:
    logger = logging.getLogger(__name__)
    SYMBOL_COLLECTOR_AVAILABLE = False

logger = logging.getLogger(__name__)

# 🚨 新增：明确定义连接类型常量
class ConnectionType:
    MASTER = "master"
    WARM_STANDBY = "warm_standby"
    MONITOR = "monitor"


class WebSocketConnection:
    """单个WebSocket连接 - 支持主备切换"""
    
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
        self.original_type = connection_type
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
        self.keepalive_task = None
        self.receive_task = None
        self.delayed_subscribe_task = None
        
        # 🚨 【关键修复】每个连接独立的计数器
        self.ticker_count = 0          # 币安ticker计数
        self.mark_price_count = 0      # 币安标记价格计数
        self.okx_ticker_count = 0      # OKX ticker计数
        self.okx_funding_count = 0     # OKX资金费率计数
        
        # 连接配置
        self.ping_interval = 15
        self.reconnect_interval = 3
        
        # 频率控制
        self.last_subscribe_time = 0
        self.min_subscribe_interval = 2.0
    
    async def connect(self):
        """建立WebSocket连接 - 修复：避免触发交易所限制"""
        try:
            logger.info(f"[{self.connection_id}] 正在连接 {self.ws_url}")
            
            # 🚨 增强：增加连接超时保护
            self.ws = await asyncio.wait_for(
                websockets.connect(
                    self.ws_url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_interval + 5,
                    close_timeout=1
                ),
                timeout=30  # 30秒超时
            )
            
            self.connected = True
            self.last_message_time = datetime.now()
            self.reconnect_count = 0
            
            logger.info(f"[{self.connection_id}] 连接成功")
            
            # 🚨 【关键修复】只有主连接立即订阅（保持原来逻辑）
            if self.connection_type == ConnectionType.MASTER and self.symbols:
                await self._subscribe()
                self.subscribed = True
                self.is_active = True
                logger.info(f"[{self.connection_id}] 主连接已激活并订阅")
            
            # 🚨 【关键修复】温备连接延迟订阅（避免触发交易所限制）
            elif self.connection_type == ConnectionType.WARM_STANDBY and self.symbols:
                # 根据连接ID决定延迟时间（错开订阅）
                delay_seconds = self._get_delay_for_warm_standby()
                self.delayed_subscribe_task = asyncio.create_task(
                    self._delayed_subscribe(delay_seconds)
                )
                logger.info(f"[{self.connection_id}] 温备连接将在 {delay_seconds} 秒后订阅心跳")
            
            # 监控连接不订阅
            elif self.connection_type == ConnectionType.MONITOR:
                logger.info(f"[{self.connection_id}] 监控连接已就绪（不订阅）")
            
            # 启动接收任务
            self.receive_task = asyncio.create_task(self._receive_messages())
            
            return True
            
        except asyncio.TimeoutError:
            logger.error(f"[{self.connection_id}] 连接超时30秒")
            self.connected = False
            return False
        except Exception as e:
            logger.error(f"[{self.connection_id}] 连接失败: {e}")
            self.connected = False
            return False
    
    def _get_delay_for_warm_standby(self):
        """根据连接ID获取延迟时间，错开订阅"""
        # 从连接ID中提取编号，如 "binance_warm_0" -> 0
        try:
            parts = self.connection_id.split('_')
            if len(parts) >= 3:
                index = int(parts[-1])
                return 10 + (index * 5)  # 第一个10秒，第二个15秒，第三个20秒
        except:
            pass
        return 10  # 默认10秒
    
    async def _delayed_subscribe(self, delay_seconds: int):
        """延迟订阅，避免触发交易所限制"""
        try:
            logger.info(f"[{self.connection_id}] 等待 {delay_seconds} 秒后订阅...")
            await asyncio.sleep(delay_seconds)
            
            if self.connected and not self.subscribed and self.symbols:
                logger.info(f"[{self.connection_id}] 开始延迟订阅")
                await self._subscribe()
                self.subscribed = True
                logger.info(f"[{self.connection_id}] 延迟订阅完成")
            elif not self.connected:
                logger.warning(f"[{self.connection_id}] 连接已断开，取消延迟订阅")
            elif self.subscribed:
                logger.info(f"[{self.connection_id}] 已经订阅，跳过延迟订阅")
                
        except Exception as e:
            logger.error(f"[{self.connection_id}] 延迟订阅失败: {e}")
    
    async def switch_role(self, new_role: str, new_symbols: list = None):
        """切换连接角色"""
        try:
            old_role = self.connection_type
            
            # 温备升级为主连接
            if new_role == ConnectionType.MASTER and old_role == ConnectionType.WARM_STANDBY:
                logger.info(f"[{self.connection_id}] 从温备切换为主连接")
                
                # 取消延迟订阅任务（如果还在等待）
                if self.delayed_subscribe_task:
                    self.delayed_subscribe_task.cancel()
                
                # 如果已经有订阅（心跳），先取消
                if self.connected and self.subscribed:
                    await self._unsubscribe()
                    self.subscribed = False
                
                # 更新合约列表
                if new_symbols:
                    self.symbols = new_symbols
                
                self.is_active = True
                self.connection_type = new_role
                
                # 订阅新合约（主连接的合约）
                if self.connected and self.symbols:
                    await self._subscribe()
                    self.subscribed = True
                
                logger.info(f"[{self.connection_id}] 切换完成，订阅 {len(self.symbols)} 个合约")
                return True
                
            # 主连接降级为温备
            elif new_role == ConnectionType.WARM_STANDBY and old_role == ConnectionType.MASTER:
                logger.info(f"[{self.connection_id}] 从主连接切换为温备")
                
                # 如果已经有订阅（主连接合约），先取消
                if self.connected and self.subscribed:
                    await self._unsubscribe()
                    self.subscribed = False
                
                # 更新为心跳合约
                if new_symbols:
                    self.symbols = new_symbols
                else:
                    # 默认心跳合约
                    if self.exchange == "binance":
                        self.symbols = ["BTCUSDT"]
                    elif self.exchange == "okx":
                        self.symbols = ["BTC-USDT-SWAP"]
                
                self.is_active = False
                self.connection_type = new_role
                
                # 订阅心跳合约
                if self.connected and self.symbols:
                    await self._subscribe()
                    self.subscribed = True
                
                logger.info(f"[{self.connection_id}] 切换完成，订阅 {len(self.symbols)} 个心跳合约")
                return True
            
            # 其他情况
            else:
                self.connection_type = new_role
                logger.info(f"[{self.connection_id}] 角色从 {old_role} 改为 {new_role}")
                return True
                
        except Exception as e:
            logger.error(f"[{self.connection_id}] 角色切换失败: {e}")
            return False
    
    async def _subscribe(self):
        """订阅数据"""
        if not self.symbols:
            logger.warning(f"[{self.connection_id}] 没有合约可订阅")
            return
        
        logger.info(f"[{self.connection_id}] 开始订阅 {len(self.symbols)} 个合约")
        
        if self.exchange == "binance":
            await self._subscribe_binance()
        elif self.exchange == "okx":
            await self._subscribe_okx()
    
    async def _subscribe_binance(self):
        """订阅币安数据"""
        try:
            streams = []
            
            for symbol in self.symbols:
                symbol_lower = symbol.lower()
                streams.append(f"{symbol_lower}@ticker")
                streams.append(f"{symbol_lower}@markPrice")
            
            logger.info(f"[{self.connection_id}] 准备订阅 {len(streams)} 个streams")
            
            batch_size = 50
            for i in range(0, len(streams), batch_size):
                batch = streams[i:i+batch_size]
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": batch,
                    "id": i // batch_size + 1
                }
                
                await self.ws.send(json.dumps(subscribe_msg))
                logger.info(f"[{self.connection_id}] 发送订阅批次 {i//batch_size+1}/{(len(streams)+batch_size-1)//batch_size}")
                
                if i + batch_size < len(streams):
                    await asyncio.sleep(1.5)
            
            self.subscribed = True
            logger.info(f"[{self.connection_id}] 订阅完成，共 {len(self.symbols)} 个合约")
            
        except Exception as e:
            logger.error(f"[{self.connection_id}] 订阅失败: {e}")
    
    async def _subscribe_okx(self):
        """订阅欧意数据"""
        try:
            logger.info(f"[{self.connection_id}] 开始订阅OKX数据，共 {len(self.symbols)} 个合约")
            
            # 检查合约格式
            if self.symbols and not self.symbols[0].endswith('-SWAP'):
                logger.warning(f"[{self.connection_id}] OKX合约格式可能错误，应为 BTC-USDT-SWAP 格式")
            
            # 🚨 【修复】同时订阅 tickers 和 funding-rate 频道
            all_subscriptions = []
            for symbol in self.symbols:
                # 订阅 tickers 频道
                all_subscriptions.append({
                    "channel": "tickers",
                    "instId": symbol
                })
                # 🚨 新增：订阅 funding-rate 频道
                all_subscriptions.append({
                    "channel": "funding-rate",
                    "instId": symbol
                })
            
            logger.info(f"[{self.connection_id}] 准备订阅 {len(all_subscriptions)} 个频道 (包含资金费率)")
            
            # 分批订阅
            batch_size = 50  # 🚨 调整为50，因为每个合约有2个频道
            total_batches = (len(all_subscriptions) + batch_size - 1) // batch_size
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(all_subscriptions))
                batch_args = all_subscriptions[start_idx:end_idx]
                
                subscribe_msg = {
                    "op": "subscribe",
                    "args": batch_args
                }
                
                await self.ws.send(json.dumps(subscribe_msg))
                logger.info(f"[{self.connection_id}] 发送批次 {batch_idx+1}/{total_batches} (包含资金费率)")
                
                if batch_idx < total_batches - 1:
                    await asyncio.sleep(1.5)
            
            self.subscribed = True
            logger.info(f"[{self.connection_id}] 订阅完成，共 {len(self.symbols)} 个合约的资金费率和tickers数据")
            return True
            
        except Exception as e:
            logger.error(f"[{self.connection_id}] 订阅失败: {e}")
            return False
    
    async def _unsubscribe(self):
        """取消订阅"""
        try:
            if not self.symbols:
                return
                
            if self.exchange == "binance":
                streams = []
                for symbol in self.symbols:
                    symbol_lower = symbol.lower()
                    streams.append(f"{symbol_lower}@ticker")
                    streams.append(f"{symbol_lower}@markPrice")
                
                batch_size = 50
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
                batch_size = 10
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
            
            logger.info(f"[{self.connection_id}] 取消订阅 {len(self.symbols)} 个合约")
            
        except Exception as e:
            logger.error(f"[{self.connection_id}] 取消订阅失败: {e}")
    
    async def _receive_messages(self):
        """接收消息"""
        try:
            async for message in self.ws:
                self.last_message_time = datetime.now()
                await self._process_message(message)
                
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"[{self.connection_id}] 连接关闭")
            self.connected = False
            self.subscribed = False
            self.is_active = False
        except Exception as e:
            logger.error(f"[{self.connection_id}] 接收消息错误: {e}")
            self.connected = False
            self.subscribed = False
            self.is_active = False
    
    async def _process_message(self, message):
        """处理接收到的消息"""
        try:
            data = json.loads(message)
            
            if self.exchange == "binance" and "id" in data:
                logger.info(f"[{self.connection_id}] 收到订阅响应 ID={data.get('id')}")
            
            if self.exchange == "binance":
                await self._process_binance_message(data)
            elif self.exchange == "okx":
                await self._process_okx_message(data)
        except json.JSONDecodeError:
            logger.warning(f"[{self.connection_id}] 无法解析JSON消息")
        except Exception as e:
            logger.error(f"[{self.connection_id}] 处理消息错误: {e}")
    
    async def _process_binance_message(self, data):
        """处理币安消息 - 完全保留原始数据，不做任何过滤"""
        # 订阅响应
        if "result" in data or "id" in data:
            return
        
        event_type = data.get("e", "")
        
        if event_type == "24hrTicker":
            symbol = data.get("s", "").upper()
            if not symbol:
                return
            
            # 🚨 【关键修复】使用每个连接独立的计数器
            self.ticker_count += 1
            
            # 🚨 【修改1】币安ticker每10000个打印一次
            if self.ticker_count % 10000 == 0:
                logger.info(f"[{self.connection_id}] 已处理 {self.ticker_count} 个币安ticker数据")
            
            # 🚨 【关键修复】完全保留所有原始数据，不进行过滤
            processed = {
                "exchange": "binance",
                "symbol": symbol,
                "data_type": "ticker",
                "event_type": event_type,
                "raw_data": data,  # 完整的原始数据
                "timestamp": datetime.now().isoformat()
            }
            
            try:
                await self.data_callback(processed)
            except Exception as e:
                logger.error(f"[{self.connection_id}] 数据回调失败: {e}")
        
        elif event_type == "markPriceUpdate":
            symbol = data.get("s", "").upper()
            
            # 🚨 新增：收集币安合约名
            if SYMBOL_COLLECTOR_AVAILABLE:
                try:
                    add_symbol_from_websocket("binance", symbol)
                except Exception as e:
                    logger.debug(f"收集币安合约失败 {symbol}: {e}")
            
            # 🚨 【新增】币安标记价格计数器
            self.mark_price_count += 1
            
            # 🚨 【修改2】币安标记价格每10000个打印一次（和ticker一致）
            if self.mark_price_count % 10000 == 0:
                logger.info(f"[{self.connection_id}] 已处理 {self.mark_price_count} 个币安标记价格数据")
            
            # 🚨 【关键修复】完全保留原始标记价格数据
            processed = {
                "exchange": "binance",
                "symbol": symbol,
                "data_type": "mark_price",
                "event_type": event_type,
                "raw_data": data,  # 完整的原始数据
                "timestamp": datetime.now().isoformat()
            }
            
            try:
                await self.data_callback(processed)
            except Exception as e:
                logger.error(f"[{self.connection_id}] 数据回调失败: {e}")
    
    async def _process_okx_message(self, data):
        """处理欧意消息 - 完全保留原始数据，不做任何过滤"""
        if data.get("event"):
            event_type = data.get("event")
            if event_type == "error":
                logger.error(f"[{self.connection_id}] OKX错误: {data}")
            elif event_type == "subscribe":
                logger.info(f"[{self.connection_id}] OKX订阅成功: {data.get('arg', {})}")
            return
        
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        symbol = arg.get("instId", "")
        
        try:
            if channel == "funding-rate":
                if data.get("data") and len(data["data"]) > 0:
                    funding_data = data["data"][0]
                    processed_symbol = symbol.replace('-USDT-SWAP', 'USDT')
                    
                    # 🚨 新增：收集OKX合约名
                    if SYMBOL_COLLECTOR_AVAILABLE:
                        try:
                            add_symbol_from_websocket("okx", processed_symbol)
                        except Exception as e:
                            logger.debug(f"收集OKX合约失败 {processed_symbol}: {e}")
                    
                    # 🚨 【新增】OKX资金费率计数器
                    self.okx_funding_count += 1
                    
                    # 🚨 【修改3】OKX资金费率每5000个打印一次统计
                    if self.okx_funding_count % 5000 == 0:
                        logger.info(f"[{self.connection_id}] 已处理 {self.okx_funding_count} 个OKX资金费率数据")
                    
                    # 🚨 【关键修复】完全保留原始资金费率数据
                    processed = {
                        "exchange": "okx",
                        "symbol": processed_symbol,
                        "data_type": "funding_rate",
                        "channel": channel,
                        "raw_data": data,  # 完整的原始数据
                        "original_symbol": symbol,
                        "timestamp": datetime.now().isoformat()
                    }
                    try:
                        await self.data_callback(processed)
                    except Exception as e:
                        logger.error(f"[{self.connection_id}] 数据回调失败: {e}")
                    
            elif channel == "tickers":
                if data.get("data") and len(data["data"]) > 0:
                    # 🚨 【关键修复】每个连接独立的计数器
                    self.okx_ticker_count += 1
                    
                    # 🚨 【修改4】OKX ticker每5000个打印一次
                    if self.okx_ticker_count % 5000 == 0:
                        logger.info(f"[{self.connection_id}] 已处理 {self.okx_ticker_count} 个OKX ticker数据")
                    
                    processed_symbol = symbol.replace('-USDT-SWAP', 'USDT')
                    
                    # 🚨 【关键修复】完全保留原始ticker数据
                    processed = {
                        "exchange": "okx",
                        "symbol": processed_symbol,
                        "data_type": "ticker",
                        "channel": channel,
                        "raw_data": data,  # 完整的原始数据
                        "original_symbol": symbol,
                        "timestamp": datetime.now().isoformat()
                    }
                    try:
                        await self.data_callback(processed)
                    except Exception as e:
                        logger.error(f"[{self.connection_id}] 数据回调失败: {e}")
        
        except Exception as e:
            logger.warning(f"[{self.connection_id}] 解析OKX数据失败: {e}")
    
    async def disconnect(self):
        """断开连接"""
        try:
            # 🚨 修复：取消延迟订阅任务
            if self.delayed_subscribe_task:
                self.delayed_subscribe_task.cancel()
                logger.debug(f"[{self.connection_id}] 延迟订阅任务已取消")
            
            # 🚨 修复：关闭WebSocket连接
            if self.ws and self.connected:
                await self.ws.close()
                self.connected = False
                logger.info(f"[{self.connection_id}] WebSocket已关闭")
                
            # 🚨 修复：取消接收任务
            if self.receive_task:
                self.receive_task.cancel()
                logger.debug(f"[{self.connection_id}] 接收任务已取消")
                
            self.subscribed = False
            self.is_active = False
            
            logger.info(f"[{self.connection_id}] 连接已完全断开")
            
        except Exception as e:
            logger.error(f"[{self.connection_id}] 断开连接时发生错误: {e}")
    
    @property
    def last_message_seconds_ago(self) -> float:
        """返回距上次消息过去了多少秒（监控调度专用）"""
        if self.last_message_time:
            return (datetime.now() - self.last_message_time).total_seconds()
        return 999  # 如果从未收到消息，返回999秒表示异常
    
    async def check_health(self) -> Dict[str, Any]:
        """检查连接健康状态"""
        now = datetime.now()
        last_msg_seconds = (now - self.last_message_time).total_seconds() if self.last_message_time else 999
        
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
            "timestamp": now.isoformat()
        }
