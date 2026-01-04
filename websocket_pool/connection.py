"""
单个WebSocket连接实现 - 支持角色互换
支持自动重连、数据解析、状态管理 - 修复心跳&阻塞BUG
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Callable
import websockets
import aiohttp
import time

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
        self.okx_ticker_count = 0      # OKX ticker计数
        
        # 🚨【新增】性能监控属性（不阻塞原有逻辑）
        self.message_count = 0  # 短期消息计数（用于计算速率）
        self.last_rate_check_time = time.time()  # 上次计算速率的时间
        self.message_rate = 0.0  # 当前消息速率（消息/秒）
        self.connection_start_time = time.time()  # 连接开始时间
        self.total_messages_received = 0  # 总消息接收数
        
        # 🚨【新增】角色显示映射
        self.role_display = {
            ConnectionType.MASTER: "主",
            ConnectionType.WARM_STANDBY: "温",
            ConnectionType.MONITOR: "监"
        }
        
        # 连接配置
        # 🚨【致命修复】OKX必须3秒心跳，否则5秒就被服务器踢
        if exchange == "okx":
            self.ping_interval = 3   # ← 改成3秒！必须小于5秒
        else:
            self.ping_interval = 10  # 币安可以10秒
        
        self.reconnect_interval = 3
        self.min_subscribe_interval = 2.0
    
    def log_with_role(self, level: str, message: str):
        """🚨【新增】带角色信息的日志"""
        role_char = self.role_display.get(self.connection_type, "?")
        full_name = f"{self.connection_id}({role_char})"
        
        if level == "info":
            logger.info(f"[{full_name}] {message}")
        elif level == "warning":
            logger.warning(f"[{full_name}] {message}")
        elif level == "error":
            logger.error(f"[{full_name}] {message}")
        elif level == "critical":
            logger.critical(f"[{full_name}] {message}")
        elif level == "debug":
            logger.debug(f"[{full_name}] {message}")
        else:
            logger.info(f"[{full_name}] {message}")
    
    async def connect(self):
        """建立WebSocket连接 - 修复：避免触发交易所限制"""
        try:
            self.log_with_role("info", f"正在连接 {self.ws_url}")
            
            # 🚨【关键】重置订阅状态
            self.subscribed = False
            self.is_active = False
            
            # 🚨【关键】重置性能监控状态（不要重置连接时间，因为这是实例的生命周期）
            self.total_messages_received = 0
            self.message_count = 0
            self.message_rate = 0.0
            self.last_rate_check_time = time.time()
            
            # 🚨 增强：增加连接超时保护
            self.ws = await asyncio.wait_for(
                websockets.connect(
                    self.ws_url,
                    ping_interval=None,  # 🚨 禁用库自带ping，用自己的保活任务
                    ping_timeout=None,
                    close_timeout=1
                ),
                timeout=30  # 30秒超时
            )
            
            self.connected = True
            self.last_message_time = datetime.now()
            self.reconnect_count = 0
            
            self.log_with_role("info", "连接成功")
            
            # 🚨【关键】启动持续保活任务（一直运行，不取消）
            self.keepalive_task = asyncio.create_task(self._periodic_ping())
            
            # 🚨【关键修复】只有主连接立即订阅（保持原来逻辑）
            if self.connection_type == ConnectionType.MASTER and self.symbols:
                subscribe_success = await self._subscribe()
                if not subscribe_success:
                    self.log_with_role("error", "主连接订阅失败，标记为未就绪")
                    self.connected = False
                    return False
                
                self.is_active = True
                self.log_with_role("info", "主连接已激活并订阅")
            
            # 🚨【关键修复】温备连接延迟订阅（避免触发交易所限制）
            elif self.connection_type == ConnectionType.WARM_STANDBY and self.symbols:
                # 根据连接ID决定延迟时间（错开订阅）
                delay_seconds = self._get_delay_for_warm_standby()
                self.delayed_subscribe_task = asyncio.create_task(
                    self._delayed_subscribe(delay_seconds)
                )
                self.log_with_role("info", f"将在 {delay_seconds} 秒后订阅心跳")
            
            # 监控连接不订阅
            elif self.connection_type == ConnectionType.MONITOR:
                self.log_with_role("info", "监控连接已就绪（不订阅）")
            
            # 启动接收任务
            self.receive_task = asyncio.create_task(self._receive_messages())
            
            return True
            
        except asyncio.TimeoutError:
            self.log_with_role("error", "连接超时30秒")
            self.connected = False
            self.subscribed = False
            return False
        except Exception as e:
            self.log_with_role("error", f"连接失败: {e}")
            self.connected = False
            self.subscribed = False
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
            self.log_with_role("info", f"等待 {delay_seconds} 秒后订阅...")
            await asyncio.sleep(delay_seconds)
            
            if self.connected and not self.subscribed and self.symbols:
                self.log_with_role("info", "开始延迟订阅")
                await self._subscribe()
                self.subscribed = True
                self.log_with_role("info", "延迟订阅完成")
            elif not self.connected:
                self.log_with_role("warning", "连接已断开，取消延迟订阅")
            elif self.subscribed:
                self.log_with_role("info", "已经订阅，跳过延迟订阅")
                
        except Exception as e:
            self.log_with_role("error", f"延迟订阅失败: {e}")
    
    async def switch_role(self, new_role: str, new_symbols: list = None):
        """切换连接角色 - 🚨【简化版】"""
        try:
            old_role_char = self.role_display.get(self.connection_type, "?")
            new_role_char = self.role_display.get(new_role, "?")
            self.log_with_role("info", f"角色切换: {old_role_char} → {new_role_char}")
            
            # 取消当前订阅（如果有）
            if self.connected and self.subscribed:
                self.log_with_role("info", "取消当前订阅")
                await self._unsubscribe()
                self.subscribed = False
                await asyncio.sleep(1)  # 给交易所处理时间
            
            # 更新角色和合约
            old_role = self.connection_type
            self.connection_type = new_role
            
            if new_symbols:
                self.symbols = new_symbols
            
            # 主连接立即订阅
            if new_role == ConnectionType.MASTER and self.symbols:
                self.log_with_role("info", f"主连接订阅{len(self.symbols)}个合约")
                success = await self._subscribe()
                if success:
                    self.subscribed = True
                    self.is_active = True
                    self.log_with_role("info", "主连接订阅成功")
                    return True
                else:
                    self.log_with_role("error", "主连接订阅失败")
                    # 订阅失败，角色回退
                    self.connection_type = old_role
                    return False
            
            # 温备延迟订阅心跳
            elif new_role == ConnectionType.WARM_STANDBY:
                self.is_active = False
                
                # 如果没有心跳合约，设置默认心跳
                if not self.symbols:
                    if self.exchange == "binance":
                        self.symbols = ["BTCUSDT"]
                    elif self.exchange == "okx":
                        self.symbols = ["BTC-USDT-SWAP"]
                
                # 延迟订阅心跳
                if self.connected and self.symbols:
                    delay_seconds = self._get_delay_for_warm_standby()
                    self.delayed_subscribe_task = asyncio.create_task(
                        self._delayed_subscribe(delay_seconds)
                    )
                    self.log_with_role("info", f"将在{delay_seconds}秒后订阅心跳")
                
                return True
            
            return True
                
        except Exception as e:
            self.log_with_role("error", f"角色切换失败: {e}")
            return False
    
    async def _subscribe(self):
        """订阅数据 - 修复返回值BUG"""
        if not self.symbols:
            self.log_with_role("warning", "没有合约可订阅")
            return False
        
        self.log_with_role("info", f"开始订阅 {len(self.symbols)} 个合约")
        
        if self.exchange == "binance":
            return await self._subscribe_binance()
        elif self.exchange == "okx":
            return await self._subscribe_okx()
        
        return False
    
    async def _subscribe_binance(self):
        """订阅币安数据 - 添加详细失败日志"""
        try:
            streams = []
            
            for symbol in self.symbols:
                symbol_lower = symbol.lower()
                streams.append(f"{symbol_lower}@ticker")
                streams.append(f"{symbol_lower}@markPrice")
            
            self.log_with_role("info", f"准备订阅 {len(streams)} 个streams")
            
            batch_size = 50
            for i in range(0, len(streams), batch_size):
                batch = streams[i:i+batch_size]
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": batch,
                    "id": i // batch_size + 1
                }
                
                await self.ws.send(json.dumps(subscribe_msg))
                self.log_with_role("info", f"发送订阅批次 {i//batch_size+1}/{(len(streams)+batch_size-1)//batch_size}")
                
                if i + batch_size < len(streams):
                    await asyncio.sleep(1.5)
            
            self.subscribed = True
            self.log_with_role("info", f"订阅完成，共 {len(self.symbols)} 个合约")
            return True
            
        except Exception as e:
            self.log_with_role("error", f"币安订阅失败: {e}")
            logger.error(f"失败详情: 交易所=binance, 合约数={len(self.symbols)}, 错误类型={type(e).__name__}")
            return False
    
    async def _subscribe_okx(self):
        """订阅欧意数据 - 大批次+详细失败日志"""
        try:
            self.log_with_role("info", f"开始订阅OKX数据，共 {len(self.symbols)} 个合约")
            
            # 检查合约格式
            if self.symbols and not self.symbols[0].endswith('-SWAP'):
                self.log_with_role("warning", "合约格式可能错误，应为 BTC-USDT-SWAP 格式")
            
            all_subscriptions = []
            for symbol in self.symbols:
                all_subscriptions.append({"channel": "tickers", "instId": symbol})
                all_subscriptions.append({"channel": "funding-rate", "instId": symbol})
            
            batch_size = 100
            inter_batch_delay = 1.0
            
            total_batches = (len(all_subscriptions) + batch_size - 1) // batch_size
            
            for batch_idx in range(total_batches):
                # 发送前检查连接健康
                if not self.connected:
                    self.log_with_role("error", f"连接在订阅过程中丢失，批次{batch_idx+1}/{total_batches}取消")
                    return False
                
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(all_subscriptions))
                batch_args = all_subscriptions[start_idx:end_idx]
                
                subscribe_msg = {"op": "subscribe", "args": batch_args}
                
                # 发送并确认
                try:
                    await asyncio.wait_for(self.ws.send(json.dumps(subscribe_msg)), timeout=10)
                except asyncio.TimeoutError:
                    self.log_with_role("error", "发送订阅批次超时")
                    return False
                
                self.log_with_role("info", f"发送批次 {batch_idx+1}/{total_batches} ({len(batch_args)}个频道)")
                
                if batch_idx < total_batches - 1:
                    await asyncio.sleep(inter_batch_delay)
            
            self.log_with_role("info", "所有批次发送完成，等待2秒确认...")
            await asyncio.sleep(2)
            
            if not self.connected:
                self.log_with_role("error", "订阅确认期间连接断开")
                return False
            
            self.subscribed = True
            self.log_with_role("info", f"✅ OKX订阅成功！频道数:{len(all_subscriptions)}")
            return True
            
        except Exception as e:
            self.log_with_role("error", f"OKX订阅失败: {e}")
            logger.error(f"失败详情: 交易所=okx, 合约数={len(self.symbols)}, 错误类型={type(e).__name__}")
            
            # 如果是限流错误，特别记录
            if "too many requests" in str(e).lower():
                logger.critical(f"[{self.connection_id}] 🔥 触发交易所限流！")
            
            return False
    
    async def _periodic_ping(self):
        """🚨【新增】持续ping保活任务，一直运行直到断开"""
        while self.connected:
            try:
                await asyncio.sleep(self.ping_interval)
                if self.ws and self.connected:
                    await self.ws.ping()
                    self.log_with_role("debug", "ping保活")
            except asyncio.CancelledError:
                self.log_with_role("debug", "保活任务取消")
                break
            except Exception as e:
                self.log_with_role("error", f"ping失败: {e}")
                self.connected = False
                break
    
    async def _unsubscribe(self):
        """取消订阅"""
        try:
            if not self.symbols:
                return
                
            self.log_with_role("info", f"取消订阅 {len(self.symbols)} 个合约")
            
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
            self.log_with_role("error", f"取消订阅失败: {e}")
    
    async def _receive_messages(self):
        """🚨【关键修复】接收消息 - 立即更新时间戳"""
        try:
            async for message in self.ws:
                # 🚨 收到消息立即更新时间戳，防止监控误判
                self.last_message_time = datetime.now()
                
                # 🚨【关键】异步处理性能监控，不阻塞消息处理
                asyncio.create_task(self._update_performance_stats())
                
                # 🚨【新增】检查消息是否为空（可能为心跳）
                if not message:
                    self.log_with_role("debug", "收到空消息（心跳）")
                    continue
                
                # 🚨【关键修复】丢到后台处理，不阻塞接收循环
                asyncio.create_task(self._process_message(message))
                
        except websockets.exceptions.ConnectionClosed as e:
            self.log_with_role("error", f"连接关闭 - 代码: {e.code}, 原因: {e.reason}")
            self.connected = False
            self.subscribed = False
            self.is_active = False
            
            # 🚨【关键】如果是OKX，记录错误码
            if self.exchange == "okx" and e.code == 1006:
                logger.critical(f"[{self.connection_id}] 🔥 OKX强制断开连接，可能触发限流")
            
        except Exception as e:
            self.log_with_role("error", f"接收消息错误: {e}")
            self.connected = False
            self.subscribed = False
            self.is_active = False
        
        finally:
            # 🚨【关键】确保连接状态被清理
            self.log_with_role("warning", "接收任务退出，连接状态重置")
            self.connected = False
            self.subscribed = False
            self.is_active = False
    
    async def _update_performance_stats(self):
        """🚨【新增】异步更新性能统计，不阻塞主流程"""
        try:
            self.message_count += 1
            self.total_messages_received += 1
            
            current_time = time.time()
            time_diff = current_time - self.last_rate_check_time
            
            # 每10秒计算一次消息速率
            if time_diff >= 10:
                self.message_rate = self.message_count / time_diff
                self.message_count = 0
                self.last_rate_check_time = current_time
                
                # 如果主连接消息速率过低，记录警告
                if (self.connection_type == ConnectionType.MASTER and 
                    self.message_rate < 1.0 and 
                    self.subscribed and 
                    self.symbols):
                    self.log_with_role("warning", f"消息速率过低: {self.message_rate:.2f} 消息/秒")
                    
        except Exception as e:
            # 性能监控不应该影响正常流程
            self.log_with_role("debug", f"性能统计更新失败: {e}")
    
    async def _process_message(self, message):
        """处理接收到的消息 - 🚨【恢复原有逻辑，不添加性能监控】"""
        try:
            data = json.loads(message)
            
            if self.exchange == "binance" and "id" in data:
                self.log_with_role("info", f"收到订阅响应 ID={data.get('id')}")
            
            if self.exchange == "binance":
                await self._process_binance_message(data)
            elif self.exchange == "okx":
                await self._process_okx_message(data)
                
        except json.JSONDecodeError:
            self.log_with_role("warning", "无法解析JSON消息")
        except Exception as e:
            self.log_with_role("error", f"处理消息错误: {e}")
    
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
            
            self.ticker_count += 1
            
            if self.ticker_count % 1000 == 0:
                self.log_with_role("info", f"已收到 {self.ticker_count} 个ticker消息")
            
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
                self.log_with_role("error", f"数据回调失败: {e}")
        
        elif event_type == "markPriceUpdate":
            symbol = data.get("s", "").upper()
            
            if not hasattr(self, 'binance_markprice_count'):
                self.binance_markprice_count = 0
                self._binance_markprice_next_milestone = 1000
            
            self.binance_markprice_count += 1
            
            if self.binance_markprice_count >= self._binance_markprice_next_milestone:
                self.log_with_role("info", f"已收到 {self.binance_markprice_count} 个标记价格数据")
                self._binance_markprice_next_milestone = ((self.binance_markprice_count // 1000) + 1) * 1000
            
            if SYMBOL_COLLECTOR_AVAILABLE:
                try:
                    add_symbol_from_websocket("binance", symbol)
                except Exception as e:
                    logger.debug(f"收集币安合约失败 {symbol}: {e}")
            
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
                self.log_with_role("error", f"数据回调失败: {e}")
    
    async def _process_okx_message(self, data):
        """处理欧意消息 - 完全保留原始数据，不做任何过滤"""
        if data.get("event"):
            event_type = data.get("event")
            self.log_with_role("info", f"OKX事件: {event_type}")
            
            if event_type == "error":
                logger.critical(f"[{self.connection_id}] 🔥 OKX错误详情: {json.dumps(data)}")
                if "too many requests" in str(data).lower():
                    self.connected = False
                    return
            
            elif event_type == "subscribe":
                channel = data.get("arg", {}).get("channel", "")
                inst_id = data.get("arg", {}).get("instId", "")
                self.log_with_role("info", f"✅ 订阅确认: channel={channel}, instId={inst_id}")
            
            return
        
        arg = data.get("arg", {})
        channel = arg.get("channel", "")
        symbol = arg.get("instId", "")
        
        try:
            if channel == "funding-rate":
                if not hasattr(self, 'funding_rate_count'):
                    self.funding_rate_count = 0
                    self._funding_next_milestone = 100
                
                if not data.get("data"):
                    self.log_with_role("warning", "资金费率消息缺少data字段")
                    return
                
                batch_size = len(data["data"])
                if batch_size == 0:
                    self.log_with_role("warning", "资金费率消息data为空数组")
                    return
                
                old_count = self.funding_rate_count
                self.funding_rate_count += batch_size
                
                if self.funding_rate_count >= self._funding_next_milestone:
                    self.log_with_role("info", f"✅ 已收到 {self.funding_rate_count} 条资金费率数据 (本批{batch_size}条)")
                    self._funding_next_milestone = ((self.funding_rate_count // 100) + 1) * 100
                
                if batch_size > 0:
                    funding_data = data["data"][0]
                    processed_symbol = symbol.replace('-USDT-SWAP', 'USDT')
                    
                    if SYMBOL_COLLECTOR_AVAILABLE:
                        try:
                            add_symbol_from_websocket("okx", processed_symbol)
                        except Exception as e:
                            logger.debug(f"收集OKX合约失败 {processed_symbol}: {e}")
                    
                    processed = {
                        "exchange": "okx",
                        "symbol": processed_symbol,
                        "data_type": "funding_rate",
                        "channel": channel,
                        "raw_data": data,
                        "original_symbol": symbol,
                        "timestamp": datetime.now().isoformat()
                    }
                    try:
                        await self.data_callback(processed)
                    except Exception as e:
                        self.log_with_role("error", f"数据回调失败: {e}")
                    
            elif channel == "tickers":
                if data.get("data") and len(data["data"]) > 0:
                    self.okx_ticker_count += 1
                    
                    if self.okx_ticker_count % 1000 == 0:
                        self.log_with_role("info", f"已收到 {self.okx_ticker_count} 个OKX ticker")
                    
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
                    try:
                        await self.data_callback(processed)
                    except Exception as e:
                        self.log_with_role("error", f"数据回调失败: {e}")
        
        except Exception as e:
            self.log_with_role("warning", f"解析OKX数据失败: {e}")
    
    async def disconnect(self):
        """断开连接"""
        try:
            if self.delayed_subscribe_task:
                self.delayed_subscribe_task.cancel()
                self.log_with_role("debug", "延迟订阅任务已取消")
            
            if self.keepalive_task:
                self.keepalive_task.cancel()
                self.log_with_role("debug", "保活任务已取消")
            
            if self.ws and self.connected:
                await self.ws.close()
                self.connected = False
                self.log_with_role("info", "WebSocket已关闭")
                
            if self.receive_task:
                self.receive_task.cancel()
                self.log_with_role("debug", "接收任务已取消")
                
            self.subscribed = False
            self.is_active = False
            
            self.log_with_role("info", "连接已完全断开")
            
        except Exception as e:
            self.log_with_role("error", f"断开连接时发生错误: {e}")
    
    @property
    def last_message_seconds_ago(self) -> float:
        """返回距上次消息过去了多少秒（监控调度专用）"""
        if self.last_message_time:
            return (datetime.now() - self.last_message_time).total_seconds()
        return 999
    
    async def check_health(self) -> Dict[str, Any]:
        """检查连接健康状态 - 🚨【新增性能指标】"""
        now = datetime.now()
        last_msg_seconds = (now - self.last_message_time).total_seconds() if self.last_message_time else 999
        
        # 计算运行时间
        uptime_seconds = time.time() - self.connection_start_time
        
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
            "message_rate": round(self.message_rate, 2),  # 🚨 新增：消息速率
            "total_messages": self.total_messages_received,  # 🚨 新增：总消息数
            "uptime_seconds": round(uptime_seconds, 1),  # 🚨 新增：运行时间
            "timestamp": now.isoformat()
        }