"""
WebSocket心跳策略模块 - 最终修正版
已根据官方文档修正OKX心跳格式，确保连接稳定。
"""
import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

class HeartbeatStrategy(ABC):
    """心跳策略抽象类"""
    
    def __init__(self, connection):
        self.connection = connection
        self._running = False
        self._task = None
    
    @abstractmethod
    async def start(self):
        """启动心跳策略"""
        pass
    
    @abstractmethod
    async def stop(self):
        """停止心跳策略"""
        pass
    
    @abstractmethod
    async def on_message_received(self, raw_message: str):
        """收到消息时的处理 - 返回True表示已处理"""
        pass
    
    def get_status(self) -> dict:
        """获取心跳状态"""
        return {
            "strategy": self.__class__.__name__,
            "running": self._running,
            "timestamp": datetime.now().isoformat()
        }


class OkxHeartbeatStrategy(HeartbeatStrategy):
    """欧意策略：主动发送纯文本ping + 捕获纯文本pong"""
    
    def __init__(self, connection):
        super().__init__(connection)
        self._ping_interval = 25  # 每25秒主动ping一次
        self._pong_timeout = 10   # 等待pong的最大时间（秒）
        self._consecutive_failures = 0
        self._max_failures = 2    # 连续2次无pong就主动断开
        self._last_ping_sent = None
        self._last_pong_received = None
        self._ping_count = 0
        self._pong_count = 0
    
    async def start(self):
        """启动主动ping循环"""
        if self._running:
            return
        
        self._running = True
        self._consecutive_failures = 0
        self._task = asyncio.create_task(self._active_ping_loop())
        self._log("info", "欧意心跳策略启动：主动ping + 断联检测")
    
    async def stop(self):
        """停止心跳"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self._log("info", "欧意心跳策略停止")
    
    async def on_message_received(self, raw_message: str) -> bool:
        """处理消息：捕获OKX返回的纯文本'pong'"""
        # 关键修正：OKX的pong是纯文本字符串，不是JSON。
        if raw_message == 'pong':
            await self._handle_captured_pong()
            return True
        # 如果不是'pong'，返回False，交由业务逻辑处理
        return False
    
    async def _handle_captured_pong(self):
        """处理捕获到的pong消息"""
        self._last_pong_received = datetime.now()
        self._consecutive_failures = 0  # 重置失败计数
        self._pong_count += 1
        
        # 低频日志
        if self._pong_count % 100 == 0:
            self._log("debug", f"已收到{self._pong_count}次pong响应")
    
    async def _active_ping_loop(self):
        """主动ping循环 + 断联检测"""
        while self._running:
            try:
                # 等待ping间隔
                await asyncio.sleep(self._ping_interval)
                
                if not self._running or not self.connection.connected:
                    break
                
                # 发送ping
                self._last_ping_sent = datetime.now()
                # 关键修正：这里调用修正后的 _send_ping
                success = await self._send_ping()
                if success:
                    self._ping_count += 1
                
                # 等待pong响应
                await asyncio.sleep(self._pong_timeout)
                
                # 🎯 断联检测：检查是否收到pong
                if (self._last_pong_received and 
                    self._last_pong_received > self._last_ping_sent):
                    # 成功收到pong
                    self._consecutive_failures = 0
                else:
                    # pong超时
                    self._consecutive_failures += 1
                    self._log("warning", 
                        f"第{self._consecutive_failures}次pong超时 "
                        f"(等待{self._pong_timeout}秒)")
                    
                    # 🚨 主动断联：连续2次失败
                    if self._consecutive_failures >= self._max_failures:
                        self._log("critical", 
                            "连续pong超时，主动断开连接")
                        await self.connection._emergency_disconnect("pong超时")
                        break
                
                # 低频统计日志
                if self._ping_count % 50 == 0:
                    self._log("info", 
                        f"欧意心跳统计: ping={self._ping_count}, "
                        f"pong={self._pong_count}, "
                        f"失败={self._consecutive_failures}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log("error", f"ping循环异常: {e}")
                await asyncio.sleep(5)
    
    async def _send_ping(self):
        """发送ping消息 - 关键修正：发送纯文本'ping'"""
        try:
            if not self.connection.connected or self.connection.ws is None:
                return False
            
            # 关键修正：OKX要求发送纯文本字符串 "ping"，而不是JSON格式。
            # 直接发送字符串，切勿使用 json.dumps()
            await self.connection.ws.send("ping")
            return True
        except Exception as e:
            self._log("error", f"发送ping失败: {e}")
            return False
    
    def get_status(self) -> dict:
        """获取详细状态"""
        status = super().get_status()
        status.update({
            "ping_interval": self._ping_interval,
            "pong_timeout": self._pong_timeout,
            "ping_count": self._ping_count,
            "pong_count": self._pong_count,
            "consecutive_failures": self._consecutive_failures,
            "max_failures": self._max_failures,
            "last_ping_sent": self._last_ping_sent.isoformat() if self._last_ping_sent else None,
            "last_pong_received": self._last_pong_received.isoformat() if self._last_pong_received else None,
        })
        return status
    
    def _log(self, level: str, message: str):
        """记录日志"""
        if hasattr(self.connection, 'log_with_role'):
            self.connection.log_with_role(level, f"[心跳] {message}")
        else:
            log_method = getattr(logger, level, logger.info)
            log_method(f"[欧意心跳] {message}")


class BinanceHeartbeatStrategy(HeartbeatStrategy):
    """币安策略：被动响应服务器的JSON格式ping"""
    
    def __init__(self, connection):
        super().__init__(connection)
        self._ping_count = 0  # 仅用于统计
        self._pong_count = 0
    
    async def start(self):
        """启动策略 - 币安无需主动发送任务，只需监听"""
        if self._running:
            return
        
        self._running = True
        self._log("info", "币安心跳策略启动：被动响应ping")
    
    async def stop(self):
        """停止策略"""
        self._running = False
        self._log("info", "币安心跳策略停止")
    
    async def on_message_received(self, raw_message: str) -> bool:
        """捕获币安服务器的ping并回复pong"""
        # 币安的ping是JSON格式：{"ping": timestamp}
        try:
            data = json.loads(raw_message)
            if isinstance(data, dict) and "ping" in data:
                await self._handle_captured_ping(data["ping"])
                return True
        except json.JSONDecodeError:
            pass
        
        return False
    
    async def _handle_captured_ping(self, ping_timestamp: int):
        """处理捕获到的ping消息 - 立即异步回复pong"""
        self._ping_count += 1
        
        # 立即异步回复pong（不阻塞消息处理）
        asyncio.create_task(self._reply_pong_async(ping_timestamp))
        
        # 低频日志
        if self._ping_count % 200 == 0:
            self._log("debug", f"已响应{self._ping_count}次ping")
    
    async def _reply_pong_async(self, ping_timestamp: int):
        """异步回复pong - 无阻塞"""
        try:
            if not self.connection.connected or self.connection.ws is None:
                return
            
            # 币安要求pong格式为 {"pong": timestamp}
            pong_msg = json.dumps({"pong": ping_timestamp})
            await self.connection.ws.send(pong_msg)
            self._pong_count += 1
        except Exception as e:
            # 静默失败，避免因发送失败引发额外错误
            self._log("debug", f"回复pong失败（通常无害）: {e}")
    
    def get_status(self) -> dict:
        """获取详细状态"""
        status = super().get_status()
        status.update({
            "ping_count": self._ping_count,
            "pong_count": self._pong_count,
            "mode": "passive_response_only",
        })
        return status
    
    def _log(self, level: str, message: str):
        """记录日志"""
        if hasattr(self.connection, 'log_with_role'):
            self.connection.log_with_role(level, f"[心跳] {message}")
        else:
            log_method = getattr(logger, level, logger.info)
            log_method(f"[币安心跳] {message}")


def create_heartbeat_strategy(exchange: str, connection) -> HeartbeatStrategy:
    """创建心跳策略工厂函数"""
    exchange_lower = exchange.lower()
    
    if exchange_lower == "okx":
        return OkxHeartbeatStrategy(connection)
    elif exchange_lower == "binance":
        return BinanceHeartbeatStrategy(connection)
    else:
        # 对于未知交易所，默认采用更保守的币安（被动响应）策略
        # 这样可以避免向不支持的服务器主动发送ping导致错误
        return BinanceHeartbeatStrategy(connection)