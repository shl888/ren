"""
WebSocket心跳策略模块
处理不同交易所的心跳差异 - 稳定性增强版
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
    """欧意策略：主动ping + 筛网捕获pong + 主动断联"""
    
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
        # 创建任务前，确保连接是活跃的
        if not self.connection.connected:
            self._log("warning", "💞❌【okx心跳策略】启动失败：连接未就绪")
            self._running = False
            return
        self._task = asyncio.create_task(self._active_ping_loop())
        self._log("info", "💞✅【okx心跳策略】已启动：主动ping + 检测断联")
    
    async def stop(self):
        """停止心跳"""
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._log("debug", f"⚠️【okx心跳策略】停止任务时捕获到异常: {e}")
            finally:
                self._task = None
        self._log("info", "❌【okx心跳策略】已停止")
    
    async def on_message_received(self, raw_message: str) -> bool:
        """筛网：快速过滤，精准捕获pong"""
        # 🎯 快速过滤：长消息不是pong 
        if len(raw_message) > 50:
            return False
        
        # 🎯 关键词过滤：不包含"pong"的不是目标 
        if 'pong' not in raw_message:
            return False
        
        # 🎯 精准捕获：确认是pong消息 (OKX返回纯文本'pong')
        # 增强：去除首尾空白字符后再比较，避免格式问题
        if raw_message.strip() == 'pong':
            await self._handle_captured_pong()
            return True
        
        return False
    
    async def _handle_captured_pong(self):
        """处理捕获到的pong消息"""
        self._last_pong_received = datetime.now()
        self._consecutive_failures = 0  # 重置失败计数
        self._pong_count += 1
        
        # 低频日志
        if self._pong_count % 12 == 0:  # 调整为每12次记录，便于观察
            self._log("debug", f"💞✅【okx心跳策略】已收到{self._pong_count}次pong响应")
    
    async def _active_ping_loop(self):
        """主动ping循环 + 检测断联 (稳定性增强版)"""
        self._log("debug", "💞✅【okx心跳策略】循环任务开始")
        
        # 循环核心条件：策略本身在运行，且底层连接是活跃的
        while self._running and self.connection.connected:
            try:
                # 1. 等待一个心跳间隔
                await asyncio.sleep(self._ping_interval)
                
                # 2. 等待后再次检查状态 (关键!)
                if not self._running or not self.connection.connected:
                    self._log("debug", "⚠️【okx心跳策略】状态变更，退出心跳循环")
                    break
                
                # 3. 发送ping
                self._last_ping_sent = datetime.now()
                self._log("debug", "💞🌎【okx心跳策略】正在发送ping...")
                success = await self._send_ping()
                
                if success:
                    self._ping_count += 1
                    self._log("debug", f"✅【okx心跳策略】 已发送ping (#{self._ping_count})")
                else:
                    self._log("warning", "❌【okx心跳策略】发送ping失败，本次循环跳过超时检测")
                    # 发送失败通常意味着连接已出问题，直接进入下一次循环，依赖外部的连接状态检查来退出
                    continue
                
                # 4. 等待pong响应
                await asyncio.sleep(self._pong_timeout)
                
                # 5. 等待后再次检查状态 (关键!)
                if not self._running or not self.connection.connected:
                    self._log("debug", "⚠️【okx心跳策略】状态变更，退出心跳循环")
                    break
                
                # 6. 🎯 断联检测：检查是否收到pong
                if (self._last_pong_received and 
                    self._last_pong_received > self._last_ping_sent):
                    # 成功收到pong
                    self._consecutive_failures = 0
                    self._log("debug", "💞✅【okx心跳策略】 收到pong响应")
                else:
                    # pong超时
                    self._consecutive_failures += 1
                    self._log("warning", 
                        f"⚠️💞【okx心跳策略】第{self._consecutive_failures}次pong超时 "
                        f"(等待{self._pong_timeout}秒)")
                    
                    # 🚨 主动断联：连续2次失败
                    if self._consecutive_failures >= self._max_failures:
                        self._log("critical", 
                            "💔⚠️【okx心跳策略】连续pong超时，将主动断开连接")
                        # 注意：此处不直接break，让外部机制或下一个循环条件检测来处理
                        # 触发紧急断开，断开后connection.connected会变为False，循环会自然退出
                        asyncio.create_task(self.connection._emergency_disconnect("心跳连续超时"))
                
                # 7. 低频统计日志
                if self._ping_count % 12 == 0:  # 调整为每12次记录，便于观察
                    self._log("info", 
                        f"💞📋【okx心跳策略】统计: ping={self._ping_count}, "
                        f"pong={self._pong_count}, "
                        f"连续失败={self._consecutive_failures}")
                
            except asyncio.CancelledError:
                self._log("debug", "心跳循环任务被取消")
                break
            except Exception as e:
                self._log("error", f"💞⚠️【okx心跳策略】心跳循环内部异常: {e}")
                # 发生未知异常，等待片刻后继续尝试，避免疯狂刷日志
                await asyncio.sleep(5)
        
        self._log("debug", "❌【okx心跳策略】心跳循环任务结束")
        # 循环退出，确保策略状态被标记为停止
        self._running = False
    
    async def _send_ping(self):
        """发送ping消息"""
        try:
            # 发送前进行连接状态检查
            if not self.connection.connected or self.connection.ws is None:
                self._log("debug", "💞❌【okx心跳策略】发送ping失败：连接无效")
                return False
            
            # 🔴 关键：发送纯文本字符串 "ping"，而不是 JSON 字符串化的 "\"ping\""
            await self.connection.ws.send("ping")  # 直接发送字符串！
            return True
        except asyncio.CancelledError:
            raise  # 向上传递取消异常
        except Exception as e:
            self._log("error", f"💞❌【okx心跳策略】发送ping失败: {e}")
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
            self.connection.log_with_role(level, f"💗【okx心跳策略】[心跳] {message}")
        else:
            log_method = getattr(logger, level, logger.info)
            log_method(f"💗【okx心跳策略】[okx心跳] {message}")

def create_heartbeat_strategy(exchange: str, connection) -> Optional[HeartbeatStrategy]:
    """创建心跳策略工厂函数"""
    exchange_lower = exchange.lower()
    
    if exchange_lower == "okx":
        return OkxHeartbeatStrategy(connection)
    # 币安和其他交易所返回None，websockets库会自动处理协议层心跳
    return None
    
    