"""
纯粹的WebSocket物理连接
职责：连接、发送、接收、断开
不包含任何业务逻辑
"""
import asyncio
import json
import logging
import time
from typing import Optional, Any, Callable
import websockets

logger = logging.getLogger(__name__)

class Connection:
    """
    WebSocket物理连接
    特性：无状态、可替换、可销毁
    """
    
    def __init__(
        self,
        conn_id: str,
        exchange: str,
        ws_url: str,
        on_message: Optional[Callable] = None
    ):
        """
        初始化连接
        
        Args:
            conn_id: 连接唯一ID（格式: conn_{exchange}_{type}_{index}_{uuid}）
            exchange: 交易所名称
            ws_url: WebSocket地址
            on_message: 消息回调函数
        """
        self.conn_id = conn_id
        self.exchange = exchange
        self.ws_url = ws_url
        self.on_message = on_message
        
        # 连接状态
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = False
        self._connecting = False
        self._last_message_time: Optional[float] = None
        self._receive_task: Optional[asyncio.Task] = None
        
        # 配置
        self.ping_interval = 20
        self.ping_timeout = 30
        self.connect_timeout = 30
        
        logger.info(f"[{conn_id}] 连接对象创建")
    
    async def connect(self) -> bool:
        """
        建立WebSocket连接
        
        Returns:
            bool: 连接是否成功
        """
        if self._connected or self._connecting:
            return self._connected
        
        self._connecting = True
        logger.info(f"[{self.conn_id}] 正在连接到 {self.ws_url}")
        
        try:
            # 建立连接
            self._ws = await asyncio.wait_for(
                websockets.connect(
                    self.ws_url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    close_timeout=1
                ),
                timeout=self.connect_timeout
            )
            
            self._connected = True
            self._last_message_time = time.time()
            
            # 启动接收任务
            self._receive_task = asyncio.create_task(
                self._receive_loop(),
                name=f"{self.conn_id}_receive"
            )
            
            logger.info(f"[{self.conn_id}] 连接成功")
            return True
            
        except asyncio.TimeoutError:
            logger.error(f"[{self.conn_id}] 连接超时")
            return False
        except Exception as e:
            logger.error(f"[{self.conn_id}] 连接失败: {e}")
            return False
        finally:
            self._connecting = False
    
    async def disconnect(self):
        """
        断开连接
        """
        if not self._connected:
            return
        
        logger.info(f"[{self.conn_id}] 正在断开连接")
        
        # 取消接收任务
        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        
        # 关闭WebSocket
        if self._ws:
            await self._ws.close()
            self._ws = None
        
        self._connected = False
        logger.info(f"[{self.conn_id}] 连接已断开")
    
    async def send(self, message: str) -> bool:
        """
        发送消息
        
        Args:
            message: 要发送的消息字符串
            
        Returns:
            bool: 发送是否成功
        """
        if not self._connected or not self._ws:
            logger.warning(f"[{self.conn_id}] 连接未就绪，无法发送")
            return False
        
        try:
            await self._ws.send(message)
            return True
        except Exception as e:
            logger.error(f"[{self.conn_id}] 发送失败: {e}")
            self._connected = False
            return False
    
    async def _receive_loop(self):
        """
        消息接收循环
        """
        logger.info(f"[{self.conn_id}] 启动接收循环")
        
        try:
            while self._connected and self._ws:
                try:
                    # 接收消息
                    message = await asyncio.wait_for(
                        self._ws.recv(),
                        timeout=self.ping_interval * 2
                    )
                    
                    # 更新时间戳
                    self._last_message_time = time.time()
                    
                    # 调用回调
                    if self.on_message:
                        try:
                            await self.on_message(message)
                        except Exception as e:
                            logger.error(f"[{self.conn_id}] 消息回调异常: {e}")
                    
                except asyncio.TimeoutError:
                    # 正常的心跳超时，继续循环
                    continue
                except websockets.exceptions.ConnectionClosed:
                    logger.warning(f"[{self.conn_id}] 连接被关闭")
                    break
                except Exception as e:
                    logger.error(f"[{self.conn_id}] 接收异常: {e}")
                    break
        
        except asyncio.CancelledError:
            logger.info(f"[{self.conn_id}] 接收循环被取消")
        except Exception as e:
            logger.error(f"[{self.conn_id}] 接收循环异常: {e}")
        finally:
            # 标记连接断开
            if self._connected:
                self._connected = False
                logger.info(f"[{self.conn_id}] 连接已断开")
    
    @property
    def is_connected(self) -> bool:
        """
        连接是否活跃
        
        Returns:
            bool: 连接状态
        """
        return self._connected
    
    @property
    def last_message_age(self) -> float:
        """
        最后消息时间（秒）
        
        Returns:
            float: 距离最后消息的秒数，如果从未收到消息返回无穷大
        """
        if self._last_message_time:
            return time.time() - self._last_message_time
        return float('inf')
    
    async def get_status(self) -> dict:
        """
        获取连接状态
        
        Returns:
            dict: 状态信息
        """
        return {
            "conn_id": self.conn_id,
            "exchange": self.exchange,
            "connected": self._connected,
            "last_message_age": self.last_message_age,
            "timestamp": time.time()
        }