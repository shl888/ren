# frontend_relay/qd_server.py
"""
前端中继服务器 - qd表示前端，避免与http_server/server.py冲突
功能：1.接收前端连接 2.推送数据 3.执行指令
"""

import asyncio
import time
import logging
import json
from typing import List, Dict, Any, Optional
from aiohttp import web

logger = logging.getLogger(__name__)


class FrontendRelayServer:
    """前端中继服务器 - 完整实现"""
    
    def __init__(self, brain_instance, port: int = 10001):
        """
        初始化前端中继服务器
        
        Args:
            brain_instance: 大脑实例引用（用于处理指令）
            port: 服务端口，默认10001（避免与现有服务冲突）
        """
        self.brain = brain_instance
        self.port = port
        
        # WebSocket客户端管理（极简，只有列表）
        self.ws_clients: List[web.WebSocketResponse] = []
        
        # 基础统计（不需要复杂监控）
        self.stats = {
            "server_start": time.time(),
            "total_connections": 0,
            "current_connections": 0,
            "messages_broadcast": 0,
            "commands_processed": 0
        }
        
        # 创建aiohttp应用
        self.app = web.Application()
        self._setup_routes()
        
        # 服务器运行器
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        
        logger.info(f"🔄 前端中继初始化完成，端口: {self.port}")
    
    def _setup_routes(self):
        """设置路由（极简版）"""
        # WebSocket端点 - 前端数据流
        self.app.router.add_get('/ws', self._handle_websocket)
        
        # HTTP API端点 - 前端指令
        self.app.router.add_post('/api/cmd', self._handle_command)
        
        # 状态查询
        self.app.router.add_get('/status', self._handle_status)
        
        # 健康检查（用于负载均衡）
        self.app.router.add_get('/health', self._handle_health)
    
    # ==================== WebSocket处理 ====================
    
    async def _handle_websocket(self, request):
        """
        处理WebSocket连接
        原则：不心跳、不保活、不断就保持
        """
        # 1. 基础验证（可选）
        token = request.query.get('token', '')
        if not self._validate_token_simple(token):
            logger.warning(f"📛 WebSocket连接被拒绝，token无效")
            return web.HTTPUnauthorized()
        
        # 2. 建立连接
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        # 3. 记录客户端（极简）
        client_ip = request.remote
        client_id = f"qd_{client_ip}_{int(time.time())}"
        self.ws_clients.append(ws)
        
        # 4. 更新统计
        self.stats["total_connections"] += 1
        self.stats["current_connections"] = len(self.ws_clients)
        
        logger.info(f"✅ 前端连接建立: {client_id} (当前: {len(self.ws_clients)}个)")
        
        try:
            # 5. 发送连接确认（可选）
            await ws.send_json({
                "type": "connected",
                "client_id": client_id,
                "timestamp": time.time()
            })
            
            # 6. 保持连接（不主动做任何事）
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    # 前端可以发送ping，我们响应pong
                    try:
                        data = json.loads(msg.data)
                        if data.get('type') == 'ping':
                            await ws.send_json({
                                "type": "pong",
                                "timestamp": time.time()
                            })
                    except:
                        pass  # 忽略格式错误
                elif msg.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                    break  # 连接关闭或错误，退出循环
        
        except Exception as e:
            logger.debug(f"WebSocket异常 {client_id}: {e}")
        
        finally:
            # 7. 清理连接（静默）
            if ws in self.ws_clients:
                self.ws_clients.remove(ws)
                self.stats["current_connections"] = len(self.ws_clients)
                logger.info(f"❌ 前端连接断开: {client_id} (剩余: {len(self.ws_clients)}个)")
        
        return ws
    
    # ==================== HTTP指令处理 ====================
    
    async def _handle_command(self, request):
        """处理前端HTTP指令"""
        try:
            # 1. 解析请求
            data = await request.json()
            command = data.get('command', '')
            params = data.get('params', {})
            client_id = data.get('client_id', 'unknown')
            
            logger.info(f"📨 收到前端指令: {command} from {client_id}")
            
            # 2. 基础验证
            token = self._get_token_from_request(request)
            if not self._validate_token_simple(token):
                return web.json_response({
                    "success": False,
                    "error": "认证失败"
                }, status=401)
            
            # 3. 调用大脑处理指令
            if not self.brain:
                return web.json_response({
                    "success": False,
                    "error": "大脑实例未连接"
                }, status=503)
            
            result = await self.brain.handle_frontend_command({
                "command": command,
                "params": params,
                "client_id": client_id
            })
            
            # 4. 更新统计
            self.stats["commands_processed"] += 1
            
            # 5. 返回结果
            return web.json_response({
                "success": True,
                "command": command,
                "result": result,
                "timestamp": time.time()
            })
            
        except json.JSONDecodeError:
            return web.json_response({
                "success": False,
                "error": "无效的JSON格式"
            }, status=400)
        except Exception as e:
            logger.error(f"处理指令失败: {e}")
            return web.json_response({
                "success": False,
                "error": str(e)
            }, status=500)
    
    async def _handle_status(self, request):
        """状态查询接口"""
        uptime = time.time() - self.stats["server_start"]
        
        return web.json_response({
            "service": "frontend_relay",
            "status": "running",
            "port": self.port,
            "uptime_seconds": uptime,
            "uptime_human": f"{int(uptime // 3600)}小时{int((uptime % 3600) // 60)}分钟",
            "stats": self.stats,
            "timestamp": time.time()
        })
    
    async def _handle_health(self, request):
        """健康检查（极简）"""
        return web.json_response({
            "status": "healthy",
            "service": "frontend_relay",
            "timestamp": time.time()
        })
    
    # ==================== 数据广播 ====================
    
    async def broadcast_market_data(self, market_data):
        """
        广播市场数据到所有前端
        原则：有数据就推，推失败就静默清理
        """
        if not self.ws_clients:
            return
        
        message = {
            "type": "market_data",
            "data": market_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_private_data(self, private_data):
        """广播私人数据到所有前端"""
        if not self.ws_clients:
            return
        
        message = {
            "type": "private_data",
            "data": private_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_system_status(self, status_data):
        """广播系统状态到所有前端"""
        if not self.ws_clients:
            return
        
        message = {
            "type": "system_status",
            "data": status_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def _safe_broadcast(self, message):
        """
        安全广播 - 推送到所有客户端，失败则静默清理
        """
        dead_clients = []
        message_json = json.dumps(message, default=str)
        
        for ws in self.ws_clients:
            try:
                await ws.send_str(message_json)
                self.stats["messages_broadcast"] += 1
            except (ConnectionError, RuntimeError):
                # 连接已断开，标记为待清理
                dead_clients.append(ws)
            except Exception as e:
                logger.debug(f"广播消息失败: {e}")
                dead_clients.append(ws)
        
        # 静默清理死连接
        if dead_clients:
            for ws in dead_clients:
                if ws in self.ws_clients:
                    self.ws_clients.remove(ws)
            self.stats["current_connections"] = len(self.ws_clients)
    
    # ==================== 辅助方法 ====================
    
    def _validate_token_simple(self, token: str) -> bool:
        """
        简化版token验证
        TODO: 实现实际验证逻辑
        目前返回True允许所有连接（测试用）
        """
        # 实际应该验证token有效性
        # 现在先允许所有连接
        return True
    
    def _get_token_from_request(self, request) -> str:
        """从HTTP请求获取token"""
        # 1. 检查Authorization头
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            return auth_header[7:]
        
        # 2. 检查查询参数
        token = request.query.get('token', '')
        if token:
            return token
        
        # 3. 检查JSON body
        try:
            if request.has_body:
                # 注意：这里不能直接读取body，会消耗它
                # 实际应该在_handle_command中处理
                pass
        except:
            pass
        
        return ''
    
    # ==================== 服务器控制 ====================
    
    async def start(self):
        """启动前端中继服务器"""
        try:
            logger.info(f"🚀 启动前端中继服务器，端口: {self.port}")
            
            # 创建运行器
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            # 启动TCP站点
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            
            logger.info(f"✅ 前端中继服务器启动成功")
            logger.info(f"   📡 WebSocket: ws://0.0.0.0:{self.port}/ws")
            logger.info(f"   📨 HTTP API: http://0.0.0.0:{self.port}/api/cmd")
            logger.info(f"   📊 状态查询: http://0.0.0.0:{self.port}/status")
            logger.info(f"   ❤️  健康检查: http://0.0.0.0:{self.port}/health")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 启动前端中继服务器失败: {e}")
            return False
    
    async def stop(self):
        """停止前端中继服务器"""
        logger.info("🛑 停止前端中继服务器...")
        
        # 关闭所有WebSocket连接
        for ws in self.ws_clients:
            try:
                await ws.close()
            except:
                pass
        self.ws_clients.clear()
        
        # 停止HTTP服务器
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
            self.site = None
        
        logger.info("✅ 前端中继服务器已停止")
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """获取统计摘要"""
        uptime = time.time() - self.stats["server_start"]
        
        return {
            "running": self.runner is not None,
            "port": self.port,
            "clients_connected": len(self.ws_clients),
            "total_connections": self.stats["total_connections"],
            "messages_broadcast": self.stats["messages_broadcast"],
            "commands_processed": self.stats["commands_processed"],
            "uptime_seconds": uptime
        }