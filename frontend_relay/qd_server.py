# frontend_relay/qd_server.py
"""
前端中继服务器 - qd表示前端，避免与http_server/server.py冲突
功能：1.接收前端连接 2.推送数据 3.执行指令
"""

import asyncio
import time
import logging
import json
import os
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
        
        # 从环境变量读取密钥
        self.valid_token = os.getenv('FRONTEND_TOKEN', '')
        if not self.valid_token:
            logger.warning(f"⚠️【客户端】 FRONTEND_TOKEN未设置，使用默认密钥（不安全）")
            self.valid_token = 'default_token_change_me'
        
        # WebSocket客户端管理（存储认证状态）
        self.ws_clients: List[Dict] = []  # 每个元素: {'ws': ws, 'authenticated': bool, 'client_id': str}
        
        # 基础统计
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
        
        logger.info(f"🔄【客户端】 前端中继初始化完成，端口: {self.port}")
        logger.info(f"🔐【客户端】 密钥验证已启用（连接后发送auth消息）")
    
    def _setup_routes(self):
        """设置路由"""
        # WebSocket端点 - 前端数据流
        self.app.router.add_get('/ws', self._handle_websocket)
        
        # HTTP API端点 - 前端指令
        self.app.router.add_post('/api/cmd', self._handle_command)
        
        # 状态查询
        self.app.router.add_get('/status', self._handle_status)
        
        # 健康检查
        self.app.router.add_get('/health', self._handle_health)
    
    # ==================== WebSocket处理 ====================
    
    async def _handle_websocket(self, request):
        """
        处理WebSocket连接
        先建立连接，等客户端发送auth消息验证
        """
        # 1. 建立连接（不验证token）
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        # 2. 记录临时连接（未认证）
        client_ip = request.remote
        client_id = f"qd_{client_ip}_{int(time.time())}"
        client_info = {
            'ws': ws,
            'authenticated': False,
            'client_id': client_id,
            'ip': client_ip
        }
        self.ws_clients.append(client_info)
        self.stats["total_connections"] += 1
        self.stats["current_connections"] = len(self.ws_clients)
        
        logger.info(f"🔌【客户端】新连接建立，等待认证: {client_id} (当前: {len(self.ws_clients)}个)")
        
        try:
            # 3. 等待客户端发送认证消息
            auth_timeout = 10  # 10秒内必须认证
            auth_received = False
            
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        
                        # 处理认证消息
                        if data.get('type') == 'auth':
                            token = data.get('token', '')
                            if self._validate_token(token):
                                client_info['authenticated'] = True
                                auth_received = True
                                
                                # 发送认证成功
                                await ws.send_json({
                                    "type": "auth_success",
                                    "client_id": client_id,
                                    "timestamp": time.time()
                                })
                                logger.info(f"✅【客户端】客户端认证成功: {client_id}")
                                
                                # 认证成功后进入正常消息循环
                                async for msg2 in ws:
                                    if msg2.type == web.WSMsgType.TEXT:
                                        try:
                                            data2 = json.loads(msg2.data)
                                            msg_type = data2.get('type')
                                            
                                            if msg_type == 'ping':
                                                await ws.send_json({
                                                    "type": "pong",
                                                    "timestamp": time.time()
                                                })
                                            
                                            elif msg_type == 'order':
                                                logger.debug(f"💰【客户端】收到开仓指令，准备转发给大脑")
                                                logger.debug(f"   参数: {data2.get('data', {})}")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "place_order",
                                                    "params": data2.get('data', {}),
                                                    "client_id": client_id
                                                })
                                                
                                                self.stats["commands_processed"] += 1
                                            
                                            elif msg_type == 'set_sl_tp':
                                                logger.debug(f"⚙️【客户端】收到止损止盈指令，准备转发给大脑")
                                                logger.info(f"   参数: {data2.get('data', {})}")
                                                logger.info(f"   客户端: {client_id}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "set_sl_tp",
                                                    "params": data2.get('data', {}),
                                                    "client_id": client_id
                                                })
                                                
                                                self.stats["commands_processed"] += 1
                                            
                                            elif msg_type == 'close_position':
                                                logger.debug(f"🔚【客户端】收到平仓指令，准备转发给大脑")
                                                logger.debug(f"   参数: {data2.get('data', {})}")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "close_position",
                                                    "params": data2.get('data', {}),
                                                    "client_id": client_id
                                                })
                                                
                                                self.stats["commands_processed"] += 1
                                            
                                            elif msg_type == 'config':
                                                logger.debug(f"💾【客户端】收到配置指令，转发给大脑")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "save_config",
                                                    "params": {"config_data": data2.get('data', '')},
                                                    "client_id": client_id
                                                })
                                            
                                            elif msg_type == 'set_trade_mode':
                                                logger.debug(f"🎮【客户端】收到交易模式指令，转发给大脑")
                                                logger.debug(f"   模式: {data2.get('mode')}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "set_trade_mode",
                                                    "params": {"mode": data2.get('mode', '')},
                                                    "client_id": client_id
                                                })
                                            
                                            elif msg_type == 'get_stats':
                                                logger.debug(f"📊【客户端】收到统计指令")
                                                logger.debug(f"   参数: {data2.get('params', {})}")
                                            
                                            else:
                                                logger.debug(f"📨【客户端】收到未知消息类型: {msg_type}")
                                                
                                        except Exception as e:
                                            logger.error(f"❌【客户端】处理消息异常: {e}")
                                    
                                    elif msg2.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                                        break
                                break
                            else:
                                # 认证失败
                                await ws.send_json({
                                    "type": "auth_failed",
                                    "error": "Invalid token",
                                    "timestamp": time.time()
                                })
                                logger.warning(f"📛【客户端】客户端认证失败: {client_id}")
                                break
                        else:
                            # 未认证前收到其他消息，要求先认证
                            await ws.send_json({
                                "type": "error",
                                "error": "Please authenticate first. Send: {'type':'auth', 'token':'your_token'}",
                                "timestamp": time.time()
                            })
                            
                    except json.JSONDecodeError:
                        pass
                        
                elif msg.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                    break
            
            # 认证超时处理
            if not auth_received and client_info in self.ws_clients:
                logger.warning(f"⏰【客户端】客户端认证超时: {client_id}")
                try:
                    await ws.send_json({
                        "type": "auth_timeout",
                        "error": "Authentication timeout",
                        "timestamp": time.time()
                    })
                except:
                    pass
                
        except Exception as e:
            logger.debug(f"WebSocket异常 {client_id}: {e}")
        
        finally:
            # 4. 清理连接
            if client_info in self.ws_clients:
                self.ws_clients.remove(client_info)
                self.stats["current_connections"] = len(self.ws_clients)
                logger.info(f"❌【客户端】连接断开: {client_id} (剩余: {len(self.ws_clients)}个)")
        
        return ws
    
    # ==================== HTTP指令处理 ====================
    
    async def _handle_command(self, request):
        """处理前端HTTP指令"""
        try:
            # 1. 验证token（HTTP指令需要验证）
            token = self._get_token_from_request(request)
            if not self._validate_token(token):
                return web.json_response({
                    "success": False,
                    "error": "认证失败"
                }, status=401)
            
            # 2. 解析请求
            data = await request.json()
            command = data.get('command', '')
            params = data.get('params', {})
            client_id = data.get('client_id', 'unknown')
            
            logger.info(f"📨【客户端】收到前端HTTP指令: {command} from {client_id}")
            
            # 3. 调用大脑处理指令
            if not self.brain:
                return web.json_response({
                    "success": False,
                    "error": "大脑实例未连接"
                }, status=503)
            
            await self.brain.handle_frontend_command({
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
                "timestamp": time.time()
            })
            
        except json.JSONDecodeError:
            return web.json_response({
                "success": False,
                "error": "无效的JSON格式"
            }, status=400)
        except Exception as e:
            logger.error(f"❌【客户端】处理前端指令失败: {e}")
            return web.json_response({
                "success": False,
                "error": str(e)
            }, status=500)
    
    async def _handle_status(self, request):
        """状态查询接口"""
        uptime = time.time() - self.stats["server_start"]
        
        # 统计已认证和未认证的客户端
        authenticated = len([c for c in self.ws_clients if c.get('authenticated', False)])
        unauthenticated = len(self.ws_clients) - authenticated
        
        return web.json_response({
            "service": "frontend_relay",
            "status": "running",
            "port": self.port,
            "uptime_seconds": uptime,
            "uptime_human": f"{int(uptime // 3600)}小时{int((uptime % 3600) // 60)}分钟",
            "stats": self.stats,
            "clients": {
                "total": len(self.ws_clients),
                "authenticated": authenticated,
                "unauthenticated": unauthenticated
            },
            "auth_enabled": True,
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
        """广播市场数据到所有前端"""
        logger.debug(f"📤【客户端】【市场数据推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【市场数据推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "market_data",
            "data": market_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_private_data(self, private_data):
        """广播私人数据到所有前端"""
        logger.debug(f"📤【客户端】【私人数据推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【私人数据推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "private_data",
            "data": private_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_reference_data(self, reference_data):
        """广播面值数据到所有前端"""
        logger.debug(f"📤【客户端】【面值数据推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【面值数据推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "reference_data",
            "data": reference_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_system_status(self, status_data):
        """广播系统状态到所有前端"""
        logger.debug(f"📤【客户端】【系统状态推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【系统状态推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "system_status",
            "data": status_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_execution_results(self, results):
        """广播订单执行结果到前端"""
        # ========== 收到数据时打印 ==========
        logger.debug(f"📥【客户端收到】results 数量: {len(results)}")
        for i, res in enumerate(results):
            logger.debug(f"📥【客户端收到】第{i+1}条: exchange={res.get('exchange')}, type={res.get('type')}, success={res.get('success')}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【执行结果推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "execution_results",
            "data": results,
            "timestamp": time.time()
        }
        
        # ========== 发送前打印 ==========
        logger.debug(f"📤【客户端发送】准备广播: type={message['type']}, data数量={len(message['data'])}")
        for i, res in enumerate(message['data']):
            logger.debug(f"📤【客户端发送】第{i+1}条: exchange={res.get('exchange')}, type={res.get('type')}")
        
        await self._safe_broadcast(message)
    
    async def broadcast_binance_ticker_24hr(self, ticker_data: Dict):
        """广播币安24小时涨跌幅数据到所有前端"""
        logger.debug(f"📤【客户端】【涨跌幅数据推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【涨跌幅数据推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "binance_ticker_24hr",
            "data": ticker_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def _safe_broadcast(self, message):
        """
        安全广播 - 只推送给已认证的客户端，带详细日志
        """
        # 过滤出已认证的客户端
        authenticated_clients = [c for c in self.ws_clients if c.get('authenticated', False)]
        
        if not authenticated_clients:
            logger.debug(f"⚠️【客户端】【广播】没有已认证的客户端，跳过")
            return
        
        message_type = message.get('type', 'unknown')
        logger.debug(f"🔥【客户端】【广播开始】类型: {message_type}, 已认证客户端数: {len(authenticated_clients)}")
        
        dead_clients = []
        message_json = json.dumps(message, default=str)
        
        for client in authenticated_clients:
            ws = client['ws']
            client_id = client.get('client_id', 'unknown')
            try:
                await ws.send_str(message_json)
                logger.debug(f"✅【客户端】【广播成功】类型: {message_type}, 客户端: {client_id}")
            except Exception as e:
                logger.error(f"❌【客户端】【广播失败】类型: {message_type}, 客户端: {client_id}, 错误: {e}")
                dead_clients.append(client)
        
        # 清理死连接
        if dead_clients:
            logger.info(f"🧹【客户端】【清理连接】清理 {len(dead_clients)} 个死连接")
            for client in dead_clients:
                if client in self.ws_clients:
                    self.ws_clients.remove(client)
            self.stats["current_connections"] = len(self.ws_clients)
        
        self.stats["messages_broadcast"] += len(authenticated_clients) - len(dead_clients)
        logger.debug(f"✅【客户端】【广播完成】类型: {message_type}, 成功发送到 {len(authenticated_clients) - len(dead_clients)} 个客户端")
    
    # ==================== 辅助方法 ====================
    
    def _validate_token(self, token: str) -> bool:
        """验证token"""
        if not token:
            return False
        
        # 从环境变量读取的密钥
        return token == self.valid_token
    
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
        
        return ''
    
    # ==================== 服务器控制 ====================
    
    async def start(self):
        """启动前端中继服务器"""
        try:
            logger.info(f"🚀【客户端】 启动前端中继服务器，端口: {self.port}")
            
            # 创建运行器
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            # 启动TCP站点
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            
            logger.info(f"✅【客户端】 前端中继服务器启动成功")
            logger.info(f"📡【客户端】 WebSocket: ws://0.0.0.0:{self.port}/ws")
            logger.info(f"📨【客户端】 HTTP API: http://0.0.0.0:{self.port}/api/cmd")
            logger.info(f"📊【客户端】状态查询: http://0.0.0.0:{self.port}/status")
            logger.info(f"❤️【客户端】健康检查: http://0.0.0.0:{self.port}/health")
            logger.info(f"🔐【客户端】认证方式: 连接WebSocket后发送 {{'type':'auth', 'token':'YOUR_TOKEN'}}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌【客户端】 启动前端中继服务器失败: {e}")
            return False
    
    async def stop(self):
        """停止前端中继服务器"""
        logger.info("🛑【客户端】 停止前端中继服务器...")
        
        # 关闭所有WebSocket连接
        for client in self.ws_clients:
            try:
                await client['ws'].close()
            except:
                pass
        self.ws_clients.clear()
        
        # 停止HTTP服务器
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
            self.site = None
        
        logger.info("✅【客户端】 前端中继服务器已停止")
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """获取统计摘要"""
        uptime = time.time() - self.stats["server_start"]
        
        authenticated = len([c for c in self.ws_clients if c.get('authenticated', False)])
        
        return {
            "running": self.runner is not None,
            "port": self.port,
            "clients_connected": len(self.ws_clients),
            "authenticated_clients": authenticated,
            "total_connections": self.stats["total_connections"],
            "messages_broadcast": self.stats["messages_broadcast"],
            "commands_processed": self.stats["commands_processed"],
            "uptime_seconds": uptime,
            "auth_enabled": True
        }