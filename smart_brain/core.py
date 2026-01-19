"""
大脑核心主控 - Render流式终极版（512MB内存优化）
支持双管道数据流：市场数据 + 私人数据
新增：前端数据推送功能
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
from datetime import datetime, timedelta

# 设置路径 - 修复路径计算
CURRENT_FILE = os.path.abspath(__file__)
SMART_BRAIN_DIR = os.path.dirname(CURRENT_FILE)
PROJECT_ROOT = os.path.dirname(SMART_BRAIN_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from websocket_pool.admin import WebSocketAdmin
from http_server.server import HTTPServer
from shared_data.data_store import data_store
from shared_data.pipeline_manager import PipelineManager
from http_server.exchange_api import ExchangeAPI

# 导入前端中继模块
try:
    from frontend_relay import FrontendRelayServer
    FRONTEND_RELAY_AVAILABLE = True
except ImportError:
    FRONTEND_RELAY_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("⚠️【智能大脑】 前端中继模块未找到，前端功能将不可用")

logger = logging.getLogger(__name__)

def start_keep_alive_background():
    """启动保活服务（后台线程）"""
    try:
        from keep_alive import start_with_http_check
        import threading
        
        def run_keeper():
            try:
                start_with_http_check()
            except Exception as e:
                logger.error(f"【智能大脑】保活服务异常: {e}")
        
        thread = threading.Thread(target=run_keeper, daemon=True)
        thread.start()
        logger.info("✅ 【智能大脑】保活服务已启动")
    except:
        logger.warning("⚠️ 【智能大脑】 保活服务未启动，但继续运行")

class SmartBrain:
    def __init__(self):
        self.ws_admin = WebSocketAdmin()
        self.http_server = None
        self.http_runner = None
        self.running = False
        
        self.funding_manager = None
        
        # 前端中继相关
        self.frontend_relay = None
        
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.last_market_time = None
        self.last_market_count = 0
        
        self.last_account_time = None
        self.last_trade_time = None
        
        self.status_log_task = None
        
    async def receive_market_data(self, processed_data):
        """
        接收市场数据处理后的数据
        并推送到前端
        """
        try:
            if isinstance(processed_data, list):
                self.last_market_count = len(processed_data)
                
                if logger.isEnabledFor(logging.DEBUG):
                    if processed_data and len(processed_data) > 0:
                        symbol = processed_data[0].get('symbol', 'unknown')
                        logger.debug(f"📣【智能大脑】收到批量数据: {len(processed_data)}条, 第一个合约: {symbol}")
            else:
                logger.warning(f"⚠️【智能大脑】 收到非列表类型市场数据: {type(processed_data)}")
                self.last_market_count = 1
            
            self.last_market_time = datetime.now()
            
            # 推送到前端
            if self.frontend_relay:
                try:
                    await self.frontend_relay.broadcast_market_data(processed_data)
                    if isinstance(processed_data, list) and len(processed_data) > 0:
                        logger.debug(f"✅【智能大脑】 已推送市场数据到前端: {len(processed_data)}条")
                except Exception as e:
                    logger.error(f"️❌【智能大脑】推送市场数据到前端失败: {e}")
            
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收数据错误: {e}")
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据
        并推送到前端
        """
        try:
            data_type = private_data.get('data_type', 'unknown')
            exchange = private_data.get('exchange', 'unknown')
            
            now = datetime.now()
            
            if data_type == 'account_update' or data_type == 'account':
                self.last_account_time = now
                logger.info(f"💰【智能大脑】 收到账户私人数据: {exchange}")
            elif data_type == 'order_update' or data_type == 'trade':
                self.last_trade_time = now
                logger.info(f"📝【智能大脑】 收到交易私人数据: {exchange}")
            else:
                self.last_account_time = now
                logger.info(f"⚠️【智能大脑】 收到未知类型私人数据: {exchange}.{data_type}")
            
            # 推送到前端
            if self.frontend_relay:
                try:
                    await self.frontend_relay.broadcast_private_data(private_data)
                    logger.debug(f"✅【智能大脑】 已推送私人数据到前端: {exchange}.{data_type}")
                except Exception as e:
                    logger.error(f"❌【智能大脑】推送私人数据到前端失败: {e}")
                
        except Exception as e:
            logger.error(f"⚠️【智能大脑】接收私人数据错误: {e}")
    
    async def _execute_exchange_api(self, exchange_name, api_method, **kwargs):
        """执行交易所API调用"""
        try:
            api = ExchangeAPI(exchange_name)
            if not await api.initialize():
                return {"success": False, "error": f"❌【智能大脑】{exchange_name} API初始化失败"}
            
            method = getattr(api, api_method)
            result = await method(**kwargs)
            await api.close()
            
            if "error" in result:
                return {"success": False, "error": result["error"]}
            
            return {"success": True, "data": result}
            
        except Exception as e:
            logger.error(f"❌【智能大脑】执行API失败: {e}")
            return {"success": False, "error": str(e)}
    
    async def handle_frontend_command(self, command_data):
        """
        处理前端指令 - 完整实现
        基于现有的 http_server/exchange_api.py
        """
        try:
            command = command_data.get('command', '')
            params = command_data.get('params', {})
            client_id = command_data.get('client_id', 'unknown')
            
            logger.info(f"🧠 【智能大脑】处理前端指令: {command} from {client_id}")
            
            # 根据指令类型处理
            if command == 'place_order':
                return await self._handle_place_order(params, client_id)
            elif command == 'cancel_order':
                return await self._handle_cancel_order(params, client_id)
            elif command == 'get_open_orders':
                return await self._handle_get_open_orders(params, client_id)
            elif command == 'get_order_history':
                return await self._handle_get_order_history(params, client_id)
            elif command == 'set_leverage':
                return await self._handle_set_leverage(params, client_id)
            elif command == 'get_account_balance':
                return await self._handle_get_account_balance(params, client_id)
            elif command == 'get_positions':
                return await self._handle_get_positions(params, client_id)
            elif command == 'get_ticker':
                return await self._handle_get_ticker(params, client_id)
            elif command == 'get_market_data':
                return await self._handle_get_market_data(params, client_id)
            elif command == 'get_connection_status':
                return await self._handle_get_connection_status(params, client_id)
            else:
                return {
                    "success": False,
                    "error": f"⚠️【智能大脑】未知指令: {command}",
                    "client_id": client_id,
                    "timestamp": datetime.now().isoformat()
                }
            
        except Exception as e:
            error_msg = f"❌【智能大脑】处理前端指令失败: {e}"
            logger.error(error_msg)
            return {
                'success': False, 
                'error': error_msg,
                'command': command_data.get('command', 'unknown'),
                'timestamp': datetime.now().isoformat()
            }
    
    async def _handle_place_order(self, params, client_id):
        """处理下单指令"""
        required = ['exchange', 'symbol', 'type', 'side', 'amount']
        for field in required:
            if field not in params:
                return {
                    "success": False,
                    "error": f"❌【智能大脑】缺少必要参数: {field}",
                    "client_id": client_id,
                    "timestamp": datetime.now().isoformat()
                }
        
        exchange = params['exchange']
        symbol = params['symbol']
        order_type = params['type']
        side = params['side']
        amount = float(params['amount'])
        price = float(params.get('price', 0))
        extra_params = params.get('params', {})
        
        result = await self._execute_exchange_api(
            exchange,
            'create_order',
            symbol=symbol,
            order_type=order_type,
            side=side,
            amount=amount,
            price=price if price > 0 else None,
            params=extra_params
        )
        
        result.update({
            "command": "place_order",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_cancel_order(self, params, client_id):
        """处理取消订单指令"""
        if 'exchange' not in params or 'symbol' not in params or 'order_id' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange、symbol或order_id参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params['symbol']
        order_id = params['order_id']
        
        result = await self._execute_exchange_api(
            exchange,
            'cancel_order',
            symbol=symbol,
            order_id=order_id
        )
        
        result.update({
            "command": "cancel_order",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_open_orders(self, params, client_id):
        """处理获取未成交订单指令"""
        if 'exchange' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params.get('symbol')
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_open_orders',
            symbol=symbol
        )
        
        result.update({
            "command": "get_open_orders",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_order_history(self, params, client_id):
        """处理获取订单历史指令"""
        if 'exchange' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params.get('symbol')
        limit = params.get('limit', 100)
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_order_history',
            symbol=symbol,
            limit=limit
        )
        
        result.update({
            "command": "get_order_history",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_set_leverage(self, params, client_id):
        """处理设置杠杆指令"""
        if 'exchange' not in params or 'symbol' not in params or 'leverage' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange、symbol或leverage参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params['symbol']
        leverage = int(params['leverage'])
        
        result = await self._execute_exchange_api(
            exchange,
            'set_leverage',
            symbol=symbol,
            leverage=leverage
        )
        
        result.update({
            "command": "set_leverage",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_account_balance(self, params, client_id):
        """处理获取账户余额指令"""
        if 'exchange' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_account_balance'
        )
        
        result.update({
            "command": "get_account_balance",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_positions(self, params, client_id):
        """处理获取持仓指令"""
        if 'exchange' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_positions'
        )
        
        result.update({
            "command": "get_positions",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_ticker(self, params, client_id):
        """处理获取ticker指令"""
        if 'exchange' not in params or 'symbol' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange或symbol参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params['symbol']
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_ticker',
            symbol=symbol
        )
        
        result.update({
            "command": "get_ticker",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_market_data(self, params, client_id):
        """处理获取市场数据指令"""
        try:
            exchange = params.get('exchange', '')
            symbol = params.get('symbol')
            
            if not exchange:
                return {
                    "success": False,
                    "error": "❌【智能大脑】缺少exchange参数",
                    "client_id": client_id,
                    "timestamp": datetime.now().isoformat()
                }
            
            # 从data_store获取市场数据
            market_data = await data_store.get_market_data(exchange, symbol)
            
            return {
                "success": True,
                "command": "get_market_data",
                "client_id": client_id,
                "data": market_data,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌【智能大脑】获取市场数据失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
    
    async def _handle_get_connection_status(self, params, client_id):
        """处理获取连接状态指令"""
        try:
            exchange = params.get('exchange')
            
            # 从data_store获取连接状态
            connection_status = await data_store.get_connection_status(exchange)
            
            return {
                "success": True,
                "command": "get_connection_status",
                "client_id": client_id,
                "data": connection_status,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌【智能大脑】获取连接状态失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
    
    def _format_time_diff(self, last_time):
        if not last_time:
            return "⚠️【智能大脑】从未收到"
        
        now = datetime.now()
        diff = now - last_time
        
        if diff.total_seconds() < 60:
            return f"{int(diff.total_seconds())}秒前"
        elif diff.total_seconds() < 3600:
            return f"{int(diff.total_seconds() / 60)}分钟前"
        else:
            return f"{int(diff.total_seconds() / 3600)}小时前"
    
    async def _log_data_status(self):
        """定期记录数据状态"""
        while self.running:
            try:
                await asyncio.sleep(60)
                
                market_count = self.last_market_count
                market_time = self._format_time_diff(self.last_market_time)
                
                if self.last_account_time:
                    account_status = f"✅【智能大脑】已更新，{self._format_time_diff(self.last_account_time)}"
                else:
                    account_status = "⚠️【智能大脑】从未收到"
                    
                if self.last_trade_time:
                    trade_status = f"✅【智能大脑】已更新，{self._format_time_diff(self.last_trade_time)}"
                else:
                    trade_status = "❌【智能大脑】从未收到"
                
                # 前端连接状态
                if self.frontend_relay:
                    frontend_stats = self.frontend_relay.get_stats_summary()
                    frontend_clients = frontend_stats.get('clients_connected', 0)
                    frontend_status = f"✅【智能大脑】已连接 {frontend_clients} 个客户端"
                else:
                    frontend_status = "⚠️【智能大脑】未启用"
                    frontend_clients = 0
                
                status_msg = f"""【智能大脑】【大脑数据状态】
成品数据，{market_count}条，已更新。{market_time}
私人数据-账户：{account_status}
私人数据-交易：{trade_status}
前端连接：{frontend_status}"""
                
                logger.info(status_msg)
                
                # 推送系统状态到前端
                if self.frontend_relay and frontend_clients > 0:
                    try:
                        system_status = {
                            'market_data': {
                                'count': market_count,
                                'last_update': market_time
                            },
                            'private_data': {
                                'account': account_status,
                                'trade': trade_status
                            },
                            'frontend': {
                                'clients': frontend_clients,
                                'messages_sent': frontend_stats.get('messages_broadcast', 0)
                            },
                            'timestamp': datetime.now().isoformat()
                        }
                        await self.frontend_relay.broadcast_system_status(system_status)
                    except Exception as e:
                        logger.debug(f"❌【智能大脑】推送系统状态失败: {e}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【智能大脑】状态日志错误: {e}")
                await asyncio.sleep(10)
    
    async def initialize(self):
        """初始化大脑核心"""
        logger.info("=" * 60)
        logger.info("智能大脑核心启动中（流式终极版，512MB优化）...")
        logger.info("=" * 60)
        
        try:
            # 步骤1：获取端口并创建HTTP服务器
            port = int(os.getenv('PORT', 10000))
            logger.info(f"【1️⃣】创建HTTP服务器 (端口: {port})...")
            self.http_server = HTTPServer(host='0.0.0.0', port=port)
            
            # 步骤2：注册路由
            logger.info("【2️⃣】注册路由...")
            from funding_settlement.api_routes import setup_funding_settlement_routes
            setup_funding_settlement_routes(self.http_server.app)
            
            # 步骤3：启动HTTP服务器
            logger.info("【3️⃣】启动HTTP服务器...")
            await self.start_http_server()
            data_store.set_http_server_ready(True)
            logger.info("✅ HTTP服务已就绪！")
            
            # 步骤4：初始化PipelineManager（双管道）
            logger.info("【4️⃣】初始化PipelineManager（双管道）...")
            self.pipeline_manager = PipelineManager(
                brain_callback=self.receive_market_data,
                private_data_callback=self.receive_private_data
            )
            await self.pipeline_manager.start()
            logger.info("✅ 数据处理管理员启动完成！")
            
            data_store.pipeline_manager = self.pipeline_manager
            
            # 步骤5：初始化资金费率管理器
            logger.info("【5️⃣】初始化资金费率管理器...")
            from funding_settlement import FundingSettlementManager
            self.funding_manager = FundingSettlementManager()
            
            # 步骤6：初始化前端中继
            if FRONTEND_RELAY_AVAILABLE:
                logger.info("【6️⃣】初始化前端中继服务器...")
                await self._initialize_frontend_relay()
                logger.info("✅ 前端中继启动完成！")
            else:
                logger.warning("⚠️ 前端中继模块未找到，跳过前端功能")
            
            # 步骤7：启动状态日志任务
            self.status_log_task = asyncio.create_task(self._log_data_status())
            
            # 步骤8：延迟启动WebSocket（后台）
            asyncio.create_task(self._delayed_ws_init())
            
            # 完成初始化
            self.running = True
            logger.info("=" * 60)
            logger.info("🚀 智能大脑核心启动完成！（流式终极版）")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"🚨 初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _initialize_frontend_relay(self):
        """初始化前端中继服务器"""
        try:
            # 创建前端中继服务器实例
            self.frontend_relay = FrontendRelayServer(
                brain_instance=self,
                port=10001
            )
            
            # 启动服务器
            success = await self.frontend_relay.start()
            if not success:
                logger.error("❌【智能大脑】 前端中继服务器启动失败")
                self.frontend_relay = None
                return
            
            logger.info("🎯【智能大脑】 前端中继服务已就绪:")
            logger.info(f"📡【智能大脑】数据推送: ws://0.0.0.0:10001/ws")
            logger.info(f"📨【智能大脑】指令接口: http://0.0.0.0:10001/api/cmd")
            logger.info(f"📊 【智能大脑】状态查询: http://0.0.0.0:10001/status")
            logger.info(f"❤️【智能大脑】健康检查: http://0.0.0.0:10001/health")
            
        except Exception as e:
            logger.error(f"❌【智能大脑】 初始化前端中继失败: {e}")
            self.frontend_relay = None
    
    async def _delayed_ws_init(self):
        """延迟启动WebSocket连接池"""
        await asyncio.sleep(10)
        try:
            logger.info("⏳ 延迟启动WebSocket...")
            await self.ws_admin.start()
            logger.info("✅ WebSocket初始化完成")
        except Exception as e:
            logger.error(f"WebSocket初始化失败: {e}")
    
    async def start_http_server(self):
        """启动HTTP服务器"""
        try:
            from aiohttp import web
            port = int(os.getenv('PORT', 10000))
            host = '0.0.0.0'
            
            runner = web.AppRunner(self.http_server.app)
            await runner.setup()
            
            site = web.TCPSite(runner, host, port)
            await site.start()
            
            self.http_runner = runner
            logger.info(f"✅ HTTP服务器已启动: http://{host}:{port}")
            
        except Exception as e:
            logger.error(f"启动HTTP服务器失败: {e}")
            raise
    
    async def run(self):
        """运行智能大脑核心"""
        try:
            success = await self.initialize()
            if not success:
                logger.error("初始化失败，程序退出")
                return
            
            logger.info("=" * 60)
            logger.info("🚀 智能大脑核心运行中（流式终极版，512MB优化）...")
            logger.info("🛑 按 Ctrl+C 停止")
            logger.info("=" * 60)
            
            # 主循环
            while self.running:
                await asyncio.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("收到键盘中断")
        except Exception as e:
            logger.error(f"运行错误: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.shutdown()
    
    def handle_signal(self, signum, frame):
        """处理系统信号"""
        logger.info(f"收到信号 {signum}，开始关闭...")
        self.running = False
    
    async def shutdown(self):
        """关闭大脑核心"""
        self.running = False
        logger.info("正在关闭大脑核心...")
        
        try:
            # 1. 取消状态日志任务
            if self.status_log_task:
                self.status_log_task.cancel()
                try:
                    await self.status_log_task
                except asyncio.CancelledError:
                    pass
            
            # 2. 关闭前端中继服务器
            if self.frontend_relay:
                await self.frontend_relay.stop()
            
            # 3. 数据处理管理员
            if hasattr(self, 'pipeline_manager') and self.pipeline_manager:
                await self.pipeline_manager.stop()
            
            # 4. 停止WebSocket管理员
            if hasattr(self, 'ws_admin') and self.ws_admin:
                await self.ws_admin.stop()
            
            # 5. 停止HTTP服务器
            if hasattr(self, 'http_runner') and self.http_runner:
                await self.http_runner.cleanup()
            
            logger.info("✅ 大脑核心已关闭（流式终极版）")
        except Exception as e:
            logger.error(f"关闭出错: {e}")