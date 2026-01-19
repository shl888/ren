"""
大脑核心主控 - Render流式终极版（512MB内存优化）
支持双管道数据流：市场数据 + 私人数据
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
                logger.error(f"保活服务异常: {e}")
        
        thread = threading.Thread(target=run_keeper, daemon=True)
        thread.start()
        logger.info("✅ 保活服务已启动")
    except:
        logger.warning("⚠️  保活服务未启动，但继续运行")

class SmartBrain:
    def __init__(self):
        self.ws_admin = WebSocketAdmin()
        self.http_server = None
        self.http_runner = None
        self.running = False
        
        self.funding_manager = None
        
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        self.last_market_time = None
        self.last_market_count = 0
        
        self.last_account_time = None
        self.last_trade_time = None
        
        self.status_log_task = None
        
    async def receive_market_data(self, processed_data):
        try:
            if isinstance(processed_data, list):
                self.last_market_count = len(processed_data)
                
                if logger.isEnabledFor(logging.DEBUG):
                    if processed_data and len(processed_data) > 0:
                        symbol = processed_data[0].get('symbol', 'unknown')
                        logger.debug(f"收到批量数据: {len(processed_data)}条, 第一个合约: {symbol}")
            else:
                logger.warning(f"⚠️ 收到非列表类型市场数据: {type(processed_data)}")
                self.last_market_count = 1
            
            self.last_market_time = datetime.now()
            
        except Exception as e:
            logger.error(f"接收数据错误: {e}")
    
    async def receive_private_data(self, private_data):
        try:
            data_type = private_data.get('data_type', 'unknown')
            exchange = private_data.get('exchange', 'unknown')
            
            now = datetime.now()
            
            if data_type == 'account_update' or data_type == 'account':
                self.last_account_time = now
                logger.info(f"💰 收到账户私人数据: {exchange}")
            elif data_type == 'order_update' or data_type == 'trade':
                self.last_trade_time = now
                logger.info(f"📝 收到交易私人数据: {exchange}")
            else:
                self.last_account_time = now
                logger.info(f"📨 收到未知类型私人数据: {exchange}.{data_type}")
                
        except Exception as e:
            logger.error(f"接收私人数据错误: {e}")
    
    def _format_time_diff(self, last_time):
        if not last_time:
            return "从未收到"
        
        now = datetime.now()
        diff = now - last_time
        
        if diff.total_seconds() < 60:
            return f"{int(diff.total_seconds())}秒前"
        elif diff.total_seconds() < 3600:
            return f"{int(diff.total_seconds() / 60)}分钟前"
        else:
            return f"{int(diff.total_seconds() / 3600)}小时前"
    
    async def _log_data_status(self):
        while self.running:
            try:
                await asyncio.sleep(60)
                
                market_count = self.last_market_count
                market_time = self._format_time_diff(self.last_market_time)
                
                if self.last_account_time:
                    account_status = f"已更新，{self._format_time_diff(self.last_account_time)}"
                else:
                    account_status = "从未收到"
                    
                if self.last_trade_time:
                    trade_status = f"已更新，{self._format_time_diff(self.last_trade_time)}"
                else:
                    trade_status = "从未收到"
                
                status_msg = f"""【大脑数据状态】
成品数据，{market_count}条，已更新。{market_time}
私人数据-账户：{account_status}
私人数据-交易：{trade_status}"""
                
                logger.info(status_msg)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"状态日志错误: {e}")
                await asyncio.sleep(10)
    
    async def initialize(self):
        logger.info("=" * 60)
        logger.info("大脑核心启动中（流式终极版，512MB优化）...")
        logger.info("=" * 60)
        
        try:
            port = int(os.getenv('PORT', 10000))
            logger.info(f"【1️⃣】创建HTTP服务器 (端口: {port})...")
            self.http_server = HTTPServer(host='0.0.0.0', port=port)
            
            logger.info("【2️⃣】注册路由...")
            from funding_settlement.api_routes import setup_funding_settlement_routes
            setup_funding_settlement_routes(self.http_server.app)
            
            logger.info("【3️⃣】启动HTTP服务器...")
            await self.start_http_server()
            data_store.set_http_server_ready(True)
            logger.info("✅ HTTP服务已就绪！")
            
            logger.info("【4️⃣】初始化PipelineManager（双管道）...")
            self.pipeline_manager = PipelineManager(
                brain_callback=self.receive_market_data,
                private_data_callback=self.receive_private_data
            )
            await self.pipeline_manager.start()
            logger.info("✅ 流水线管理员启动完成！")
            
            data_store.pipeline_manager = self.pipeline_manager
            
            logger.info("【5️⃣】初始化资金费率管理器...")
            from funding_settlement import FundingSettlementManager
            self.funding_manager = FundingSettlementManager()
            
            self.status_log_task = asyncio.create_task(self._log_data_status())
            
            asyncio.create_task(self._delayed_ws_init())
            
            self.running = True
            logger.info("=" * 60)
            logger.info("🚀 大脑核心启动完成！（流式终极版）")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"🚨 初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _delayed_ws_init(self):
        await asyncio.sleep(10)
        try:
            logger.info("⏳ 延迟启动WebSocket...")
            await self.ws_admin.start()
            logger.info("✅ WebSocket初始化完成")
        except Exception as e:
            logger.error(f"WebSocket初始化失败: {e}")
    
    async def start_http_server(self):
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
        try:
            success = await self.initialize()
            if not success:
                logger.error("初始化失败，程序退出")
                return
            
            logger.info("=" * 60)
            logger.info("🚀 大脑核心运行中（流式终极版，512MB优化）...")
            logger.info("🛑 按 Ctrl+C 停止")
            logger.info("=" * 60)
            
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
        logger.info(f"收到信号 {signum}，开始关闭...")
        self.running = False
    
    async def shutdown(self):
        self.running = False
        logger.info("正在关闭大脑核心...")
        
        try:
            if self.status_log_task:
                self.status_log_task.cancel()
                try:
                    await self.status_log_task
                except asyncio.CancelledError:
                    pass
            
            if hasattr(self, 'pipeline_manager') and self.pipeline_manager:
                await self.pipeline_manager.stop()
            
            if hasattr(self, 'ws_admin') and self.ws_admin:
                await self.ws_admin.stop()
            
            if hasattr(self, 'http_runner') and self.http_runner:
                await self.http_runner.cleanup()
            
            logger.info("✅ 大脑核心已关闭（流式终极版）")
        except Exception as e:
            logger.error(f"关闭出错: {e}")