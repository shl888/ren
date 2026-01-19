# smart_brain/core.py
"""
大脑核心主控 - Render流式终极版（512MB内存优化）
智能大脑版本 - 重构自原brain_core.py
支持双管道数据流：市场数据 + 私人数据
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
from datetime import datetime

# 设置路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from websocket_pool.admin import WebSocketAdmin
from http_server.server import HTTPServer
from shared_data.data_store import data_store
from shared_data.pipeline_manager import PipelineManager

from .data_receiver import DataReceiver
from .status_monitor import StatusMonitor

logger = logging.getLogger(__name__)


class SmartBrain:
    def __init__(self):
        # 初始化子组件
        self.data_receiver = DataReceiver()
        self.status_monitor = StatusMonitor(self.data_receiver)
        
        # ✅ 不传递任何回调，让WebSocketAdmin使用pool_manager的默认回调
        self.ws_admin = WebSocketAdmin()
        self.http_server = None
        self.http_runner = None
        self.running = False
        
        # 初始化资金费率管理器
        self.funding_manager = None
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        # Pipeline管理器
        self.pipeline_manager = None
        
        # 状态日志定时器（由status_monitor管理）
    
    async def initialize(self):
        """初始化 - 流式终极版"""
        logger.info("=" * 60)
        logger.info("🧠 智能大脑启动中（流式终极版，512MB优化）...")
        logger.info("=" * 60)
        
        # 🚨【临时关闭shared_data日志】- 要恢复日志请注释掉这一行
        # logging.getLogger('shared_data').setLevel(logging.ERROR)
        
        try:
            # 1. 创建HTTP服务器
            port = int(os.getenv('PORT', 10000))
            logger.info(f"【1️⃣】创建HTTP服务器 (端口: {port})...")
            self.http_server = HTTPServer(host='0.0.0.0', port=port)
            
            # 2. 注册路由
            logger.info("【2️⃣】注册路由...")
            from funding_settlement.api_routes import setup_funding_settlement_routes
            setup_funding_settlement_routes(self.http_server.app)
            
            # 3. 启动服务器
            logger.info("【3️⃣】启动HTTP服务器...")
            await self.start_http_server()
            data_store.set_http_server_ready(True)
            logger.info("✅ HTTP服务已就绪！")
            
            # 4. 初始化PipelineManager（双管道）
            logger.info("【4️⃣】初始化PipelineManager（双管道）...")
            self.pipeline_manager = PipelineManager(
                brain_callback=self.data_receiver.receive_market_data,
                private_data_callback=self.data_receiver.receive_private_data
            )
            await self.pipeline_manager.start()
            logger.info("✅ 流水线管理员启动完成！")
            
            # 5. 让data_store引用管理员
            data_store.pipeline_manager = self.pipeline_manager
            
            # 6. 初始化资金费率管理器
            logger.info("【5️⃣】初始化资金费率管理器...")
            from funding_settlement import FundingSettlementManager
            self.funding_manager = FundingSettlementManager()
            
            # 7. 启动状态监控
            logger.info("【6️⃣】启动状态监控...")
            self.status_monitor.start()
            
            # 8. 启动WebSocket（延迟10秒）
            asyncio.create_task(self._delayed_ws_init())
            
            self.running = True
            logger.info("=" * 60)
            logger.info("🚀 智能大脑启动完成！（流式终极版）")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"🚨 初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _delayed_ws_init(self):
        """延迟10秒启动WebSocket"""
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
        """主循环 - 流式版"""
        try:
            success = await self.initialize()
            if not success:
                logger.error("初始化失败，程序退出")
                return
            
            logger.info("=" * 60)
            logger.info("🧠 智能大脑运行中（流式终极版，512MB优化）...")
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
        """处理系统信号"""
        logger.info(f"收到信号 {signum}，开始关闭...")
        self.running = False
    
    async def shutdown(self):
        """优雅关闭"""
        self.running = False
        logger.info("正在关闭智能大脑...")
        
        try:
            # 停止状态监控
            await self.status_monitor.stop()
            
            # 停止PipelineManager
            if self.pipeline_manager:
                await self.pipeline_manager.stop()
            
            # 停止WebSocket
            if self.ws_admin:
                await self.ws_admin.stop()
            
            # 停止HTTP服务
            if self.http_runner:
                await self.http_runner.cleanup()
            
            logger.info("✅ 智能大脑已关闭（流式终极版）")
        except Exception as e:
            logger.error(f"关闭出错: {e}")
            