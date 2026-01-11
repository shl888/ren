"""
大脑核心主控 - 简洁数据接收版
功能：1. 接收双管道数据 2. 分钟级状态报告
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
import time

# 设置路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from websocket_pool.admin import WebSocketAdmin
from http_server.server import HTTPServer
from shared_data.data_store import data_store
from shared_data.pipeline_manager import PipelineManager

logger = logging.getLogger(__name__)

class BrainCore:
    def __init__(self):
        self.ws_admin = WebSocketAdmin()
        self.http_server = None
        self.http_runner = None
        self.running = False
        
        # 状态统计
        self.status_stats = {
            "last_report_time": 0,
            "report_interval": 60,  # 1分钟报告一次
            
            # 成品数据状态
            "market_data": {
                "last_receive_time": 0,      # 最后收到时间
                "current_batch_count": 0,    # 当前批次数量（不累计）
                "last_batch_time": 0,        # 当前批次时间
            },
            
            # 私人数据状态
            "private_data": {
                "last_account_time": 0,      # 最后账户更新
                "last_order_time": 0,        # 最后订单更新
                "last_connection_time": 0,   # 最后连接状态
            }
        }
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def receive_market_data(self, processed_data):
        """
        【管道1】接收成品数据（CrossPlatformData）
        只更新统计，不打印日志
        """
        try:
            # 更新最后接收时间
            current_time = time.time()
            self.status_stats["market_data"]["last_receive_time"] = current_time
            
            # 计数（每条成品数据就是1个双平台合约）
            self.status_stats["market_data"]["current_batch_count"] += 1
            
            # 记录当前批次时间（用于判断是否是新批次）
            if current_time - self.status_stats["market_data"]["last_batch_time"] > 1.0:
                # 超过1秒，认为是新批次，重置计数
                self.status_stats["market_data"]["current_batch_count"] = 1
                self.status_stats["market_data"]["last_batch_time"] = current_time
            
        except Exception as e:
            # 静默错误，不刷屏
            pass
    
    async def receive_private_data(self, private_data):
        """
        【管道2】接收私人数据
        只更新时间戳，不打印日志
        """
        try:
            current_time = time.time()
            data_type = private_data.get('data_type', 'unknown')
            
            # 根据数据类型更新对应的时间戳
            if data_type == 'account_update':
                self.status_stats["private_data"]["last_account_time"] = current_time
            elif data_type == 'order_update':
                self.status_stats["private_data"]["last_order_time"] = current_time
            elif data_type == 'connection_status':
                self.status_stats["private_data"]["last_connection_time"] = current_time
            
        except Exception as e:
            # 静默错误，不刷屏
            pass
    
    def _format_time_ago(self, timestamp: float) -> str:
        """
        格式化时间差为"X秒前"、"X分钟前"、"X小时前"
        """
        if timestamp == 0:
            return "从未收到"
        
        current_time = time.time()
        diff_seconds = current_time - timestamp
        
        if diff_seconds < 60:  # 1分钟内
            return f"{int(diff_seconds)}秒前"
        elif diff_seconds < 3600:  # 1小时内
            minutes = int(diff_seconds / 60)
            return f"{minutes}分钟前"
        else:  # 超过1小时
            hours = int(diff_seconds / 3600)
            return f"{hours}小时前"
    
    async def _print_status_report(self):
        """打印状态报告（1分钟1次）"""
        current_time = time.time()
        if current_time - self.status_stats["last_report_time"] < self.status_stats["report_interval"]:
            return
        
        try:
            # 获取当前状态
            market_data = self.status_stats["market_data"]
            private_data = self.status_stats["private_data"]
            
            # 计算时间差描述
            market_time_ago = self._format_time_ago(market_data["last_receive_time"])
            account_time_ago = self._format_time_ago(private_data["last_account_time"])
            order_time_ago = self._format_time_ago(private_data["last_order_time"])
            
            # 获取成品数据数量（当前批次）
            market_count = market_data["current_batch_count"]
            
            # 打印简洁状态报告
            logger.info("【大脑数据状态】")
            logger.info(f"成品数据 {market_count} 条已更新。{market_time_ago}")
            logger.info(f"私人数据-账户 {account_time_ago}")
            logger.info(f"私人数据-订单 {order_time_ago}")
            
            # 重置状态（除了时间戳）
            self.status_stats["market_data"]["current_batch_count"] = 0
            self.status_stats["last_report_time"] = current_time
            
        except Exception as e:
            # 静默错误，不刷屏
            pass
    
    async def initialize(self):
        """初始化 - 简洁数据接收版"""
        logger.info("🧠 大脑核心启动（数据接收监控）")
        logger.info("📡 等待数据流入...")
        
        # 🚨 完全静默所有非核心模块日志
        logging.getLogger('shared_data').setLevel(logging.WARNING)
        logging.getLogger('websocket_pool').setLevel(logging.WARNING)
        logging.getLogger('aiohttp').setLevel(logging.WARNING)
        logging.getLogger('asyncio').setLevel(logging.WARNING)
        
        try:
            # 记录启动时间
            self.status_stats["last_report_time"] = time.time()
            
            # 1. 创建HTTP服务器
            port = int(os.getenv('PORT', 10000))
            self.http_server = HTTPServer(host='0.0.0.0', port=port)
            
            # 2. 注册路由
            from funding_settlement.api_routes import setup_funding_settlement_routes
            setup_funding_settlement_routes(self.http_server.app)
            
            # 3. 启动服务器
            await self.start_http_server()
            data_store.set_http_server_ready(True)
            
            # 4. 初始化PipelineManager（双管道）
            self.pipeline_manager = PipelineManager(
                brain_callback=self.receive_market_data,
                private_data_callback=self.receive_private_data
            )
            
            await self.pipeline_manager.start()
            
            # 5. 初始化资金费率管理器
            from funding_settlement import FundingSettlementManager
            self.funding_manager = FundingSettlementManager()
            
            # 6. 延迟启动WebSocket
            asyncio.create_task(self._delayed_ws_init())
            
            self.running = True
            return True
            
        except Exception as e:
            logger.error(f"初始化失败: {e}")
            return False
    
    async def _delayed_ws_init(self):
        """延迟启动WebSocket"""
        await asyncio.sleep(10)
        try:
            await self.ws_admin.start()
        except Exception as e:
            pass  # 静默错误
    
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
        except Exception as e:
            logger.error(f"HTTP服务器启动失败: {e}")
            raise
    
    async def run(self):
        """主循环 - 简洁监控版"""
        try:
            success = await self.initialize()
            if not success:
                return
            
            # 主循环：只做分钟级状态报告
            while self.running:
                await asyncio.sleep(1)
                await self._print_status_report()
        
        except KeyboardInterrupt:
            pass  # 静默退出
        except Exception as e:
            logger.error(f"运行错误: {e}")
        finally:
            await self.shutdown()
    
    def handle_signal(self, signum, frame):
        """处理系统信号"""
        self.running = False
    
    async def shutdown(self):
        """优雅关闭"""
        self.running = False
        
        try:
            if hasattr(self, 'pipeline_manager') and self.pipeline_manager:
                await self.pipeline_manager.stop()
            
            if hasattr(self, 'ws_admin') and self.ws_admin:
                await self.ws_admin.stop()
            
            if hasattr(self, 'http_runner') and self.http_runner:
                await self.http_runner.cleanup()
            
        except Exception as e:
            pass  # 静默关闭错误

def main():
    """主函数"""
    # 简洁日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    brain = BrainCore()
    
    try:
        asyncio.run(brain.run())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"程序错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()