#!/usr/bin/env python3
"""
大脑核心主控 - Render流式终极版（512MB内存优化）
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
import time
from datetime import datetime

# 设置路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from websocket_pool.admin import WebSocketAdmin
from http_server.server import HTTPServer
from shared_data.data_store import data_store
from shared_data.pipeline_manager import PipelineManager  # ✅ 删除 PipelineConfig

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

class BrainCore:
    def __init__(self):
        async def direct_to_datastore(data: dict):
            """WebSocket回调，直接对接data_store - 添加完整日志"""
            try:
                # 🚨 验证数据
                if not data:
                    logger.warning("[direct_to_datastore] 收到空数据")
                    return
                    
                exchange = data.get("exchange", "")
                symbol = data.get("symbol", "")
                data_type = data.get("data_type", "unknown")
                
                if not exchange:
                    logger.error(f"[direct_to_datastore] 数据缺少exchange字段: {data}")
                    return
                if not symbol:
                    logger.error(f"[direct_to_datastore] 数据缺少symbol字段: {data}")
                    return
                
                # 🚨 添加计数器
                direct_to_datastore.counter = getattr(direct_to_datastore, 'counter', 0) + 1
                
                # 🚨 每10条记录一次（更容易看到）
                if direct_to_datastore.counter % 10 == 0:
                    logger.info(
                        f"📥 [direct_to_datastore#{direct_to_datastore.counter}] "
                        f"{exchange} {symbol} ({data_type})"
                    )
                
                # 🚨 第一条数据特别记录
                if direct_to_datastore.counter == 1:
                    logger.info(f"🎉 [direct_to_datastore] 第一条数据: {exchange} {symbol}")
                    logger.info(f"📋 数据键: {list(data.keys())[:10]}")  # 显示前10个键
                
                # 🚨 调试信息：显示数据大小
                if "raw_data" in data:
                    raw_data_size = len(str(data.get('raw_data', {})))
                    logger.debug(f"[direct_to_datastore] 数据包含raw_data字段，大小: {raw_data_size} 字符")
                
                # 🚨 调用 data_store
                logger.debug(f"[direct_to_datastore] 存储数据: {exchange} {symbol}")
                await data_store.update_market_data(exchange, symbol, data)
                logger.debug(f"[direct_to_datastore] 存储完成: {exchange} {symbol}")
                    
            except TypeError as e:
                logger.error(f"[direct_to_datastore] 参数错误: {e}")
                logger.error(f"[direct_to_datastore] 数据内容: exchange={data.get('exchange')}, symbol={data.get('symbol')}")
            except Exception as e:
                logger.error(f"[direct_to_datastore] 存储失败: {e}")
                logger.error(f"[direct_to_datastore] 失败数据: {data}")
        
        self.ws_admin = WebSocketAdmin(direct_to_datastore)
        self.http_server = None
        self.http_runner = None
        self.running = False
        
        # 初始化资金费率管理器
        self.funding_manager = None
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def receive_processed_data(self, processed_data):
        """接收流水线处理后的成品数据"""
        try:
            data_type = processed_data.get('data_type', 'unknown')
            exchange = processed_data.get('exchange', 'unknown')
            symbol = processed_data.get('symbol', 'unknown')
            
            if data_type.startswith('account_') or data_type in ['order', 'trade']:
                logger.info(f"💰 账户/订单数据: {exchange}.{symbol} ({data_type})")
            else:
                logger.info(f"📊 市场套利数据: {exchange}.{symbol} ({data_type})")
                
        except Exception as e:
            logger.error(f"接收数据错误: {e}")
    
    async def initialize(self):
        """初始化 - 流式终极版"""
        logger.info("=" * 60)
        logger.info("大脑核心启动中（流式终极版，512MB优化）...")
        logger.info("=" * 60)
        
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
            
            # 4. 初始化PipelineManager（流式版，无需配置）
            logger.info("【4️⃣】初始化PipelineManager（流式终极版）...")
            # ✅ 删除 PipelineConfig，直接传回调
            self.pipeline_manager = PipelineManager(
                brain_callback=self.receive_processed_data
            )
            await self.pipeline_manager.start()
            logger.info("✅ 流水线管理员启动完成！")
            
            # 5. 让data_store引用管理员
            data_store.pipeline_manager = self.pipeline_manager
            
            # 6. 初始化资金费率管理器
            logger.info("【5️⃣】初始化资金费率管理器...")
            from funding_settlement import FundingSettlementManager
            self.funding_manager = FundingSettlementManager()
            
            # 7. 启动WebSocket（延迟10秒）
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
        """处理系统信号"""
        logger.info(f"收到信号 {signum}，开始关闭...")
        self.running = False
    
    async def shutdown(self):
        """优雅关闭"""
        self.running = False
        logger.info("正在关闭大脑核心...")
        
        try:
            # 停止PipelineManager
            if hasattr(self, 'pipeline_manager') and self.pipeline_manager:
                await self.pipeline_manager.stop()
            
            # 停止WebSocket
            if hasattr(self, 'ws_admin') and self.ws_admin:
                await self.ws_admin.stop()
            
            # 停止HTTP服务
            if hasattr(self, 'http_runner') and self.http_runner:
                await self.http_runner.cleanup()
            
            logger.info("✅ 大脑核心已关闭（流式终极版）")
        except Exception as e:
            logger.error(f"关闭出错: {e}")

def main():
    """主函数"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    brain = BrainCore()
    
    try:
        asyncio.run(brain.run())
    except KeyboardInterrupt:
        logger.info("程序已停止")
    except Exception as e:
        logger.error(f"程序错误: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()