#!/usr/bin/env python3
"""
大脑核心主控 - Render流式终极版（512MB内存优化）
完整修复版：WebSocket启动参数修复
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
from datetime import datetime

# 设置路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from websocket_pool.admin import WebSocketAdmin
from http_server.server import HTTPServer
from shared_data.data_store import data_store
from shared_data.pipeline_manager import PipelineManager
from websocket_pool.static_symbols import STATIC_SYMBOLS  # ✅ 新增导入

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
            """WebSocket回调，直接对接data_store"""
            try:
                exchange = data.get("exchange")
                symbol = data.get("symbol")
                if exchange and symbol:
                    await data_store.update_market_data(exchange, symbol, data)
            except Exception as e:
                logger.error(f"回调错误: {e}")
        
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
            
            # ✅ 修复：提供必需的合约列表参数
            # 配置：币安 2主2备，欧意 1主1备
            # 每个主连接最多300个合约
            
            all_symbols = {
                "binance": STATIC_SYMBOLS["binance"][:600],  # 2个数据工作者 × 300
                "okx": STATIC_SYMBOLS["okx"][:300]           # 1个数据工作者 × 300
            }
            
            logger.info(f"📊 合约配置:")
            logger.info(f"  币安: {len(all_symbols['binance'])} 个合约 (2个数据工作者)")
            logger.info(f"  欧意: {len(all_symbols['okx'])} 个合约 (1个数据工作者)")
            logger.info(f"  总计: {len(all_symbols['binance']) + len(all_symbols['okx'])} 个合约")
            
            # 启动WebSocket
            success = await self.ws_admin.start(all_symbols)
            
            if success:
                logger.info("✅ WebSocket连接池启动成功")
                logger.info("  币安: 2个数据工作者 + 2个备份工作者")
                logger.info("  欧意: 1个数据工作者 + 1个备份工作者")
                logger.info("  监控: 1个全局监控中心")
            else:
                logger.error("❌ WebSocket连接池启动失败")
                # 尝试用更少的合约重试
                logger.info("🔄 尝试用少量合约启动...")
                fallback_symbols = {
                    "binance": ["BTCUSDT", "ETHUSDT"],
                    "okx": ["BTC-USDT-SWAP"]
                }
                fallback_success = await self.ws_admin.start(fallback_symbols)
                if fallback_success:
                    logger.info("✅ WebSocket连接池（少量合约）启动成功")
                else:
                    logger.error("❌ WebSocket连接池完全启动失败")
        
        except Exception as e:
            logger.error(f"WebSocket初始化失败: {e}")
            logger.error(traceback.format_exc())
    
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
            
            # 启动保活服务（如果需要）
            try:
                start_keep_alive_background()
            except:
                pass
            
            # 主循环
            while self.running:
                await asyncio.sleep(1)
                
                # 定期检查WebSocket状态（可选）
                if hasattr(self, 'ws_admin') and self.ws_admin:
                    try:
                        status = await self.ws_admin.health_check()
                        if not status['healthy']:
                            logger.warning(f"WebSocket健康检查异常: {status.get('message', '未知错误')}")
                    except:
                        pass
        
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
                logger.info("✅ PipelineManager已停止")
            
            # 停止WebSocket
            if hasattr(self, 'ws_admin') and self.ws_admin:
                await self.ws_admin.stop()
                logger.info("✅ WebSocket连接池已停止")
            
            # 停止HTTP服务
            if hasattr(self, 'http_runner') and self.http_runner:
                await self.http_runner.cleanup()
                logger.info("✅ HTTP服务器已停止")
            
            # 停止资金费率管理器
            if hasattr(self, 'funding_manager') and self.funding_manager:
                try:
                    await self.funding_manager.stop()
                    logger.info("✅ 资金费率管理器已停止")
                except:
                    pass
            
            logger.info("✅ 大脑核心已完全关闭")
            
        except Exception as e:
            logger.error(f"关闭出错: {e}")
            logger.error(traceback.format_exc())

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
