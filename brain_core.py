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
from datetime import datetime

# 设置路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

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

class BrainCore:
    def __init__(self):
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
    
    async def receive_market_data(self, processed_data):
        """
        🚨【管道1】接收市场数据（经过流水线加工的成品套利数据）
        CrossPlatformData 对象转换的字典
        """
        try:
            symbol = processed_data.get('symbol', 'unknown')
            price_diff = processed_data.get('price_diff', 0)
            price_diff_percent = processed_data.get('price_diff_percent', 0)
            rate_diff = processed_data.get('rate_diff', 0)
            
            # 🚨 只记录重要数据，避免日志过多
            if price_diff_percent > 0.1 or rate_diff > 0.0002:  # 阈值
                logger.info(f"🎯【市场数据】套利信号: {symbol} | "
                          f"价差: {price_diff_percent:.4f}% | "
                          f"费率差: {rate_diff:.6f}")
                
                # 这里可以添加交易决策逻辑
                # if price_diff_percent > 0.2:
                #     await self.send_trade_signal(symbol, price_diff)
            
            # 调试用：偶尔记录普通数据
            elif logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"📊【市场数据】普通: {symbol} | "
                           f"价差: {price_diff_percent:.4f}%")
                
        except Exception as e:
            logger.error(f"【市场数据】处理错误: {e}")
    
    async def receive_private_data(self, private_data):
        """
        🚨【管道2】接收私人数据（账户、订单等，直通不加工）
        """
        try:
            data_type = private_data.get('data_type', 'unknown')
            exchange = private_data.get('exchange', 'unknown')
            timestamp = private_data.get('timestamp', '')
            
            if data_type == 'account_update':
                # 处理账户更新
                account_data = private_data.get('data', {})
                balance = account_data.get('total_balance', '未知')
                available = account_data.get('available_balance', '未知')
                
                logger.info(f"💰【私人数据】账户更新: {exchange} | "
                          f"总余额: {balance} | 可用: {available}")
                
                # 更新风险控制
                await self.update_risk_management(exchange, account_data)
                
            elif data_type == 'order_update':
                # 处理订单更新
                order_id = private_data.get('order_id', 'unknown')
                order_data = private_data.get('data', {})
                status = order_data.get('status', 'unknown')
                symbol = order_data.get('symbol', 'unknown')
                
                logger.info(f"📝【私人数据】订单更新: {exchange}.{order_id} | "
                          f"合约: {symbol} | 状态: {status}")
                
                # 监控订单执行
                if status in ['FILLED', 'PARTIALLY_FILLED']:
                    await self.handle_order_filled(exchange, order_id, order_data)
                elif status in ['CANCELED', 'REJECTED']:
                    await self.handle_order_canceled(exchange, order_id, order_data)
            
            else:
                logger.warning(f"⚠️【私人数据】未知类型: {data_type}")
                
        except Exception as e:
            logger.error(f"【私人数据】处理错误: {e}")
    
    async def update_risk_management(self, exchange: str, account_data: dict):
        """更新风险控制"""
        try:
            # 这里实现风险控制逻辑
            # 例如：检查仓位、计算风险度等
            logger.debug(f"🛡️  更新{exchange}风险控制")
            
            # 示例：检查余额是否过低
            available = float(account_data.get('available_balance', 0))
            if available < 100:  # 假设阈值
                logger.warning(f"⚠️【风险】{exchange}可用余额过低: {available}")
                
        except Exception as e:
            logger.error(f"风险控制更新错误: {e}")
    
    async def handle_order_filled(self, exchange: str, order_id: str, order_data: dict):
        """处理订单成交"""
        try:
            symbol = order_data.get('symbol', 'unknown')
            filled_qty = order_data.get('filled_qty', 0)
            avg_price = order_data.get('avg_price', 0)
            
            logger.info(f"✅ 订单成交: {exchange}.{order_id} | "
                       f"合约: {symbol} | 数量: {filled_qty} | 均价: {avg_price}")
            
            # 更新仓位
            await self.update_position(exchange, symbol, filled_qty, avg_price)
            
        except Exception as e:
            logger.error(f"订单成交处理错误: {e}")
    
    async def handle_order_canceled(self, exchange: str, order_id: str, order_data: dict):
        """处理订单取消"""
        try:
            symbol = order_data.get('symbol', 'unknown')
            reason = order_data.get('cancel_reason', '未知')
            
            logger.warning(f"❌ 订单取消: {exchange}.{order_id} | "
                          f"合约: {symbol} | 原因: {reason}")
            
        except Exception as e:
            logger.error(f"订单取消处理错误: {e}")
    
    async def update_position(self, exchange: str, symbol: str, qty: float, price: float):
        """更新仓位（示例）"""
        logger.debug(f"📊 更新仓位: {exchange}.{symbol} | 数量: {qty} | 价格: {price}")
    
    async def initialize(self):
        """初始化 - 双管道流式版"""
        logger.info("=" * 60)
        logger.info("大脑核心启动中（双管道流式版）...")
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
            
            # 4. ✅【关键修改】初始化PipelineManager（双管道）
            logger.info("【4️⃣】初始化PipelineManager（双管道流式版）...")
            
            # 使用实例方法而不是类方法，传递双回调
            self.pipeline_manager = PipelineManager(
                brain_callback=self.receive_market_data,           # 市场数据回调
                private_data_callback=self.receive_private_data    # ✅ 新增：私人数据回调
            )
            
            await self.pipeline_manager.start()
            logger.info("✅ 流水线管理员启动完成（双管道）！")
            
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
            logger.info("🚀 大脑核心启动完成！（双管道流式版）")
            logger.info("📡 数据管道:")
            logger.info("  • 管道1: 市场数据 → 流水线 → 套利信号 → 大脑")
            logger.info("  • 管道2: 私人数据 → 直通 → 大脑（实时）")
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
        """主循环 - 双管道版"""
        try:
            success = await self.initialize()
            if not success:
                logger.error("初始化失败，程序退出")
                return
            
            logger.info("=" * 60)
            logger.info("🚀 大脑核心运行中（双管道流式版）...")
            logger.info("📊 等待数据流入:")
            logger.info("  • 市场数据: 定时1秒流水线处理")
            logger.info("  • 私人数据: 实时直通")
            logger.info("🛑 按 Ctrl+C 停止")
            logger.info("=" * 60)
            
            # 保持运行
            while self.running:
                await asyncio.sleep(1)
                
                # 可选：定期检查状态
                if logger.isEnabledFor(logging.DEBUG):
                    await self._check_system_status()
        
        except KeyboardInterrupt:
            logger.info("收到键盘中断")
        except Exception as e:
            logger.error(f"运行错误: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.shutdown()
    
    async def _check_system_status(self):
        """检查系统状态（调试用）"""
        try:
            # 每60秒检查一次
            import time
            if hasattr(self, '_last_status_check'):
                if time.time() - self._last_status_check < 60:
                    return
            
            self._last_status_check = time.time()
            
            # 获取流水线状态
            if hasattr(self, 'pipeline_manager'):
                status = self.pipeline_manager.get_system_status()
                market_processed = status.get('stats', {}).get('total_processed', 0)
                private_account = status.get('stats', {}).get('private_data', {}).get('account_updates', 0)
                private_order = status.get('stats', {}).get('private_data', {}).get('order_updates', 0)
                
                logger.debug(f"📈 系统状态 | "
                           f"市场数据: {market_processed}条 | "
                           f"账户更新: {private_account}次 | "
                           f"订单更新: {private_order}次")
            
        except Exception as e:
            logger.debug(f"状态检查错误: {e}")
    
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
            
            logger.info("✅ 大脑核心已关闭（双管道流式版）")
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