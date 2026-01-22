"""
极简启动器 - 重构版：接管所有模块启动
按照新流程：大脑初始化时会获取令牌，然后启动连接管理器
"""

import asyncio
import logging
import sys
import traceback
import os
import signal
from datetime import datetime
import threading

# ==================== 新增：加载环境变量 ====================
from dotenv import load_dotenv
load_dotenv()  # 从 .env 文件加载环境变量
# =======================================================

# 设置路径
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_FILE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from websocket_pool.admin import WebSocketAdmin
from http_server.server import HTTPServer
from shared_data.pipeline_manager import PipelineManager
from frontend_relay import FrontendRelayServer
from funding_settlement import FundingSettlementManager
from smart_brain.core import SmartBrain

logger = logging.getLogger(__name__)

def start_keep_alive_background():
    """启动保活服务（后台线程）"""
    try:
        from keep_alive import start_with_http_check
        
        def run_keeper():
            try:
                start_with_http_check()
            except Exception as e:
                logger.error(f"【智能大脑】保活服务异常: {e}")
        
        thread = threading.Thread(target=run_keeper, daemon=True)
        thread.start()
        logger.info("✅ 【智能大脑】保活服务已启动")
    except Exception as e:
        logger.warning(f"⚠️ 【智能大脑】保活服务未启动: {e}")

async def start_http_server(http_server):
    """启动HTTP服务器"""
    try:
        from aiohttp import web
        port = int(os.getenv('PORT', 10000))
        host = '0.0.0.0'
        
        runner = web.AppRunner(http_server.app)
        await runner.setup()
        
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        logger.info(f"✅ HTTP服务器已启动: http://{host}:{port}")
        return runner
    except Exception as e:
        logger.error(f"启动HTTP服务器失败: {e}")
        raise

async def delayed_ws_init(ws_admin):
    """延迟启动WebSocket连接池"""
    await asyncio.sleep(10)
    try:
        logger.info("⏳ 延迟启动WebSocket...")
        await ws_admin.start()
        logger.info("✅ WebSocket初始化完成")
    except Exception as e:
        logger.error(f"WebSocket初始化失败: {e}")

async def delayed_start_private_connections(brain):
    """延迟启动私人连接（等HTTP服务器就绪后）"""
    await asyncio.sleep(8)  # 等待HTTP服务器稳定
    
    try:
        logger.info("🔄 准备启动私人连接...")
        
        # 检查连接管理器状态
        cm_status = brain.get_connection_manager_status()
        if cm_status.get('initialized'):
            logger.info("✅ 私人连接管理器已初始化，开始启动连接...")
            
            # 启动私人连接
            success = await brain.start_private_connections()
            
            if success:
                logger.info("✅ 私人连接启动命令已发送")
                
                # 检查连接状态
                await asyncio.sleep(5)
                final_status = brain.get_connection_manager_status()
                
                logger.info(f"📊 私人连接状态:")
                logger.info(f"  • 币安: {final_status.get('connections', {}).get('binance', {}).get('status', 'unknown')}")
                logger.info(f"  • 欧意: {final_status.get('connections', {}).get('okx', {}).get('status', 'unknown')}")
                
                # 检查令牌状态
                if hasattr(brain.data_manager, 'has_binance_token'):
                    has_token = brain.data_manager.has_binance_token()
                    token_status = "✅ 有效" if has_token else "❌ 缺失"
                    logger.info(f"  • 币安令牌: {token_status}")
            else:
                logger.error("❌ 启动私人连接失败")
        else:
            logger.warning("⚠️ 私人连接管理器未初始化，跳过连接启动")
            
    except Exception as e:
        logger.error(f"❌ 启动私人连接异常: {e}")

async def main():
    """主启动函数 - 完全按照大脑原来的启动顺序"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 启动保活服务
    start_keep_alive_background()
    
    logger.info("=" * 60)
    logger.info("🚀 智能大脑启动中（新流程：启动器负责所有模块启动）...")
    logger.info("=" * 60)
    
    brain = None  # 提前声明变量
    
    try:
        # ==================== 新增：验证环境变量 ====================
        # 检查必要的环境变量
        required_vars = ['BINANCE_API_KEY', 'BINANCE_API_SECRET', 
                        'OKX_API_KEY', 'OKX_API_SECRET']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            logger.warning(f"⚠️ 以下环境变量未设置，私人连接可能不可用: {missing_vars}")
        else:
            logger.info("✅ 所有私人连接环境变量已就绪")
        # =========================================================
        
        # ==================== 1. 获取端口并创建HTTP服务器 ====================
        logger.info("【1️⃣】创建HTTP服务器...")
        port = int(os.getenv('PORT', 10000))
        http_server = HTTPServer(host='0.0.0.0', port=port)
        
        # ==================== 2. 注册路由（基础路由 + 大脑路由） ====================
        logger.info("【2️⃣】注册路由...")
        
        # 2.1 创建大脑实例（仅实例化，不建立连接）
        logger.info("【2️⃣.1】创建精简版大脑实例...")
        brain = SmartBrain(
            http_server=http_server,
            http_runner=None,  # 稍后设置
            pipeline_manager=None,  # 稍后设置
            funding_manager=None,  # 稍后设置
            frontend_relay=None
        )
        
        # 2.2 ✅ 注册大脑DataManager API路由到主服务器（带计数）
        logger.info("【2️⃣.2】注册大脑DataManager API路由到主服务器...")
        
        app = http_server.app
        brain_routes_count = 0  # ✅ 初始化计数器
        
        # 基本路由
        app.router.add_get('/api/brain/', brain.data_manager.handle_api_root)
        brain_routes_count += 1
        app.router.add_get('/api/brain/health', brain.data_manager.handle_health)
        brain_routes_count += 1
        
        # 数据查看路由
        app.router.add_get('/api/brain/data', brain.data_manager.handle_get_all_data)
        brain_routes_count += 1
        app.router.add_get('/api/brain/data/market', brain.data_manager.handle_get_market_data)
        brain_routes_count += 1
        app.router.add_get('/api/brain/data/market/{exchange}', brain.data_manager.handle_get_market_data_by_exchange)
        brain_routes_count += 1
        app.router.add_get('/api/brain/data/market/{exchange}/{symbol}', brain.data_manager.handle_get_market_data_detail)
        brain_routes_count += 1
        
        # 私人数据路由
        app.router.add_get('/api/brain/data/private', brain.data_manager.handle_get_private_data)
        brain_routes_count += 1
        app.router.add_get('/api/brain/data/private/{exchange}', brain.data_manager.handle_get_private_data_by_exchange)
        brain_routes_count += 1
        app.router.add_get('/api/brain/data/private/{exchange}/{data_type}', brain.data_manager.handle_get_private_data_detail)
        brain_routes_count += 1
        
        # 状态路由
        app.router.add_get('/api/brain/apis', brain.data_manager.handle_get_apis)
        brain_routes_count += 1
        app.router.add_get('/api/brain/status', brain.data_manager.handle_get_status)
        brain_routes_count += 1
        
        # 新增：令牌状态路由
        app.router.add_get('/api/brain/token-status', brain.data_manager.handle_get_token_status)
        brain_routes_count += 1
        
        # 清空数据路由
        app.router.add_delete('/api/brain/data/clear', brain.data_manager.handle_clear_data)
        brain_routes_count += 1
        app.router.add_delete('/api/brain/data/clear/{data_type}', brain.data_manager.handle_clear_data_type)
        brain_routes_count += 1
        
        # ✅ 输出大脑路由统计
        logger.info(f"✅ 大脑DataManager API路由已注册到主服务器（共 {brain_routes_count} 条）")
        logger.info("📊 大脑数据查看地址:")
        logger.info(f"  • 所有数据: /api/brain/data")
        logger.info(f"  • 市场数据: /api/brain/data/market")
        logger.info(f"  • 私人数据: /api/brain/data/private")
        logger.info(f"  • 系统状态: /api/brain/status")
        logger.info(f"  • API状态: /api/brain/apis")
        logger.info(f"  • 令牌状态: /api/brain/token-status")
        logger.info(f"  • 清空数据: /api/brain/data/clear (谨慎使用)")
        
        # 2.3 注册基础路由（资金费率）
        from funding_settlement.api_routes import setup_funding_settlement_routes
        setup_funding_settlement_routes(http_server.app)

        # ==================== 3. 启动HTTP服务器 ====================
        logger.info("【3️⃣】启动HTTP服务器...")
        http_runner = await start_http_server(http_server)
        
        from shared_data.data_store import data_store
        data_store.set_http_server_ready(True)
        logger.info("✅ HTTP服务已就绪！")

        # ==================== 4. 初始化PipelineManager（双管道） ====================
        logger.info("【4️⃣】初始化PipelineManager（双管道）...")
        pipeline_manager = PipelineManager()
        
        # ==================== 5. 初始化资金费率管理器 ====================
        logger.info("【5️⃣】初始化资金费率管理器...")
        funding_manager = FundingSettlementManager()
        
        # ==================== 6. 更新大脑实例（补全依赖） ====================
        logger.info("【6️⃣】更新大脑实例...")
        brain.http_runner = http_runner
        brain.pipeline_manager = pipeline_manager
        brain.funding_manager = funding_manager
        
        # 设置数据存储的引用
        data_store.pipeline_manager = pipeline_manager
        
        # ==================== 7. 大脑初始化（新流程：获取令牌 → 初始化连接管理器） ====================
        logger.info("【7️⃣】大脑初始化（新流程）...")
        logger.info("📝 新流程：大脑 → HTTP模块 → 获取令牌 → 初始化连接管理器")
        
        brain_init_success = await brain.initialize()
        
        if not brain_init_success:
            logger.error("❌ 大脑初始化失败，程序将退出")
            return
        
        # 检查私人连接管理器状态
        if hasattr(brain, 'private_connection_manager'):
            cm_status = brain.get_connection_manager_status()
            pm_status = "✅ 已初始化" if cm_status.get('initialized') else "❌ 初始化失败"
            logger.info(f"🧠 私人连接管理器状态: {pm_status}")
            
            # 显示详细状态
            logger.info(f"📊 连接管理器详情:")
            logger.info(f"  • 运行状态: {'✅ 运行中' if cm_status.get('running') else '❌ 已停止'}")
            logger.info(f"  • 币安状态: {cm_status.get('connections', {}).get('binance', {}).get('status', 'unknown')}")
            logger.info(f"  • 欧意状态: {cm_status.get('connections', {}).get('okx', {}).get('status', 'unknown')}")
        
        # ==================== 8. 初始化前端中继（需要大脑实例） ====================
        logger.info("【8️⃣】初始化前端中继服务器...")
        try:
            # 现在有大脑实例了，创建前端中继
            frontend_relay = FrontendRelayServer(
                brain_instance=brain,  # ✅ 传入大脑实例
                port=10001
            )
            await frontend_relay.start()
            
            # ✅ 将前端中继设置回大脑
            brain.frontend_relay = frontend_relay
            
            logger.info("✅ 前端中继启动完成！")
        except ImportError:
            logger.warning("⚠️ 前端中继模块未找到，跳过前端功能")
        except Exception as e:
            logger.error(f"❌ 前端中继启动失败: {e}")
        
        # ==================== 9. 设置PipelineManager回调 ====================
        logger.info("【9️⃣】设置数据处理回调...")
        pipeline_manager.set_brain_callback(brain.data_manager.receive_market_data)
        pipeline_manager.set_private_data_callback(brain.data_manager.receive_private_data)
        
        # ==================== 10. 启动数据处理管道 ====================
        logger.info("【🔟】启动数据处理管道...")
        await pipeline_manager.start()
        
        # ==================== 11. 延迟启动WebSocket ====================
        logger.info("【1️⃣1️⃣】准备延迟启动WebSocket...")
        ws_admin = WebSocketAdmin()
        asyncio.create_task(delayed_ws_init(ws_admin))
        brain.ws_admin = ws_admin  # 传递给大脑
        
        # ==================== 12. 延迟启动私人连接（等HTTP服务器完全就绪） ====================
        logger.info("【1️⃣2️⃣】准备启动私人连接（延迟执行）...")
        asyncio.create_task(delayed_start_private_connections(brain))
        
        # ==================== 完成初始化 ====================
        brain.running = True
        logger.info("=" * 60)
        logger.info("🎉 所有模块启动完成！")
        logger.info("=" * 60)
        
        # 显示最终状态汇总
        await asyncio.sleep(3)
        logger.info("📋 系统状态汇总:")
        logger.info(f"  • HTTP服务器: ✅ 运行中 (端口: {port})")
        logger.info(f"  • 数据处理管道: ✅ 已启动")
        logger.info(f"  • WebSocket连接池: ⏳ 延迟启动中")
        
        # 检查大脑数据管理器状态
        if brain.data_manager:
            has_binance_token = brain.data_manager.has_binance_token()
            token_status = "✅ 有效" if has_binance_token else "❌ 缺失"
            logger.info(f"  • 币安令牌状态: {token_status}")
            
            # 检查API状态
            binance_apis = brain.data_manager.memory_store['env_apis'].get('binance', {})
            okx_apis = brain.data_manager.memory_store['env_apis'].get('okx', {})
            
            binance_api_status = "✅ 可用" if binance_apis.get('api_key') else "❌ 缺失"
            okx_api_status = "✅ 可用" if okx_apis.get('api_key') else "❌ 缺失"
            
            logger.info(f"  • 币安API: {binance_api_status}")
            logger.info(f"  • 欧意API: {okx_api_status}")
        
        logger.info("=" * 60)
        
        # ==================== 13. 运行大脑 ====================
        logger.info("🚀 大脑核心运行中...")
        logger.info("🛑 按 Ctrl+C 停止")
        logger.info("=" * 60)
        
        # 主循环
        while brain.running:
            await asyncio.sleep(1)
        
    except KeyboardInterrupt:
        logger.info("收到键盘中断")
    except Exception as e:
        logger.error(f"运行错误: {e}")
        logger.error(traceback.format_exc())
    finally:
        # 关闭
        if brain:
            await brain.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
    