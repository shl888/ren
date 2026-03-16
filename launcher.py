"""
极简启动器 - 重构版：接管所有模块启动（多线程并行版）
"""

import asyncio
import logging
import sys
import traceback
import os
import signal
from datetime import datetime
import threading

# ==================== 强制启动标记 ====================
print("🚨🚨🚨 LAUNCHER.PY 开始执行", file=sys.stderr)
sys.stderr.flush()  # 强制刷新，确保输出
# ====================================================

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

from public_http_fetcher.binance_funding_rate import FundingSettlementManager
from smart_brain.core import SmartBrain

# ✅ 导入设置brain实例的函数
from smart_brain import set_brain_instance

logger = logging.getLogger(__name__)

# ==================== 新增：模块线程管理器 ====================
class ModuleThreadManager:
    """模块线程管理器 - 让每个模块在独立线程中运行"""
    
    def __init__(self):
        self.threads = []
        self.modules = {}
    
    def start_module(self, name, async_func, *args, **kwargs):
        """在独立线程中启动模块"""
        def run_in_thread():
            """线程函数 - 创建新的事件循环运行模块"""
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                logger.info(f"🧵【{name}】线程启动")
                loop.run_until_complete(async_func(*args, **kwargs))
            except Exception as e:
                logger.error(f"❌【{name}】线程异常: {e}")
            finally:
                loop.close()
                logger.info(f"🛑【{name}】线程结束")
        
        thread = threading.Thread(
            target=run_in_thread,
            name=f"module_{name}",
            daemon=True
        )
        thread.start()
        self.threads.append(thread)
        self.modules[name] = thread
        logger.info(f"✅【{name}】模块在独立线程中启动")
        return thread
    
    def check_threads(self):
        """检查所有线程状态"""
        for name, thread in self.modules.items():
            if not thread.is_alive():
                logger.error(f"⚠️【{name}】线程已死")
                return False
        return True

# 保活服务（保持原来的线程方式）
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

async def safe_get_pool_status(pool):
    """安全获取连接池状态"""
    try:
        if not pool:
            return {'connections': {}}
        
        status = pool.get_status()
        # 确保 status 是字典
        if not isinstance(status, dict):
            return {'connections': {}}
        
        # 确保有 connections 键
        if 'connections' not in status:
            status['connections'] = {}
        
        return status
    except Exception as e:
        logger.error(f"获取连接池状态失败: {e}")
        return {'connections': {}}

# ==================== 各个模块的入口函数 ====================
async def run_public_websocket(brain):
    """公共WebSocket模块入口"""
    ws_admin = WebSocketAdmin()
    await delayed_ws_init(ws_admin)
    brain.ws_admin = ws_admin
    # 这里可以添加保活逻辑
    while True:
        await asyncio.sleep(60)

async def run_private_websocket(brain):
    """私人WebSocket模块入口"""
    try:
        from private_ws_pool import PrivateWebSocketPool
        private_pool = PrivateWebSocketPool()
        await private_pool.start(brain.data_manager)
        brain.private_pool = private_pool
        logger.info("✅ 私人WebSocket连接池启动成功")
    except Exception as e:
        logger.error(f"❌ 启动私人连接池失败: {e}")
    # 这里可以添加保活逻辑
    while True:
        await asyncio.sleep(60)

async def run_public_pipeline(brain):
    """公开数据处理模块入口"""
    try:
        # 启动数据处理管道
        await brain.pipeline_manager.start()
        logger.info("✅ 公开数据处理管道启动成功")
    except Exception as e:
        logger.error(f"❌ 公开数据处理管道启动失败: {e}")
    while True:
        await asyncio.sleep(60)

async def run_private_pipeline(brain):
    """私人数据处理模块入口"""
    try:
        # 私人数据处理已经在 brain.initialize() 中启动
        logger.info("✅ 私人数据处理模块已就绪")
    except Exception as e:
        logger.error(f"❌ 私人数据处理模块异常: {e}")
    while True:
        await asyncio.sleep(60)

async def run_completion_module(brain):
    """成品数据完成模块入口"""
    try:
        from data_completion_department import (
            get_receiver,
            DataDetector,
            Scheduler,
            Database,
            BinanceRepairArea,
            OkxMissingRepair,
        )
        
        # 1. 创建接收器实例
        data_receiver = get_receiver()
        
        # 2. 创建数据库实例
        database = Database()
        await database.initialize()
        
        # 3. 创建调度器
        scheduler = Scheduler(brain.data_manager)
        
        # 4. 创建检测区
        detector = DataDetector(scheduler)
        
        # 5. 创建修复区实例
        binance_repair = BinanceRepairArea(scheduler)
        okx_repair = OkxMissingRepair(scheduler)
        
        # 6. 建立连接
        data_receiver.subscribe(detector.handle_store_snapshot)
        data_receiver.subscribe(binance_repair.handle_store_snapshot)
        data_receiver.subscribe(okx_repair.handle_store_snapshot)
        
        # 7. 调度器连接下游模块
        scheduler.set_database(database)
        scheduler.set_repair_binance(binance_repair)
        scheduler.set_repair_okx(okx_repair)
        
        # 8. 保存到brain实例
        brain.data_receiver = data_receiver
        brain.data_detector = detector
        brain.data_scheduler = scheduler
        brain.data_database = database
        brain.binance_repair = binance_repair
        brain.okx_repair = okx_repair
        
        logger.info("✅ 成品数据完成模块启动成功")
    except Exception as e:
        logger.error(f"❌ 成品数据完成模块启动失败: {e}")
    while True:
        await asyncio.sleep(60)

async def main():
    """主启动函数"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout
    )
    
    logger.info("🚨🚨🚨 MAIN 函数开始执行")
    
    # 启动保活服务
    start_keep_alive_background()
    
    logger.info("=" * 60)
    logger.info("🚀 智能大脑启动中...")
    logger.info("=" * 60)
    
    brain = None
    module_manager = ModuleThreadManager()
    
    try:
        # ==================== 验证环境变量 ====================
        required_vars = ['BINANCE_API_KEY', 'BINANCE_API_SECRET', 
                        'OKX_API_KEY', 'OKX_API_SECRET']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            logger.warning(f"⚠️ 以下环境变量未设置: {missing_vars}")
        else:
            logger.info("✅ 所有环境变量已就绪")
        
        # ==================== 1. 创建大脑实例 ====================
        logger.info("【1️⃣】创建大脑实例...")
        brain = SmartBrain(
            http_server=None,
            http_runner=None,
            pipeline_manager=None,
            funding_manager=None,
            frontend_relay=None
        )
        
        # ✅ 设置全局brain实例
        set_brain_instance(brain)
        logger.info("✅ 全局大脑实例已设置")
        
        # ==================== 2. 创建HTTP服务器 ====================
        logger.info("【2️⃣】创建HTTP服务器...")
        port = int(os.getenv('PORT', 10000))
        http_server = HTTPServer(host='0.0.0.0', port=port, brain=brain)
        brain.http_server = http_server
        
        # ==================== 3. 启动HTTP服务器 ====================
        logger.info("【3️⃣】启动HTTP服务器...")
        http_runner = await start_http_server(http_server)
        brain.http_runner = http_runner
        
        from shared_data.data_store import data_store
        data_store.set_http_server_ready(True)
        logger.info("✅ HTTP服务已就绪！")

        # ==================== 4. 初始化PipelineManager ====================
        logger.info("【4️⃣】初始化PipelineManager...")
        pipeline_manager = PipelineManager()
        brain.pipeline_manager = pipeline_manager
        
        # ==================== 5. 初始化资金费率管理器 ====================
        logger.info("【5️⃣】初始化资金费率管理器...")
        funding_manager = FundingSettlementManager()
        brain.funding_manager = funding_manager
        data_store.pipeline_manager = pipeline_manager
        
        # ==================== 6. 大脑初始化 ====================
        logger.info("【6️⃣】大脑初始化...")
        brain_init_success = await brain.initialize()
        if not brain_init_success:
            logger.error("❌ 大脑初始化失败")
            return
        
        # ==================== 7. 初始化前端中继 ====================
        logger.info("【7️⃣】初始化前端中继服务器...")
        try:
            frontend_relay = FrontendRelayServer(
                brain_instance=brain,
                port=10001
            )
            await frontend_relay.start()
            brain.frontend_relay = frontend_relay
            logger.info("✅ 前端中继启动完成！")
        except ImportError:
            logger.warning("⚠️ 前端中继模块未找到")
        except Exception as e:
            logger.error(f"❌ 前端中继启动失败: {e}")
        
        # ==================== 8. 设置PipelineManager回调 ====================
        logger.info("【8️⃣】设置数据处理回调...")
        pipeline_manager.set_brain_callback(brain.data_manager.receive_market_data)
        
        # ==================== 9. 启动各个模块的独立线程 ====================
        logger.info("=" * 60)
        logger.info("🚀 启动各个模块独立线程...")
        logger.info("=" * 60)
        
        # 模块A：公共WebSocket
        module_manager.start_module("public_websocket", run_public_websocket, brain)
        
        # 模块B：私人WebSocket
        module_manager.start_module("private_websocket", run_private_websocket, brain)
        
        # 模块C：公开数据处理
        module_manager.start_module("public_pipeline", run_public_pipeline, brain)
        
        # 模块D：私人数据处理
        module_manager.start_module("private_pipeline", run_private_pipeline, brain)
        
        # 模块E：成品数据完成
        module_manager.start_module("completion", run_completion_module, brain)
        
        # ==================== 10. 启动其他异步任务 ====================
        logger.info("【🔟】启动其他异步任务...")
        
        # 启动币安令牌任务
        try:
            from private_http_fetcher.binance_token.listen_key_manager import ListenKeyManager
            token_manager = ListenKeyManager(brain.data_manager)
            await token_manager.start()
            brain.token_manager = token_manager
            logger.info("✅ 币安令牌任务已启动")
        except Exception as e:
            logger.error(f"❌ 启动币安令牌任务失败: {e}")
        
        # 启动OKX合约面值系统
        try:
            from public_http_fetcher.okx_contract_info.fetcher import OKXContractFetcher
            from public_http_fetcher.okx_contract_info.cleaner import OKXContractCleaner
            
            okx_fetcher = OKXContractFetcher()
            raw_data = await okx_fetcher.startup_fetch()
            
            if raw_data:
                okx_cleaner = OKXContractCleaner()
                await okx_cleaner.clean_and_push(raw_data)
                brain.okx_cleaner = okx_cleaner
            
            brain.okx_fetcher = okx_fetcher
            logger.info("✅ OKX合约面值系统启动完成")
        except Exception as e:
            logger.error(f"❌ 启动OKX合约面值系统失败: {e}")
        
        # 启动币安资产获取任务
        try:
            from private_http_fetcher.binance_account.fetcher import PrivateHTTPFetcher
            account_fetcher = PrivateHTTPFetcher()
            await account_fetcher.start(brain.data_manager)
            brain.private_fetcher = account_fetcher
            logger.info("✅ 币安资产获取任务已启动")
        except Exception as e:
            logger.error(f"❌ 启动币安资产获取任务失败: {e}")
        
        # ==================== 完成初始化 ====================
        brain.running = True
        logger.info("=" * 60)
        logger.info("🎉 所有模块启动完成！")
        logger.info("=" * 60)
        
        # 输出连接池状态
        if brain.private_pool:
            pool_status = await safe_get_pool_status(brain.private_pool)
            connections = pool_status.get('connections', {})
            
            logger.info(f"🔗 私人连接池状态: 运行中")
            if connections:
                for exchange, status in connections.items():
                    if status.get('connected', False):
                        logger.info(f"  • {exchange}: ✅ 已连接")
                    else:
                        logger.info(f"  • {exchange}: ⏳ 连接中...")
            else:
                logger.info(f"  • 连接池正在初始化中，稍后查看状态")
        
        # ==================== 主监控循环 ====================
        logger.info("🚀 主监控循环运行中...")
        logger.info("🛑 按 Ctrl+C 停止")
        logger.info("=" * 60)
        
        while brain.running:
            await asyncio.sleep(5)  # 每5秒检查一次
            # 检查所有模块线程是否健康
            if not module_manager.check_threads():
                logger.error("⚠️ 有模块线程异常，但系统继续运行")
        
    except KeyboardInterrupt:
        logger.info("收到键盘中断")
    except Exception as e:
        logger.error(f"运行错误: {e}")
        logger.error(traceback.format_exc())
    finally:
        if brain:
            await brain.shutdown()

if __name__ == "__main__":
    print("🚨🚨🚨 进入 __main__", file=sys.stderr)
    sys.stderr.flush()
    asyncio.run(main())