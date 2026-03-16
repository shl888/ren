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
from concurrent.futures import ThreadPoolExecutor

# ==================== 强制启动标记 ====================
print("🚨🚨🚨 LAUNCHER.PY 开始执行", file=sys.stderr)
sys.stderr.flush()
# ====================================================

# ==================== 加载环境变量 ====================
from dotenv import load_dotenv
load_dotenv()
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
from smart_brain import set_brain_instance

logger = logging.getLogger(__name__)

# ==================== 全局事件循环管理器 ====================
class LoopManager:
    """事件循环管理器 - 让线程可以访问主事件循环"""
    
    _main_loop = None
    _executor = ThreadPoolExecutor(max_workers=4)
    
    @classmethod
    def set_main_loop(cls, loop):
        """设置主事件循环"""
        cls._main_loop = loop
    
    @classmethod
    def get_main_loop(cls):
        """获取主事件循环"""
        return cls._main_loop
    
    @classmethod
    async def run_in_main(cls, coro):
        """在主事件循环中运行协程"""
        if not cls._main_loop:
            raise RuntimeError("主事件循环未设置")
        
        future = asyncio.run_coroutine_threadsafe(coro, cls._main_loop)
        return await asyncio.wrap_future(future)
    
    @classmethod
    def run_sync_in_thread(cls, func, *args, **kwargs):
        """在线程池中运行同步函数"""
        return cls._executor.submit(func, *args, **kwargs)

# 保活服务
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
        if not isinstance(status, dict):
            return {'connections': {}}
        
        if 'connections' not in status:
            status['connections'] = {}
        
        return status
    except Exception as e:
        logger.error(f"获取连接池状态失败: {e}")
        return {'connections': {}}

# ==================== 模块线程管理器（3线程版）====================
class ModuleThreadManager:
    """模块线程管理器 - 3线程优化版"""
    
    def __init__(self, brain):
        self.brain = brain
        self.threads = []
        self.modules = {}
        
        # 3线程配置
        self.thread_configs = [
            {
                'name': 'thread_1_heavy',
                'modules': [
                    ('public_websocket', self._run_public_websocket),
                    ('public_pipeline', self._run_public_pipeline),
                ]
            },
            {
                'name': 'thread_2_io',
                'modules': [
                    ('private_websocket', self._run_private_websocket),
                    ('private_pipeline', self._run_private_pipeline),
                    ('token_manager', self._run_token_manager),
                ]
            },
            {
                'name': 'thread_3_mixed',
                'modules': [
                    ('completion', self._run_completion_module),
                    ('okx_contract', self._run_okx_contract),
                    ('account_fetcher', self._run_account_fetcher),
                ]
            }
        ]
    
    def _run_in_thread(self, coro_func, *args):
        """在线程中运行协程（使用主事件循环）- 无超时版"""
        main_loop = LoopManager.get_main_loop()
        if not main_loop:
            logger.error("主事件循环未设置")
            return
        
        try:
            # 无限等待，不设超时
            future = asyncio.run_coroutine_threadsafe(
                coro_func(*args),
                main_loop
            )
            # 等待结果（无限等待）
            future.result()
        except Exception as e:
            logger.error(f"线程执行失败: {e}")
    
    def _run_public_websocket(self):
        """运行公共WebSocket - 带重试版"""
        async def _run():
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    ws_admin = WebSocketAdmin()
                    await delayed_ws_init(ws_admin)
                    self.brain.ws_admin = ws_admin
                    logger.info("✅ 公共WebSocket启动成功")
                    
                    # 成功启动后进入保活循环
                    while True:
                        await asyncio.sleep(60)
                        
                except Exception as e:
                    retry_count += 1
                    logger.error(f"❌ 公共WebSocket启动失败 (尝试 {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        wait_time = retry_count * 5
                        logger.info(f"⏳ {wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error("❌ 公共WebSocket启动失败，放弃重试")
                        break
        
        self._run_in_thread(_run)
    
    def _run_private_websocket(self):
        """运行私人WebSocket - 带重试版"""
        async def _run():
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    from private_ws_pool import PrivateWebSocketPool
                    private_pool = PrivateWebSocketPool()
                    await private_pool.start(self.brain.data_manager)
                    self.brain.private_pool = private_pool
                    logger.info("✅ 私人WebSocket连接池启动成功")
                    
                    # 成功启动后进入保活循环
                    while True:
                        await asyncio.sleep(60)
                        
                except Exception as e:
                    retry_count += 1
                    logger.error(f"❌ 私人连接池启动失败 (尝试 {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        wait_time = retry_count * 5
                        logger.info(f"⏳ {wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error("❌ 私人连接池启动失败，放弃重试")
                        break
        
        self._run_in_thread(_run)
    
    def _run_public_pipeline(self):
        """运行公开数据处理 - 带重试版"""
        async def _run():
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    await self.brain.pipeline_manager.start()
                    logger.info("✅ 公开数据处理管道启动成功")
                    
                    while True:
                        await asyncio.sleep(60)
                        
                except Exception as e:
                    retry_count += 1
                    logger.error(f"❌ 公开数据处理启动失败 (尝试 {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        wait_time = retry_count * 5
                        logger.info(f"⏳ {wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error("❌ 公开数据处理启动失败，放弃重试")
                        break
        
        self._run_in_thread(_run)
    
    def _run_private_pipeline(self):
        """运行私人数据处理 - 带重试版"""
        async def _run():
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    logger.info("✅ 私人数据处理模块已就绪")
                    
                    while True:
                        await asyncio.sleep(60)
                        
                except Exception as e:
                    retry_count += 1
                    logger.error(f"❌ 私人数据处理异常 (尝试 {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        wait_time = retry_count * 5
                        logger.info(f"⏳ {wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error("❌ 私人数据处理异常，放弃重试")
                        break
        
        self._run_in_thread(_run)
    
    def _run_token_manager(self):
        """运行令牌管理器 - 带重试版"""
        async def _run():
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    from private_http_fetcher.binance_token.listen_key_manager import ListenKeyManager
                    token_manager = ListenKeyManager(self.brain.data_manager)
                    await token_manager.start()
                    self.brain.token_manager = token_manager
                    logger.info("✅ 币安令牌任务已启动")
                    
                    while True:
                        await asyncio.sleep(60)
                        
                except Exception as e:
                    retry_count += 1
                    logger.error(f"❌ 币安令牌任务启动失败 (尝试 {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        wait_time = retry_count * 5
                        logger.info(f"⏳ {wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error("❌ 币安令牌任务启动失败，放弃重试")
                        break
        
        self._run_in_thread(_run)
    
    def _run_completion_module(self):
        """运行成品数据完成模块 - 带重试版"""
        async def _run():
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
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
                    scheduler = Scheduler(self.brain.data_manager)
                    
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
                    self.brain.data_receiver = data_receiver
                    self.brain.data_detector = detector
                    self.brain.data_scheduler = scheduler
                    self.brain.data_database = database
                    self.brain.binance_repair = binance_repair
                    self.brain.okx_repair = okx_repair
                    
                    logger.info("✅ 成品数据完成模块启动成功")
                    
                    while True:
                        await asyncio.sleep(60)
                        
                except Exception as e:
                    retry_count += 1
                    logger.error(f"❌ 成品数据完成模块启动失败 (尝试 {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        wait_time = retry_count * 5
                        logger.info(f"⏳ {wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error("❌ 成品数据完成模块启动失败，放弃重试")
                        break
        
        self._run_in_thread(_run)
    
    def _run_okx_contract(self):
        """运行OKX合约系统 - 带重试版"""
        async def _run():
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    from public_http_fetcher.okx_contract_info.fetcher import OKXContractFetcher
                    from public_http_fetcher.okx_contract_info.cleaner import OKXContractCleaner
                    
                    okx_fetcher = OKXContractFetcher()
                    raw_data = await okx_fetcher.startup_fetch()
                    
                    if raw_data:
                        okx_cleaner = OKXContractCleaner()
                        await okx_cleaner.clean_and_push(raw_data)
                        self.brain.okx_cleaner = okx_cleaner
                    
                    self.brain.okx_fetcher = okx_fetcher
                    logger.info("✅ OKX合约面值系统启动完成")
                    
                    while True:
                        await asyncio.sleep(60)
                        
                except Exception as e:
                    retry_count += 1
                    logger.error(f"❌ OKX合约系统启动失败 (尝试 {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        wait_time = retry_count * 5
                        logger.info(f"⏳ {wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error("❌ OKX合约系统启动失败，放弃重试")
                        break
        
        self._run_in_thread(_run)
    
    def _run_account_fetcher(self):
        """运行资产获取器 - 带重试版"""
        async def _run():
            retry_count = 0
            max_retries = 3
            
            while retry_count < max_retries:
                try:
                    from private_http_fetcher.binance_account.fetcher import PrivateHTTPFetcher
                    account_fetcher = PrivateHTTPFetcher()
                    await account_fetcher.start(self.brain.data_manager)
                    self.brain.private_fetcher = account_fetcher
                    logger.info("✅ 币安资产获取任务已启动")
                    
                    while True:
                        await asyncio.sleep(60)
                        
                except Exception as e:
                    retry_count += 1
                    logger.error(f"❌ 币安资产获取启动失败 (尝试 {retry_count}/{max_retries}): {e}")
                    if retry_count < max_retries:
                        wait_time = retry_count * 5
                        logger.info(f"⏳ {wait_time}秒后重试...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error("❌ 币安资产获取启动失败，放弃重试")
                        break
        
        self._run_in_thread(_run)
    
    def start_all(self):
        """启动所有线程"""
        logger.info("=" * 60)
        logger.info("🚀 启动3个优化线程...")
        logger.info("=" * 60)
        
        for config in self.thread_configs:
            thread = threading.Thread(
                target=self._run_thread_modules,
                args=(config['name'], config['modules']),
                name=config['name'],
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
            logger.info(f"✅【{config['name']}】线程启动，包含 {len(config['modules'])} 个模块")
    
    def _run_thread_modules(self, thread_name, modules):
        """在线程中运行多个模块"""
        logger.info(f"🧵【{thread_name}】开始运行")
        
        for module_name, module_func in modules:
            try:
                logger.info(f"  └─ 启动模块: {module_name}")
                module_func()
            except Exception as e:
                logger.error(f"  └─ ❌ 模块 {module_name} 失败: {e}")
    
    def restart_thread(self, thread_name):
        """重启指定线程"""
        logger.warning(f"🔄 正在重启线程: {thread_name}")
        
        for config in self.thread_configs:
            if config['name'] == thread_name:
                new_thread = threading.Thread(
                    target=self._run_thread_modules,
                    args=(config['name'], config['modules']),
                    name=config['name'],
                    daemon=True
                )
                new_thread.start()
                
                for i, t in enumerate(self.threads):
                    if t.name == thread_name:
                        self.threads[i] = new_thread
                        break
                
                logger.info(f"✅ 线程 {thread_name} 已重启")
                return True
        
        logger.error(f"❌ 找不到线程配置: {thread_name}")
        return False
    
    def check_all(self):
        """检查所有线程状态，自动重启死亡的线程"""
        all_alive = True
        for thread in self.threads:
            if not thread.is_alive():
                logger.error(f"⚠️ 线程 {thread.name} 已死亡，尝试重启...")
                if self.restart_thread(thread.name):
                    logger.info(f"✅ 线程 {thread.name} 重启成功")
                else:
                    logger.error(f"❌ 线程 {thread.name} 重启失败")
                    all_alive = False
        return all_alive

async def main():
    """主启动函数"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout
    )
    
    logger.info("🚨🚨🚨 MAIN 函数开始执行")
    
    # 保存主事件循环
    main_loop = asyncio.get_running_loop()
    LoopManager.set_main_loop(main_loop)
    logger.info("✅ 主事件循环已保存")
    
    # 启动保活服务
    start_keep_alive_background()
    
    logger.info("=" * 60)
    logger.info("🚀 智能大脑启动中...")
    logger.info("=" * 60)
    
    brain = None
    
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
        
        # ==================== 9. 启动3个优化线程 ====================
        thread_manager = ModuleThreadManager(brain)
        thread_manager.start_all()
        
        # ==================== 完成初始化 ====================
        brain.running = True
        logger.info("=" * 60)
        logger.info("🎉 所有模块启动完成！")
        logger.info("=" * 60)
        
        # 🔥 修复：安全输出连接池状态（使用 getattr 避免属性错误）
        private_pool = getattr(brain, 'private_pool', None)
        if private_pool:
            pool_status = await safe_get_pool_status(private_pool)
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
        else:
            logger.info(f"🔗 私人连接池: ⏳ 正在初始化...")
        
        # ==================== 主监控循环 ====================
        logger.info("🚀 主监控循环运行中...")
        logger.info("🛑 按 Ctrl+C 停止")
        logger.info("=" * 60)
        
        while brain.running:
            await asyncio.sleep(5)
            # 检查线程健康，自动重启死亡的线程
            if not thread_manager.check_all():
                logger.warning("⚠️ 有线程重启失败，但系统继续运行")
        
    except KeyboardInterrupt:
        logger.info("收到键盘中断")
    except Exception as e:
        logger.error(f"运行错误: {e}")
        logger.error(traceback.format_exc())
    finally:
        if brain:
            await brain.shutdown()
            # 关闭线程池
            LoopManager._executor.shutdown(wait=False)

if __name__ == "__main__":
    print("🚨🚨🚨 进入 __main__", file=sys.stderr)
    sys.stderr.flush()
    asyncio.run(main())