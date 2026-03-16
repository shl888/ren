"""
大脑核心主控 - 精简重构版（删除私人连接管理器）
"""

import asyncio
import logging
import signal
import sys
import os
import traceback

# 设置路径
CURRENT_FILE = os.path.abspath(__file__)
SMART_BRAIN_DIR = os.path.dirname(CURRENT_FILE)
PROJECT_ROOT = os.path.dirname(SMART_BRAIN_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)

class SmartBrain:
    def __init__(self, http_server=None, http_runner=None, 
                 pipeline_manager=None, funding_manager=None, 
                 frontend_relay=None):
        # 注入的服务
        self.http_server = http_server
        self.http_runner = http_runner
        self.pipeline_manager = pipeline_manager
        self.funding_manager = funding_manager
        self.frontend_relay = frontend_relay
        
        # 自己的管理器
        from .data_manager import DataManager
        self.data_manager = DataManager(self)
        
        self.command_router = None

        # 🔴【已删除】WebSocket管理员（从未使用）
        # self.ws_admin = None
        
        # 🔴【已删除】私人连接池实例（已移除）
        # self.private_pool = None
        
        # ✅ 新增：HTTP模块服务（用于执行交易）
        self.http_module = None
        
        # 运行状态
        self.running = False
        self.status_log_task = None
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def initialize(self):
        """初始化大脑核心"""
        logger.info("🧠 【智能大脑】大脑核心初始化中...")
        
        try:
            # 1. 初始化其他管理器
            from .command_router import CommandRouter
            
            self.command_router = CommandRouter(self)
            
            # 2. ✅ 初始化HTTP模块服务（用于执行交易）
            try:
                from http_server.service import HTTPModuleService
                self.http_module = HTTPModuleService()
                http_init_success = await self.http_module.initialize(self)
                if not http_init_success:
                    logger.error("❌【智能大脑】 HTTP模块服务初始化失败")
                    return False
                logger.info("✅【智能大脑】 HTTP模块服务初始化成功（仅用于执行交易）")
            except ImportError as e:
                logger.error(f"❌【智能大脑】 无法导入HTTP模块服务: {e}")
                return False
            except Exception as e:
                logger.error(f"❌【智能大脑】 HTTP模块服务初始化异常: {e}")
                return False
            
            # 3. 🔴【已删除】启动私人连接池
            # await self._start_private_connections()
            
            # 4. 🔴【已删除】令牌任务已移至private_http_fetcher，不再在这里启动
            # if self.http_module:
            #     listen_key_started = await self.http_module.start_listen_key_service('binance')
            #     if not listen_key_started:
            #         logger.warning("⚠️ 币安令牌服务启动失败（可能API未就绪）")
            
            # 5. 启动状态日志任务
            self.status_log_task = asyncio.create_task(self.data_manager._log_data_status())
            
            # 6. 完成初始化
            self.running = True
            logger.info("✅【智能大脑】 大脑核心初始化完成")
            
            # 输出HTTP模块状态
            if self.http_module:
                http_status = self.http_module.get_status()
                logger.info(f"📊【智能大脑】 HTTP模块状态: {http_status}")
            
            return True
            
        except Exception as e:
            logger.error(f"🚨 【智能大脑】大脑初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def receive_market_data(self, processed_data):
        """接收市场数据（委托给data_manager）"""
        return await self.data_manager.receive_market_data(processed_data)
    
    async def receive_private_data(self, private_data):
        """接收私人数据（委托给data_manager）"""
        return await self.data_manager.receive_private_data(private_data)
    
    async def handle_frontend_command(self, command_data):
        """处理前端指令（委托给command_router）"""
        return await self.command_router.handle_frontend_command(command_data)
    
    async def run(self):
        """运行大脑核心"""
        try:
            logger.info("🧠 【智能大脑】大脑核心运行中...")
            
            # 主循环
            while self.running:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU，避免长时间占用
                await asyncio.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("🚫【智能大脑】收到键盘中断")
        except Exception as e:
            logger.error(f"🚫【智能大脑】运行错误: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.shutdown()
    
    def handle_signal(self, signum, frame):
        """处理系统信号"""
        logger.info(f"☑️【智能大脑】收到信号 {signum}，开始关闭...")
        self.running = False
    
    async def shutdown(self):
        """关闭大脑核心"""
        self.running = False
        logger.info("☑️【智能大脑】正在关闭大脑核心...")
        
        try:
            # 1. 关闭HTTP模块服务
            if self.http_module:
                await self.http_module.shutdown()
            
            # 🔴【已删除】关闭私人连接池（已移除）
            # if self.private_pool:
            #     await self.private_pool.shutdown()
            
            # 2. 取消状态日志任务
            if self.status_log_task:
                self.status_log_task.cancel()
                try:
                    await self.status_log_task
                except asyncio.CancelledError:
                    pass
            
            # 3. 关闭前端中继服务器
            if self.frontend_relay:
                await self.frontend_relay.stop()
            
            # 🔴【已删除】停止WebSocket管理员（从未使用）
            # if self.ws_admin:
            #     await self.ws_admin.stop()
            
            logger.info("✅ 【智能大脑】大脑核心已关闭")
        except Exception as e:
            logger.error(f"❌【智能大脑】关闭出错: {e}")