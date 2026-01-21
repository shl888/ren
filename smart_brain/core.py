"""
大脑核心主控 - 精简重构版（只做协调）
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
        self.data_manager = None
        self.command_router = None
        self.security_manager = None
        self.private_connection_manager = None  # 新增：私人连接指挥官
        
        # WebSocket管理员
        self.ws_admin = None
        
        # 运行状态
        self.running = False
        self.status_log_task = None
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def initialize(self):
        """初始化大脑核心 - 只初始化自己的组件"""
        logger.info("🧠 大脑核心初始化中...")
        
        try:
            # 1. 初始化各个管理器
            from .data_manager import DataManager
            from .command_router import CommandRouter
            from .security_manager import SecurityManager
            from .private_connection_manager import PrivateConnectionManager  # 新增导入
            
            self.data_manager = DataManager(self)
            self.command_router = CommandRouter(self)
            self.security_manager = SecurityManager(self)
            self.private_connection_manager = PrivateConnectionManager(self)  # 新增实例化
            
            # 2. 初始化私人连接管理器
            logger.info("🧠 正在初始化私人连接管理器...")
            pm_success = await self.private_connection_manager.initialize()
            if pm_success:
                logger.info("✅ 私人连接管理器初始化成功")
                # 启动所有私人连接
                asyncio.create_task(self.private_connection_manager.start_all_connections())
            else:
                logger.warning("⚠️ 私人连接管理器初始化失败，私人功能将不可用")
            
            # 3. ✅ 启动DataManager的API服务器
            logger.info("🧠 正在启动DataManager API服务器...")
            try:
                api_success = await self.data_manager.start_api_server()
                if api_success:
                    logger.info("✅ DataManager API服务器启动成功")
                else:
                    logger.warning("⚠️ DataManager API服务器启动失败，数据查看功能可能不可用")
            except Exception as e:
                logger.error(f"❌ 启动DataManager API服务器失败: {e}")
            
            # 4. 启动状态日志任务
            self.status_log_task = asyncio.create_task(self.data_manager._log_data_status())
            
            # 5. 完成初始化
            self.running = True
            logger.info("✅ 大脑核心初始化完成")
            
            return True
            
        except Exception as e:
            logger.error(f"🚨 大脑初始化失败: {e}")
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
            logger.info("🧠 大脑核心运行中...")
            
            # 主循环
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
        """关闭大脑核心"""
        self.running = False
        logger.info("正在关闭大脑核心...")
        
        try:
            # 1. 关闭私人连接管理器
            if self.private_connection_manager:
                await self.private_connection_manager.shutdown()
            
            # 2. ✅ 关闭DataManager API服务器
            if self.data_manager:
                await self.data_manager.stop_api_server()
            
            # 3. 取消状态日志任务
            if self.status_log_task:
                self.status_log_task.cancel()
                try:
                    await self.status_log_task
                except asyncio.CancelledError:
                    pass
            
            # 4. 关闭前端中继服务器
            if self.frontend_relay:
                await self.frontend_relay.stop()
            
            # 5. 停止WebSocket管理员
            if self.ws_admin:
                await self.ws_admin.stop()
            
            logger.info("✅ 大脑核心已关闭")
        except Exception as e:
            logger.error(f"关闭出错: {e}")
            