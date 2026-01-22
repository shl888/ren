"""
大脑核心主控 - 精简重构版（只做协调）
按照新流程：大脑负责获取初始令牌，然后初始化连接管理器
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
        
        # 自己的管理器 - ✅ 立即创建data_manager，其他保持延迟
        from .data_manager import DataManager
        self.data_manager = DataManager(self)  # ✅ 关键：这里立即创建
        
        self.command_router = None
        self.security_manager = None
        self.private_connection_manager = None  # 新增：私人连接指挥官
        
        # WebSocket管理员
        self.ws_admin = None
        
        # 运行状态
        self.running = False
        self.status_log_task = None
        
        # 启动任务跟踪
        self.startup_tasks = {}
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def initialize(self):
        """初始化大脑核心 - 按照新流程：获取令牌 → 初始化连接管理器"""
        logger.info("🧠 大脑核心初始化中（新流程）...")
        
        try:
            # ==================== 阶段1: 基础初始化 ====================
            # 1. 初始化除data_manager外的其他管理器
            from .command_router import CommandRouter
            from .security_manager import SecurityManager
            from .private_connection_manager import PrivateConnectionManager
            
            self.command_router = CommandRouter(self)
            self.security_manager = SecurityManager(self)
            self.private_connection_manager = PrivateConnectionManager(self)
            
            logger.info("✅ 基础管理器初始化完成")
            
            # ==================== 阶段2: 获取初始令牌 ====================
            logger.info("🔄 进入阶段2: 获取初始交易所令牌...")
            
            # 获取币安初始令牌（通过HTTP模块）
            binance_listen_key = await self._acquire_initial_binance_token()
            
            if binance_listen_key:
                logger.info(f"✅ 已获取初始币安令牌: {binance_listen_key[:15]}...")
            else:
                logger.warning("⚠️ 无法获取币安初始令牌，币安私人连接将不可用")
            
            # ==================== 阶段3: 初始化连接管理器 ====================
            logger.info("🔄 进入阶段3: 初始化私人连接管理器...")
            
            # 获取所有API凭证
            binance_apis = self.data_manager.memory_store['env_apis'].get('binance', {})
            okx_apis = self.data_manager.memory_store['env_apis'].get('okx', {})
            
            # 验证API是否存在
            if not binance_apis or not binance_apis.get('api_key'):
                logger.warning("⚠️ 币安API凭证不完整或缺失")
            
            if not okx_apis or not okx_apis.get('api_key'):
                logger.warning("⚠️ 欧意API凭证不完整或缺失")
            
            # 初始化连接管理器（提供资源）
            pm_success = await self.private_connection_manager.initialize_with_resources(
                binance_token=binance_listen_key,  # 可能为None
                binance_apis=binance_apis,
                okx_apis=okx_apis
            )
            
            if pm_success:
                logger.info("✅ 私人连接管理器初始化成功")
            else:
                logger.warning("⚠️ 私人连接管理器初始化失败，私人功能将不可用")
            
            # ==================== 阶段4: 启动状态日志任务 ====================
            self.status_log_task = asyncio.create_task(self.data_manager._log_data_status())
            
            # ==================== 阶段5: 完成初始化 ====================
            self.running = True
            
            # 记录初始化完成状态
            initialization_report = self._generate_initialization_report(
                binance_token_acquired=bool(binance_listen_key),
                binance_apis_available=bool(binance_apis and binance_apis.get('api_key')),
                okx_apis_available=bool(okx_apis and okx_apis.get('api_key')),
                connection_manager_ready=pm_success
            )
            
            logger.info("✅ 大脑核心初始化完成")
            logger.info(f"📊 初始化报告:\n{initialization_report}")
            
            return True
            
        except Exception as e:
            logger.error(f"🚨 大脑初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def _acquire_initial_binance_token(self) -> Optional[str]:
        """
        获取初始币安令牌
        流程：大脑 → HTTP模块 → 交易所 → 大脑存储
        """
        logger.info("🔑 正在获取初始币安listen_key...")
        
        try:
            # 1. 检查是否有币安API
            binance_apis = self.data_manager.memory_store['env_apis'].get('binance')
            if not binance_apis or not binance_apis.get('api_key'):
                logger.warning("⚠️ 没有币安API凭证，跳过令牌获取")
                return None
            
            # 2. 调用HTTP模块获取listen_key
            from http_server.exchange_api import ExchangeAPI
            
            logger.info(f"📞 调用HTTP模块获取币安listen_key (API Key: {binance_apis['api_key'][:8]}...)")
            
            result = await ExchangeAPI.get_binance_listen_key(
                api_key=binance_apis['api_key'],
                api_secret=binance_apis['api_secret']
            )
            
            # 3. 处理结果
            if result.get('success'):
                listen_key = result['listenKey']
                
                # 4. 保存到大脑的data_manager
                await self.data_manager.save_binance_token(listen_key)
                
                logger.info(f"✅ 币安listen_key获取成功: {listen_key[:15]}...")
                return listen_key
            else:
                error_msg = result.get('error', '未知错误')
                logger.error(f"❌ 获取币安listen_key失败: {error_msg}")
                return None
                
        except ImportError as e:
            logger.error(f"❌ 无法导入HTTP模块: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ 获取币安令牌异常: {e}")
            logger.error(traceback.format_exc())
            return None
    
    def _generate_initialization_report(self, **kwargs) -> str:
        """生成初始化报告"""
        report_lines = []
        report_lines.append("=" * 50)
        report_lines.append("🧠 大脑初始化报告")
        report_lines.append("=" * 50)
        
        # 令牌状态
        binance_token_status = "✅ 已获取" if kwargs.get('binance_token_acquired') else "❌ 未获取"
        report_lines.append(f"币安令牌: {binance_token_status}")
        
        # API状态
        binance_api_status = "✅ 可用" if kwargs.get('binance_apis_available') else "❌ 不可用"
        okx_api_status = "✅ 可用" if kwargs.get('okx_apis_available') else "❌ 不可用"
        report_lines.append(f"币安API: {binance_api_status}")
        report_lines.append(f"欧意API: {okx_api_status}")
        
        # 连接管理器状态
        cm_status = "✅ 就绪" if kwargs.get('connection_manager_ready') else "❌ 未就绪"
        report_lines.append(f"连接管理器: {cm_status}")
        
        # 数据管理器状态
        has_binance_token = self.data_manager.has_binance_token()
        token_status = "✅ 已存储" if has_binance_token else "❌ 未存储"
        report_lines.append(f"令牌存储: {token_status}")
        
        report_lines.append("=" * 50)
        
        return "\n".join(report_lines)
    
    async def start_private_connections(self):
        """
        启动私人连接
        应该在HTTP服务器就绪后调用
        """
        if not self.private_connection_manager:
            logger.error("❌ 连接管理器未初始化，无法启动连接")
            return False
        
        logger.info("🚀 大脑：正在启动私人连接...")
        
        try:
            # 延迟启动，确保HTTP服务器已就绪
            await asyncio.sleep(3)
            
            # 启动连接
            success = await self.private_connection_manager.start_all_connections()
            
            if success:
                logger.info("✅ 大脑：私人连接启动命令已发送")
            else:
                logger.error("❌ 大脑：启动私人连接失败")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 启动私人连接异常: {e}")
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
    
    def get_connection_manager_status(self):
        """获取连接管理器状态"""
        if self.private_connection_manager:
            return self.private_connection_manager.get_status()
        return {"error": "连接管理器未初始化"}
    
    async def run(self):
        """运行大脑核心"""
        try:
            logger.info("🧠 大脑核心运行中...")
            
            # 检查连接管理器状态
            if self.private_connection_manager:
                cm_status = self.private_connection_manager.get_status()
                if cm_status.get('initialized'):
                    logger.info("✅ 私人连接管理器已就绪")
                else:
                    logger.warning("⚠️ 私人连接管理器未就绪")
            
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
            
            # 4. 停止WebSocket管理员
            if self.ws_admin:
                await self.ws_admin.stop()
            
            logger.info("✅ 大脑核心已关闭")
        except Exception as e:
            logger.error(f"关闭出错: {e}")