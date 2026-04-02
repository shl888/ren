"""
大脑核心主控 - 完整版
- 接收数据完成模块的数据
- 接收前端指令（通过 qd_server）
- 拥有记忆中枢 (data_manager)
- 拥有逻辑中枢 (trading)
- 拥有执行中枢（调用 http_module）
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

from smart_brain.data_manager import DataManager
from smart_brain.trading import TradingLogic

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
        
        # ========== 大脑的三个中枢 ==========
        # 记忆中枢
        self.data_manager = DataManager(self)
        
        # 逻辑中枢（交易规则）
        self.trading = TradingLogic(self)
        
        # ========== 控制指令存储 ==========
        self.config_data = None      # 配置数据
        self.trade_mode = "half"     # full / half / forbidden
        
        # 运行状态
        self.running = False
        self.status_log_task = None
        self.auto_trade_task = None   # 全自动交易任务
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    # ==================== 接收数据（来自数据完成模块）====================
    
    async def receive_market_data(self, processed_data):
        """接收市场数据 - 直接使用，同时存储"""
        # 1. 直接使用收到的数据判断条件（全自动模式）
        if self.trade_mode == "full":
            await self._check_auto_trade_conditions(processed_data)
        
        # 2. 存储到记忆中枢（供以后查询）
        await self.data_manager.receive_market_data(processed_data)
    
    async def receive_private_data(self, private_data):
        """接收私人数据"""
        await self.data_manager.receive_private_data(private_data)
    
    # ==================== 接收前端指令（来自 qd_server）====================
    
    async def handle_frontend_command(self, command_data):
        """
        接收前端指令 - 直接处理，不保存
        command_data 格式: {"command": "xxx", "params": {...}, "client_id": "xxx"}
        """
        command = command_data.get('command')
        params = command_data.get('params', {})
        client_id = command_data.get('client_id', 'unknown')
        
        logger.info(f"🧠【智能大脑】收到前端指令: {command} from {client_id}")
        
        # ========== 交易指令：直接执行 ==========
        if command == 'place_order':
            return await self._execute_order(params)
        
        elif command == 'close_position':
            return await self._execute_close(params)
        
        elif command == 'set_sl_tp':
            return await self._execute_set_sl_tp(params)
        
        # ========== 控制指令：保存（以后再说）==========
        elif command == 'save_config':
            self.config_data = params.get('config_data', '')
            logger.info(f"💾【智能大脑】配置已保存")
            return {"success": True, "message": "配置已保存"}
        
        elif command == 'set_trade_mode':
            self.trade_mode = params.get('mode', 'half')
            logger.info(f"🔄【智能大脑】交易模式已切换为: {self.trade_mode}")
            return {"success": True, "message": f"交易模式已切换为 {self.trade_mode}"}
        
        else:
            return {"success": False, "error": f"未知指令: {command}"}
    
    # ==================== 执行中枢 ====================
    
    async def _execute_order(self, params):
        """执行开仓"""
        try:
            # 1. 使用逻辑中枢计算订单参数
            order_params = await self.trading.calculate_order(params)
            
            # 2. 调用 HTTP 模块发单
            if not self.http_server:
                return {"success": False, "error": "HTTP模块未连接"}
            
            result = await self.http_server.place_order(order_params)
            return result
            
        except Exception as e:
            logger.error(f"❌【智能大脑】开仓失败: {e}")
            return {"success": False, "error": str(e)}
    
    async def _execute_close(self, params):
        """执行平仓"""
        try:
            # 1. 使用逻辑中枢计算平仓参数
            close_params = await self.trading.calculate_close(params)
            
            # 2. 调用 HTTP 模块平仓
            if not self.http_server:
                return {"success": False, "error": "HTTP模块未连接"}
            
            result = await self.http_server.close_position(close_params)
            return result
            
        except Exception as e:
            logger.error(f"❌【智能大脑】平仓失败: {e}")
            return {"success": False, "error": str(e)}
    
    async def _execute_set_sl_tp(self, params):
        """执行止损止盈"""
        try:
            # 1. 使用逻辑中枢计算止损止盈参数
            sl_tp_params = await self.trading.calculate_sl_tp(params)
            
            # 2. 调用 HTTP 模块设置
            if not self.http_server:
                return {"success": False, "error": "HTTP模块未连接"}
            
            result = await self.http_server.set_sl_tp(sl_tp_params)
            return result
            
        except Exception as e:
            logger.error(f"❌【智能大脑】设置止损止盈失败: {e}")
            return {"success": False, "error": str(e)}
    
    # ==================== 全自动交易 ====================
    
    async def _check_auto_trade_conditions(self, data):
        """检查全自动交易条件"""
        if self.trade_mode != "full":
            return
        
        # 使用逻辑中枢的规则判断
        decision = await self.trading.check_conditions(data)
        
        if decision.get('action') == 'BUY':
            await self._execute_order(decision.get('params', {}))
        elif decision.get('action') == 'SELL':
            await self._execute_close(decision.get('params', {}))
    
    # ==================== 初始化/运行/关闭 ====================
    
    async def initialize(self):
        """初始化大脑核心"""
        logger.info("🧠【智能大脑】初始化中...")
        
        try:
            # 初始化逻辑中枢
            if hasattr(self.trading, 'initialize'):
                await self.trading.initialize()
            # ✅ 加在这里！启动状态日志任务
            self.status_log_task = asyncio.create_task(self.data_manager._log_data_status())
            
            self.running = True
            logger.info("✅【智能大脑】初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"❌【智能大脑】初始化失败: {e}")
            return False
    
    async def run(self):
        """运行大脑核心"""
        logger.info("🧠【智能大脑】运行中...")
        
        while self.running:
            await asyncio.sleep(1)
    
    def handle_signal(self, signum, frame):
        """处理系统信号"""
        logger.info(f"☑️【智能大脑】收到信号 {signum}，正在关闭...")
        self.running = False
    
    async def shutdown(self):
        """关闭大脑核心"""
        self.running = False
        logger.info("☑️【智能大脑】正在关闭...")
        
        if self.status_log_task:
            self.status_log_task.cancel()
            try:
                await self.status_log_task
            except asyncio.CancelledError:
                pass
        
        if self.auto_trade_task:
            self.auto_trade_task.cancel()
            try:
                await self.auto_trade_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅【智能大脑】已关闭")