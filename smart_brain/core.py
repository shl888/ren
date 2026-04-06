"""
大脑核心主控 - 调度版（只转发，不干活）
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
from datetime import datetime

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

        # HTTP模块服务（用于执行交易）
        self.http_module = None
        
        # 下单工人（只负责执行，大脑不直接发数据给它）
        self.trader = None
        
        # 半自动工人
        self.leverage_worker = None
        self.open_worker = None
        
        # 运行状态
        self.running = False
        self.status_log_task = None
        
        # 控制指令存储
        self.config_data = None
        
        # 交易模式：默认禁止交易
        self.trade_mode = "forbidden"  # forbidden / half / full
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    # ========== 大脑不再有 send_to_worker 方法 ==========
    
    async def on_trader_results(self, data):
        """
        接收下单工人发来的数据（独立的一条数据）
        
        判断规则：
        - 如果数据里有 "info" 字段 → 这是信息标签，根据内容转发给对应工人
        - 否则 → 这是原始数据，直接推送到前端
        """
        # 判断是否是信息标签
        if "info" in data:
            info_value = data["info"]
            logger.info(f"🏷️【智能大脑】收到信息标签: {info_value}")
            
            # 根据标签内容路由
            if info_value in ["欧易杠杆设置成功", "币安杠杆设置成功"]:
                if self.open_worker:
                    self.open_worker.on_data(data)
                    logger.info(f"📤【智能大脑】信息标签已转发给开仓工人: {info_value}")
            elif info_value in ["欧易开仓成功", "币安开仓成功"]:
                # TODO: 转发给全自动文件夹的止损止盈工人
                logger.info(f"📌【智能大脑】开仓成功标签待转发: {info_value}")
            else:
                logger.warning(f"⚠️【智能大脑】未知信息标签: {info_value}")
        else:
            # 原始数据，直接推送到前端
            if self.frontend_relay:
                await self.frontend_relay.broadcast_execution_results([data])
                logger.info(f"📤【智能大脑】原始数据已推送到前端")
            else:
                logger.warning("⚠️【智能大脑】frontend_relay 未设置，无法推送")
    
    async def initialize(self):
        """初始化智能大脑核心"""
        logger.info("🧠【智能大脑】大脑核心初始化中...")
        logger.info(f"🔒【智能大脑】初始交易模式: {self.trade_mode}（禁止交易）")
        
        try:
            # 1. 初始化HTTP模块服务
            try:
                from http_server.service import HTTPModuleService
                self.http_module = HTTPModuleService()
                http_init_success = await self.http_module.initialize(self)
                if not http_init_success:
                    logger.error("❌【智能大脑】 HTTP模块服务初始化失败")
                    return False
                logger.info("✅【智能大脑】 HTTP模块服务初始化成功")
            except ImportError as e:
                logger.error(f"❌【智能大脑】 无法导入HTTP模块服务: {e}")
                return False
            except Exception as e:
                logger.error(f"❌【智能大脑】 HTTP模块服务初始化异常: {e}")
                return False
            
            # 2. 创建下单工人
            from http_server.trader import Trader
            self.trader = Trader(self, use_sandbox=True)
            asyncio.create_task(self.trader.start())
            logger.info("✅【智能大脑】下单工人已创建并启动")
            
            # 3. 创建半自动工人
            from .trading.semi_auto.leverage_worker import LeverageWorker
            from .trading.semi_auto.open_position_worker import OpenPositionWorker
            
            self.leverage_worker = LeverageWorker(self)
            self.open_worker = OpenPositionWorker(self)
            logger.info("✅【智能大脑】杠杆工人和开仓工人已创建")
            
            # 4. 启动状态日志任务
            self.status_log_task = asyncio.create_task(self.data_manager._log_data_status())
            
            # 5. 完成初始化
            self.running = True
            logger.info("✅【智能大脑】 大脑核心初始化完成")
            
            # 输出HTTP模块状态
            if self.http_module:
                http_status = self.http_module.get_status()
                logger.info(f"📊【智能大脑】 HTTP模块状态: {http_status}")
            
            return True
            
        except Exception as e:
            logger.error(f"🚨【智能大脑】大脑初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def receive_market_data(self, processed_data):
        """接收市场数据（委托给data_manager）"""
        return await self.data_manager.receive_market_data(processed_data)
    
    async def receive_private_data(self, private_data):
        """接收私人数据（委托给data_manager）"""
        return await self.data_manager.receive_private_data(private_data)
    
    # ==================== 前端指令处理 ====================
    
    async def handle_frontend_command(self, command_data):
        """
        接收前端指令
        
        大脑不再解析指令内容，直接转发给对应的工人
        """
        command = command_data.get('command')
        params = command_data.get('params', {})
        client_id = command_data.get('client_id', 'unknown')
        
        # ========== 交易模式指令 ==========
        if command == 'set_trade_mode':
            new_mode = params.get('mode', 'forbidden')
            old_mode = self.trade_mode
            self.trade_mode = new_mode
            logger.info(f"🎮【智能大脑】交易模式已切换: {old_mode} → {self.trade_mode}")
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": f"交易模式已切换为 {self.trade_mode}",
                "old_mode": old_mode,
                "new_mode": self.trade_mode
            }
        
        # ========== 配置指令 ==========
        if command == 'save_config':
            self.config_data = params.get('config_data', '')
            logger.info(f"💾【智能大脑】收到配置指令")
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": f"配置已保存",
                "config_length": len(self.config_data)
            }
        
        # ========== 开仓指令 ==========
        if command == 'place_order':
            # 检查是否是禁止交易模式
            if self.trade_mode == "forbidden":
                logger.warning(f"🚫【智能大脑】 当前为禁止交易模式，开仓指令被拒绝")
                return {
                    "success": False,
                    "received": True,
                    "command": command,
                    "error": "当前为禁止交易模式，无法执行开仓"
                }
            
            logger.info(f"💰【智能大脑】收到开仓指令，直接转发给工人")
            
            # 大脑不解析，直接转发给杠杆工人和开仓工人
            if self.leverage_worker:
                self.leverage_worker.on_data({"command": "place_order", "params": params})
                logger.info(f"📤【智能大脑】开仓指令已转发给杠杆工人")
            if self.open_worker:
                self.open_worker.on_data({"command": "place_order", "params": params})
                logger.info(f"📤【智能大脑】开仓指令已转发给开仓工人")
            
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": "开仓指令已转发给工人"
            }
        
        # ========== 止损止盈指令（暂留空） ==========
        if command == 'set_sl_tp':
            if self.trade_mode == "forbidden":
                logger.warning(f"🚫【智能大脑】 当前为禁止交易模式，止损止盈指令被拒绝")
                return {
                    "success": False,
                    "received": True,
                    "command": command,
                    "error": "当前为禁止交易模式，无法执行止损止盈"
                }
            
            logger.info(f"⚙️【智能大脑】收到止损止盈指令: {params}")
            # TODO: 转发给止损止盈工人（待实现）
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": "指令已接收，止损止盈工人待实现"
            }
        
        # ========== 平仓指令（暂留空） ==========
        if command == 'close_position':
            if self.trade_mode == "forbidden":
                logger.warning(f"🚫【智能大脑】 当前为禁止交易模式，平仓指令被拒绝")
                return {
                    "success": False,
                    "received": True,
                    "command": command,
                    "error": "当前为禁止交易模式，无法执行平仓"
                }
            
            logger.info(f"🔚【智能大脑】收到平仓指令: {params}")
            # TODO: 转发给平仓工人（待实现）
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": "指令已接收，平仓工人待实现"
            }
        
        # ========== 未知指令 ==========
        logger.warning(f"⚠️【智能大脑】收到未知指令: {command}")
        return {
            "success": False,
            "received": True,
            "error": f"未知指令: {command}",
            "command": command
        }
    
    async def run(self):
        """运行大脑核心"""
        try:
            logger.info("🧠【智能大脑】大脑核心运行中...")
            
            # 主循环
            while self.running:
                await asyncio.sleep(0)
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
            
            # 4. 关闭下单工人
            if self.trader:
                await self.trader.stop()
            
            logger.info("✅【智能大脑】大脑核心已关闭")
        except Exception as e:
            logger.error(f"❌【智能大脑】关闭出错: {e}")