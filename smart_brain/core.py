"""
大脑核心主控 - 精简重构版（删除私人连接管理器）
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

        # ✅ HTTP模块服务（用于执行交易）
        self.http_module = None
        
        # ✅ 下单工人（直接持有，用于发单）
        self.trader = None
        
        # ✅ 逻辑中枢（空模块，以后放规则）
        from .trading import TradingLogic
        self.trading = TradingLogic(self)
        
        # 运行状态
        self.running = False
        self.status_log_task = None
        
        # 控制指令存储（以后用）
        self.config_data = None
        
        # ========== 交易模式：默认禁止交易 ==========
        self.trade_mode = "forbidden"  # forbidden / half / full
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    async def initialize(self):
        """初始化大脑核心"""
        logger.info("🧠【智能大脑】大脑核心初始化中...")
        logger.info(f"🔒【智能大脑】初始交易模式: {self.trade_mode}（禁止交易）")
        
        try:
            # 1. ✅ 初始化HTTP模块服务（用于执行交易）
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
            
            # 2. ✅ 初始化下单工人
            try:
                from http_server.trader import Trader
                self.trader = Trader(self)
                logger.info("✅【智能大脑】 下单工人初始化成功")
            except ImportError as e:
                logger.error(f"❌【智能大脑】 无法导入下单工人: {e}")
                return False
            except Exception as e:
                logger.error(f"❌【智能大脑】 下单工人初始化异常: {e}")
                return False
            
            # 3. 启动状态日志任务
            self.status_log_task = asyncio.create_task(self.data_manager._log_data_status())
            
            # 4. 完成初始化
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
        接收前端指令（qd转发过来）
        直接处理，不经过 command_router
        """
        command = command_data.get('command')
        params = command_data.get('params', {})
        client_id = command_data.get('client_id', 'unknown')
        
        # ========== 交易模式指令：最高优先级，立即处理 ==========
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
        
        # ========== 禁止交易模式：拒绝所有交易指令 ==========
        if self.trade_mode == 'forbidden':
            logger.warning(f"⚠️【智能大脑】禁止交易模式，拒绝指令: {command}")
            return {
                "success": False,
                "received": True,
                "error": f"当前为禁止交易模式，无法执行交易指令。请先通过前端设置交易模式（半自动/全自动）",
                "command": command,
                "current_mode": self.trade_mode
            }
        
        # ========== 开仓指令 ==========
        if command == 'place_order':
            logger.info(f"💰【智能大脑】收到开仓指令（当前模式: {self.trade_mode}）")
            logger.info(f"   参数: {params}")
            
            # 调用交易逻辑执行
            result = await self._execute_order(params)
            return result
        
        # ========== 平仓指令 ==========
        elif command == 'close_position':
            logger.info(f"🔚【智能大脑】收到平仓指令（当前模式: {self.trade_mode}）")
            logger.info(f"   参数: {params}")
            
            # 调用交易逻辑执行
            result = await self._execute_close(params)
            return result
        
        # ========== 止损止盈指令 ==========
        elif command == 'set_sl_tp':
            logger.info(f"⚙️【智能大脑】收到止损止盈指令（当前模式: {self.trade_mode}）")
            logger.info(f"   参数: {params}")
            
            # 调用交易逻辑执行
            result = await self._execute_set_sl_tp(params)
            return result
        
        else:
            logger.warning(f"⚠️【智能大脑】收到未知指令: {command}")
            return {
                "success": False,
                "received": True,
                "error": f"未知指令: {command}",
                "command": command
            }
    
    # ==================== 交易执行方法 ====================
    
    async def _execute_order(self, params: dict) -> dict:
        """
        执行开仓/平仓
        
        大脑根据指令类型，组装好完整参数，交给工人执行
        """
        try:
            # 从 params 中提取交易所信息
            # 注意：前端传来的开仓指令可能同时包含两个交易所
            # 这里需要根据 direction 分别处理
            
            direction = params.get('direction')
            symbol = params.get('symbol')
            margin = params.get('margin')
            leverage = params.get('leverage')
            
            # 获取当前价格（从记忆中枢）
            market_data = await self.data_manager.get_public_market_data()
            current_price = None
            if symbol in market_data.get('data', {}):
                current_price = market_data['data'][symbol].get('binance_trade_price')
            
            if not current_price:
                return {
                    "success": False,
                    "error": "无法获取当前价格",
                    "command": "place_order"
                }
            
            # 计算开仓数量
            quantity = margin * leverage / current_price
            
            results = []
            
            # 根据方向分别处理币安和欧易
            if direction == 'long_binance_short_okx':
                # 币安开多
                binance_params = {
                    'symbol': symbol,
                    'side': 'buy',
                    'type': 'market',
                    'amount': quantity,
                }
                binance_result = await self.trader.place_order('binance', binance_params)
                results.append({'exchange': 'binance', 'result': binance_result})
                
                # 欧易开空
                okx_params = {
                    'symbol': symbol.replace('USDT', '/USDT:USDT'),  # 欧易格式
                    'side': 'sell',
                    'type': 'market',
                    'amount': quantity,
                }
                okx_result = await self.trader.place_order('okx', okx_params)
                results.append({'exchange': 'okx', 'result': okx_result})
                
            elif direction == 'long_okx_short_binance':
                # 欧易开多
                okx_params = {
                    'symbol': symbol.replace('USDT', '/USDT:USDT'),
                    'side': 'buy',
                    'type': 'market',
                    'amount': quantity,
                }
                okx_result = await self.trader.place_order('okx', okx_params)
                results.append({'exchange': 'okx', 'result': okx_result})
                
                # 币安开空
                binance_params = {
                    'symbol': symbol,
                    'side': 'sell',
                    'type': 'market',
                    'amount': quantity,
                }
                binance_result = await self.trader.place_order('binance', binance_params)
                results.append({'exchange': 'binance', 'result': binance_result})
            
            return {
                "success": True,
                "command": "place_order",
                "results": results
            }
            
        except Exception as e:
            logger.error(f"❌【智能大脑】执行开仓失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "command": "place_order"
            }
    
    async def _execute_close(self, params: dict) -> dict:
        """
        执行平仓
        """
        try:
            exchange = params.get('exchange')
            symbol = params.get('symbol')
            
            # 获取当前持仓（从记忆中枢）
            user_data = await self.data_manager.get_private_user_data()
            position = None
            
            # 查找持仓
            for exchange_name, data in user_data.get('data', {}).items():
                if exchange_name.lower() == exchange:
                    positions = data.get('positions', [])
                    for pos in positions:
                        if pos.get('symbol') == symbol:
                            position = pos
                            break
            
            if not position:
                return {
                    "success": False,
                    "error": f"未找到{exchange}的{symbol}持仓",
                    "command": "close_position"
                }
            
            # 获取持仓方向和数量
            position_side = position.get('side')  # 'long' 或 'short'
            quantity = abs(position.get('position_amt', 0))
            
            # 平仓方向与持仓相反
            close_side = 'sell' if position_side == 'long' else 'buy'
            
            close_params = {
                'symbol': symbol,
                'side': close_side,
                'type': 'market',
                'amount': quantity,
                'reduceOnly': True  # 只减仓
            }
            
            # 调用工人执行
            result = await self.trader.place_order(exchange, close_params)
            
            return {
                "success": True,
                "command": "close_position",
                "exchange": exchange,
                "symbol": symbol,
                "result": result
            }
            
        except Exception as e:
            logger.error(f"❌【智能大脑】执行平仓失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "command": "close_position"
            }
    
    async def _execute_set_sl_tp(self, params: dict) -> dict:
        """
        执行设置止损止盈
        """
        try:
            exchange = params.get('exchange')
            symbol = params.get('symbol')
            stop_loss_percent = params.get('stop_loss_percent')
            take_profit_percent = params.get('take_profit_percent')
            
            # 获取当前持仓（从记忆中枢）
            user_data = await self.data_manager.get_private_user_data()
            position = None
            
            for exchange_name, data in user_data.get('data', {}).items():
                if exchange_name.lower() == exchange:
                    positions = data.get('positions', [])
                    for pos in positions:
                        if pos.get('symbol') == symbol:
                            position = pos
                            break
            
            if not position:
                return {
                    "success": False,
                    "error": f"未找到{exchange}的{symbol}持仓",
                    "command": "set_sl_tp"
                }
            
            # 获取开仓价
            entry_price = position.get('entry_price', 0)
            position_side = position.get('side')
            
            # 计算止损止盈触发价
            sl_params = {}
            
            if stop_loss_percent is not None:
                if position_side == 'long':
                    stop_loss_price = entry_price * (1 + stop_loss_percent / 100)
                else:
                    stop_loss_price = entry_price * (1 - abs(stop_loss_percent) / 100)
                sl_params['stopLossPrice'] = stop_loss_price
            
            if take_profit_percent is not None:
                if position_side == 'long':
                    take_profit_price = entry_price * (1 + take_profit_percent / 100)
                else:
                    take_profit_price = entry_price * (1 - take_profit_percent / 100)
                sl_params['takeProfitPrice'] = take_profit_price
            
            if not sl_params:
                return {
                    "success": False,
                    "error": "未提供止损或止盈参数",
                    "command": "set_sl_tp"
                }
            
            sl_params['symbol'] = symbol
            
            # 调用工人执行
            result = await self.trader.set_sl_tp(exchange, sl_params)
            
            return {
                "success": True,
                "command": "set_sl_tp",
                "exchange": exchange,
                "symbol": symbol,
                "result": result
            }
            
        except Exception as e:
            logger.error(f"❌【智能大脑】执行止损止盈失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "command": "set_sl_tp"
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
            
            logger.info("✅【智能大脑】大脑核心已关闭")
        except Exception as e:
            logger.error(f"❌【智能大脑】关闭出错: {e}")