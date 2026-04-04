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
        
        # ========== 下单工人 ==========
        self.trader = None
        
        # 信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    def send_to_worker(self, orders):
        """发数据给下单工人"""
        if self.trader:
            self.trader.send_orders(orders)
    
    async def on_trader_results(self, results):
        """接收下单工人发来的数据"""
        logger.info(f"📥【智能大脑】收到下单工人发来的执行结果，共 {len(results)} 条")
        
        parsed_results = []
        
        for idx, result in enumerate(results):
            if not result.get("success"):
                # 下单工人层面失败（发请求前就失败了）
                error_msg = result.get("error", "未知错误")
                logger.error(f"   ❌ 订单{idx+1}: 下单工人发送失败 | 错误: {error_msg}")
                parsed_results.append({
                    "original": result,
                    "parsed_status": "failed",
                    "parsed_error": error_msg,
                    "error_code": None
                })
                continue
            
            # 下单工人发送成功，解析交易所返回的真实结果
            exchange = result.get("exchange")
            order_type = result.get("type")
            data = result.get("data", {})
            
            if exchange == "binance":
                # 币安：判断 status 字段
                status = data.get("status")
                
                if status == "FILLED":
                    logger.info(f"   ✅ 订单{idx+1}: 币安成交 | 订单ID: {data.get('orderId')} | 成交均价: {data.get('avgPrice')} | 成交量: {data.get('executedQty')}")
                    parsed_results.append({
                        "original": result,
                        "parsed_status": "filled",
                        "parsed_error": None,
                        "error_code": None,
                        "order_id": data.get("orderId"),
                        "avg_price": data.get("avgPrice"),
                        "executed_qty": data.get("executedQty")
                    })
                elif status == "PARTIALLY_FILLED":
                    logger.info(f"   ⏳ 订单{idx+1}: 币安部分成交 | 订单ID: {data.get('orderId')} | 已成交量: {data.get('executedQty')}")
                    parsed_results.append({
                        "original": result,
                        "parsed_status": "partially_filled",
                        "parsed_error": None,
                        "error_code": None,
                        "order_id": data.get("orderId"),
                        "executed_qty": data.get("executedQty")
                    })
                elif status == "NEW":
                    logger.info(f"   ⏳ 订单{idx+1}: 币安已接收待处理 | 订单ID: {data.get('orderId')}")
                    parsed_results.append({
                        "original": result,
                        "parsed_status": "pending",
                        "parsed_error": None,
                        "error_code": None,
                        "order_id": data.get("orderId")
                    })
                elif status in ["REJECTED", "EXPIRED", "EXPIRED_IN_MATCH", "CANCELED"]:
                    error_msg = data.get("msg", f"状态: {status}")
                    logger.error(f"   ❌ 订单{idx+1}: 币安失败 | {error_msg}")
                    parsed_results.append({
                        "original": result,
                        "parsed_status": "failed",
                        "parsed_error": error_msg,
                        "error_code": status
                    })
                else:
                    logger.warning(f"   ⚠️ 订单{idx+1}: 币安未知状态 | status={status}")
                    parsed_results.append({
                        "original": result,
                        "parsed_status": "unknown",
                        "parsed_error": f"未知状态: {status}",
                        "error_code": status
                    })
            
            elif exchange == "okx":
                # 欧易：code == "0" 成功，非0失败
                code = str(data.get("code", ""))
                msg = data.get("msg", "")
                
                if code == "0":
                    ord_id = data.get("ordId")
                    logger.info(f"   ✅ 订单{idx+1}: 欧易成功 | 订单ID: {ord_id}")
                    parsed_results.append({
                        "original": result,
                        "parsed_status": "success",
                        "parsed_error": None,
                        "error_code": None,
                        "order_id": ord_id
                    })
                else:
                    logger.error(f"   ❌ 订单{idx+1}: 欧易失败 | 错误码: {code} | 错误信息: {msg}")
                    parsed_results.append({
                        "original": result,
                        "parsed_status": "failed",
                        "parsed_error": msg,
                        "error_code": code
                    })
            
            else:
                logger.warning(f"   ⚠️ 订单{idx+1}: 未知交易所 {exchange}，无法解析")
                parsed_results.append({
                    "original": result,
                    "parsed_status": "unknown",
                    "parsed_error": f"未知交易所: {exchange}",
                    "error_code": None
                })
        
        # 推送到前端（推送解析后的结果）
        if self.frontend_relay:
            await self.frontend_relay.broadcast_execution_results(parsed_results)
    
    async def initialize(self):
        """初始化智能大脑核心"""
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
            
            # 2. 启动状态日志任务
            self.status_log_task = asyncio.create_task(self.data_manager._log_data_status())
            
            # 3. 完成初始化
            self.running = True
            logger.info("✅【智能大脑】 大脑核心初始化完成")
            
            # 输出HTTP模块状态
            if self.http_module:
                http_status = self.http_module.get_status()
                logger.info(f"📊【智能大脑】 HTTP模块状态: {http_status}")
            
            # 4. 创建并连接下单工人
            from http_server.trader import Trader
            self.trader = Trader(self, use_sandbox=True)
            asyncio.create_task(self.trader.start())
            logger.info("✅【智能大脑】下单工人已创建并启动")
            
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
            logger.info(f"💰【智能大脑】收到开仓指令: {params}")
            
            # 读取参数
            symbol = params.get('symbol', '')
            margin = params.get('margin', 0)
            leverage = params.get('leverage', 20)
            direction = params.get('direction', '')
            
            logger.info(f"   📊 读取结果: symbol={symbol}, margin={margin}, leverage={leverage}, direction={direction}")
            
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": "指令已接收，暂不执行任何操作"
            }
        
        # ========== 止损止盈指令 ==========
        if command == 'set_sl_tp':
            logger.info(f"⚙️【智能大脑】收到止损止盈指令: {params}")
            
            # 读取参数
            okx_symbol = params.get('okx', {}).get('symbol', '') if 'okx' in params else ''
            binance_symbol = params.get('binance', {}).get('symbol', '') if 'binance' in params else ''
            stop_loss = params.get('stop_loss_percent', 0)
            take_profit = params.get('take_profit_percent', 0)
            calc_type = params.get('type', 'price_percent')
            
            logger.info(f"   📊 读取结果: okx={okx_symbol}, binance={binance_symbol}, 止损={stop_loss}%, 止盈={take_profit}%, 类型={calc_type}")
            
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": "指令已接收，暂不执行任何操作"
            }
        
        # ========== 平仓指令 ==========
        if command == 'close_position':
            logger.info(f"🔚【智能大脑】收到平仓指令: {params}")
            
            # 读取参数
            okx_symbol = params.get('okx', {}).get('symbol', '') if 'okx' in params else ''
            binance_symbol = params.get('binance', {}).get('symbol', '') if 'binance' in params else ''
            order_type = params.get('order_type', 'market')
            
            logger.info(f"   📊 读取结果: okx={okx_symbol}, binance={binance_symbol}, 订单类型={order_type}")
            
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": "指令已接收，暂不执行任何操作"
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