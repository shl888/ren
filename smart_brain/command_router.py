# smart_brain/command_router.py
"""
指令路由器 - 处理前端所有指令
🚨 已删除所有交易功能，只保留数据查询
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CommandRouter:
    def __init__(self, brain):
        self.brain = brain
    
    async def _execute_exchange_api(self, exchange_name, api_method, **kwargs):
        """🚨 交易API调用 - 已完全删除，返回明确错误"""
        return {
            "success": False, 
            "error": "🚨 交易功能已从架构中删除。系统当前只支持市场数据监控和listenKey管理。",
            "details": "如需交易功能，请实现直接HTTP交易方案。",
            "timestamp": datetime.now().isoformat()
        }
    
    async def handle_frontend_command(self, command_data):
        """
        处理前端指令 - 简化版（只保留数据查询）
        """
        try:
            command = command_data.get('command', '')
            params = command_data.get('params', {})
            client_id = command_data.get('client_id', 'unknown')
            
            logger.info(f"🧠 【智能大脑】处理前端指令: {command} from {client_id}")
            
            # 根据指令类型处理 - 🚨 只保留数据查询指令
            if command == 'place_order':
                return await self._handle_removed_command("下单", command, client_id)
            elif command == 'cancel_order':
                return await self._handle_removed_command("取消订单", command, client_id)
            elif command == 'get_open_orders':
                return await self._handle_removed_command("获取未成交订单", command, client_id)
            elif command == 'get_order_history':
                return await self._handle_removed_command("获取订单历史", command, client_id)
            elif command == 'set_leverage':
                return await self._handle_removed_command("设置杠杆", command, client_id)
            elif command == 'get_account_balance':
                return await self._handle_removed_command("获取账户余额", command, client_id)
            elif command == 'get_positions':
                return await self._handle_removed_command("获取持仓", command, client_id)
            elif command == 'get_ticker':
                return await self._handle_removed_command("获取ticker", command, client_id)
            elif command == 'get_market_data':
                return await self._handle_get_market_data(params, client_id)
            elif command == 'get_connection_status':
                return await self._handle_get_connection_status(params, client_id)
            else:
                return {
                    "success": False,
                    "error": f"⚠️【智能大脑】未知指令: {command}",
                    "client_id": client_id,
                    "timestamp": datetime.now().isoformat()
                }
            
        except Exception as e:
            error_msg = f"❌【智能大脑】处理前端指令失败: {e}"
            logger.error(error_msg)
            return {
                'success': False, 
                'error': error_msg,
                'command': command_data.get('command', 'unknown'),
                'timestamp': datetime.now().isoformat()
            }
    
    async def _handle_removed_command(self, command_name: str, command: str, client_id: str):
        """处理已删除的交易指令"""
        return {
            "success": False,
            "error": f"🚨【智能大脑】{command_name}功能已从架构中删除",
            "details": "系统当前只支持市场数据监控和listenKey管理。",
            "command": command,
            "client_id": client_id,
            "timestamp": datetime.now().isoformat(),
            "suggestion": "如需交易功能，请实现直接HTTP交易方案，不依赖ccxt包装。"
        }
    
    # 🚨 删除以下所有交易处理方法：
    # _handle_place_order
    # _handle_cancel_order
    # _handle_get_open_orders
    # _handle_get_order_history
    # _handle_set_leverage
    # _handle_get_account_balance
    # _handle_get_positions
    # _handle_get_ticker
    
    # ✅ 只保留这两个数据查询方法：
    
    async def _handle_get_market_data(self, params, client_id):
        """处理获取市场数据指令"""
        try:
            exchange = params.get('exchange', '')
            symbol = params.get('symbol')
            
            if not exchange:
                return {
                    "success": False,
                    "error": "❌【智能大脑】缺少exchange参数",
                    "client_id": client_id,
                    "timestamp": datetime.now().isoformat()
                }
            
            # 从data_store获取市场数据
            from shared_data.data_store import data_store
            market_data = await data_store.get_market_data(exchange, symbol)
            
            return {
                "success": True,
                "command": "get_market_data",
                "client_id": client_id,
                "data": market_data,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌【智能大脑】获取市场数据失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
    
    async def _handle_get_connection_status(self, params, client_id):
        """处理获取连接状态指令"""
        try:
            exchange = params.get('exchange')
            
            # 从data_store获取连接状态
            from shared_data.data_store import data_store
            connection_status = await data_store.get_connection_status(exchange)
            
            return {
                "success": True,
                "command": "get_connection_status",
                "client_id": client_id,
                "data": connection_status,
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌【智能大脑】获取连接状态失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }