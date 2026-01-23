"""
指令路由器 - 处理前端所有指令
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CommandRouter:
    def __init__(self, brain):
        self.brain = brain
    
    async def _execute_exchange_api(self, exchange_name, api_method, **kwargs):
        """执行交易所API调用 - 通过HTTP模块服务"""
        try:
            # ✅ 修改：检查HTTP模块服务是否就绪
            if not hasattr(self.brain, 'http_module') or not self.brain.http_module:
                return {
                    "success": False, 
                    "error": f"❌【智能大脑】HTTP模块服务未就绪"
                }
            
            # ✅ 修改：通过HTTP模块服务执行
            result = await self.brain.http_module.execute_api(
                exchange=exchange_name,
                method=api_method,
                **kwargs
            )
            
            return result
            
        except Exception as e:
            logger.error(f"❌【智能大脑】执行API失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "error": str(e)}
    
    # ============ 【后面所有方法保持不变】============
    # 全部保留原有逻辑，只是调用方式改为通过HTTP模块服务
    
    async def handle_frontend_command(self, command_data):
        """
        处理前端指令 - 完整实现
        基于现有的 http_server/exchange_api.py
        """
        try:
            command = command_data.get('command', '')
            params = command_data.get('params', {})
            client_id = command_data.get('client_id', 'unknown')
            
            logger.info(f"🧠 【智能大脑】处理前端指令: {command} from {client_id}")
            
            # 根据指令类型处理
            if command == 'place_order':
                return await self._handle_place_order(params, client_id)
            elif command == 'cancel_order':
                return await self._handle_cancel_order(params, client_id)
            elif command == 'get_open_orders':
                return await self._handle_get_open_orders(params, client_id)
            elif command == 'get_order_history':
                return await self._handle_get_order_history(params, client_id)
            elif command == 'set_leverage':
                return await self._handle_set_leverage(params, client_id)
            elif command == 'get_account_balance':
                return await self._handle_get_account_balance(params, client_id)
            elif command == 'get_positions':
                return await self._handle_get_positions(params, client_id)
            elif command == 'get_ticker':
                return await self._handle_get_ticker(params, client_id)
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
    
    async def _handle_place_order(self, params, client_id):
        """处理下单指令"""
        required = ['exchange', 'symbol', 'type', 'side', 'amount']
        for field in required:
            if field not in params:
                return {
                    "success": False,
                    "error": f"❌【智能大脑】缺少必要参数: {field}",
                    "client_id": client_id,
                    "timestamp": datetime.now().isoformat()
                }
        
        exchange = params['exchange']
        symbol = params['symbol']
        order_type = params['type']
        side = params['side']
        amount = float(params['amount'])
        price = float(params.get('price', 0))
        extra_params = params.get('params', {})
        
        result = await self._execute_exchange_api(
            exchange,
            'create_order',
            symbol=symbol,
            order_type=order_type,
            side=side,
            amount=amount,
            price=price if price > 0 else None,
            params=extra_params
        )
        
        result.update({
            "command": "place_order",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_cancel_order(self, params, client_id):
        """处理取消订单指令"""
        if 'exchange' not in params or 'symbol' not in params or 'order_id' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange、symbol或order_id参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params['symbol']
        order_id = params['order_id']
        
        result = await self._execute_exchange_api(
            exchange,
            'cancel_order',
            symbol=symbol,
            order_id=order_id
        )
        
        result.update({
            "command": "cancel_order",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_open_orders(self, params, client_id):
        """处理获取未成交订单指令"""
        if 'exchange' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params.get('symbol')
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_open_orders',
            symbol=symbol
        )
        
        result.update({
            "command": "get_open_orders",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_order_history(self, params, client_id):
        """处理获取订单历史指令"""
        if 'exchange' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params.get('symbol')
        limit = params.get('limit', 100)
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_order_history',
            symbol=symbol,
            limit=limit
        )
        
        result.update({
            "command": "get_order_history",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_set_leverage(self, params, client_id):
        """处理设置杠杆指令"""
        if 'exchange' not in params or 'symbol' not in params or 'leverage' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange、symbol或leverage参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params['symbol']
        leverage = int(params['leverage'])
        
        result = await self._execute_exchange_api(
            exchange,
            'set_leverage',
            symbol=symbol,
            leverage=leverage
        )
        
        result.update({
            "command": "set_leverage",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_account_balance(self, params, client_id):
        """处理获取账户余额指令"""
        if 'exchange' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_account_balance'
        )
        
        result.update({
            "command": "get_account_balance",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_positions(self, params, client_id):
        """处理获取持仓指令"""
        if 'exchange' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_positions'
        )
        
        result.update({
            "command": "get_positions",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
    async def _handle_get_ticker(self, params, client_id):
        """处理获取ticker指令"""
        if 'exchange' not in params or 'symbol' not in params:
            return {
                "success": False,
                "error": "❌【智能大脑】缺少exchange或symbol参数",
                "client_id": client_id,
                "timestamp": datetime.now().isoformat()
            }
        
        exchange = params['exchange']
        symbol = params['symbol']
        
        result = await self._execute_exchange_api(
            exchange,
            'fetch_ticker',
            symbol=symbol
        )
        
        result.update({
            "command": "get_ticker",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        })
        return result
    
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