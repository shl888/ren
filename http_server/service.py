"""
HTTP模块服务接口 - 提供完整的交易和令牌服务
大脑通过这个接口调用，不直接操作内部工具
"""
import asyncio
import logging
from typing import Dict, Any, Optional, Callable

from .exchange_api import ExchangeAPI
from .listen_key_manager import ListenKeyManager

logger = logging.getLogger(__name__)

class HTTPModuleService:
    """HTTP模块服务主类 - 大脑的唯一接口"""
    
    def __init__(self):
        self.brain = None  # 大脑引用
        self.exchange_apis = {}  # 各交易所API工具 {exchange: ExchangeAPI实例}
        self.listen_key_managers = {}  # 令牌管理器 {exchange: ListenKeyManager实例}
        self.initialized = False
        
        # API方法映射
        self.api_methods = {
            'create_order': self._execute_create_order,
            'cancel_order': self._execute_cancel_order,
            'fetch_open_orders': self._execute_fetch_open_orders,
            'fetch_order_history': self._execute_fetch_order_history,
            'fetch_account_balance': self._execute_fetch_account_balance,
            'fetch_positions': self._execute_fetch_positions,
            'set_leverage': self._execute_set_leverage,
            'fetch_ticker': self._execute_fetch_ticker,
        }
    
    async def initialize(self, brain) -> bool:
        """初始化HTTP模块服务"""
        self.brain = brain
        logger.info("🚀 HTTP模块服务初始化中...")
        
        # 创建所有交易所的API工具（按需创建，先不初始化）
        # 这里只创建结构，等实际需要时再获取API并初始化
        
        self.initialized = True
        logger.info("✅ HTTP模块服务初始化完成")
        return True
    
    async def execute_api(self, exchange: str, method: str, **kwargs) -> Dict[str, Any]:
        """
        执行交易所API - 统一入口
        大脑调用此方法，不直接创建ExchangeAPI
        """
        if not self.initialized:
            return {"success": False, "error": "HTTP模块未初始化"}
        
        if method not in self.api_methods:
            return {"success": False, "error": f"不支持的API方法: {method}"}
        
        # 调用对应的执行方法
        executor = self.api_methods[method]
        return await executor(exchange, **kwargs)
    
    async def _execute_create_order(self, exchange: str, **kwargs) -> Dict[str, Any]:
        """执行创建订单"""
        return await self._execute_with_api(
            exchange, 
            'create_order',
            **{k: v for k, v in kwargs.items() if k not in ['price']}
        )
    
    async def _execute_cancel_order(self, exchange: str, **kwargs) -> Dict[str, Any]:
        """执行取消订单"""
        return await self._execute_with_api(exchange, 'cancel_order', **kwargs)
    
    async def _execute_fetch_open_orders(self, exchange: str, **kwargs) -> Dict[str, Any]:
        """执行获取未成交订单"""
        return await self._execute_with_api(exchange, 'fetch_open_orders', **kwargs)
    
    async def _execute_fetch_order_history(self, exchange: str, **kwargs) -> Dict[str, Any]:
        """执行获取订单历史"""
        return await self._execute_with_api(exchange, 'fetch_order_history', **kwargs)
    
    async def _execute_fetch_account_balance(self, exchange: str, **kwargs) -> Dict[str, Any]:
        """执行获取账户余额"""
        return await self._execute_with_api(exchange, 'fetch_account_balance', **kwargs)
    
    async def _execute_fetch_positions(self, exchange: str, **kwargs) -> Dict[str, Any]:
        """执行获取持仓"""
        return await self._execute_with_api(exchange, 'fetch_positions', **kwargs)
    
    async def _execute_set_leverage(self, exchange: str, **kwargs) -> Dict[str, Any]:
        """执行设置杠杆"""
        return await self._execute_with_api(exchange, 'set_leverage', **kwargs)
    
    async def _execute_fetch_ticker(self, exchange: str, **kwargs) -> Dict[str, Any]:
        """执行获取ticker"""
        return await self._execute_with_api(exchange, 'fetch_ticker', **kwargs)
    
    async def _execute_with_api(self, exchange: str, method: str, **kwargs) -> Dict[str, Any]:
        """使用ExchangeAPI执行具体方法"""
        try:
            # 1. 从大脑获取API凭证（HTTP模块自己有权限）
            api_creds = await self.brain.data_manager.get_api_credentials(exchange)
            if not api_creds or not api_creds.get('api_key'):
                return {
                    "success": False, 
                    "error": f"{exchange} API凭证不存在或无效"
                }
            
            # 2. 获取或创建ExchangeAPI工具
            api = await self._get_or_create_exchange_api(exchange, api_creds)
            if not api:
                return {
                    "success": False, 
                    "error": f"创建{exchange} API工具失败"
                }
            
            # 3. 执行方法
            api_method = getattr(api, method)
            result = await api_method(**kwargs)
            
            # 4. 格式化返回结果
            if "error" in result:
                return {"success": False, "error": result["error"]}
            else:
                return {"success": True, "data": result}
                
        except Exception as e:
            logger.error(f"❌ HTTP模块执行{exchange}.{method}失败: {e}")
            return {"success": False, "error": str(e)}
    
    async def _get_or_create_exchange_api(self, exchange: str, api_creds: Dict[str, str]) -> Optional[ExchangeAPI]:
        """获取或创建ExchangeAPI工具"""
        if exchange not in self.exchange_apis:
            # 创建新工具 - 不初始化
            try:
                api = ExchangeAPI(exchange, api_creds)
                self.exchange_apis[exchange] = api
                logger.info(f"✅ HTTP模块创建{exchange} ExchangeAPI工具（懒加载）")
            except Exception as e:
                logger.error(f"❌ HTTP模块创建{exchange} ExchangeAPI异常: {e}")
                return None
        
        return self.exchange_apis[exchange]
    
    async def start_listen_key_service(self, exchange: str = 'binance') -> bool:
        """启动令牌服务 - 简化版本，严格按老板方案"""
        if not self.initialized:
            logger.error("HTTP模块未初始化")
            return False
        
        if exchange in self.listen_key_managers:
            logger.info(f"⚠️ {exchange}令牌服务已在运行")
            return True
        
        try:
            # 步骤1：启动任务 - 不需要API
            # 创建ListenKeyManager，不传ExchangeAPI
            manager = ListenKeyManager(self.brain.data_manager)
            
            if await manager.start():
                self.listen_key_managers[exchange] = manager
                logger.info(f"✅ HTTP模块启动{exchange}令牌服务")
                return True
            else:
                logger.error(f"❌ HTTP模块启动{exchange}令牌服务失败")
                return False
                
        except Exception as e:
            logger.error(f"❌ HTTP模块启动{exchange}令牌服务异常: {e}")
            return False
    
    async def shutdown(self):
        """关闭HTTP模块服务"""
        logger.info("🛑 HTTP模块服务关闭中...")
        
        # 关闭所有令牌管理器
        for exchange, manager in self.listen_key_managers.items():
            try:
                await manager.stop()
                logger.info(f"✅ 关闭{exchange}令牌服务")
            except Exception as e:
                logger.error(f"❌ 关闭{exchange}令牌服务失败: {e}")
        
        # 关闭所有API工具
        for exchange, api in self.exchange_apis.items():
            try:
                await api.close()
                logger.info(f"✅ 关闭{exchange} API工具")
            except Exception as e:
                logger.error(f"❌ 关闭{exchange} API工具失败: {e}")
        
        self.exchange_apis.clear()
        self.listen_key_managers.clear()
        self.initialized = False
        logger.info("✅ HTTP模块服务已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取HTTP模块状态"""
        return {
            'initialized': self.initialized,
            'exchange_apis_count': len(self.exchange_apis),
            'listen_key_managers_count': len(self.listen_key_managers),
            'exchanges_ready': list(self.exchange_apis.keys()),
            'listen_key_services': list(self.listen_key_managers.keys())
        }