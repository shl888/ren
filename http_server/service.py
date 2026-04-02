# http_server/service.py
"""
HTTP模块服务接口 - 简化版
🚨 只保留HTTP服务器基础功能
"""
import logging
from typing import Dict, Any

# ========== 导入工人 ==========
from .trader import Trader

logger = logging.getLogger(__name__)

class HTTPModuleService:
    """HTTP模块服务主类 - 只提供HTTP服务器基础功能"""
    
    def __init__(self):
        self.brain = None
        self.initialized = False
        self.trader = None
        
        logger.info("✅ HTTP模块服务初始化完成")
    
    async def initialize(self, brain) -> bool:
        """初始化HTTP模块服务"""
        self.brain = brain
        logger.info("🚀 HTTP模块服务初始化中...")
        
        # 初始化工人
        self.trader = Trader(brain)
        logger.info("✅ 下单工人已初始化")
        
        self.initialized = True
        logger.info("✅ HTTP模块服务初始化完成")
        return True
    
    # ========== 交易接口 ==========
    async def place_order(self, exchange: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """开仓/平仓"""
        return await self.trader.place_order(exchange, params)
    
    async def close_position(self, exchange: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """平仓"""
        return await self.trader.close_position(exchange, params)
    
    async def set_sl_tp(self, exchange: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """设置止损止盈"""
        return await self.trader.set_sl_tp(exchange, params)
    
    # ========== 保留原有方法 ==========
    async def execute_api(self, exchange: str, method: str, **kwargs) -> Dict[str, Any]:
        """已废弃"""
        return {
            "success": False, 
            "error": "🚨 请使用 place_order / close_position / set_sl_tp 方法"
        }
    
    async def shutdown(self):
        """关闭HTTP模块服务"""
        logger.info("🛑 HTTP模块服务关闭中...")
        self.initialized = False
        logger.info("✅ HTTP模块服务已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取HTTP模块状态"""
        return {
            'initialized': self.initialized,
            'trader_ready': self.trader is not None,
            'note': 'HTTP模块服务已集成下单工人'
        }