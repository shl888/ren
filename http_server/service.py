# http_server/service.py
"""
HTTP模块服务接口 - 简化版，只负责listenKey服务
🚨 完全删除交易功能，不预留任何代码
"""
import asyncio
import logging
from typing import Dict, Any

# 🚨 删除：from .exchange_api import ExchangeAPI
from .listen_key_manager import ListenKeyManager

logger = logging.getLogger(__name__)

class HTTPModuleService:
    """HTTP模块服务主类 - 简化版，只管理listenKey"""
    
    def __init__(self):
        self.brain = None  # 大脑引用
        self.listen_key_managers = {}  # 令牌管理器 {exchange: ListenKeyManager实例}
        self.initialized = False
        
        # 🚨 删除所有API方法映射
        # 🚨 删除所有ExchangeAPI相关代码
        
        logger.info("✅ HTTP模块服务初始化完成（简化版）")
    
    async def initialize(self, brain) -> bool:
        """初始化HTTP模块服务 - 简化版"""
        self.brain = brain
        logger.info("🚀 HTTP模块服务初始化中...")
        
        # 🚨 删除：创建ExchangeAPI工具的代码
        
        self.initialized = True
        logger.info("✅ HTTP模块服务初始化完成")
        return True
    
    async def execute_api(self, exchange: str, method: str, **kwargs) -> Dict[str, Any]:
        """🚨 交易API - 已删除，返回明确错误"""
        return {
            "success": False, 
            "error": "🚨 交易功能已从架构中删除。如需交易，请实现直接HTTP方案。"
        }
    
    # 🚨 删除所有 _execute_* 方法（共8个）
    
    async def start_listen_key_service(self, exchange: str = 'binance') -> bool:
        """启动令牌服务 - 简化版本"""
        if not self.initialized:
            logger.error("HTTP模块未初始化")
            return False
        
        if exchange in self.listen_key_managers:
            logger.info(f"⚠️ {exchange}令牌服务已在运行")
            return True
        
        try:
            # ✅ 关键修改：传入data_manager（和私人连接池一样）
            # 确保大脑有data_manager
            if not hasattr(self.brain, 'data_manager'):
                logger.error("❌ 大脑没有data_manager属性")
                return False
            
            # 创建ListenKeyManager，传入data_manager
            manager = ListenKeyManager(self.brain.data_manager)  # ✅ 传入data_manager
            
            if await manager.start():
                self.listen_key_managers[exchange] = manager
                logger.info(f"✅ HTTP模块启动{exchange}令牌服务（直接HTTP）")
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
        
        self.listen_key_managers.clear()
        self.initialized = False
        logger.info("✅ HTTP模块服务已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取HTTP模块状态"""
        return {
            'initialized': self.initialized,
            'listen_key_managers_count': len(self.listen_key_managers),
            'listen_key_services': list(self.listen_key_managers.keys()),
            'implementation': 'direct_http_only',
            'note': '交易功能已删除，只保留listenKey服务'
        }