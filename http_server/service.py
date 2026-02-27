# http_server/service.py
"""
HTTP模块服务接口 - 简化版
🚨 只保留HTTP服务器基础功能
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class HTTPModuleService:
    """HTTP模块服务主类 - 只提供HTTP服务器基础功能"""
    
    def __init__(self):
        self.brain = None  # 大脑引用
        self.initialized = False
        
        logger.info("✅ HTTP模块服务初始化完成")
    
    async def initialize(self, brain) -> bool:
        """初始化HTTP模块服务"""
        self.brain = brain
        logger.info("🚀 HTTP模块服务初始化中...")
        
        self.initialized = True
        logger.info("✅ HTTP模块服务初始化完成")
        return True
    
    async def execute_api(self, exchange: str, method: str, **kwargs) -> Dict[str, Any]:
        """🚨 交易API - 已删除，返回明确错误"""
        return {
            "success": False, 
            "error": "🚨 交易功能已从架构中删除。如需交易，请实现直接HTTP方案。"
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
            'note': '纯HTTP服务器模块，所有业务功能已迁移'
        }