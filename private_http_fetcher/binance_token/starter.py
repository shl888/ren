# private_http_fetcher/binance_token/starter.py
"""
令牌任务启动器 - 在私人模块内部自己启动
"""
import asyncio
import logging
from .listen_key_manager import ListenKeyManager
from shared_data import brain_store  # 假设大脑可以从这里获取

logger = logging.getLogger(__name__)

class TokenTaskStarter:
    """令牌任务启动器"""
    
    def __init__(self):
        self.manager = None
        self.task = None
    
    async def start(self):
        """启动令牌任务"""
        logger.info("🚀 启动币安令牌任务（私人模块内部）")
        
        # 创建管理器
        self.manager = ListenKeyManager(brain_store)
        
        # 启动
        if await self.manager.start():
            logger.info("✅ 币安令牌任务已启动")
            return True
        else:
            logger.error("❌ 币安令牌任务启动失败")
            return False
    
    async def stop(self):
        """停止令牌任务"""
        if self.manager:
            await self.manager.stop()
    
    async def run_forever(self):
        """保持运行"""
        while True:
            await asyncio.sleep(60)
            # 可以在这里添加健康检查

# 创建全局实例
token_starter = TokenTaskStarter()

async def start_token_task():
    """供外部调用的启动函数"""
    return await token_starter.start()

# 在模块导入时自动启动（可选）
# asyncio.create_task(token_starter.start())