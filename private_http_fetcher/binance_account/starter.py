"""
币安账户资产获取 - 启动器
"""
import asyncio
import logging
from .fetcher import PrivateHTTPFetcher

logger = logging.getLogger(__name__)

# 创建全局实例
_http_fetcher = None

async def start_account_task():
    """启动币安账户资产获取任务"""
    global _http_fetcher
    
    logger.info("🚀 [账户资产] 启动币安账户资产获取任务...")
    
    try:
        # 创建获取器实例
        _http_fetcher = PrivateHTTPFetcher()
        
        # 从shared_data获取brain_store（需要在launcher中设置）
        from shared_data import brain_store
        
        # 启动
        success = await _http_fetcher.start(brain_store)
        
        if success:
            logger.info("✅ [账户资产] 币安账户资产获取任务已启动")
            return _http_fetcher
        else:
            logger.error("❌ [账户资产] 启动失败")
            return None
            
    except Exception as e:
        logger.error(f"❌ [账户资产] 启动异常: {e}")
        return None

async def stop_account_task():
    """停止账户资产获取任务"""
    global _http_fetcher
    
    if _http_fetcher:
        await _http_fetcher.shutdown()
        _http_fetcher = None
        logger.info("✅ [账户资产] 已停止")

def get_account_fetcher():
    """获取当前的获取器实例（用于状态查询）"""
    return _http_fetcher