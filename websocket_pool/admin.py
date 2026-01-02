"""
管理员接口 - 极简版本
"""
import asyncio
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime

from .pools.global_pool import GlobalPoolManager

logger = logging.getLogger(__name__)

class WebSocketAdmin:
    """
    WebSocket模块管理员
    大脑核心只调用这个类
    """
    
    def __init__(self, data_callback: Optional[Callable] = None):
        """
        初始化管理员
        
        Args:
            data_callback: 数据回调函数，如果为None则使用默认回调
        """
        # 使用传入的回调或创建默认回调
        self.data_callback = data_callback or self._create_default_callback()
        
        # 全局管理器
        self.global_pool = GlobalPoolManager(self.data_callback)
        
        # 状态
        self._running = False
        
        logger.info("[Admin] WebSocket管理员初始化完成")
    
    def _create_default_callback(self):
        """
        创建默认回调函数
        """
        async def default_callback(data):
            # 这里应该调用你的data_store
            try:
                from shared_data.data_store import data_store
                await data_store.update_market_data(
                    data["exchange"],
                    data["symbol"],
                    data
                )
            except ImportError:
                logger.warning(f"[Admin] 数据回调: {data.get('exchange')} {data.get('symbol')}")
            except Exception as e:
                logger.error(f"[Admin] 数据回调失败: {e}")
        
        return default_callback
    
    async def start(self, all_symbols: Dict[str, list]):
        """
        启动整个WebSocket模块
        
        Args:
            all_symbols: 交易所 -> 合约列表 的字典
        """
        if self._running:
            logger.warning("[Admin] 模块已在运行中")
            return True
        
        logger.info("=" * 60)
        logger.info("[Admin] 正在启动WebSocket模块...")
        logger.info("=" * 60)
        
        try:
            # 初始化全局连接池
            await self.global_pool.initialize(all_symbols)
            
            self._running = True
            
            logger.info("[Admin] ✅ WebSocket模块启动成功")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"[Admin] 启动失败: {e}")
            await self.stop()
            return False
    
    async def stop(self):
        """
        停止整个WebSocket模块
        """
        if not self._running:
            logger.info("[Admin] 模块未在运行")
            return
        
        logger.info("[Admin] 正在停止WebSocket模块...")
        
        # 关闭全局连接池
        await self.global_pool.shutdown()
        
        self._running = False
        
        logger.info("[Admin] ✅ WebSocket模块已停止")
    
    async def get_status(self) -> Dict[str, Any]:
        """
        获取模块状态
        """
        try:
            # 获取详细状态
            detailed_status = await self.global_pool.get_status()
            
            # 生成摘要
            summary = {
                "module": "websocket_pool",
                "running": self._running,
                "initialized": self.global_pool.is_initialized,
                "exchanges": {},
                "timestamp": datetime.now().isoformat()
            }
            
            # 提取关键信息
            for exchange, ex_status in detailed_status.get("exchanges", {}).items():
                data_workers = ex_status.get("data_workers", [])
                backup_workers = ex_status.get("backup_workers", [])
                
                connected_data = sum(1 for w in data_workers if w.get("connected", False))
                connected_backup = sum(1 for w in backup_workers if w.get("connected", False))
                
                summary["exchanges"][exchange] = {
                    "data_workers_total": len(data_workers),
                    "data_workers_connected": connected_data,
                    "backup_workers_total": len(backup_workers),
                    "backup_workers_connected": connected_backup,
                    "health": "good" if connected_data == len(data_workers) else "warning"
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"[Admin] 获取状态失败: {e}")
            return {
                "module": "websocket_pool",
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """
        健康检查
        """
        if not self._running:
            return {
                "healthy": False,
                "message": "模块未运行"
            }
        
        try:
            status = await self.get_status()
            
            # 检查关键指标
            all_healthy = True
            messages = []
            
            for exchange, info in status.get("exchanges", {}).items():
                data_connected = info.get("data_workers_connected", 0)
                data_total = info.get("data_workers_total", 0)
                
                if data_connected < data_total:
                    all_healthy = False
                    messages.append(f"{exchange}: {data_total - data_connected}个数据工作者断开")
            
            if all_healthy:
                return {
                    "healthy": True,
                    "message": "所有数据工作者正常",
                    "details": status
                }
            else:
                return {
                    "healthy": False,
                    "message": ", ".join(messages),
                    "details": status
                }
            
        except Exception as e:
            return {
                "healthy": False,
                "message": f"健康检查异常: {e}"
            }
    
    def is_running(self) -> bool:
        """
        判断模块是否在运行
        
        Returns:
            bool: 运行状态
        """
        return self._running