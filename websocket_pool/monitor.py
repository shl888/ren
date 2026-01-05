"""
连接池健康监控 - 只读模式
仅监控外部状态，不干预内部运行
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ConnectionMonitor:
    """连接健康监控器 - 只读模式"""
    
    def __init__(self, pool_manager):
        self.pool_manager = pool_manager
        self.monitoring = False
        self.monitor_task = None
        
        logger.info("ConnectionMonitor 初始化（只读模式）")
    
    async def start_monitoring(self):
        """开始监控"""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("连接监控已启动（只读模式）")
    
    async def _monitor_loop(self):
        """监控循环 - 30秒一次"""
        logger.info("开始连接监控循环（只读模式）")
        
        while self.monitoring:
            try:
                if hasattr(self.pool_manager, 'get_all_status'):
                    status = await self.pool_manager.get_all_status()
                    
                    # 只读检查，不干预
                    for exchange, exchange_status in status.items():
                        if isinstance(exchange_status, dict):
                            # 检查主连接
                            masters = exchange_status.get("masters", [])
                            if masters:
                                disconnected = [m for m in masters if isinstance(m, dict) and not m.get("connected", False)]
                                if disconnected:
                                    logger.warning(f"[监控-只读] [{exchange}] {len(disconnected)}个主连接断开")
                            
                            # 检查重启标志
                            need_restart = exchange_status.get("need_restart", False)
                            if need_restart:
                                logger.critical(f"[监控-只读] 🆘 [{exchange}] 连接池需要重启！")
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"监控循环错误: {e}")
                await asyncio.sleep(10)
    
    async def stop_monitoring(self):
        """停止监控"""
        self.monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("连接监控已停止")
    
    async def generate_report(self) -> Dict[str, Any]:
        """生成监控报告"""
        try:
            status = await self.pool_manager.get_all_status()
            
            report = {
                "timestamp": datetime.now().isoformat(),
                "status": "healthy",
                "mode": "read_only",
                "exchanges": {},
                "issues": []
            }
            
            for exchange, exchange_status in status.items():
                if isinstance(exchange_status, dict):
                    masters = exchange_status.get("masters", [])
                    warm_standbys = exchange_status.get("warm_standbys", [])
                    
                    connected_masters = [m for m in masters if isinstance(m, dict) and m.get("connected", False)]
                    connected_warm = [w for w in warm_standbys if isinstance(w, dict) and w.get("connected", False)]
                    
                    report["exchanges"][exchange] = {
                        "masters_total": len(masters),
                        "masters_connected": len(connected_masters),
                        "warm_standbys_total": len(warm_standbys),
                        "warm_standbys_connected": len(connected_warm),
                        "self_managed": exchange_status.get("self_managed", True)
                    }
                    
                    if len(connected_masters) < len(masters):
                        report["issues"].append(f"{exchange}: {len(masters)-len(connected_masters)}个主连接断开")
                        report["status"] = "warning"
                    
                    if exchange_status.get("need_restart", False):
                        report["issues"].append(f"{exchange}: 连接池需要重启")
                        report["status"] = "critical"
            
            return report
            
        except Exception as e:
            logger.error(f"生成监控报告错误: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "error",
                "error": str(e)
            }
[file content end]