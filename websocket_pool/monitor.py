"""
连接池健康监控 - 修复版 + 冗余感知
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ConnectionMonitor:
    """连接健康监控器 - 修复版"""
    
    def __init__(self, pool_manager):
        self.pool_manager = pool_manager
        self.monitoring = False
        self.monitor_task = None
        
    async def start_monitoring(self):
        """开始监控"""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("连接监控已启动")
    
    async def _monitor_loop(self):
        """监控循环"""
        while self.monitoring:
            try:
                if hasattr(self.pool_manager, 'get_all_status'):
                    status = await self.pool_manager.get_all_status()
                    
                    for exchange, exchange_status in status.items():
                        if isinstance(exchange_status, dict):
                            masters = exchange_status.get("masters", [])
                            if masters:
                                disconnected = [m for m in masters if isinstance(m, dict) and not m.get("connected", False)]
                                
                                # 🚨 修复：考虑冗余的告警
                                min_required = max(1, len(masters) - 1)
                                if len(disconnected) > len(masters) - min_required:
                                    logger.warning(f"[{exchange}] {len(disconnected)}个主连接断开，低于最小要求{min_required}")
                
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
        """生成监控报告 - 修复版"""
        try:
            status = await self.pool_manager.get_all_status()
            
            report = {
                "timestamp": datetime.now().isoformat(),
                "status": "healthy",
                "exchanges": {},
                "issues": []
            }
            
            for exchange, exchange_status in status.items():
                if isinstance(exchange_status, dict):
                    masters = exchange_status.get("masters", [])
                    warm_standbys = exchange_status.get("warm_standbys", [])
                    
                    connected_masters = [m for m in masters if isinstance(m, dict) and m.get("connected", False)]
                    connected_warm = [w for w in warm_standbys if isinstance(w, dict) and w.get("connected", False)]
                    
                    # 🚨 修复：考虑冗余的健康判断
                    min_required_masters = max(1, len(masters) - 1)
                    
                    report["exchanges"][exchange] = {
                        "masters_total": len(masters),
                        "masters_connected": len(connected_masters),
                        "warm_standbys_total": len(warm_standbys),
                        "warm_standbys_connected": len(connected_warm),
                        "last_check": exchange_status.get("timestamp", datetime.now().isoformat()),
                        "health": "good" if len(connected_masters) >= min_required_masters else "warning"
                    }
                    
                    if len(connected_masters) < min_required_masters:
                        report["issues"].append(f"{exchange}: 主连接不足（{len(connected_masters)}/{min_required_masters}）")
                        report["status"] = "warning"
                    
                    if len(connected_warm) < len(warm_standbys):
                        report["issues"].append(f"{exchange}: {len(warm_standbys)-len(connected_warm)}个温备连接断开")
            
            return report
            
        except Exception as e:
            logger.error(f"生成监控报告错误: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "error",
                "error": str(e)
            }
