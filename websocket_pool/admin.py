"""
WebSocket连接池管理员 - 修复版
支持被动接收重启请求
"""

import asyncio
import logging
from typing import Dict, Any, List
from datetime import datetime

from .pool_manager import WebSocketPoolManager
from .monitor import ConnectionMonitor
from .exchange_pool import ExchangeWebSocketPool

logger = logging.getLogger(__name__)

class WebSocketAdmin:
    """WebSocket模块管理员 - 修复版"""
    
    def __init__(self):
        """初始化"""
        logger.info("WebSocketAdmin: 启动（被动接收模式）")
        
        # ✅ 创建pool_manager时传入self引用
        self._pool_manager = WebSocketPoolManager(admin_instance=self)
        self._monitor = ConnectionMonitor(self._pool_manager)
        
        self._running = False
        self._initialized = False
        self._restart_requests = {}  # 存储重启请求
        self._processing_restart = set()  # ✅ 新增：正在处理的重启集合
        
        logger.info("✅ WebSocketAdmin 初始化完成")
    
    async def start(self):
        """启动整个WebSocket模块"""
        if self._running:
            logger.warning("WebSocket模块已在运行中")
            return True
        
        try:
            logger.info("=" * 60)
            logger.info("WebSocketAdmin 正在启动模块...")
            logger.info("=" * 60)
            
            # 1. 初始化连接池
            logger.info("[管理员] 步骤1: 初始化WebSocket连接池")
            await self._pool_manager.initialize()
            
            # 2. 启动监控
            logger.info("[管理员] 步骤2: 启动连接监控")
            await self._monitor.start_monitoring()
            
            # 3. 启动重启请求检查
            asyncio.create_task(self._check_restart_requests_loop())
            
            self._running = True
            self._initialized = True
            
            logger.info("✅ WebSocketAdmin 模块启动成功")
            logger.info("=" * 60)
            logger.info("💡 模式: 被动接收重启请求（直接调用）")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"WebSocketAdmin 启动失败: {e}")
            await self.stop()
            return False
    
    async def _check_restart_requests_loop(self):
        """检查重启请求循环"""
        logger.info("[管理员] 开始检查重启请求循环")
        
        while self._running:
            try:
                # ✅ 直接检查连接池状态，不需要通过data_store
                restart_needed = await self._check_pool_restart_needs()
                if restart_needed:
                    for exchange in restart_needed:
                        # ✅ 检查是否已经在处理中
                        if exchange not in self._processing_restart:
                            logger.critical(f"[管理员] 🆘 检测到 {exchange} 需要重启")
                            await self._handle_restart_request(exchange, "健康检查检测")
                
                await asyncio.sleep(10)  # 10秒检查一次
                
            except Exception as e:
                logger.error(f"检查重启请求错误: {e}")
                await asyncio.sleep(10)
    
    async def handle_restart_request(self, exchange: str, reason: str):
        """✅ 新增：处理连接池直接发来的重启请求"""
        logger.critical(f"[管理员] 🆘 收到直接重启请求: {exchange} - {reason}")
        
        if exchange not in self._restart_requests:
            self._restart_requests[exchange] = {
                "reason": reason,
                "timestamp": datetime.now().isoformat(),
                "handled": False,
                "source": "direct_request"  # 标记为直接请求
            }
        
        # ✅ 检查是否已经在处理中，防止双重调用
        if (exchange in self._restart_requests and 
            not self._restart_requests[exchange]["handled"] and
            exchange not in self._processing_restart):
            
            await self._handle_restart_request(exchange, reason)
    
    async def _check_pool_restart_needs(self) -> List[str]:
        """检查连接池是否需要重启"""
        restart_needed = []
        
        try:
            status = await self._pool_manager.get_all_status()
            
            for exchange, ex_status in status.items():
                if isinstance(ex_status, dict):
                    need_restart = ex_status.get("need_restart", False)
                    takeover_attempts = ex_status.get("takeover_attempts", 0)
                    failed_count = ex_status.get("failed_connections_count", 0)
                    
                    # 条件1：连接池明确要求重启
                    if need_restart:
                        restart_needed.append(exchange)
                    
                    # 条件2：接管尝试过多
                    elif takeover_attempts > 10:
                        logger.warning(f"[管理员] {exchange} 接管尝试过多: {takeover_attempts}")
                        restart_needed.append(exchange)
                    
                    # 条件3：失败连接过多
                    total_connections = len(ex_status.get("masters", [])) + len(ex_status.get("warm_standbys", []))
                    if failed_count >= total_connections and total_connections > 0:
                        logger.warning(f"[管理员] {exchange} 所有连接都失败过")
                        restart_needed.append(exchange)
        
        except Exception as e:
            logger.error(f"检查连接池重启需求失败: {e}")
        
        return restart_needed
    
    async def _handle_restart_request(self, exchange: str, reason: str):
        """处理重启请求"""
        # ✅ 添加到处理集合，防止重复处理
        self._processing_restart.add(exchange)
        
        try:
            if exchange not in self._pool_manager.exchange_pools:
                logger.error(f"[管理员] 交易所不存在: {exchange}")
                return
            
            logger.critical(f"[管理员] 🔄 正在重启 {exchange} 连接池，原因: {reason}")
            
            pool = self._pool_manager.exchange_pools[exchange]
            symbols = pool.symbols
            
            # 1. 关闭旧池
            await pool.shutdown()
            await asyncio.sleep(3)
            
            # 2. 创建新池（传入管理员引用）
            new_pool = ExchangeWebSocketPool(exchange, self._pool_manager.data_callback, self)
            await new_pool.initialize(symbols)
            
            # 3. 替换池
            self._pool_manager.exchange_pools[exchange] = new_pool
            
            # 4. 标记为已处理
            if exchange in self._restart_requests:
                self._restart_requests[exchange]["handled"] = True
            
            logger.critical(f"[管理员] ✅ {exchange} 连接池重启完成")
            
        except Exception as e:
            logger.error(f"[管理员] ❌ {exchange} 重启失败: {e}")
        finally:
            # ✅ 从处理集合中移除
            self._processing_restart.discard(exchange)
    
    async def stop(self):
        """停止整个WebSocket模块"""
        if not self._running:
            logger.info("WebSocket模块未在运行")
            return
        
        logger.info("WebSocketAdmin 正在停止模块...")
        
        if self._monitor:
            await self._monitor.stop_monitoring()
        
        if self._pool_manager:
            await self._pool_manager.shutdown()
        
        self._running = False
        logger.info("✅ WebSocketAdmin 模块已停止")
    
    async def get_status(self) -> Dict[str, Any]:
        """获取模块状态"""
        try:
            internal_status = await self._pool_manager.get_all_status()
            
            summary = {
                "module": "websocket_pool",
                "status": "healthy" if self._running else "stopped",
                "initialized": self._initialized,
                "mode": "self_managed",
                "restart_requests": self._restart_requests,
                "processing_restart": list(self._processing_restart),
                "exchanges": {},
                "timestamp": datetime.now().isoformat()
            }
            
            for exchange, ex_status in internal_status.items():
                if isinstance(ex_status, dict):
                    masters = ex_status.get("masters", [])
                    warm_standbys = ex_status.get("warm_standbys", [])
                    
                    connected_masters = sum(1 for m in masters if isinstance(m, dict) and m.get("connected", False))
                    connected_warm = sum(1 for w in warm_standbys if isinstance(w, dict) and w.get("connected", False))
                    
                    summary["exchanges"][exchange] = {
                        "masters_connected": connected_masters,
                        "masters_total": len(masters),
                        "standbys_connected": connected_warm,
                        "standbys_total": len(warm_standbys),
                        "need_restart": ex_status.get("need_restart", False),
                        "takeover_attempts": ex_status.get("takeover_attempts", 0),
                        "failed_connections": ex_status.get("failed_connections_count", 0)
                    }
            
            return summary
            
        except Exception as e:
            logger.error(f"WebSocketAdmin 获取状态失败: {e}")
            return {
                "module": "websocket_pool",
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        if not self._running:
            return {
                "healthy": False,
                "message": "模块未运行"
            }
        
        try:
            status = await self.get_status()
            
            # 检查主连接
            issues = []
            for exchange, exchange_info in status.get("exchanges", {}).items():
                masters_connected = exchange_info.get("masters_connected", 0)
                masters_total = exchange_info.get("masters_total", 0)
                
                if masters_connected == 0 and masters_total > 0:
                    issues.append(f"{exchange}: 主连接全部断开")
                
                if exchange_info.get("need_restart", False):
                    issues.append(f"{exchange}: 需要重启")
            
            if issues:
                return {
                    "healthy": False,
                    "message": f"发现问题: {', '.join(issues)}",
                    "details": status,
                    "action": "check_restart_needs"
                }
            
            return {
                "healthy": True,
                "message": "所有交易所主连接正常",
                "details": status
            }
            
        except Exception as e:
            return {
                "healthy": False,
                "message": f"健康检查异常: {e}"
            }
    
    async def reconnect_exchange(self, exchange_name: str):
        """重连指定交易所"""
        if exchange_name in self._pool_manager.exchange_pools:
            pool = self._pool_manager.exchange_pools[exchange_name]
            logger.info(f"[管理员] 正在重连交易所: {exchange_name}")
            
            symbols = pool.symbols
            await pool.shutdown()
            await asyncio.sleep(2)
            await pool.initialize(symbols)
            
            logger.info(f"[管理员] 交易所重连完成: {exchange_name}")
            return True
        
        logger.error(f"[管理员] 交易所不存在: {exchange_name}")
        return False
    
    def is_running(self) -> bool:
        """判断模块是否在运行"""
        return self._running