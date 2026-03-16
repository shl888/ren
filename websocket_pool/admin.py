"""
WebSocket连接池管理员 - 纯被动模式
只接收连接池的直接重启请求，不进行任何主动检查
"""

import asyncio
import logging
from typing import Dict, Any, List
from datetime import datetime

from .pool_manager import WebSocketPoolManager
from .exchange_pool import ExchangeWebSocketPool

logger = logging.getLogger(__name__)

class WebSocketAdmin:
    """WebSocket模块管理员 - 纯被动模式"""
    
    def __init__(self):
        """初始化"""
        logger.info("[管理员]WebSocketAdmin: 启动（纯被动模式）")
        
        # ✅ 创建pool_manager时传入self引用
        self._pool_manager = WebSocketPoolManager(admin_instance=self)
        
        # 🚨 完全删除monitor相关代码
        
        self._running = False
        self._initialized = False
        self._restart_requests = {}  # 存储重启请求
        self._processing_restart = set()  # ✅ 正在处理的重启集合
        
        logger.info("✅ [管理员]WebSocketAdmin 初始化完成")
    
    async def start(self):
        """启动整个WebSocket模块"""
        if self._running:
            logger.warning("[管理员]WebSocket模块已在运行中")
            return True
        
        try:
            logger.info("=" * 60)
            logger.info("[管理员]WebSocketAdmin 正在启动模块...")
            logger.info("=" * 60)
            
            # 1. 初始化连接池
            logger.info("[管理员] 步骤1: 初始化WebSocket连接池")
            await self._pool_manager.initialize()
            
            # 2. 启动重启请求等待循环（纯被动）
            logger.info("[管理员] 步骤2: 启动重启请求等待循环（纯被动模式）")
            restart_task = asyncio.create_task(self._check_restart_requests_loop())
            logger.info(f"[管理员] ✅ 重启请求循环任务已启动")
            
            self._running = True
            self._initialized = True
            
            logger.info("✅ [管理员]WebSocketAdmin 模块启动成功")
            logger.info("=" * 60)
            logger.info("💡[管理员] 模式: 纯被动接收（只响应直接请求）")
            logger.info("💡[管理员] 重启路径: 连接池 → 直接调用 → 管理员")
            logger.info("=" * 60)
            return True
            
        except Exception as e:
            logger.error(f"[管理员]WebSocketAdmin 启动失败: {e}")
            await self.stop()
            return False
    
    async def _check_restart_requests_loop(self):
        """纯被动重启请求循环 - 只等待直接调用"""
        logger.info("[管理员] 🔕 进入纯被动模式，等待连接池直接请求")
        
        while self._running:
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU
            try:
                # ✅ 什么都不做，只保持循环运行
                # 重启请求通过 handle_restart_request() 直接调用
                await asyncio.sleep(30)  # 长时间睡眠，减少CPU使用
                
            except Exception as e:
                logger.error(f"[管理员]重启循环错误: {e}")
                await asyncio.sleep(30)
    
    async def handle_restart_request(self, exchange: str, reason: str):
        """✅ [管理员]处理连接池直接发来的重启请求"""
        logger.critical(f"[管理员] 🆘 收到直接重启请求: {exchange} - {reason}")
        
        if exchange not in self._restart_requests:
            self._restart_requests[exchange] = {
                "reason": reason,
                "timestamp": datetime.now().isoformat(),
                "handled": False,
                "source": "direct_request"
            }
        
        # ✅ 检查是否已经在处理中，防止双重调用
        if (exchange in self._restart_requests and 
            not self._restart_requests[exchange]["handled"] and
            exchange not in self._processing_restart):
            
            await self._handle_restart_request(exchange, reason)
    
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
            logger.info("[管理员]WebSocket模块未在运行")
            return
        
        logger.info("[管理员]WebSocketAdmin 正在停止模块...")
        
        # 🚨 完全删除monitor相关代码
        
        if self._pool_manager:
            await self._pool_manager.shutdown()
        
        self._running = False
        logger.info("✅ [管理员]WebSocketAdmin 模块已停止")
    
    async def get_status(self) -> Dict[str, Any]:
        """获取模块状态"""
        try:
            internal_status = await self._pool_manager.get_all_status()
            
            summary = {
                "module": "websocket_pool",
                "status": "healthy" if self._running else "stopped",
                "initialized": self._initialized,
                "mode": "pure_passive",  # 更新模式名称
                "restart_requests": self._restart_requests,
                "processing_restart": list(self._processing_restart),
                "exchanges": {},
                "timestamp": datetime.now().isoformat()
            }
            
            for exchange, ex_status in internal_status.items():
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
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
            logger.error(f"[管理员]WebSocketAdmin 获取状态失败: {e}")
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
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
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