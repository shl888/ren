# smart_brain/status_monitor.py
"""
状态监控器 - 定期报告系统状态
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class StatusMonitor:
    """状态监控器"""
    
    def __init__(self, data_receiver):
        self.data_receiver = data_receiver
        self.running = False
        self.monitor_task = None
    
    @staticmethod
    def _format_time_diff(last_time: Optional[datetime]) -> str:
        """格式化时间差"""
        if not last_time:
            return "从未收到"
        
        now = datetime.now()
        diff = now - last_time
        
        if diff.total_seconds() < 60:
            return f"{int(diff.total_seconds())}秒前"
        elif diff.total_seconds() < 3600:
            return f"{int(diff.total_seconds() / 60)}分钟前"
        else:
            return f"{int(diff.total_seconds() / 3600)}小时前"
    
    async def _log_data_status(self):
        """每分钟打印一次数据状态日志"""
        while self.running:
            try:
                await asyncio.sleep(60)  # 每分钟一次
                
                # 获取数据接收器的状态
                status = self.data_receiver.get_status_info()
                
                # 准备日志信息
                market_count = status['last_market_count']
                market_time = self._format_time_diff(status['last_market_time'])
                
                # 私人数据状态
                account_time = status['last_account_time']
                trade_time = status['last_trade_time']
                
                if account_time:
                    account_status = f"已更新，{self._format_time_diff(account_time)}"
                else:
                    account_status = "从未收到"
                    
                if trade_time:
                    trade_status = f"已更新，{self._format_time_diff(trade_time)}"
                else:
                    trade_status = "从未收到"
                
                # 打印状态日志
                status_msg = f"""【大脑数据状态】
成品数据，{market_count}条，已更新。{market_time}
私人数据-账户：{account_status}
私人数据-交易：{trade_status}"""
                
                logger.info(status_msg)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"状态日志错误: {e}")
                await asyncio.sleep(10)
    
    def start(self):
        """启动状态监控"""
        self.running = True
        self.monitor_task = asyncio.create_task(self._log_data_status())
        logger.info("✅ 状态监控已启动")
    
    async def stop(self):
        """停止状态监控"""
        self.running = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("✅ 状态监控已停止")
        
        