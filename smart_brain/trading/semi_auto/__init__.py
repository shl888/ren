# trading/semi_auto/__init__.py
"""
半自动交易模式

包含半自动模式下的各个工人：
- leverage_worker: 杠杆工人
- open_position_worker: 开仓工人
- stop_loss_take_profit: 止损止盈工人（待实现）
- close_position: 平仓工人（待实现）

大脑收到指令后，转发给对应的工人。
"""

from .leverage_worker import LeverageWorker
from .open_position_worker import OpenPositionWorker

__all__ = ["LeverageWorker", "OpenPositionWorker"]