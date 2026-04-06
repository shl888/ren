# trading/semi_auto/__init__.py
"""
半自动交易模式

包含半自动模式下的各个工人：
- leverage_worker: 杠杆工人
- open_position_worker: 开仓工人
- sl_tp_worker: 止损止盈工人
- close_position_worker: 平仓工人

大脑收到指令后，转发给对应的工人。
"""

from .leverage_worker import LeverageWorker
from .open_position_worker import OpenPositionWorker
from .sl_tp_worker import SlTpWorker
from .close_position_worker import ClosePositionWorker

__all__ = [
    "LeverageWorker",
    "OpenPositionWorker",
    "SlTpWorker",
    "ClosePositionWorker"
]