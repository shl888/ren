# trading/semi_auto/__init__.py
"""
半自动交易模式

包含半自动模式下的各个交易流程房间：
- open_position: 开仓流程房间
- stop_loss_take_profit: 止损止盈流程房间（待实现）
- close_position: 平仓流程房间（待实现）

大脑收到指令后，进入对应的房间，按照房间里的步骤执行。
"""

from .open_position import OpenPositionFlow

__all__ = ["OpenPositionFlow"]