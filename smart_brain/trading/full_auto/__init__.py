# trading/full_auto/__init__.py
"""
全自动交易模块

包含三个工人：
- auto_open: 侦察兵，负责检测开仓条件，生成开仓指令
- auto_sltp: 止损止盈工人，负责设置止损止盈
- auto_close: 清仓工人，负责持续监控并触发清仓
"""

from .auto_open import Scout as AutoOpen
from .auto_sltp import FullAutoSlTpWorker as AutoSlTp
from .auto_close import FullAutoCloser as AutoClose

__all__ = ['AutoOpen', 'AutoSlTp', 'AutoClose']
