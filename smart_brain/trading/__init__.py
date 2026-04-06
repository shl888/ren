# trading/__init__.py
"""
交易逻辑

现在采用工人模式：
- 杠杆工人和开仓工人独立执行
- TradingLogic 作为转发层，把指令转发给对应的工人
"""

from .semi_auto.leverage_worker import LeverageWorker
from .semi_auto.open_position_worker import OpenPositionWorker


class TradingLogic:
    """
    交易逻辑 - 指令转发层
    
    大脑通过这个类，把指令转发给对应的工人。
    """
    
    def __init__(self, brain):
        """
        初始化交易逻辑
        
        Args:
            brain: 大脑实例
        """
        self.brain = brain
        self.leverage_worker = LeverageWorker(brain)
        self.open_worker = OpenPositionWorker(brain)
    
    async def enter_room(self, command: str, params: dict) -> dict:
        """
        大脑根据指令类型，转发给对应的工人
        
        Args:
            command: 指令类型，如 "place_order", "set_sl_tp", "close_position"
            params: 指令参数
            
        Returns:
            执行结果
        """
        if command == "place_order":
            # 转发给杠杆工人和开仓工人
            self.leverage_worker.on_data({"command": "place_order", "params": params})
            self.open_worker.on_data({"command": "place_order", "params": params})
            return {"success": True, "message": "开仓指令已转发给工人"}
        
        elif command == "set_sl_tp":
            # TODO: 转发给止损止盈工人（待实现）
            return {"success": False, "error": "止损止盈流程未实现"}
        
        elif command == "close_position":
            # TODO: 转发给平仓工人（待实现）
            return {"success": False, "error": "平仓流程未实现"}
        
        else:
            return {"success": False, "error": f"未知指令: {command}"}