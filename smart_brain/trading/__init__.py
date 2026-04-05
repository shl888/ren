# trading/__init__.py
"""
交易逻辑

大脑收到前端指令后，根据指令类型，进入对应的流程房间。
流程房间里有详细的步骤，大脑按照步骤执行。
"""

from .semi_auto.open_position import OpenPositionFlow


class TradingLogic:
    """
    交易逻辑 - 流程房间的入口
    
    大脑通过这个类，进入不同的流程房间。
    """
    
    def __init__(self, brain):
        """
        初始化交易逻辑
        
        Args:
            brain: 大脑实例，进入房间后，大脑按照步骤干活
        """
        self.brain = brain
    
    async def enter_room(self, command: str, params: dict) -> dict:
        """
        大脑根据指令类型，进入对应的流程房间
        
        Args:
            command: 指令类型，如 "place_order", "set_sl_tp", "close_position"
            params: 指令参数
            
        Returns:
            执行结果
        """
        if command == "place_order":
            # 大脑进入开仓流程房间
            room = OpenPositionFlow(self.brain)
            return await room.execute(params)
        
        elif command == "set_sl_tp":
            # TODO: 大脑进入止损止盈流程房间（待实现）
            return {"success": False, "error": "止损止盈流程房间未实现"}
        
        elif command == "close_position":
            # TODO: 大脑进入平仓流程房间（待实现）
            return {"success": False, "error": "平仓流程房间未实现"}
        
        else:
            return {"success": False, "error": f"未知指令: {command}"}