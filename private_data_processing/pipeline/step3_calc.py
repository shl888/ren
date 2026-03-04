"""
第三步：计算衍生字段
原则：
1. 同时计算，都用原始字段
2. 缺少必要字段就不计算，保持空值
3. 除杠杆外，结果保留4位小数
4. 杠杆保留整数
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class Step3Calc:
    """第三步：计算衍生字段"""
    
    def _round_4(self, value):
        """四舍五入保留4位小数"""
        if value is None:
            return None
        try:
            return round(float(value), 4)
        except (ValueError, TypeError):
            return None
    
    def _round_int(self, value):
        """四舍五入保留整数"""
        if value is None:
            return None
        try:
            return round(float(value))
        except (ValueError, TypeError):
            return None
    
    def process(self, container: Dict[str, Any]):
        """
        计算容器中的衍生字段
        
        Args:
            container: step2返回的成品数据副本
        """
        # ===== 统一正负逻辑（先处理） =====
        exchange = container.get("交易所")
        
        # 开仓手续费统一为负值（支出）
        if container.get("开仓手续费") is not None:
            try:
                fee = float(container["开仓手续费"])
                container["开仓手续费"] = self._round_4(-abs(fee))
            except (ValueError, TypeError):
                pass
        
        # 平仓手续费统一为负值（支出）
        if container.get("平仓手续费") is not None:
            try:
                fee = float(container["平仓手续费"])
                container["平仓手续费"] = self._round_4(-abs(fee))
            except (ValueError, TypeError):
                pass
        
        # 标记价仓位价值统一为绝对值
        if container.get("标记价仓位价值") is not None:
            try:
                mark_value = float(container["标记价仓位价值"])
                container["标记价仓位价值"] = self._round_4(abs(mark_value))
            except (ValueError, TypeError):
                pass
        
        # ===== 1. 开仓价仓位价值 =====
        if container.get("开仓价") is not None and container.get("持仓币数") is not None:
            try:
                value = float(container["开仓价"]) * float(container["持仓币数"])
                container["开仓价仓位价值"] = self._round_4(value)
            except (ValueError, TypeError):
                pass
        
        # ===== 2. 杠杆（取绝对值） =====
        if container.get("标记价仓位价值") is not None and container.get("标记价保证金") is not None:
            try:
                mark_value = float(container["标记价仓位价值"])
                mark_margin = float(container["标记价保证金"])
                if mark_margin > 0:
                    # 标记价仓位价值已经是绝对值，直接计算
                    container["杠杆"] = self._round_int(mark_value / mark_margin)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 3. 开仓保证金 + 标记价浮盈百分比（同时计算）=====
        if (container.get("开仓价") is not None and 
            container.get("持仓币数") is not None and 
            container.get("标记价保证金") is not None and 
            container.get("标记价仓位价值") is not None):
            
            try:
                open_price = float(container["开仓价"])
                amount = float(container["持仓币数"])
                mark_margin = float(container["标记价保证金"])
                mark_value = float(container["标记价仓位价值"])  # 已经是绝对值
                
                # 开仓保证金（分子分母都用绝对值，但分母已经是绝对值）
                open_margin = abs(open_price * amount * mark_margin) / mark_value
                container["开仓保证金"] = self._round_4(open_margin)
                
                # 标记价浮盈百分比（标记价仓位价值已经是绝对值）
                if container.get("标记价浮盈") is not None:
                    unrealized = float(container["标记价浮盈"])
                    if mark_margin != 0:
                        percent = (unrealized * mark_value * 100) / (open_price * amount * mark_margin)
                        container["标记价浮盈百分比"] = self._round_4(percent)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 4. 止损幅度 =====
        # 计算逻辑：先取绝对值，再乘以-100
        if container.get("止损触发价") is not None and container.get("开仓价") is not None:
            try:
                stop_price = float(container["止损触发价"])
                open_price = float(container["开仓价"])
                if open_price != 0:
                    # 先计算绝对值
                    abs_value = abs((stop_price - open_price) / open_price)
                    # 再乘以 -100
                    value = abs_value * -100
                    container["止损幅度"] = self._round_4(value)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 5. 止盈幅度 =====
        # 计算逻辑：先取绝对值，再乘以100
        if container.get("止盈触发价") is not None and container.get("开仓价") is not None:
            try:
                take_price = float(container["止盈触发价"])
                open_price = float(container["开仓价"])
                if open_price != 0:
                    # 先计算绝对值
                    abs_value = abs((take_price - open_price) / open_price)
                    # 再乘以 100
                    value = abs_value * 100
                    container["止盈幅度"] = self._round_4(value)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 6. 平仓价涨跌盈亏幅 =====
        if container.get("平仓价") is not None and container.get("开仓价") is not None:
            try:
                close_price = float(container["平仓价"])
                open_price = float(container["开仓价"])
                if open_price != 0:
                    value = (close_price - open_price) / open_price
                    container["平仓价涨跌盈亏幅"] = self._round_4(value)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 7. 平仓价仓位价值 =====
        if container.get("平仓价") is not None and container.get("持仓币数") is not None:
            try:
                value = float(container["平仓价"]) * float(container["持仓币数"])
                container["平仓价仓位价值"] = self._round_4(value)
            except (ValueError, TypeError):
                pass
        
        # ===== 8. 平仓收益率（同时计算）=====
        if (container.get("平仓收益") is not None and 
            container.get("开仓价") is not None and 
            container.get("持仓币数") is not None and 
            container.get("标记价保证金") is not None and 
            container.get("标记价仓位价值") is not None):
            
            try:
                pnl = float(container["平仓收益"])
                open_price = float(container["开仓价"])
                amount = float(container["持仓币数"])
                mark_margin = float(container["标记价保证金"])
                mark_value = float(container["标记价仓位价值"])  # 已经是绝对值
                
                if mark_margin != 0:
                    # 平仓收益率（标记价仓位价值已经是绝对值）
                    value = (pnl * mark_value * 100) / (open_price * amount * mark_margin)
                    container["平仓收益率"] = self._round_4(value)
            except (ValueError, TypeError, ZeroDivisionError):
                pass