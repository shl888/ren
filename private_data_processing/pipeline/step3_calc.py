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
        # ===== 统一手续费正负逻辑 =====
        if container.get("开仓手续费") is not None:
            try:
                fee = float(container["开仓手续费"])
                container["开仓手续费"] = self._round_4(-abs(fee))
            except (ValueError, TypeError):
                pass
        
        if container.get("平仓手续费") is not None:
            try:
                fee = float(container["平仓手续费"])
                container["平仓手续费"] = self._round_4(-abs(fee))
            except (ValueError, TypeError):
                pass
        
        # ===== 标记价仓位价值转绝对值 =====
        if container.get("标记价仓位价值") is not None:
            try:
                mark_value = float(container["标记价仓位价值"])
                container["标记价仓位价值_abs"] = self._round_4(abs(mark_value))
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
                    container["杠杆"] = self._round_int(abs(mark_value / mark_margin))
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
                mark_value = float(container["标记价仓位价值"])
                
                # 开仓保证金（分子分母都取绝对值）
                if abs(mark_value) > 0:
                    open_margin = abs(open_price * amount * mark_margin) / abs(mark_value)
                    container["开仓保证金"] = self._round_4(open_margin)
                
                # 标记价浮盈百分比（只有标记价仓位价值取绝对值）
                if container.get("标记价浮盈") is not None:
                    unrealized = float(container["标记价浮盈"])
                    if mark_margin != 0 and abs(mark_value) > 0:
                        percent = (unrealized * abs(mark_value) * 100) / (open_price * amount * mark_margin)
                        container["标记价浮盈百分比"] = self._round_4(percent)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 4. 止损幅度 =====
        if container.get("止损触发价") is not None and container.get("开仓价") is not None:
            try:
                stop_price = float(container["止损触发价"])
                open_price = float(container["开仓价"])
                if open_price != 0:
                    abs_value = abs((stop_price - open_price) / open_price)
                    container["止损幅度"] = self._round_4(abs_value * -100)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 5. 止盈幅度 =====
        if container.get("止盈触发价") is not None and container.get("开仓价") is not None:
            try:
                take_price = float(container["止盈触发价"])
                open_price = float(container["开仓价"])
                if open_price != 0:
                    abs_value = abs((take_price - open_price) / open_price)
                    container["止盈幅度"] = self._round_4(abs_value * 100)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 6. 平仓价涨跌盈亏幅 =====
        if container.get("平仓价") is not None and container.get("开仓价") is not None:
            try:
                close_price = float(container["平仓价"])
                open_price = float(container["开仓价"])
                if open_price != 0:
                    container["平仓价涨跌盈亏幅"] = self._round_4((close_price - open_price) / open_price)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 7. 平仓价仓位价值 =====
        if container.get("平仓价") is not None and container.get("持仓币数") is not None:
            try:
                value = float(container["平仓价"]) * float(container["持仓币数"])
                container["平仓价仓位价值"] = self._round_4(value)
            except (ValueError, TypeError):
                pass
        
        # ===== 8. 平仓收益 + 平仓收益率（同时计算）=====
        if (container.get("平仓价") is not None and 
            container.get("开仓价") is not None and 
            container.get("持仓币数") is not None and 
            container.get("标记价保证金") is not None and 
            container.get("标记价仓位价值") is not None and
            container.get("开仓方向") is not None):
            
            try:
                close_price = float(container["平仓价"])
                open_price = float(container["开仓价"])
                amount = float(container["持仓币数"])
                mark_margin = float(container["标记价保证金"])
                mark_value = float(container["标记价仓位价值"])
                direction = container["开仓方向"]
                
                # 严格按照公式计算平仓收益和平仓收益率
                if direction == "LONG":
                    # 做多：平仓收益 = (平仓价 - 开仓价) * 持仓币数
                    pnl = (close_price - open_price) * amount
                    # 做多：平仓收益率 = (平仓价 - 开仓价) * 持仓币数 * 标记价保证金 / |标记价仓位价值|
                    if mark_margin != 0 and abs(mark_value) > 0:
                        rate = (close_price - open_price) * amount * mark_margin / abs(mark_value)
                    else:
                        rate = None
                        
                elif direction == "SHORT":
                    # 做空：平仓收益 = (开仓价 - 平仓价) * 持仓币数
                    pnl = (open_price - close_price) * amount
                    # 做空：平仓收益率 = (开仓价 - 平仓价) * 持仓币数 * 标记价保证金 / |标记价仓位价值|
                    if mark_margin != 0 and abs(mark_value) > 0:
                        rate = (open_price - close_price) * amount * mark_margin / abs(mark_value)
                    else:
                        rate = None
                else:
                    pnl = None
                    rate = None
                
                if pnl is not None:
                    container["平仓收益"] = self._round_4(pnl)
                
                if rate is not None:
                    container["平仓收益率"] = self._round_4(rate)
                    
            except (ValueError, TypeError, ZeroDivisionError):
                pass