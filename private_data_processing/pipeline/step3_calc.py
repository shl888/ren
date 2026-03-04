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
    
    def _safe_float(self, value):
        """安全转换为float，None或无效返回None"""
        if value is None:
            return None
        try:
            return float(value)
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
        
        # ===== 标记价仓位价值转绝对值（直接覆盖原字段）=====
        if container.get("标记价仓位价值") is not None:
            try:
                mark_value = float(container["标记价仓位价值"])
                container["标记价仓位价值"] = self._round_4(abs(mark_value))
            except (ValueError, TypeError):
                pass
        
        # ===== 安全获取所有可能用到的字段值 =====
        open_price = self._safe_float(container.get("开仓价"))
        amount = self._safe_float(container.get("持仓币数"))
        mark_margin = self._safe_float(container.get("标记价保证金"))
        mark_value = self._safe_float(container.get("标记价仓位价值"))
        direction = container.get("开仓方向")
        unrealized = self._safe_float(container.get("标记价浮盈"))
        stop_price = self._safe_float(container.get("止损触发价"))
        take_price = self._safe_float(container.get("止盈触发价"))
        close_price = self._safe_float(container.get("平仓价"))
        
        # ===== 1. 开仓价仓位价值 =====
        if open_price is not None and amount is not None:
            try:
                value = open_price * amount
                container["开仓价仓位价值"] = self._round_4(value)
            except (ValueError, TypeError):
                pass
        
        # ===== 2. 杠杆（取绝对值） =====
        if mark_value is not None and mark_margin is not None:
            try:
                if mark_margin > 0:
                    container["杠杆"] = self._round_int(abs(mark_value / mark_margin))
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 3. 开仓保证金 + 标记价浮盈百分比（同时计算）=====
        if open_price is not None and amount is not None and mark_margin is not None and mark_value is not None:
            try:
                # 开仓保证金（分子分母都取绝对值）
                if abs(mark_value) > 0:
                    open_margin = abs(open_price * amount * mark_margin) / abs(mark_value)
                    container["开仓保证金"] = self._round_4(open_margin)
                
                # 标记价浮盈百分比
                if unrealized is not None:
                    if mark_margin != 0 and abs(mark_value) > 0 and open_price * amount != 0:
                        percent = (unrealized * abs(mark_value) * 100) / (open_price * amount * mark_margin)
                        container["标记价浮盈百分比"] = self._round_4(percent)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 4. 止损幅度 =====
        if stop_price is not None and open_price is not None:
            try:
                if open_price != 0:
                    abs_value = abs((stop_price - open_price) / open_price)
                    container["止损幅度"] = self._round_4(abs_value * -100)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 5. 止盈幅度 =====
        if take_price is not None and open_price is not None:
            try:
                if open_price != 0:
                    abs_value = abs((take_price - open_price) / open_price)
                    container["止盈幅度"] = self._round_4(abs_value * 100)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 6. 平仓价涨跌盈亏幅 =====
        if close_price is not None and open_price is not None:
            try:
                if open_price != 0:
                    container["平仓价涨跌盈亏幅"] = self._round_4((close_price - open_price) * 100 / open_price)
            except (ValueError, TypeError, ZeroDivisionError):
                pass
        
        # ===== 7. 平仓价仓位价值 =====
        if close_price is not None and amount is not None:
            try:
                value = close_price * amount
                container["平仓价仓位价值"] = self._round_4(value)
            except (ValueError, TypeError):
                pass
        
        # ===== 8. 平仓收益 + 平仓收益率（同时计算）=====
        if (close_price is not None and 
            open_price is not None and 
            amount is not None and 
            mark_margin is not None and 
            mark_value is not None and
            direction is not None):
            
            try:
                # 平仓收益计算
                if direction == "LONG":
                    # 做多：平仓收益 = (平仓价 - 开仓价) * 持仓币数
                    pnl = (close_price - open_price) * amount
                    
                elif direction == "SHORT":
                    # 做空：平仓收益 = (开仓价 - 平仓价) * 持仓币数
                    pnl = (open_price - close_price) * amount
                    
                else:
                    pnl = None
                
                if pnl is not None:
                    container["平仓收益"] = self._round_4(pnl)
                
                # 平仓收益率计算（修正后的公式）
                if mark_margin != 0 and open_price != 0 and amount != 0 and mark_value is not None:
                    if direction == "LONG":
                        # 做多：平仓收益率 = (平仓价 - 开仓价) * |标记价仓位价值| * 100 / (开仓价 * 标记价保证金)
                        rate = (close_price - open_price) * abs(mark_value) * 100 / (open_price * mark_margin)
                        
                    elif direction == "SHORT":
                        # 做空：平仓收益率 = (开仓价 - 平仓价) * |标记价仓位价值| * 100 / (开仓价 * 标记价保证金)
                        rate = (open_price - close_price) * abs(mark_value) * 100 / (open_price * mark_margin)
                        
                    else:
                        rate = None
                    
                    if rate is not None:
                        container["平仓收益率"] = self._round_4(rate)
                    
            except (ValueError, TypeError, ZeroDivisionError):
                pass