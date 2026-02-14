"""
币安订单事件分类器 - 纯函数，无状态
16种事件分类规则，输入原始data，返回分类字符串
"""
from typing import Dict, Any


def classify_binance_order(data: Dict[str, Any]) -> str:
    """
    币安订单更新事件分类
    返回: 
    '01_开仓(部分成交)', '02_开仓(全部成交)',
    '03_设置止损', '04_设置止盈',
    '05_触发止损(部分成交)', '06_触发止损(全部成交)',
    '07_触发止盈(部分成交)', '08_触发止盈(全部成交)',
    '09_主动平仓(部分成交)', '10_主动平仓(全部成交)',
    '11_取消止损', '12_取消止盈',
    '13_止损过期(被触发)', '14_止损过期(被取消)',
    '15_止盈过期(被触发)', '16_止盈过期(被取消)',
    '99_其他'
    """
    try:
        o = data['data']['o']
        
        s = o.get('S', '')        # 方向 BUY/SELL
        ot = o.get('ot', '')      # 原始订单类型
        o_type = o.get('o', '')   # 当前订单类型
        x_status = o.get('X', '') # 订单状态
        sp = o.get('sp', '0')     # 触发价
        cp = o.get('cp', False)   # 是否条件单
        er = o.get('er', '0')     # 错误码
        
        # ===== 开仓 =====
        if s == 'BUY' and ot == 'MARKET':
            if x_status == 'PARTIALLY_FILLED':
                return '01_开仓(部分成交)'
            if x_status == 'FILLED':
                return '02_开仓(全部成交)'
        
        # ===== 设置类 =====
        if ot == 'STOP_MARKET' and x_status == 'NEW':
            return '03_设置止损'
        
        if ot == 'TAKE_PROFIT_MARKET' and x_status == 'NEW':
            return '04_设置止盈'
        
        # ===== 触发止损 =====
        if o_type == 'MARKET' and ot == 'STOP_MARKET' and sp != '0':
            if x_status == 'PARTIALLY_FILLED':
                return '05_触发止损(部分成交)'
            if x_status == 'FILLED':
                return '06_触发止损(全部成交)'
        
        # ===== 触发止盈 =====
        if o_type == 'MARKET' and ot == 'TAKE_PROFIT_MARKET' and sp != '0':
            if x_status == 'PARTIALLY_FILLED':
                return '07_触发止盈(部分成交)'
            if x_status == 'FILLED':
                return '08_触发止盈(全部成交)'
        
        # ===== 主动平仓 =====
        if s == 'SELL' and ot == 'MARKET' and sp == '0' and cp is False:
            if x_status == 'PARTIALLY_FILLED':
                return '09_主动平仓(部分成交)'
            if x_status == 'FILLED':
                return '10_主动平仓(全部成交)'
        
        # ===== 取消类 =====
        if ot == 'STOP_MARKET' and x_status == 'CANCELED':
            return '11_取消止损'
        
        if ot == 'TAKE_PROFIT_MARKET' and x_status == 'CANCELED':
            return '12_取消止盈'
        
        # ===== 过期类 =====
        if x_status == 'EXPIRED':
            # 止损过期
            if ot == 'STOP_MARKET':
                if er == '8':
                    return '13_止损过期(被触发)'
                else:
                    return '14_止损过期(被取消)'
            
            # 止盈过期
            if ot == 'TAKE_PROFIT_MARKET':
                if er == '8':
                    return '15_止盈过期(被触发)'
                else:
                    return '16_止盈过期(被取消)'
        
        # ===== 其他 =====
        return '99_其他'
    
    except (KeyError, TypeError, AttributeError):
        return '99_其他'


def is_closing_event(category: str) -> bool:
    """判断是否是平仓类事件（需要清理缓存）- 只有全部成交才触发"""
    return category in [
        '06_触发止损(全部成交)',
        '08_触发止盈(全部成交)', 
        '10_主动平仓(全部成交)'
    ]