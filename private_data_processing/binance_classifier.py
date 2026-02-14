"""
币安订单事件分类器 - 纯函数，无状态
12种事件分类规则（包含过期细分），输入原始data，返回分类字符串
"""
from typing import Dict, Any


def classify_binance_order(data: Dict[str, Any]) -> str:
    """
    币安订单更新事件分类
    返回: 
    '01_开仓', '02_设置止损', '03_设置止盈', 
    '04_触发止损', '05_触发止盈', '06_主动平仓',
    '07_取消止损', '08_取消止盈', 
    '09_止损过期(触发)', '10_止损过期(取消)', 
    '11_止盈过期(触发)', '12_止盈过期(取消)',
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
        
        # 1. 开仓：买单 + 原始市价单 + 成交
        if s == 'BUY' and ot == 'MARKET' and x_status == 'FILLED':
            return '01_开仓'
        
        # 2. 设置止损：止损单 + 新建状态
        if ot == 'STOP_MARKET' and x_status == 'NEW':
            return '02_设置止损'
        
        # 3. 设置止盈：止盈单 + 新建状态
        if ot == 'TAKE_PROFIT_MARKET' and x_status == 'NEW':
            return '03_设置止盈'
        
        # 4. 触发止损：当前市价单 + 原始止损单 + 有触发价 + 成交
        if o_type == 'MARKET' and ot == 'STOP_MARKET' and sp != '0' and x_status == 'FILLED':
            return '04_触发止损'
        
        # 5. 触发止盈：当前市价单 + 原始止盈单 + 有触发价 + 成交
        if o_type == 'MARKET' and ot == 'TAKE_PROFIT_MARKET' and sp != '0' and x_status == 'FILLED':
            return '05_触发止盈'
        
        # 6. 主动平仓：卖单 + 原始市价单 + 无触发价 + 不是条件单 + 成交
        if s == 'SELL' and ot == 'MARKET' and sp == '0' and cp is False and x_status == 'FILLED':
            return '06_主动平仓'
        
        # 7. 取消止损：止损单 + 取消状态
        if ot == 'STOP_MARKET' and x_status == 'CANCELED':
            return '07_取消止损'
        
        # 8. 取消止盈：止盈单 + 取消状态
        if ot == 'TAKE_PROFIT_MARKET' and x_status == 'CANCELED':
            return '08_取消止盈'
        
        # 9-12. 过期分类（核心规律）
        if x_status == 'EXPIRED':
            # 止损单过期
            if ot == 'STOP_MARKET':
                if er == '8':
                    return '09_止损过期(触发)'  # 被触发而结束
                else:  # er == '6' 或其他
                    return '10_止损过期(取消)'  # 被动取消
            
            # 止盈单过期
            if ot == 'TAKE_PROFIT_MARKET':
                if er == '8':
                    return '11_止盈过期(触发)'  # 被触发而结束
                else:  # er == '6' 或其他
                    return '12_止盈过期(取消)'  # 被动取消
        
        # 99. 其他：所有未匹配的情况
        return '99_其他'
    
    except (KeyError, TypeError, AttributeError):
        return '99_其他'


def is_closing_event(category: str) -> bool:
    """判断是否是平仓类事件（需要清理缓存）"""
    return category in ['04_触发止损', '05_触发止盈', '06_主动平仓']