"""
币安订单事件分类器
职责：将ORDER_TRADE_UPDATE事件细分为8种具体类型
"""
from typing import Dict, Any

def classify_binance_order(data: Dict[str, Any]) -> str:
    """
    币安订单事件分类
    
    Args:
        data: 完整的private_data字典，包含data.o字段
        
    Returns:
        分类标识：01_开仓, 02_设止损, 03_取消止损, 04_设止盈, 
                05_取消止盈, 06_触发止损, 07_触发止盈, 08_主动平仓, 99_其他
    """
    try:
        o = data['data']['o']
        
        s = o.get('S', '')        # 方向 BUY/SELL
        ot = o.get('ot', '')      # 原始订单类型
        o_type = o.get('o', '')   # 当前订单类型
        x_status = o.get('X', '') # 订单状态
        sp = o.get('sp', '0')     # 触发价
        cp = o.get('cp', False)   # 是否条件单
        
        # 1. 开仓：买单 + 原始市价单 + 成交
        if s == 'BUY' and ot == 'MARKET' and x_status == 'FILLED':
            return '01_开仓'
        
        # 2. 设止损：止损单 + 新建状态
        if ot == 'STOP_MARKET' and x_status == 'NEW':
            return '02_设止损'
        
        # 3. 取消止损：止损单 + 取消状态
        if ot == 'STOP_MARKET' and x_status == 'CANCELED':
            return '03_取消止损'
        
        # 4. 设止盈：止盈单 + 新建状态
        if ot == 'TAKE_PROFIT_MARKET' and x_status == 'NEW':
            return '04_设止盈'
        
        # 5. 取消止盈：止盈单 + 取消状态
        if ot == 'TAKE_PROFIT_MARKET' and x_status == 'CANCELED':
            return '05_取消止盈'
        
        # 6. 触发止损：当前市价单 + 原始止损单 + 有触发价
        if o_type == 'MARKET' and ot == 'STOP_MARKET' and sp != '0':
            return '06_触发止损'
        
        # 7. 触发止盈：当前市价单 + 原始止盈单 + 有触发价
        if o_type == 'MARKET' and ot == 'TAKE_PROFIT_MARKET' and sp != '0':
            return '07_触发止盈'
        
        # 8. 主动平仓：卖单 + 原始市价单 + 无触发价 + 不是条件单
        if s == 'SELL' and ot == 'MARKET' and sp == '0' and cp is False:
            return '08_主动平仓'
        
        # 未知类型
        return '99_其他'
        
    except Exception as e:
        logger.error(f"❌ 币安订单分类失败: {e}")
        return '99_其他'