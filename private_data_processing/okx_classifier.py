"""
OKX订单事件分类器 - 纯函数，无状态
3种事件分类规则：开仓、平仓、其他
"""
from typing import Dict, Any


def classify_okx_order(data: Dict[str, Any]) -> str:
    """
    OKX订单更新事件分类
    返回: 
    '01_开仓',
    '02_平仓',
    '99_其他'
    """
    try:
        # 提取订单数据
        d = data['data']['data'][0]  # OKX的数据结构：data.data是一个数组
        
        # 获取关键字段
        pos_side = d.get('posSide', '')      # long/short
        reduce_only = d.get('reduceOnly', '')  # true/false
        pnl = d.get('pnl', '0')               # 盈亏
        state = d.get('state', '')            # 订单状态
        ord_type = d.get('ordType', '')        # 订单类型
        
        # 只处理已成交的订单（避免中间状态干扰）
        if state != 'filled':
            return '99_其他'
        
        # ===== 开仓判断 =====
        # reduceOnly=false 且 pnl == 0（无盈亏） 且 订单已成交
        if reduce_only == 'false' and (pnl == '0' or pnl == 0):
            return '01_开仓'
            
        # ===== 平仓判断 =====
        # reduceOnly=true 且 pnl != 0（有盈亏） 且 订单已成交
        if reduce_only == 'true' and pnl != '0' and pnl != 0:
            return '02_平仓'
            
        # ===== 其他 =====
        return '99_其他'
    
    except (KeyError, TypeError, IndexError, AttributeError):
        return '99_其他'


def is_closing_event(category: str) -> bool:
    """判断是否是平仓事件"""
    return category == '02_平仓'