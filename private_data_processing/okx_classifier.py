"""
OKX订单事件分类器 - 纯函数，无状态
5种事件分类规则：挂单、开仓、平仓、部分成交、已取消、其他
"""
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def classify_okx_order(data: Dict[str, Any]) -> str:
    """
    OKX订单更新事件分类
    返回: 
    '01_挂单',          # 新挂单（未成交）
    '02_开仓成交',       # 开仓成交
    '03_平仓成交',       # 平仓成交
    '04_部分成交',       # 部分成交
    '05_已取消',         # 已取消
    '99_其他'           # 其他情况
    """
    try:
        # 提取原始数据 - OKX的数据结构: data是一个数组
        raw_data = data.get('data', {})
        
        # OKX的数据结构: data 是一个数组
        if not isinstance(raw_data, list):
            logger.debug(f"OKX分类: data不是数组: {type(raw_data)}")
            return '99_其他'
        
        if len(raw_data) == 0:
            logger.debug(f"OKX分类: data数组为空")
            return '99_其他'
        
        # 获取第一个订单数据（通常在data数组的第一个元素）
        order_data = raw_data[0]
        
        if not isinstance(order_data, dict):
            logger.debug(f"OKX分类: 订单数据不是字典: {type(order_data)}")
            return '99_其他'
        
        # 获取关键字段
        reduce_only = order_data.get('reduceOnly', 'false')
        pnl = order_data.get('pnl', '0')
        state = order_data.get('state', '')
        inst_id = order_data.get('instId', 'unknown')
        ord_id = order_data.get('ordId', 'unknown')
        pos_side = order_data.get('posSide', '')
        ord_type = order_data.get('ordType', '')
        acc_fill_sz = order_data.get('accFillSz', '0')
        
        # ===== 挂单状态处理（未成交）- 需要被过滤 =====
        if state == 'live':
            logger.debug(f"OKX分类: 挂单 - {inst_id}, reduceOnly={reduce_only}, ordId={ord_id}")
            return '01_挂单'
        
        # ===== 部分成交状态 =====
        if state == 'partially_filled':
            logger.debug(f"OKX分类: 部分成交 - {inst_id}, accFillSz={acc_fill_sz}")
            return '04_部分成交'
        
        # ===== 已成交订单处理 =====
        if state == 'filled':
            # 开仓判断：reduceOnly=false 且 pnl == 0（无盈亏）
            if reduce_only == 'false' and (pnl == '0' or pnl == 0):
                logger.debug(f"OKX分类: 开仓成交 - {inst_id}, pnl={pnl}, ordId={ord_id}")
                return '02_开仓成交'
                
            # 平仓判断：reduceOnly=true 且 pnl != 0（有盈亏）
            if reduce_only == 'true' and pnl != '0' and pnl != 0:
                logger.debug(f"OKX分类: 平仓成交 - {inst_id}, pnl={pnl}, ordId={ord_id}")
                return '03_平仓成交'
        
        # ===== 已取消订单 =====
        if state == 'canceled':
            logger.debug(f"OKX分类: 已取消 - {inst_id}, ordId={ord_id}")
            return '05_已取消'
        
        # ===== 其他状态 =====
        logger.debug(f"OKX分类: 其他 - state={state}, reduceOnly={reduce_only}, pnl={pnl}")
        return '99_其他'
    
    except (KeyError, TypeError, IndexError, AttributeError) as e:
        logger.error(f"OKX分类器错误: {e}")
        return '99_其他'
    except Exception as e:
        logger.error(f"OKX分类器未知错误: {e}")
        return '99_其他'


def is_closing_event(category: str) -> bool:
    """判断是否是平仓事件"""
    return category == '03_平仓成交'