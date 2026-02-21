"""
OKX订单事件分类器 - 纯函数，无状态
8种事件分类规则：挂单、开仓部分成交、开仓全部成交、平仓部分成交、平仓全部成交、已取消、其他
"""
from typing import Dict, Any, Union
import logging

logger = logging.getLogger(__name__)


def classify_okx_order(data: Union[Dict[str, Any], list]) -> str:
    """
    OKX订单更新事件分类
    返回: 
    '01_挂单',                # 新挂单（未成交）- 过滤
    '02_开仓(部分成交)',       # 开仓部分成交 - 过滤
    '03_开仓(全部成交)',       # 开仓全部成交
    '04_平仓(部分成交)',       # 平仓部分成交 - 过滤
    '05_平仓(全部成交)',       # 平仓全部成交
    '06_已取消',              # 已取消
    '99_其他'                 # 其他情况
    """
    try:
        # 处理不同可能的数据结构
        order_data = None
        
        # 情况1：传入的是整个private_data（有data字段）
        if isinstance(data, dict) and 'data' in data:
            inner_data = data['data']
            if isinstance(inner_data, list) and len(inner_data) > 0:
                order_data = inner_data[0]
            elif isinstance(inner_data, dict):
                if 'data' in inner_data and isinstance(inner_data['data'], list):
                    # OKX标准格式：{"data": {"data": [...]}}
                    if len(inner_data['data']) > 0:
                        order_data = inner_data['data'][0]
                else:
                    order_data = inner_data
        
        # 情况2：传入的是raw_data（直接是data数组）
        elif isinstance(data, list) and len(data) > 0:
            order_data = data[0]
        
        # 情况3：传入的是单个订单对象
        elif isinstance(data, dict):
            # 检查是否包含订单关键字段
            if 'ordId' in data or 'instId' in data:
                order_data = data
        
        if not order_data:
            logger.debug(f"OKX分类: 无法提取订单数据")
            return '99_其他'
        
        # 获取关键字段
        reduce_only = str(order_data.get('reduceOnly', 'false')).lower()
        pnl = order_data.get('pnl', '0')
        state = order_data.get('state', '')
        inst_id = order_data.get('instId', 'unknown')
        ord_id = order_data.get('ordId', 'unknown')
        acc_fill_sz = order_data.get('accFillSz', '0')
        sz = order_data.get('sz', '0')
        
        logger.debug(f"OKX分类: 处理订单 {ord_id}, state={state}, reduceOnly={reduce_only}, pnl={pnl}, accFillSz={acc_fill_sz}, sz={sz}")
        
        # ===== 挂单状态处理（未成交）- 需要被过滤 =====
        if state == 'live':
            logger.info(f"OKX分类: 挂单 - {inst_id}, reduceOnly={reduce_only}, ordId={ord_id}")
            return '01_挂单'
        
        # ===== 部分成交状态 =====
        if state == 'partially_filled':
            # 判断是开仓还是平仓的部分成交
            if reduce_only == 'false':
                logger.info(f"OKX分类: 开仓(部分成交) - {inst_id}, accFillSz={acc_fill_sz}/{sz}, ordId={ord_id}")
                return '02_开仓(部分成交)'
            elif reduce_only == 'true':
                logger.info(f"OKX分类: 平仓(部分成交) - {inst_id}, accFillSz={acc_fill_sz}/{sz}, ordId={ord_id}")
                return '04_平仓(部分成交)'
            else:
                logger.info(f"OKX分类: 其他部分成交 - {inst_id}, ordId={ord_id}")
                return '99_其他'
        
        # ===== 已成交订单处理 =====
        if state == 'filled':
            # 判断是否全部成交
            is_full_filled = (acc_fill_sz == sz) or (float(acc_fill_sz) >= float(sz) - 0.000001)
            
            if reduce_only == 'false':
                # 开仓成交
                if is_full_filled:
                    logger.info(f"OKX分类: 开仓(全部成交) - {inst_id}, ordId={ord_id}")
                    return '03_开仓(全部成交)'
                else:
                    logger.info(f"OKX分类: 开仓(部分成交) - {inst_id}, accFillSz={acc_fill_sz}/{sz}, ordId={ord_id}")
                    return '02_开仓(部分成交)'
            elif reduce_only == 'true':
                # 平仓成交
                if is_full_filled:
                    logger.info(f"OKX分类: 平仓(全部成交) - {inst_id}, ordId={ord_id}")
                    return '05_平仓(全部成交)'
                else:
                    logger.info(f"OKX分类: 平仓(部分成交) - {inst_id}, accFillSz={acc_fill_sz}/{sz}, ordId={ord_id}")
                    return '04_平仓(部分成交)'
            else:
                logger.info(f"OKX分类: 其他成交 - {inst_id}, ordId={ord_id}")
                return '99_其他'
        
        # ===== 已取消订单 =====
        if state == 'canceled':
            logger.info(f"OKX分类: 已取消 - {inst_id}, ordId={ord_id}")
            return '06_已取消'
        
        # ===== 其他状态 =====
        logger.info(f"OKX分类: 其他 - state={state}, reduceOnly={reduce_only}, pnl={pnl}")
        return '99_其他'
    
    except Exception as e:
        logger.error(f"OKX分类器错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return '99_其他'


def is_closing_event(category: str) -> bool:
    """判断是否是平仓全部成交事件（需要清理缓存）"""
    return category == '05_平仓(全部成交)'