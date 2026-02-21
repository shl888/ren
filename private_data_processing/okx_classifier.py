"""
OKX订单事件分类器 - 纯函数，无状态
3种事件分类规则：开仓、平仓、其他
"""
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


def classify_okx_order(data: Dict[str, Any]) -> str:
    """
    OKX订单更新事件分类
    返回: 
    '01_开仓',
    '02_平仓',
    '99_其他'
    """
    try:
        # 提取订单数据 - OKX的数据结构: data.data是一个数组
        # data 参数是完整的 private_data，包含外层包装
        raw_data = data.get('data', {})
        
        # 检查数据结构
        if 'data' not in raw_data:
            logger.debug(f"OKX分类: 缺少data字段")
            return '99_其他'
        
        # data.data 是一个数组
        if not isinstance(raw_data['data'], list) or len(raw_data['data']) == 0:
            logger.debug(f"OKX分类: data.data不是数组或为空")
            return '99_其他'
        
        # 获取第一个订单数据
        order_data = raw_data['data'][0]
        
        # 获取关键字段
        pos_side = order_data.get('posSide', '')      # long/short
        reduce_only = order_data.get('reduceOnly', 'false')  # true/false
        pnl = order_data.get('pnl', '0')               # 盈亏
        state = order_data.get('state', '')            # 订单状态
        ord_type = order_data.get('ordType', '')        # 订单类型
        inst_id = order_data.get('instId', 'unknown')   # 交易对
        
        # 只处理已成交的订单（避免中间状态干扰）
        if state != 'filled':
            logger.debug(f"OKX分类: 订单未成交, state={state}")
            return '99_其他'
        
        # ===== 开仓判断 =====
        # reduceOnly=false 且 pnl == 0（无盈亏） 且 订单已成交
        if reduce_only == 'false' and (pnl == '0' or pnl == 0):
            logger.debug(f"OKX分类: 开仓 - {inst_id}, pnl={pnl}")
            return '01_开仓'
            
        # ===== 平仓判断 =====
        # reduceOnly=true 且 pnl != 0（有盈亏） 且 订单已成交
        if reduce_only == 'true' and pnl != '0' and pnl != 0:
            logger.debug(f"OKX分类: 平仓 - {inst_id}, pnl={pnl}")
            return '02_平仓'
            
        # ===== 其他 =====
        logger.debug(f"OKX分类: 其他情况 - reduceOnly={reduce_only}, pnl={pnl}")
        return '99_其他'
    
    except (KeyError, TypeError, IndexError, AttributeError) as e:
        logger.error(f"OKX分类器错误: {e}")
        return '99_其他'
    except Exception as e:
        logger.error(f"OKX分类器未知错误: {e}")
        return '99_其他'


def is_closing_event(category: str) -> bool:
    """判断是否是平仓事件"""
    return category == '02_平仓'