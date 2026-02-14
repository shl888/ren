"""
币安订单事件分类器 - 纯函数，无状态
12种事件分类规则（包含过期细分），输入原始data，返回分类字符串
"""
from typing import Dict, Any, List, Optional
import time
import logging

logger = logging.getLogger(__name__)


def classify_binance_order(data: Dict[str, Any]) -> str:
    """
    币安订单更新事件分类
    返回: 
    '01_开仓', '02_设置止损', '03_设置止盈', 
    '04_触发止损', '05_触发止盈', '06_主动平仓',
    '07_取消止损', '08_取消止盈', 
    '09_止损过期(被触发)', '10_止损过期(被取消)', 
    '11_止盈过期(被触发)', '12_止盈过期(被取消)',
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
        
        # 9-12. 过期分类
        if x_status == 'EXPIRED':
            # 止损单过期
            if ot == 'STOP_MARKET':
                if er == '8':
                    return '09_止损过期(被触发)'  # 被触发而结束
                else:  # er == '6' 或其他
                    return '10_止损过期(被取消)'  # 被动取消
            
            # 止盈单过期
            if ot == 'TAKE_PROFIT_MARKET':
                if er == '8':
                    return '11_止盈过期(被触发)'  # 被触发而结束
                else:  # er == '6' 或其他
                    return '12_止盈过期(被取消)'  # 被动取消
        
        # 99. 其他：所有未匹配的情况
        return '99_其他'
    
    except (KeyError, TypeError, AttributeError) as e:
        logger.error(f"分类器错误: {e}")
        return '99_其他'


def is_looking_event(category: str) -> bool:
    """
    判断是否是关注的事件
    所有12种事件都是需要关注的
    """
    return category in [
        '01_开仓',
        '02_设置止损',
        '03_设置止盈',
        '04_触发止损',
        '05_触发止盈',
        '06_主动平仓',
        '07_取消止损',
        '08_取消止盈',
        '09_止损过期(被触发)',
        '10_止损过期(被取消)',
        '11_止盈过期(被触发)',
        '12_止盈过期(被取消)'
    ]


def is_trigger_event(category: str) -> bool:
    """
    判断是否是触发清理动作的事件
    只有这三种事件会触发缓存清理动作：
    - 04_触发止损
    - 05_触发止盈
    - 06_主动平仓
    """
    return category in ['04_触发止损', '05_触发止盈', '06_主动平仓']


# 所有需要被清理的数据类型列表（共12种）
ALL_DATA_TYPES_TO_CLEAN = [
    '01_开仓',
    '02_设置止损',
    '03_设置止盈',
    '04_触发止损',
    '05_触发止盈',
    '06_主动平仓',
    '07_取消止损',
    '08_取消止盈',
    '09_止损过期(被触发)',
    '10_止损过期(被取消)',
    '11_止盈过期(被触发)',
    '12_止盈过期(被取消)'
]


class StrategyCache:
    """策略缓存管理类"""
    
    def __init__(self, delay_seconds: int = 300):
        """
        初始化缓存管理器
        :param delay_seconds: 延迟清理时间，默认300秒（5分钟）
        """
        self.delay_seconds = delay_seconds
        # 策略数据存储结构
        # {
        #     'strategy_name': {
        #         '01_开仓': {'data': {}, 'timestamp': 1234567890},
        #         '02_设置止损': {'data': {}, 'timestamp': 1234567890},
        #         ...
        #     }
        # }
        self.strategies_data = {}
        
    def update_strategy_data(self, strategy_name: str, category: str, data: Dict[str, Any]):
        """
        更新策略数据
        :param strategy_name: 策略名称（如 XANUSDT_10）
        :param category: 事件分类（01-12）
        :param data: 原始数据
        """
        if strategy_name not in self.strategies_data:
            self.strategies_data[strategy_name] = {}
        
        # 存储数据并记录时间戳
        self.strategies_data[strategy_name][category] = {
            'data': data,
            'timestamp': time.time()
        }
        logger.debug(f"更新缓存: {strategy_name} - {category}")
        
    def cleanup_strategy_data(self, strategy_name: str, current_time: float = None):
        """
        清理指定策略的缓存数据（延迟5分钟）
        :param strategy_name: 策略名称
        :param current_time: 当前时间戳，默认使用time.time()
        """
        if current_time is None:
            current_time = time.time()
            
        if strategy_name not in self.strategies_data:
            return
            
        strategy_data = self.strategies_data[strategy_name]
        categories_to_delete = []
        
        # 找出所有需要清理的数据类型（超过延迟时间的）
        for category in ALL_DATA_TYPES_TO_CLEAN:
            if category in strategy_data:
                data_timestamp = strategy_data[category]['timestamp']
                if current_time - data_timestamp >= self.delay_seconds:
                    categories_to_delete.append(category)
        
        # 执行清理
        for category in categories_to_delete:
            logger.info(f"清理缓存: {strategy_name} - {category} (延迟{self.delay_seconds}秒)")
            del strategy_data[category]
        
        # 如果策略下没有数据了，删除整个策略条目
        if not strategy_data:
            del self.strategies_data[strategy_name]
            logger.info(f"删除空策略: {strategy_name}")
            
    def on_order_update(self, strategy_name: str, category: str, data: Dict[str, Any]):
        """
        订单更新事件处理
        :param strategy_name: 策略名称
        :param category: 事件分类
        :param data: 原始数据
        """
        # 1. 先存储当前数据
        self.update_strategy_data(strategy_name, category, data)
        
        # 2. 判断是否触发清理动作
        if is_trigger_event(category):
            logger.info(f"触发清理动作: {strategy_name} - {category}")
            self.cleanup_strategy_data(strategy_name)
            
    def get_strategy_data(self, strategy_name: str, category: str = None) -> Optional[Dict]:
        """
        获取策略数据
        :param strategy_name: 策略名称
        :param category: 可选，指定分类
        :return: 策略数据
        """
        if strategy_name not in self.strategies_data:
            return {} if category is None else None
            
        if category is None:
            return self.strategies_data[strategy_name]
            
        data_entry = self.strategies_data[strategy_name].get(category)
        return data_entry.get('data') if data_entry else None
    
    def get_strategy_timestamp(self, strategy_name: str, category: str) -> Optional[float]:
        """
        获取策略数据的时间戳
        :param strategy_name: 策略名称
        :param category: 分类
        :return: 时间戳或None
        """
        if strategy_name not in self.strategies_data:
            return None
            
        data_entry = self.strategies_data[strategy_name].get(category)
        return data_entry.get('timestamp') if data_entry else None
    
    def force_cleanup_all(self):
        """强制清理所有过期的策略数据（可定时调用）"""
        current_time = time.time()
        strategies_to_delete = []
        
        for strategy_name in list(self.strategies_data.keys()):
            self.cleanup_strategy_data(strategy_name, current_time)
            # 检查策略是否为空
            if not self.strategies_data[strategy_name]:
                strategies_to_delete.append(strategy_name)
        
        # 清理空策略
        for strategy_name in strategies_to_delete:
            del self.strategies_data[strategy_name]
        
        if strategies_to_delete:
            logger.info(f"定时清理完成，清理了 {len(strategies_to_delete)} 个空策略")


# 创建全局缓存管理器实例
global_cache = StrategyCache(delay_seconds=300)


def get_cache_manager() -> StrategyCache:
    """获取全局缓存管理器实例"""
    return global_cache


# 使用示例
if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO)
    
    # 创建缓存管理器（延迟5分钟）
    cache_manager = StrategyCache(delay_seconds=300)
    
    # 模拟收到订单更新
    def process_order_update(raw_data: Dict[str, Any], strategy_name: str):
        """
        处理收到的订单更新
        :param raw_data: 原始数据
        :param strategy_name: 策略名称
        """
        # 分类事件
        category = classify_binance_order(raw_data)
        
        if is_looking_event(category):
            # 更新缓存并触发清理（如果需要）
            cache_manager.on_order_update(strategy_name, category, raw_data)
            print(f"处理事件: {strategy_name} - {category}")
        else:
            print(f"忽略事件: {strategy_name} - {category}")
    
    # 模拟数据
    mock_data = {
        "data": {
            "o": {
                "s": "XANUSDT",
                "S": "SELL",
                "ot": "STOP_MARKET",
                "o": "STOP_MARKET",
                "X": "EXPIRED",
                "sp": "0.00812",
                "cp": False,
                "er": "6"
            }
        }
    }
    
    # 测试
    process_order_update(mock_data, "XANUSDT_10")
    
    # 定时清理任务（可每5分钟执行一次）
    def scheduled_cleanup():
        """定时清理任务"""
        logger.info("执行定时清理任务...")
        cache_manager.force_cleanup_all()
    
    # 模拟5分钟后清理
    time.sleep(1)  # 实际使用时改为300秒
    scheduled_cleanup()