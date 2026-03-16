"""
数据完成部门 - 数据接收器
==================================================
【文件职责】
这个文件是整个数据完成模块的入口，负责：
1. 接收外部推送的两种数据：私人数据（币安/欧易）和行情数据
2. 在内存中覆盖更新存储（永远只保留最新数据）
3. 将整个存储区快照推送给所有订阅者

【推送目的地】
根据你的设计，订阅者包括：
1. 检测区文件 (detector.py)
2. 币安修复区入口 (repair/binance/__init__.py)
3. 欧易修复文件 (repair/okx/missing_repair.py)

【存储结构】
memory_store = {
    'market_data': {           # 行情数据
        'BTCUSDT': {...},      # key=合约名
        'ETHUSDT': {...},
        ...
    },
    'user_data': {             # 私人数据
        'binance_user': {      # key=交易所_user
            'exchange': 'binance',
            'data': {...}      # 真正的业务数据
        },
        'okx_user': {...}
    }
}

【推送格式】
每次数据更新后，推送整个存储区快照：
{
    'market_data': {...},  # 所有合约的最新行情
    'user_data': {...},    # 所有交易所的最新私人数据
    'timestamp': '2026-03-11T...'
}

【覆盖更新规则】
- 行情数据：按合约名覆盖，新数据直接替换旧数据
- 私人数据：按交易所覆盖，每个交易所只有一条最新数据
==================================================
"""

from datetime import datetime
from typing import Dict, Any, Optional, List, Union, Callable
import logging
import asyncio

logger = logging.getLogger(__name__)


class DataCompletionReceiver:
    """
    数据接收器（单例模式）
    ==================================================
    这个类在整个模块中只有一个实例，所有数据都通过它接收和分发。
    
    为什么用单例？
        - 确保所有订阅者收到的是同一份数据
        - 避免多个实例造成数据不一致
        - 统一管理内存存储
    ==================================================
    """
    
    _instance = None
    
    def __new__(cls):
        """单例模式：确保只有一个实例"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化接收器（只执行一次）"""
        if not self._initialized:
            # ===== 内存存储 - 分库存储，覆盖更新 =====
            self.memory_store = {
                'market_data': {},      # 行情数据专用库，key=symbol
                'user_data': {},        # 私人数据专用库，key=交易所_user
            }
            
            # ===== 时间戳跟踪（用于监控）=====
            self.last_market_time = None      # 最后一次收到行情的时间
            self.last_account_time = None     # 最后一次收到私人数据的时间
            self.last_market_count = 0        # 最后一次收到的行情条数
            
            # ===== 订阅者列表 =====
            # 所有订阅者的回调函数都会在这里注册
            # 当有新数据时，会遍历这个列表，给每个订阅者推送
            self.subscribers = []
            
            self._initialized = True
            logger.info("✅【接收存储区】 数据完成接收器初始化完成")
    
    # ==================== 订阅管理 ====================
    
    def subscribe(self, callback: Callable):
        """
        订阅数据推送
        ==================================================
        订阅者需要提供回调函数：async def callback(store_snapshot: dict)
        
        调用示例：
            receiver.subscribe(detector.handle_store_snapshot)
            receiver.subscribe(binance_repair.handle_store_snapshot)
            receiver.subscribe(okx_repair.handle_store_snapshot)
        
        订阅后立即推送一次当前数据，让新订阅者快速获取最新状态
        
        :param callback: 异步回调函数，接收一个参数（存储区快照）
        :return: self，支持链式调用
        ==================================================
        """
        if callback not in self.subscribers:
            self.subscribers.append(callback)
            logger.info(f"✅【接收存储区】 新增订阅者，当前共 {len(self.subscribers)} 个订阅者")
            
            # 订阅后立即推送一次当前数据
            asyncio.create_task(self._push_to_subscriber(callback))
            
        return self
    
    def unsubscribe(self, callback: Callable):
        """
        取消订阅
        :param callback: 之前订阅时传入的回调函数
        """
        if callback in self.subscribers:
            self.subscribers.remove(callback)
            logger.info(f"✅【接收存储区】 移除订阅者，当前共 {len(self.subscribers)} 个订阅者")
    
    # ==================== 数据接收入口 ====================
    
    async def receive_private_data(self, private_data: Dict[str, Any]):
        """
        接收私人数据
        ==================================================
        这是外部模块调用入口，当有新的私人数据时调用此方法。
        
        数据来源：
            - 币安账户数据
            - 欧易账户数据
        
        处理流程：
            1. 提取交易所和数据类型
            2. 覆盖更新内存存储（key=交易所_user）
            3. 推送整个存储区给所有订阅者
        
        :param private_data: 私人数据，格式为：
            {
                'exchange': 'binance',        # 交易所
                'data_type': 'position',      # 数据类型
                'data': {...},                 # 真正的业务数据（所有字段都是中文）
                'timestamp': '2026-03-11...'   # 原始时间戳
            }
        ==================================================
        """
        try:
            exchange = private_data.get('exchange', 'unknown')
            data_type = private_data.get('data_type', 'unknown')
            
            logger.info(f"📨【接收存储区】 收到私人数据: {exchange}.{data_type}")
            
            now = datetime.now()
            
            # ===== 覆盖更新存储 =====
            # 用 {exchange}_user 作为key，确保每个交易所只有一条数据
            storage_key = f"{exchange}_user"
            
            self.memory_store['user_data'][storage_key] = {
                'exchange': exchange,
                'data_type': data_type,
                'data': private_data.get('data', {}),          # 真正的业务数据
                'timestamp': private_data.get('timestamp', now.isoformat()),
                'received_at': now.isoformat()
            }
            
            self.last_account_time = now
            logger.debug(f"✅ 【接收存储区】私人数据已更新: {exchange}")
            
            # ===== 推送整个存储区给所有订阅者 =====
            await self._push_full_store()
            
        except Exception as e:
            logger.error(f"❌ 【接收存储区】接收私人数据失败: {e}", exc_info=True)
    
    async def receive_market_data(self, market_data: Union[List, Dict]):
        """
        接收市场数据
        ==================================================
        这是外部模块调用入口，当有新的行情数据时调用此方法。
        
        数据来源：
            - 跨模块计算后的行情数据（step5_cross_calc）
        
        处理流程：
            1. 如果是列表，遍历存储每个合约的行情数据
            2. 覆盖更新内存存储（key=合约名）
            3. 推送整个存储区给所有订阅者
        
        :param market_data: 行情数据，通常是列表，每个元素是一个合约的行情
        ==================================================
        """
        try:
            if isinstance(market_data, list):
                self.last_market_count = len(market_data)
                await self._store_market_data(market_data)
            else:
                logger.warning(f"⚠️【接收存储区】 收到非列表市场数据: {type(market_data)}")
                self.last_market_count = 0
            
            self.last_market_time = datetime.now()
            
            # ===== 推送整个存储区给所有订阅者 =====
            await self._push_full_store()
            
        except Exception as e:
            logger.error(f"❌ 【接收存储区】接收市场数据失败: {e}", exc_info=True)
    
    async def _store_market_data(self, data_list: List):
        """
        存储市场数据列表 - 覆盖更新
        ==================================================
        遍历行情数据列表，按合约名存储到内存中。
        每个合约只有一条最新数据，新数据直接覆盖旧数据。
        
        :param data_list: 行情数据列表
        :return: 成功存储的条数
        ==================================================
        """
        try:
            if not data_list:
                return 0
                
            stored_count = 0
            for item in data_list:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU，避免大量合约阻塞事件循环
                symbol = item.get('symbol')
                if not symbol:
                    continue
                
                # 创建简化版市场数据
                simplified_data = self._create_simplified_market_data(item)
                self.memory_store['market_data'][symbol] = simplified_data
                stored_count += 1
            
            logger.debug(f"✅ 【接收存储区】市场数据已更新: {stored_count} 条")
            return stored_count
            
        except Exception as e:
            logger.error(f"❌【接收存储区】 存储市场数据失败: {e}")
            return 0
    
    def _create_simplified_market_data(self, raw_data: Dict) -> Dict:
        """
        创建简化格式的市场数据
        ==================================================
        从原始数据中提取需要的字段，去掉不需要的包装。
        这样存储更简洁，查询更方便。
        
        币安修复需要用到：
            - binance_trade_price  → 币安最新价
            - binance_mark_price   → 币安标记价
        
        欧易修复需要用到：
            - okx数据本身已包含价格，不需要行情数据
        ==================================================
        
        :param raw_data: 原始行情数据
        :return: 简化后的字典
        """
        try:
            metadata = raw_data.get('metadata', {})
            
            return {
                'symbol': raw_data.get('symbol'),
                'trade_price_diff': raw_data.get('trade_price_diff'),
                'trade_price_diff_percent': raw_data.get('trade_price_diff_percent'),
                'rate_diff': raw_data.get('rate_diff'),
                'okx_trade_price': raw_data.get('okx_trade_price'),
                'okx_mark_price': raw_data.get('okx_mark_price'),
                'okx_price_to_mark_diff': raw_data.get('okx_price_to_mark_diff'),
                'okx_price_to_mark_diff_percent': raw_data.get('okx_price_to_mark_diff_percent'),
                'okx_funding_rate': raw_data.get('okx_funding_rate'),
                'okx_period_seconds': raw_data.get('okx_period_seconds'),
                'okx_countdown_seconds': raw_data.get('okx_countdown_seconds'),
                'okx_last_settlement': raw_data.get('okx_last_settlement'),
                'okx_current_settlement': raw_data.get('okx_current_settlement'),
                'okx_next_settlement': raw_data.get('okx_next_settlement'),
                'binance_trade_price': raw_data.get('binance_trade_price'),
                'binance_mark_price': raw_data.get('binance_mark_price'),
                'binance_price_to_mark_diff': raw_data.get('binance_price_to_mark_diff'),
                'binance_price_to_mark_diff_percent': raw_data.get('binance_price_to_mark_diff_percent'),
                'binance_funding_rate': raw_data.get('binance_funding_rate'),
                'binance_period_seconds': raw_data.get('binance_period_seconds'),
                'binance_countdown_seconds': raw_data.get('binance_countdown_seconds'),
                'binance_last_settlement': raw_data.get('binance_last_settlement'),
                'binance_current_settlement': raw_data.get('binance_current_settlement'),
                'binance_next_settlement': raw_data.get('binance_next_settlement'),
                'calculated_at': metadata.get('calculated_at', datetime.now().isoformat()),
                'source': metadata.get('source', 'step5_cross_calc')
            }
        except Exception as e:
            logger.error(f"❌【接收存储区】 创建简化市场数据失败: {e}")
            return {'symbol': raw_data.get('symbol', 'unknown')}
    
    # ==================== 推送逻辑 ====================
    
    async def _push_full_store(self):
        """
        推送完整存储区给所有订阅者
        ==================================================
        每次数据更新后调用此方法，将整个存储区快照推送给所有订阅者。
        
        推送规则：
            - 创建快照副本，避免推送过程中数据被修改
            - 遍历所有订阅者，为每个订阅者创建异步任务
            - 不等待订阅者处理完成，立即返回
        
        推送格式：
            {
                'market_data': {...},  # 所有合约的最新行情
                'user_data': {...},    # 所有交易所的最新私人数据
                'timestamp': '...'      # 推送时间戳
            }
        ==================================================
        """
        if not self.subscribers:
            return
        
        # 创建存储区快照（复制一份，避免推送过程中被修改）
        snapshot = {
            'market_data': self.memory_store['market_data'].copy(),
            'user_data': self.memory_store['user_data'].copy(),
            'timestamp': datetime.now().isoformat()
        }
        
        # ✅ [蚂蚁基因修复] 改为并行推送：创建所有任务，然后并行执行
        tasks = []
        for callback in self.subscribers:
            tasks.append(self._push_to_subscriber(callback, snapshot))
        
        # 并行执行所有推送任务，不等待结果
        if tasks:
            asyncio.create_task(asyncio.gather(*tasks, return_exceptions=True))
    
    async def _push_to_subscriber(self, callback: Callable, snapshot: dict = None):
        """
        推送给单个订阅者
        ==================================================
        为每个订阅者创建独立的异步任务，互不影响。
        如果某个订阅者处理失败，不影响其他订阅者。
        
        :param callback: 订阅者的回调函数
        :param snapshot: 要推送的快照，如果为None则创建当前快照
        ==================================================
        """
        try:
            if snapshot is None:
                # 如果没有提供快照，创建当前存储区的快照
                snapshot = {
                    'market_data': self.memory_store['market_data'].copy(),
                    'user_data': self.memory_store['user_data'].copy(),
                    'timestamp': datetime.now().isoformat()
                }
            
            # 创建异步任务推送，不等待结果
            asyncio.create_task(callback(snapshot))
            
        except Exception as e:
            logger.error(f"【接收存储区】推送数据给订阅者失败: {e}")
    
    # ==================== 查询接口 ====================
    # 以下接口供其他模块（如前端）查询当前数据状态
    
    def get_data_summary(self):
        """
        获取数据大纲
        用于前端显示有哪些数据来源
        """
        sources = []
        
        if self.memory_store['market_data']:
            sources.append({
                "name": "public_market",
                "description": "公开市场数据（实时行情、费率差）",
                "item_count": len(self.memory_store['market_data']),
                "endpoint": "/api/completion/data/public_market",
                "last_update": self.last_market_time.isoformat() if self.last_market_time else None
            })
        
        if self.memory_store['user_data']:
            sources.append({
                "name": "private_user",
                "description": "私人用户数据（欧易+币安）",
                "user_count": len(self.memory_store['user_data']),
                "endpoint": "/api/completion/data/private_user",
                "last_update": self.last_account_time.isoformat() if self.last_account_time else None
            })
        
        return {
            "timestamp": datetime.now().isoformat(),
            "source_count": len(sources),
            "sources": sources,
            "note": f"共{len(sources)}个数据来源，点击endpoint查看详情"
        }
    
    def get_public_market_data(self):
        """
        获取行情数据详情
        返回所有合约的最新行情数据
        """
        return {
            "source": "public_market",
            "description": "公开市场数据（实时行情、费率差）",
            "timestamp": self.last_market_time.isoformat() if self.last_market_time else datetime.now().isoformat(),
            "count": len(self.memory_store['market_data']),
            "data": self.memory_store['market_data']
        }
    
    def get_private_user_data(self):
        """
        获取私人数据详情
        按交易所合并，返回币安和欧易的最新数据
        """
        user_data = {}
        for key, data in self.memory_store['user_data'].items():
            exchange = data.get('exchange')
            if exchange in ['binance', 'okx']:
                # 只返回真正的业务数据，去掉包装
                user_data[exchange] = data.get('data', {})
        
        return {
            "source": "private_user",
            "description": "私人用户数据（币安 + 欧易）",
            "timestamp": self.last_account_time.isoformat() if self.last_account_time else datetime.now().isoformat(),
            "count": len(user_data),
            "data": user_data
        }


# ==================== 全局单例 ====================

_global_receiver = DataCompletionReceiver()


def get_receiver():
    """获取接收器单例实例"""
    return _global_receiver


# 对外只暴露这两个独立入口
async def receive_private_data(data):
    """外部调用入口：接收私人数据"""
    await _global_receiver.receive_private_data(data)


async def receive_market_data(data):
    """外部调用入口：接收市场数据"""
    await _global_receiver.receive_market_data(data)