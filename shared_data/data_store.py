"""
共享内存数据存储 - 定时全量版
功能：存储数据 + 按规则执行全量推送
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List, Set
import logging

logger = logging.getLogger(__name__)

class DataStore:
    """共享数据存储 - 按规则执行推送"""
    
    def __init__(self):
        # 交易所实时数据
        self.market_data = {}
        
        # 资金费率结算数据
        self.funding_settlement = {"binance": {}}
        
        # 账户数据
        self.account_data = {}
        self.order_data = {}
        self.connection_status = {}
        
        # HTTP服务就绪状态
        self._http_server_ready = False
        
        # 大脑回调（备用）
        self.brain_callback = None
        
        # 流水线管理员（单例）
        from shared_data.pipeline_manager import PipelineManager
        self.pipeline_manager = PipelineManager.instance()
        
        # 锁，确保线程安全
        self.locks = {
            'market_data': asyncio.Lock(),
            'account_data': asyncio.Lock(),
            'order_data': asyncio.Lock(),
            'connection_status': asyncio.Lock(),
        }
        
        logger.info("✅ DataStore初始化完成（定时全量执行模式）")
    
    async def update_market_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """
        更新市场数据（仅存储，不推送）
        """
        async with self.locks['market_data']:
            # 初始化数据结构
            if exchange not in self.market_data:
                self.market_data[exchange] = {}
            if symbol not in self.market_data[exchange]:
                self.market_data[exchange][symbol] = {}
            
            # 获取数据类型
            data_type = data.get("data_type", "unknown")
            
            # 存储数据
            self.market_data[exchange][symbol][data_type] = {
                **data,
                'store_timestamp': datetime.now().isoformat(),
                'source': 'websocket'
            }
            
            # 存储最新引用
            self.market_data[exchange][symbol]['latest'] = data_type
            
            # 调试日志
            if data_type in ['funding_rate', 'mark_price']:
                funding_rate = data.get('funding_rate', 0)
                logger.debug(f"[DataStore] 存储 {exchange} {symbol} {data_type} = {funding_rate:.6f}")
    
    async def push_all_data_by_rules(self, history_flowed_contracts: Set[str]):
        """
        按规则推送全量数据（由pipeline_manager定时调用）
        规则：币安历史费率每个合约只推送1次
        """
        pushed_count = 0
        
        async with self.locks['market_data']:
            for exchange, symbols in self.market_data.items():
                for symbol, data_dict in symbols.items():
                    for data_type, data in data_dict.items():
                        # 跳过元数据
                        if data_type in ['latest', 'store_timestamp']:
                            continue
                        
                        # 应用规则：币安历史费率每个合约只推送1次
                        if exchange == "binance" and data_type == "funding_settlement":
                            contract_key = f"binance_{symbol}"
                            if contract_key in history_flowed_contracts:
                                continue  # 已推送过，跳过
                        
                        # 推送数据
                        await self._push_single_data(exchange, symbol, data_type, data)
                        pushed_count += 1
        
        if pushed_count > 0:
            logger.debug(f"📤 定时推送完成: {pushed_count} 条数据")
    
    async def _push_single_data(self, exchange: str, symbol: str, data_type: str, data: Dict[str, Any]):
        """推送单条数据到流水线"""
        try:
            # 如果是币安历史费率首次流出，通知管理员记录
            if exchange == "binance" and data_type == "funding_settlement":
                self.pipeline_manager.mark_history_flowed(exchange, symbol)
            
            pipeline_data = {
                "exchange": exchange,
                "symbol": symbol,
                "data_type": data_type,
                "raw_data": data.get("raw_data", data),
                "timestamp": data.get("timestamp"),
                "priority": 5
            }
            
            # 推送到流水线
            await self.pipeline_manager.ingest_data(pipeline_data)
            
        except Exception as e:
            logger.error(f"推送数据失败: {exchange}.{symbol}.{data_type} - {e}")
    
    async def update_account_data(self, exchange: str, data: Dict[str, Any]):
        """
        更新账户数据 → 直连大脑（立即推送）
        """
        async with self.locks['account_data']:
            self.account_data[exchange] = {
                **data,
                'timestamp': datetime.now().isoformat()
            }
        
        # 立即推送账户数据
        try:
            account_payload = {
                "exchange": exchange,
                "data_type": f"account_{data.get('type', 'balance')}",
                "symbol": "N/A",
                "payload": data,
                "timestamp": datetime.now().isoformat(),
                "priority": 1
            }
            await self.pipeline_manager.ingest_data(account_payload)
            logger.debug(f"📤 账户数据推送: {exchange}")
        except Exception as e:
            logger.error(f"账户数据推送失败: {e}")
    
    async def update_order_data(self, exchange: str, order_id: str, data: Dict[str, Any]):
        """
        更新订单数据 → 直连大脑（立即推送）
        """
        async with self.locks['order_data']:
            if exchange not in self.order_data:
                self.order_data[exchange] = {}
            self.order_data[exchange][order_id] = {
                **data,
                'update_time': datetime.now().isoformat()
            }
        
        # 立即推送订单数据
        try:
            order_payload = {
                "exchange": exchange,
                "data_type": "order",
                "symbol": data.get('symbol', 'N/A'),
                "order_id": order_id,
                "payload": data,
                "timestamp": datetime.now().isoformat(),
                "priority": 2
            }
            await self.pipeline_manager.ingest_data(order_payload)
            logger.debug(f"📤 订单数据推送: {exchange}.{order_id}")
        except Exception as e:
            logger.error(f"订单数据推送失败: {e}")
    
    # 其他方法保持不变...
    async def update_connection_status(self, exchange: str, connection_type: str, status: Dict[str, Any]):
        """更新连接状态"""
        async with self.locks['connection_status']:
            if exchange not in self.connection_status:
                self.connection_status[exchange] = {}
            self.connection_status[exchange][connection_type] = {
                **status,
                'timestamp': datetime.now().isoformat()
            }
    
    async def get_connection_status(self, exchange: str = None) -> Dict[str, Any]:
        """获取连接状态"""
        async with self.locks['connection_status']:
            if exchange:
                return self.connection_status.get(exchange, {}).copy()
            return self.connection_status.copy()
    
    async def get_market_data(self, exchange: str, symbol: str = None, 
                             data_type: str = None, get_latest: bool = False) -> Dict[str, Any]:
        async with self.locks['market_data']:
            if exchange not in self.market_data:
                return {}
            if not symbol:
                result = {}
                for sym, data_dict in self.market_data[exchange].items():
                    if get_latest and 'latest' in data_dict:
                        result[sym] = data_dict.get(data_dict['latest'], {})
                    else:
                        result[sym] = {k: v for k, v in data_dict.items() 
                                     if k not in ['latest', 'store_timestamp']}
                return result
            if symbol not in self.market_data[exchange]:
                return {}
            symbol_data = self.market_data[exchange][symbol]
            if data_type:
                return symbol_data.get(data_type, {})
            return {k: v for k, v in symbol_data.items() 
                   if k not in ['latest', 'store_timestamp']}
    
    def get_market_data_stats(self) -> Dict[str, Any]:
        """获取统计数据"""
        stats = {'exchanges': {}, 'total_symbols': 0, 'total_data_types': 0}
        for exchange, symbols in self.market_data.items():
            symbol_count = len(symbols)
            data_type_count = sum(
                len([k for k in v.keys() if k not in ['latest', 'store_timestamp']])
                for v in symbols.values()
            )
            stats['exchanges'][exchange] = {
                'symbols': symbol_count,
                'data_types': data_type_count
            }
            stats['total_symbols'] += symbol_count
            stats['total_data_types'] += data_type_count
        return stats
    
    def set_http_server_ready(self, ready: bool):
        self._http_server_ready = ready
    
    def is_http_server_ready(self) -> bool:
        return self._http_server_ready

# 全局实例
data_store = DataStore()