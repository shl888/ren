"""
共享内存数据存储 - 执行者版
功能：只负责存储 + 执行管理员指令
所有决策逻辑都在pipeline_manager的FlowInstructions中
优化：日志优化，移除冗余代码
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging

# ✅ 导入管理员指令
from shared_data.pipeline_manager import FlowInstructions

logger = logging.getLogger(__name__)

# 统一的日志工具函数
def log_data_process(module: str, action: str, message: str, level: str = "INFO"):
    """统一的数据处理日志格式"""
    prefix = f"[数据处理][{module}][{action}]"
    full_message = f"{prefix} {message}"
    
    if level == "INFO":
        logger.info(full_message)
    elif level == "ERROR":
        logger.error(full_message)
    elif level == "WARNING":
        logger.warning(full_message)
    elif level == "DEBUG":
        logger.debug(full_message)

class DataStore:
    """共享数据存储 - 只执行，不决策"""
    
    def __init__(self):
        # 交易所实时数据
        self.market_data = {}
        
        # 资金费率结算数据
        self.funding_settlement = {"binance": {}}
        
        # 账户数据
        self.account_data = {}
        # 订单数据
        self.order_data = {}
        # 连接状态
        self.connection_status = {}
        
        # HTTP服务就绪状态
        self._http_server_ready = False
        
        # 大脑回调（用于直连）
        self.brain_callback = None
        
        # 流水线管理器（用于流向流水线）
        self.pipeline_manager = None
        
        # 锁，确保线程安全
        self.locks = {
            'market_data': asyncio.Lock(),
            'account_data': asyncio.Lock(),
            'order_data': asyncio.Lock(),
            'connection_status': asyncio.Lock(),
        }
        
        log_data_process("数据存储", "启动", "DataStore初始化完成（执行者版）")
    
    def set_brain_callback(self, callback):
        """设置大脑回调函数"""
        self.brain_callback = callback
        log_data_process("数据存储", "设置", "大脑回调已设置")
    
    def set_pipeline_manager(self, manager):
        """设置流水线管理器"""
        self.pipeline_manager = manager
        log_data_process("数据存储", "设置", "流水线管理器已设置")
    
    async def update_market_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """
        更新市场数据 → 存储 + 执行管理员指令
        """
        data_type = data.get("data_type", "unknown")
        
        # 1. 存储数据
        async with self.locks['market_data']:
            # 初始化数据结构
            if exchange not in self.market_data:
                self.market_data[exchange] = {}
            if symbol not in self.market_data[exchange]:
                self.market_data[exchange][symbol] = {}
            
            # 存储数据
            self.market_data[exchange][symbol][data_type] = {
                **data,
                'store_timestamp': datetime.now().isoformat(),
                'source': data.get('source', 'websocket')
            }
            
            # 存储最新引用
            self.market_data[exchange][symbol]['latest'] = data_type
        
        # 只记录重要数据的存储日志
        if data_type in ["funding_settlement", "funding_rate", "mark_price", "ticker"]:
            log_data_process("数据存储", "存储", f"{exchange} {symbol} {data_type}")
        
        # 2. ✅ 执行管理员指令：是否应该流出
        if not FlowInstructions.should_flow_market_data(exchange, symbol, data_type, data):
            return
        
        # 3. ✅ 执行管理员指令：流向哪里
        destination = FlowInstructions.get_flow_destination(data_type)
        
        if destination == "pipeline":
            await self._flow_to_pipeline(exchange, symbol, data_type, data)
        elif destination == "brain":
            await self._flow_to_brain(exchange, symbol, data_type, data)
    
    async def update_account_data(self, exchange: str, data: Dict[str, Any]):
        """
        更新账户数据 → 存储 + 执行管理员指令
        """
        async with self.locks['account_data']:
            self.account_data[exchange] = {
                **data,
                'timestamp': datetime.now().isoformat()
            }
        
        data_type = f"account_{data.get('type', 'balance')}"
        
        # ✅ 执行管理员指令：是否应该流出
        if not FlowInstructions.should_flow_account_data(exchange, data_type, data):
            return
        
        # ✅ 执行管理员指令：流向大脑
        await self._flow_to_brain(exchange, "N/A", data_type, data)
    
    async def update_order_data(self, exchange: str, order_id: str, data: Dict[str, Any]):
        """
        更新订单数据 → 存储 + 执行管理员指令
        """
        async with self.locks['order_data']:
            if exchange not in self.order_data:
                self.order_data[exchange] = {}
            self.order_data[exchange][order_id] = {
                **data,
                'update_time': datetime.now().isoformat()
            }
        
        data_type = "order"
        
        # ✅ 执行管理员指令：是否应该流出
        if not FlowInstructions.should_flow_order_data(exchange, order_id, data):
            return
        
        # ✅ 执行管理员指令：流向大脑
        await self._flow_to_brain(exchange, data.get('symbol', 'N/A'), data_type, data)
    
    async def update_connection_status(self, exchange: str, connection_type: str, status: Dict[str, Any]):
        """更新连接状态（只存储，不流出）"""
        async with self.locks['connection_status']:
            if exchange not in self.connection_status:
                self.connection_status[exchange] = {}
            self.connection_status[exchange][connection_type] = {
                **status,
                'timestamp': datetime.now().isoformat()
            }
        
        log_data_process("连接状态", "更新", f"{exchange} {connection_type}: {status.get('status', 'unknown')}")
    
    async def _flow_to_pipeline(self, exchange: str, symbol: str, data_type: str, data: Dict[str, Any]):
        """流向流水线"""
        if not self.pipeline_manager:
            log_data_process("数据存储", "错误", "流水线管理器未设置", "ERROR")
            return
        
        try:
            pipeline_data = {
                "exchange": exchange,
                "symbol": symbol,
                "data_type": data_type,
                "raw_data": data.get("raw_data", data),
                "timestamp": data.get("timestamp"),
                "priority": 5
            }
            
            await self.pipeline_manager.ingest_data(pipeline_data)
            log_data_process("数据存储", "流向", f"{exchange} {symbol} {data_type} → 流水线")
            
        except Exception as e:
            log_data_process("数据存储", "错误", f"流向流水线失败: {e}", "ERROR")
    
    async def _flow_to_brain(self, exchange: str, symbol: str, data_type: str, data: Dict[str, Any]):
        """流向大脑"""
        if not self.brain_callback:
            log_data_process("数据存储", "错误", "大脑回调未设置", "ERROR")
            return
        
        try:
            brain_data = {
                "exchange": exchange,
                "symbol": symbol,
                "data_type": data_type,
                "payload": data,
                "timestamp": datetime.now().isoformat()
            }
            
            await self.brain_callback(brain_data)
            log_data_process("数据存储", "流向", f"{exchange} {symbol} {data_type} → 大脑")
            
        except Exception as e:
            log_data_process("数据存储", "错误", f"流向大脑失败: {e}", "ERROR")
    
    # 其他获取方法保持不变...
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