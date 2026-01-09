"""
共享内存数据存储 - PipelineManager集成版
功能：存储数据 + 智能分流（市场数据→流水线，账户数据→直连大脑）
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
import logging
import time  # ✅ 添加time模块用于计时

# 导入管理员
from shared_data.pipeline_manager import PipelineManager, DataType

logger = logging.getLogger(__name__)

# ✅ 添加：统一的日志工具函数
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
    """共享数据存储，线程安全 - PipelineManager集成版"""
    
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
        
        # 大脑回调（备用）
        self.brain_callback = None
        
        # 流水线管理员（单例）
        self.pipeline_manager = PipelineManager.instance()
        
        # 锁，确保线程安全
        self.locks = {
            'market_data': asyncio.Lock(),
            'account_data': asyncio.Lock(),
            'order_data': asyncio.Lock(),
            'connection_status': asyncio.Lock(),
        }
        
        # ✅ 修改：统一日志格式
        log_data_process("数据存储", "启动", "已集成PipelineManager")
        
        # ✅ 添加：5分钟统计计时器
        self.last_report_time = time.time()
        self.market_data_count = 0
        self.account_data_count = 0
        self.order_data_count = 0
    
    # 设置大脑回调（备用）
    def set_brain_callback(self, callback):
        """设置大脑回调函数（接收成品数据）"""
        self.brain_callback = callback
        log_data_process("数据存储", "设置", "大脑回调已设置")
    
    # 推送成品数据给大脑（备用）
    async def _push_to_brain(self, processed_data: Dict[str, Any]):
        try:
            if self.brain_callback:
                await self.brain_callback(processed_data)
        except Exception as e:
            log_data_process("数据存储", "错误", f"推送数据给大脑失败: {e}", "ERROR")
    
    async def update_market_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """
        更新市场数据 → 自动进入5步流水线
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
        
        # **核心：推送到流水线**
        try:
            pipeline_data = {
                "exchange": exchange,
                "symbol": symbol,
                "data_type": data_type,
                "raw_data": data.get("raw_data", data),  # 兼容格式
                "timestamp": data.get("timestamp"),
                "priority": 5  # 默认优先级
            }
            await self.pipeline_manager.ingest_data(pipeline_data)
            
            # ✅ 修改：移除单条日志，改为计数
            self.market_data_count += 1
            
            # ✅ 添加：5分钟统计
            current_time = time.time()
            if current_time - self.last_report_time >= 300:  # 5分钟
                log_data_process("数据存储", "统计", 
                               f"5分钟: 市场数据{self.market_data_count}次, "
                               f"账户数据{self.account_data_count}次, "
                               f"订单数据{self.order_data_count}次")
                self.last_report_time = current_time
                self.market_data_count = 0
                self.account_data_count = 0
                self.order_data_count = 0
                
        except Exception as e:
            log_data_process("数据存储", "错误", f"市场数据推送流水线失败: {e}", "ERROR")
    
    async def update_account_data(self, exchange: str, data: Dict[str, Any]):
        """
        更新账户数据 → 直连大脑（不经过流水线）
        """
        async with self.locks['account_data']:
            self.account_data[exchange] = {
                **data,
                'timestamp': datetime.now().isoformat()
            }
        
        # **核心：直连大脑**
        try:
            account_payload = {
                "exchange": exchange,
                "data_type": f"account_{data.get('type', 'balance')}",
                "symbol": "N/A",  # 账户数据无symbol
                "payload": data,
                "timestamp": datetime.now().isoformat(),
                "priority": 1  # 高优先级
            }
            await self.pipeline_manager.ingest_data(account_payload)
            
            # ✅ 修改：实时打印账户数据
            data_type = account_payload["data_type"]
            log_data_process("账户数据", "接收", f"{exchange} {data_type}")
            
            self.account_data_count += 1
            
        except Exception as e:
            log_data_process("数据存储", "错误", f"账户数据推送失败: {e}", "ERROR")
    
    async def update_order_data(self, exchange: str, order_id: str, data: Dict[str, Any]):
        """
        更新订单数据 → 直连大脑（不经过流水线）
        """
        async with self.locks['order_data']:
            if exchange not in self.order_data:
                self.order_data[exchange] = {}
            self.order_data[exchange][order_id] = {
                **data,
                'update_time': datetime.now().isoformat()
            }
        
        # **核心：直连大脑**
        try:
            order_payload = {
                "exchange": exchange,
                "data_type": "order",
                "symbol": data.get('symbol', 'N/A'),
                "order_id": order_id,
                "payload": data,
                "timestamp": datetime.now().isoformat(),
                "priority": 2  # 次高优先级
            }
            await self.pipeline_manager.ingest_data(order_payload)
            
            # ✅ 修改：实时打印交易数据
            symbol = data.get('symbol', 'N/A')
            side = data.get('side', 'unknown')
            status = data.get('status', 'unknown')
            log_data_process("交易数据", "接收", f"{exchange} {symbol} {side} 订单 {status}")
            
            self.order_data_count += 1
            
        except Exception as e:
            log_data_process("数据存储", "错误", f"订单数据推送失败: {e}", "ERROR")
    
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
    
    # 其他方法保持不变...
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