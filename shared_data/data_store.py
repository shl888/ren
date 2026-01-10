"""
DataStore - 执行者/执法者
功能：1. 接收管理员规则 2. 按规则放水 3. 自动执行
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class DataStore:
    """执行者：按管理员规则放水"""
    
    def __init__(self):
        # 数据存储
        self.market_data = {
            "binance": defaultdict(dict),
            "okx": defaultdict(dict)
        }
        
        # 账户数据
        self.account_data = {}
        self.order_data = {}
        self.connection_status = {}
        
        # HTTP服务就绪状态
        self._http_server_ready = False
        
        # 管理员规则（等待接收）
        self.rules = None
        self.rule_lock = asyncio.Lock()
        
        # 执行状态
        self.flowing = False
        self.flow_task = None
        self.water_callback = None
        
        # 规则执行记录
        self.execution_records = {
            "binance_history": {
                "flowed_contracts": set(),      # 已流过的合约
                "total_flowed": 0,              # 总共流过多少次
                "history_complete": False,      # 是否已完成
                "last_flow_time": 0
            },
            "total_flows": 0,                   # 总共放水次数
            "last_flow_time": 0
        }
        
        # 数据锁
        self.locks = {
            'market_data': asyncio.Lock(),
            'account_data': asyncio.Lock(),
            'order_data': asyncio.Lock(),
            'connection_status': asyncio.Lock(),
            'execution_records': asyncio.Lock(),
        }
        
        logger.info("✅ DataStore初始化完成（执行者）")
    
    # ==================== HTTP服务相关方法 ====================
    
    def set_http_server_ready(self, ready: bool):
        """设置HTTP服务就绪状态"""
        self._http_server_ready = ready
        logger.info(f"🌐 HTTP服务状态: {'就绪' if ready else '未就绪'}")
    
    def is_http_server_ready(self) -> bool:
        """检查HTTP服务是否就绪"""
        return self._http_server_ready
    
    # ==================== 接收规则 ====================
    
    async def receive_rules(self, rules: Dict[str, Any]):
        """接收管理员规则"""
        async with self.rule_lock:
            self.rules = rules
            logger.info("📋 已接收管理员规则")
    
    async def receive_rule_update(self, rule_key: str, rule_value: Any):
        """接收规则更新"""
        async with self.rule_lock:
            if self.rules and rule_key in self.rules:
                self.rules[rule_key] = rule_value
                logger.info(f"📝 规则更新接收: {rule_key} = {rule_value}")
    
    # ==================== 放水系统 ====================
    
    async def start_flowing(self, water_callback: Callable):
        """
        开始按规则放水
        water_callback: 放水回调函数，水放给流水线
        """
        if self.flowing:
            logger.warning("⚠️ 已经在放水中")
            return
        
        if not self.rules:
            logger.error("❌ 没有接收到规则，无法开始放水")
            return
        
        self.flowing = True
        self.water_callback = water_callback
        
        logger.info("🚰 开始按规则放水...")
        
        # 启动放水任务
        self.flow_task = asyncio.create_task(self._flow_loop())
    
    async def stop_flowing(self):
        """停止放水"""
        if not self.flowing:
            return
        
        logger.info("🛑 停止放水...")
        self.flowing = False
        
        if self.flow_task:
            self.flow_task.cancel()
            try:
                await self.flow_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ 放水已停止")
    
    async def _flow_loop(self):
        """放水循环 - 按规则执行"""
        while self.flowing:
            try:
                # 检查规则是否允许放水
                if not self.rules["flow"]["enabled"]:
                    await asyncio.sleep(1)
                    continue
                
                # 按规则收集水
                water = await self._collect_water_by_rules()
                
                # 放水
                if water and self.water_callback:
                    await self.water_callback(water)
                    
                    # 记录
                    async with self.locks['execution_records']:
                        self.execution_records["total_flows"] += 1
                        self.execution_records["last_flow_time"] = time.time()
                
                # 按规则间隔等待
                interval = self.rules["flow"]["interval_seconds"]
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"放水循环错误: {e}")
                await asyncio.sleep(5)
    
    async def _collect_water_by_rules(self) -> List[Dict[str, Any]]:
        """按规则收集水"""
        if not self.rules:
            return []
        
        water = []
        
        async with self.locks['market_data']:
            # 检查币安历史费率是否已完成
            history_complete = self.execution_records["binance_history"]["history_complete"]
            
            # 遍历所有数据
            for exchange in ["binance", "okx"]:
                if exchange not in self.market_data:
                    continue
                
                for symbol, data_dict in self.market_data[exchange].items():
                    for data_type, data in data_dict.items():
                        # 跳过内部字段
                        if data_type in ['latest', 'store_timestamp']:
                            continue
                        
                        # ==================== 规则执行 ====================
                        # 规则1：币安历史费率每个合约最多流1次
                        if exchange == "binance" and data_type == "funding_settlement":
                            # 如果已完成，跳过所有
                            if history_complete:
                                continue
                            
                            # 检查是否已流过
                            if symbol in self.execution_records["binance_history"]["flowed_contracts"]:
                                continue  # 按规则：已流过，跳过
                            
                            # 按规则：标记为已流过
                            async with self.locks['execution_records']:
                                self.execution_records["binance_history"]["flowed_contracts"].add(symbol)
                                self.execution_records["binance_history"]["total_flowed"] += 1
                                self.execution_records["binance_history"]["last_flow_time"] = time.time()
                            
                            # 按规则：检查是否完成
                            expected = self.rules["binance_history"]["expected_total_contracts"]
                            threshold = self.rules["binance_history"]["complete_threshold"]
                            flowed_count = len(self.execution_records["binance_history"]["flowed_contracts"])
                            
                            if flowed_count >= threshold:
                                self.execution_records["binance_history"]["history_complete"] = True
                                logger.info(f"🎉 按规则完成：币安历史费率已流过 {flowed_count} 个合约（阈值 {threshold}）")
                        
                        # ==================== 添加到水 ====================
                        water.append({
                            'exchange': exchange,
                            'symbol': symbol,
                            'data_type': data_type,
                            'data': data,
                            'store_timestamp': data.get('store_timestamp', datetime.now().isoformat())
                        })
        
        return water
    
    # ==================== 数据接收接口 ====================
    
    async def update_market_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """接收市场数据"""
        async with self.locks['market_data']:
            if exchange not in self.market_data:
                self.market_data[exchange] = defaultdict(dict)
            
            data_type = data.get("data_type", "unknown")
            
            # 存储数据（新数据覆盖旧数据）
            self.market_data[exchange][symbol][data_type] = {
                **data,
                'store_timestamp': datetime.now().isoformat(),
                'source': 'websocket'
            }
            
            # 存储最新引用
            self.market_data[exchange][symbol]['latest'] = data_type
            
            # 调试日志
            if data_type in ['funding_rate', 'mark_price'] and exchange == "binance":
                rate = data.get('funding_rate', 0)
                if isinstance(rate, (int, float)):
                    logger.debug(f"[DataStore] 存储 {exchange} {symbol} {data_type} = {rate:.6f}")
    
    async def update_account_data(self, exchange: str, data: Dict[str, Any]):
        """接收账户数据"""
        async with self.locks['account_data']:
            self.account_data[exchange] = {
                **data,
                'timestamp': datetime.now().isoformat()
            }
    
    async def update_order_data(self, exchange: str, order_id: str, data: Dict[str, Any]):
        """接收订单数据"""
        async with self.locks['order_data']:
            if exchange not in self.order_data:
                self.order_data[exchange] = {}
            self.order_data[exchange][order_id] = {
                **data,
                'update_time': datetime.now().isoformat()
            }
    
    async def update_connection_status(self, exchange: str, connection_type: str, status: Dict[str, Any]):
        """更新连接状态"""
        async with self.locks['connection_status']:
            if exchange not in self.connection_status:
                self.connection_status[exchange] = {}
            self.connection_status[exchange][connection_type] = {
                **status,
                'timestamp': datetime.now().isoformat()
            }
    
    # ==================== 数据查询接口（兼容原有系统） ====================
    
    async def get_market_data(self, exchange: str, symbol: str = None, 
                             data_type: str = None, get_latest: bool = False) -> Dict[str, Any]:
        """获取市场数据（兼容原有接口）"""
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
    
    async def get_account_data(self, exchange: str = None) -> Dict[str, Any]:
        """获取账户数据"""
        async with self.locks['account_data']:
            if exchange:
                return self.account_data.get(exchange, {}).copy()
            return self.account_data.copy()
    
    async def get_order_data(self, exchange: str = None) -> Dict[str, Any]:
        """获取订单数据"""
        async with self.locks['order_data']:
            if exchange:
                return self.order_data.get(exchange, {}).copy()
            return self.order_data.copy()
    
    async def get_connection_status(self, exchange: str = None) -> Dict[str, Any]:
        """获取连接状态"""
        async with self.locks['connection_status']:
            if exchange:
                return self.connection_status.get(exchange, {}).copy()
            return self.connection_status.copy()
    
    def get_market_data_stats(self) -> Dict[str, Any]:
        """获取统计数据（兼容原有接口）"""
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
    
    # ==================== 状态查询 ====================
    
    async def get_execution_status(self) -> Dict[str, Any]:
        """获取规则执行状态"""
        async with self.locks['execution_records']:
            records = self.execution_records.copy()
            # 转换set为list以便序列化
            records["binance_history"]["flowed_contracts"] = list(
                records["binance_history"]["flowed_contracts"]
            )
        
        return {
            "flowing": self.flowing,
            "has_rules": self.rules is not None,
            "execution_records": records,
            "data_stats": self._get_data_stats(),
            "timestamp": datetime.now().isoformat()
        }
    
    def _get_data_stats(self) -> Dict[str, Any]:
        """获取数据统计"""
        stats = {
            "binance_symbols": len(self.market_data.get("binance", {})),
            "okx_symbols": len(self.market_data.get("okx", {})),
            "binance_data_types": defaultdict(int),
            "okx_data_types": defaultdict(int)
        }
        
        for exchange in ["binance", "okx"]:
            for symbol_data in self.market_data.get(exchange, {}).values():
                for data_type in symbol_data.keys():
                    if data_type not in ['latest', 'store_timestamp']:
                        stats[f"{exchange}_data_types"][data_type] += 1
        
        return stats
    
    async def force_one_flow(self):
        """强制放水一次（测试用）"""
        if not self.flowing:
            logger.warning("⚠️ 放水系统未启动")
            return
        
        water = await self._collect_water_by_rules()
        if water and self.water_callback:
            await self.water_callback(water)
            logger.info(f"⚡ 强制放水完成: {len(water)} 条数据")
    
    async def clear_market_data(self, exchange: str = None):
        """
        清空市场数据（谨慎使用）
        """
        async with self.locks['market_data']:
            if exchange:
                if exchange in self.market_data:
                    self.market_data[exchange].clear()
                    logger.warning(f"⚠️ 已清空 {exchange} 市场数据")
            else:
                self.market_data["binance"].clear()
                self.market_data["okx"].clear()
                logger.warning("⚠️ 已清空所有市场数据")
    
    async def health_check(self) -> Dict[str, Any]:
        """
        健康检查
        """
        stats = self.get_market_data_stats()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "market_data": {
                "total_symbols": stats["total_symbols"],
                "total_data_types": stats["total_data_types"],
                "exchanges": list(stats["exchanges"].keys())
            },
            "account_data": {
                "exchanges": list(self.account_data.keys())
            },
            "order_data": {
                "exchanges": list(self.order_data.keys())
            },
            "http_server_ready": self._http_server_ready,
            "flowing": self.flowing
        }

# 全局实例
data_store = DataStore()