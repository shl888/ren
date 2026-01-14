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
        
        # 市场数据执行状态
        self.flowing = False
        self.flow_task = None
        self.water_callback = None
        
        # ✅ 新增：私人数据管道
        self.private_water_callback = None  # 私人数据→管理员
        self.private_flowing = True         # 私人管道默认常开
        
        # 规则执行记录
        self.execution_records = {
            "total_flows": 0,                   # 总共放水次数
            "last_flow_time": 0,
            # ✅ 新增：私人数据执行记录
            "private_flows": {
                "account_updates": 0,
                "order_updates": 0,
                "last_account_update": 0,
                "last_order_update": 0
            }
        }
        
        # 数据锁
        self.locks = {
            'market_data': asyncio.Lock(),
            'account_data': asyncio.Lock(),
            'order_data': asyncio.Lock(),
            'connection_status': asyncio.Lock(),
            'execution_records': asyncio.Lock(),
        }
        
        logger.info("✅【数据池】初始化完成")
    
    # ==================== 管道设置方法 ====================
    
    def set_water_callback(self, callback: Callable):
        """设置市场数据回调"""
        self.water_callback = callback
    
    def set_private_water_callback(self, callback: Callable):
        """✅ 新增：设置私人数据回调"""
        self.private_water_callback = callback
        logger.info("✅【数据池】私人数据管道已连接")
    
    def set_private_flowing(self, flowing: bool):
        """✅ 新增：设置私人数据管道开关"""
        self.private_flowing = flowing
        status = "开启" if flowing else "关闭"
        logger.info(f"✅【数据池】私人数据管道{status}")
    
    # ==================== HTTP服务相关方法 ====================
    
    def set_http_server_ready(self, ready: bool):
        """设置HTTP服务就绪状态"""
        self._http_server_ready = ready
    
    def is_http_server_ready(self) -> bool:
        """检查HTTP服务是否就绪"""
        return self._http_server_ready
    
    # ==================== 接收规则 ====================
    
    async def receive_rules(self, rules: Dict[str, Any]):
        """接收管理员规则"""
        async with self.rule_lock:
            self.rules = rules
            logger.info("📋【数据池】已接收管理员规则")
    
    async def receive_rule_update(self, rule_key: str, rule_value: Any):
        """接收规则更新"""
        async with self.rule_lock:
            if self.rules and rule_key in self.rules:
                self.rules[rule_key] = rule_value
    
    # ==================== 市场数据放水系统 ====================
    
    async def start_flowing(self, water_callback: Callable = None):
        """
        开始按规则放水
        water_callback: 放水回调函数，水放给流水线
        """
        if water_callback:
            self.water_callback = water_callback
            
        if self.flowing:
            logger.warning("⚠️【数据池】已经在放水中")
            return
        
        if not self.rules:
            logger.error("❌【数据池】没有接收到规则，无法开始放水")
            return
        
        self.flowing = True
        
        logger.info("🚰【数据池】开始按规则放水...")
        
        # 启动放水任务
        self.flow_task = asyncio.create_task(self._flow_loop())
    
    async def stop_flowing(self):
        """停止放水"""
        if not self.flowing:
            return
        
        logger.info("🛑【数据池】停止放水...")
        self.flowing = False
        
        if self.flow_task:
            self.flow_task.cancel()
            try:
                await self.flow_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅【数据池】放水已停止")
    
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
                logger.error(f"❌【数据池】放水循环错误: {e}")
                await asyncio.sleep(5)
    
    async def _collect_water_by_rules(self) -> List[Dict[str, Any]]:
        """按规则收集水 - 统一化处理所有数据类型"""
        if not self.rules:
            return []
        
        water = []
        
        async with self.locks['market_data']:
            # ==================== 币安历史费率数据控制逻辑 ====================
            BINANCE_FUNDING_SETTLEMENT = "funding_settlement"
            
            # 第一步：全局开关检查
            if getattr(self, '_binance_funding_settlement_closed', False):
                # 已关闭，跳过所有币安历史费率数据处理
                pass
            else:
                # 初始化已流过合约集合（只在第一次运行时）
                if not hasattr(self, '_binance_funding_settlement_flowed'):
                    self._binance_funding_settlement_flowed = set()
            
            # ==================== 遍历所有数据 ====================
            for exchange in ["binance", "okx"]:
                if exchange not in self.market_data:
                    continue
                
                for symbol, data_dict in self.market_data[exchange].items():
                    for data_type, data in data_dict.items():
                        # 跳过内部字段
                        if data_type in ['latest', 'store_timestamp']:
                            continue
                        
                        # ==================== 针对币安历史费率的特殊处理 ====================
                        if exchange == "binance" and data_type == BINANCE_FUNDING_SETTLEMENT:
                            # 1. 检查全局开关是否已关闭
                            if getattr(self, '_binance_funding_settlement_closed', False):
                                continue
                            
                            # 2. 检查该合约是否已流过
                            if symbol in getattr(self, '_binance_funding_settlement_flowed', set()):
                                continue
                            
                            # 🔴 关键修正：只在首次遇到数据时才统计总数
                            if not hasattr(self, '_binance_funding_settlement_total'):
                                # 首次遇到funding_settlement数据，统计所有合约
                                total_symbols = set()
                                for sym, sym_dict in self.market_data.get("binance", {}).items():
                                    if BINANCE_FUNDING_SETTLEMENT in sym_dict:
                                        total_symbols.add(sym)
                                
                                if total_symbols:
                                    self._binance_funding_settlement_total = len(total_symbols)
                                    logger.info(f"📊【数据池】首次检测到币安历史费率数据: {len(total_symbols)}个合约")
                                else:
                                    # 理论上不会走到这里，因为当前就是funding_settlement数据
                                    # 但为了安全起见，先标记当前合约，不统计总数
                                    self._binance_funding_settlement_flowed.add(symbol)
                                    continue
                            
                            # 3. 标记当前合约为已流过
                            self._binance_funding_settlement_flowed.add(symbol)
                            
                            # 4. 检查是否所有合约都已流过
                            flowed_count = len(self._binance_funding_settlement_flowed)
                            total_count = self._binance_funding_settlement_total
                            
                            if flowed_count >= total_count:
                                # 所有合约都已流过，永久关闭该类型数据流
                                self._binance_funding_settlement_closed = True
                                logger.info(f"🛑【数据池】币安历史费率数据已完成全量流入({flowed_count}/{total_count}个合约)，永久关闭该类型数据流")
                        
                        # ✅ 关键修改：直接传数据，不包装！
                        water_item = {
                            'exchange': exchange,
                            'symbol': symbol,
                            'data_type': data_type,
                            'data': data,  # ⚠️ 直接传数据，不包装！
                            'timestamp': data.get('timestamp'),
                            'priority': 5
                        }
                        
                        water.append(water_item)
        
        return water
    
    # ==================== 数据接收接口 ====================
    
    async def update_market_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """接收市场数据"""
        async with self.locks['market_data']:
            if exchange not in self.market_data:
                self.market_data[exchange] = defaultdict(dict)
            
            data_type = data.get("data_type", "unknown")
            
            # ✅ 使用传入的source，如果没有则默认websocket
            source = data.get("source", "websocket")
            
            # 存储数据
            self.market_data[exchange][symbol][data_type] = {
                **data,
                'store_timestamp': datetime.now().isoformat(),
                'source': source  # ✅ 保留传入的source
            }
            
            # 存储最新引用
            self.market_data[exchange][symbol]['latest'] = data_type
    
    async def update_account_data(self, exchange: str, data: Dict[str, Any]):
        """✅ 增强：接收账户数据（立即自动流出）"""
        async with self.locks['account_data']:
            self.account_data[exchange] = {
                **data,
                'timestamp': datetime.now().isoformat()
            }
        
        # ✅ 新增：立即从私人管道流出！
        if self.private_water_callback and self.private_flowing:
            try:
                private_data = {
                    'data_type': 'account_update',
                    'exchange': exchange,
                    'data': data,
                    'timestamp': datetime.now().isoformat(),
                    'flow_type': 'private_immediate'
                }
                
                await self.private_water_callback(private_data)
                
                # 记录
                async with self.locks['execution_records']:
                    self.execution_records["private_flows"]["account_updates"] += 1
                    self.execution_records["private_flows"]["last_account_update"] = time.time()
                    
            except Exception as e:
                logger.error(f"❌【数据池】私人数据(账户)流出失败: {e}")
    
    async def update_order_data(self, exchange: str, order_id: str, data: Dict[str, Any]):
        """✅ 增强：接收交易数据（立即自动流出）"""
        async with self.locks['order_data']:
            if exchange not in self.order_data:
                self.order_data[exchange] = {}
            self.order_data[exchange][order_id] = {
                **data,
                'update_time': datetime.now().isoformat()
            }
        
        # ✅ 新增：立即从私人管道流出！
        if self.private_water_callback and self.private_flowing:
            try:
                private_data = {
                    'data_type': 'order_update',
                    'exchange': exchange,
                    'order_id': order_id,
                    'data': data,
                    'timestamp': datetime.now().isoformat(),
                    'flow_type': 'private_immediate'
                }
                
                await self.private_water_callback(private_data)
                
                # 记录
                async with self.locks['execution_records']:
                    self.execution_records["private_flows"]["order_updates"] += 1
                    self.execution_records["private_flows"]["last_order_update"] = time.time()
                    
            except Exception as e:
                logger.error(f"❌【数据池】私人数据(交易)流出失败: {e}")
    
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
        """获取交易数据"""
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
        """✅ 增强：获取规则执行状态"""
        async with self.locks['execution_records']:
            records = self.execution_records.copy()
        
        # 添加币安历史费率控制状态
        binance_funding_settlement_status = {
            "closed": getattr(self, '_binance_funding_settlement_closed', False),
            "total_contracts": getattr(self, '_binance_funding_settlement_total', 0),
            "flowed_contracts": len(getattr(self, '_binance_funding_settlement_flowed', set())),
            "flowed_contracts_list": sorted(list(getattr(self, '_binance_funding_settlement_flowed', set()))),
            "data_type": "funding_settlement"
        }
        
        return {
            "flowing": self.flowing,
            "has_rules": self.rules is not None,
            "execution_records": records,
            "private_pipeline": {
                "connected": self.private_water_callback is not None,
                "flowing": self.private_flowing,
                "stats": records["private_flows"]
            },
            "binance_funding_settlement_control": binance_funding_settlement_status,
            "timestamp": datetime.now().isoformat()
        }
    
    async def force_one_flow(self):
        """强制放水一次（测试用）"""
        if not self.flowing:
            logger.warning("⚠️【数据池】放水系统未启动")
            return
        
        water = await self._collect_water_by_rules()
        if water and self.water_callback:
            await self.water_callback(water)
    
    async def clear_market_data(self, exchange: str = None):
        """
        清空市场数据（谨慎使用）
        """
        async with self.locks['market_data']:
            if exchange:
                if exchange in self.market_data:
                    self.market_data[exchange].clear()
                    logger.warning(f"⚠️【数据池】已清空 {exchange} 市场数据")
            else:
                self.market_data["binance"].clear()
                self.market_data["okx"].clear()
                logger.warning("⚠️【数据池】已清空所有市场数据")
    
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
            "flowing": self.flowing,
            "private_pipeline": {
                "connected": self.private_water_callback is not None,
                "flowing": self.private_flowing
            },
            "binance_funding_settlement_control": {
                "closed": getattr(self, '_binance_funding_settlement_closed', False),
                "total_contracts": getattr(self, '_binance_funding_settlement_total', 0),
                "flowed_contracts": len(getattr(self, '_binance_funding_settlement_flowed', set()))
            }
        }

# 全局实例
data_store = DataStore()