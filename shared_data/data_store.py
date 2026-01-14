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
        
        # 私人数据管道
        self.private_water_callback = None
        self.private_flowing = True
        
        # 规则执行记录
        self.execution_records = {
            "total_flows": 0,
            "last_flow_time": 0,
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
        
        # ✅ 币安历史费率数据控制器（新增，仅3个字段）
        self._binance_funding_controller = {
            "enabled": True,           # 第1步：总开关，默认开启
            "init_done": False,        # 第4步：标记是否已统计合约数（只统计1次）
            "flowed_contracts": set()  # 第5步：记录已流出合约（每个只流1次）
        }
        
        logger.info("【数据池】初始化完成")
    
    # ==================== 管道设置方法 ====================
    
    def set_water_callback(self, callback: Callable):
        """设置市场数据回调"""
        self.water_callback = callback
        logger.info("【数据池】市场数据管道已连接")
    
    def set_private_water_callback(self, callback: Callable):
        """设置私人数据回调"""
        self.private_water_callback = callback
        logger.info("【数据池】私人数据管道已连接")
    
    def set_private_flowing(self, flowing: bool):
        """设置私人数据管道开关"""
        self.private_flowing = flowing
        status = "开启" if flowing else "关闭"
        logger.info(f"【数据池】私人数据管道{status}")
    
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
            logger.info("【数据池】已接收管理员规则")
    
    async def receive_rule_update(self, rule_key: str, rule_value: Any):
        """接收规则更新"""
        async with self.rule_lock:
            if self.rules and rule_key in self.rules:
                self.rules[rule_key] = rule_value
                logger.info(f"【数据池】规则已更新: {rule_key}")
    
    # ==================== 市场数据放水系统 ====================
    
    async def start_flowing(self, water_callback: Callable = None):
        """开始按规则放水"""
        if water_callback:
            self.water_callback = water_callback
            
        if self.flowing:
            logger.warning("【数据池】已经在放水中")
            return
        
        if not self.rules:
            logger.error("【数据池】没有接收到规则，无法开始放水")
            return
        
        self.flowing = True
        logger.info("【数据池】开始按规则放水...")
        
        # 启动放水任务
        self.flow_task = asyncio.create_task(self._flow_loop())
    
    async def stop_flowing(self):
        """停止放水"""
        if not self.flowing:
            return
        
        logger.info("【数据池】停止放水...")
        self.flowing = False
        
        if self.flow_task:
            self.flow_task.cancel()
            try:
                await self.flow_task
            except asyncio.CancelledError:
                pass
        
        logger.info("【数据池】放水已停止")
    
    async def _flow_loop(self):
        """放水循环 - 强制验证版"""
        while self.flowing:
            try:
                if not self.rules["flow"]["enabled"]:
                    await asyncio.sleep(1)
                    continue
                
                water = await self._collect_water_by_rules()
                
                # 强制验证1：water列表内容
                logger.info(f"【数据池】收集完成，准备调用回调，water条数: {len(water)}")
                funding_count = sum(1 for item in water if item.get('exchange') == 'binance' and item.get('data_type') == 'funding_settlement')
                logger.info(f"【数据池】其中币安费率数据条数: {funding_count}")
                
                if water:
                    logger.info(f"【数据池】water第1条样本: {water[0]}")
                    logger.info(f"【数据池】water最后1条样本: {water[-1]}")
                else:
                    logger.debug("【数据池】water为空，跳过回调")
                    await asyncio.sleep(self.rules["flow"]["interval_seconds"])
                    continue
                
                # 强制验证2：回调函数
                if not self.water_callback:
                    logger.error("【数据池】致命错误：water_callback 为 None！")
                    self.flowing = False
                    break
                
                if not callable(self.water_callback):
                    logger.error(f"【数据池】致命错误：water_callback 不可调用！类型: {type(self.water_callback)}")
                    self.flowing = False
                    break
                
                # 执行回调
                logger.info(f"【数据池】正在调用回调函数...")
                await self.water_callback(water)
                logger.info("【数据池】回调执行完成")
                
                # 记录
                async with self.locks['execution_records']:
                    self.execution_records["total_flows"] += 1
                    self.execution_records["last_flow_time"] = time.time()
                
                await asyncio.sleep(self.rules["flow"]["interval_seconds"])
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"【数据池】放水循环错误: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    async def _collect_water_by_rules(self) -> List[Dict[str, Any]]:
        """按规则收集水 - 完美7步方案"""
        if not self.rules:
            return []
        
        water = []
        controller = self._binance_funding_controller
        
        async with self.locks['market_data']:
            # ===== 第1步：检查总开关状态 =====
            if not controller["enabled"]:
                logger.debug("【数据池】闸门已关闭，跳过费率逻辑")
            
            # ===== 第2-4步：统计合约数（只执行1次）=====
            if controller["enabled"] and not controller["init_done"]:
                # 检查是否有费率数据
                funding_symbols = [
                    sym for sym, data_dict in self.market_data.get("binance", {}).items()
                    if "funding_settlement" in data_dict
                ]
                logger.info(f"【数据池】扫描费率合约: {len(funding_symbols)}个")
                
                if funding_symbols:
                    logger.info(f"【数据池】费率合约示例: {funding_symbols[:5]}")
                    controller["init_done"] = True
                else:
                    logger.debug("【数据池】等待币安历史费率数据...")
            
            # ===== 第5步：收集所有数据（闸门只在此处拦截）=====
            for exchange in ["binance", "okx"]:
                if exchange not in self.market_data:
                    continue
                
                for symbol, data_dict in self.market_data[exchange].items():
                    for data_type, data in data_dict.items():
                        if data_type in ['latest', 'store_timestamp']:
                            continue
                        
                        # ✅ 闸门逻辑：只针对币安费率数据
                        is_funding = (exchange == "binance" and data_type == "funding_settlement")
                        
                        if is_funding:
                            # 开关关闭 → 跳过
                            if not controller["enabled"]:
                                continue
                            # 已流出 → 跳过
                            if symbol in controller["flowed_contracts"]:
                                continue
                            # 第一次流出，标记
                            controller["flowed_contracts"].add(symbol)
                            logger.info(f"【数据池】币安费率数据流出: {symbol} (累计: {len(controller['flowed_contracts'])}个)")
                        
                        # ✅ 所有数据（包括第一次的费率数据）都添加到water
                        water_item = {
                            'exchange': exchange,
                            'symbol': symbol,
                            'data_type': data_type,
                            'data': data,
                            'timestamp': data.get('timestamp'),
                            'priority': 5
                        }
                        water.append(water_item)
            
            # ===== 第6-7步：关闭闸门 =====
            if controller["enabled"] and controller["init_done"]:
                total_symbols = len([
                    sym for sym, data_dict in self.market_data["binance"].items()
                    if "funding_settlement" in data_dict
                ])
                flowed_count = len(controller["flowed_contracts"])
                
                logger.info(f"【数据池】流出进度: {flowed_count}/{total_symbols}")
                
                if flowed_count >= total_symbols and total_symbols > 0:
                    controller["enabled"] = False
                    logger.info(f"【数据池】币安费率数据全部流出，闸门关闭")
        
        logger.info(f"【数据池】收集完成，返回water列表，共{len(water)}条")
        return water
    
    # ==================== 数据接收接口 ====================
    
    async def update_market_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """接收市场数据"""
        async with self.locks['market_data']:
            if exchange not in self.market_data:
                self.market_data[exchange] = defaultdict(dict)
            
            data_type = data.get("data_type", "unknown")
            source = data.get("source", "websocket")
            
            # 存储数据
            self.market_data[exchange][symbol][data_type] = {
                **data,
                'store_timestamp': datetime.now().isoformat(),
                'source': source
            }
            
            # 存储最新引用
            self.market_data[exchange][symbol]['latest'] = data_type
            
            # ✅ 记录币安费率数据接收
            if exchange == "binance" and data_type == "funding_settlement":
                logger.info(f"【数据池】收到币安历史费率数据: {symbol}")
    
    async def update_account_data(self, exchange: str, data: Dict[str, Any]):
        """接收账户数据（立即自动流出）"""
        async with self.locks['account_data']:
            self.account_data[exchange] = {
                **data,
                'timestamp': datetime.now().isoformat()
            }
        
        # 立即从私人管道流出
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
                logger.error(f"【数据池】私人数据(账户)流出失败: {e}")
    
    async def update_order_data(self, exchange: str, order_id: str, data: Dict[str, Any]):
        """接收交易数据（立即自动流出）"""
        async with self.locks['order_data']:
            if exchange not in self.order_data:
                self.order_data[exchange] = {}
            self.order_data[exchange][order_id] = {
                **data,
                'update_time': datetime.now().isoformat()
            }
        
        # 立即从私人管道流出
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
                logger.error(f"【数据池】私人数据(交易)流出失败: {e}")
    
    async def update_connection_status(self, exchange: str, connection_type: str, status: Dict[str, Any]):
        """更新连接状态"""
        async with self.locks['connection_status']:
            if exchange not in self.connection_status:
                self.connection_status[exchange] = {}
            self.connection_status[exchange][connection_type] = {
                **status,
                'timestamp': datetime.now().isoformat()
            }
    
    # ==================== 数据查询接口 ====================
    
    async def get_market_data(self, exchange: str, symbol: str = None, 
                             data_type: str = None, get_latest: bool = False) -> Dict[str, Any]:
        """获取市场数据"""
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
    
    # ==================== 状态查询 ====================
    
    async def get_execution_status(self) -> Dict[str, Any]:
        """获取规则执行状态"""
        async with self.locks['execution_records']:
            records = self.execution_records.copy()
        
        return {
            "flowing": self.flowing,
            "has_rules": self.rules is not None,
            "execution_records": records,
            "private_pipeline": {
                "connected": self.private_water_callback is not None,
                "flowing": self.private_flowing,
                "stats": records["private_flows"]
            },
            "binance_funding_controller": {
                "enabled": self._binance_funding_controller["enabled"],
                "init_done": self._binance_funding_controller["init_done"],
                "flowed_count": len(self._binance_funding_controller["flowed_contracts"])
            },
            "timestamp": datetime.now().isoformat()
        }


# 全局实例
data_store = DataStore()
