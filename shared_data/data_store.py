"""
DataStore - 执行者/执法者
功能：1. 接收管理员规则 2. 按规则放水 3. 自动执行
"""

import asyncio
import time
import traceback
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
        
        # 币安历史费率数据控制器
        self._binance_funding_controller = {
            "enabled": True,
            "total_contracts": 0,
            "flowed_contracts": set(),
            "init_done": False
        }
        
        logger.info("✅【数据池】初始化完成")
        logger.info(f"🔍【数据池】初始化状态: 市场回调={self.water_callback}, 私人回调={self.private_water_callback}")
    
    # ==================== 管道设置方法 ====================
    
    def set_water_callback(self, callback: Callable):
        """设置市场数据回调"""
        self.water_callback = callback
        logger.info("✅【数据池】市场数据管道已连接")
        logger.info(f"✅【数据池】回调函数对象: {callback}")
        logger.info(f"✅【数据池】回调函数类型: {type(callback)}")
        logger.info(f"✅【数据池】是否可调用: {callable(callback)}")
    
    def set_private_water_callback(self, callback: Callable):
        """设置私人数据回调"""
        self.private_water_callback = callback
        logger.info("✅【数据池】私人数据管道已连接")
    
    def set_private_flowing(self, flowing: bool):
        """设置私人数据管道开关"""
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
            logger.debug(f"规则内容: {rules}")
    
    async def receive_rule_update(self, rule_key: str, rule_value: Any):
        """接收规则更新"""
        async with self.rule_lock:
            if self.rules and rule_key in self.rules:
                self.rules[rule_key] = rule_value
                logger.info(f"📋【数据池】规则已更新: {rule_key}")
    
    # ==================== 市场数据放水系统 ====================
    
    async def start_flowing(self, water_callback: Callable = None):
        """开始按规则放水"""
        if water_callback:
            self.water_callback = water_callback
            
        if self.flowing:
            logger.warning("⚠️【数据池】已经在放水中")
            return
        
        if not self.rules:
            logger.error("❌【数据池】没有接收到规则，无法开始放水")
            return
        
        # 强制检查回调
        if not self.water_callback:
            logger.error("❌【数据池】致命错误：water_callback 未设置！")
            return
        
        if not callable(self.water_callback):
            logger.error(f"❌【数据池】致命错误：water_callback 不可调用！类型: {type(self.water_callback)}")
            return
        
        self.flowing = True
        logger.info("🚰【数据池】开始按规则放水...")
        logger.info(f"🚰【数据池】回调函数确认: {self.water_callback}")
        
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
        """放水循环 - 终极调试版"""
        while self.flowing:
            try:
                # 检查规则
                if not self.rules or not self.rules.get("flow", {}).get("enabled", False):
                    logger.debug("⏳【数据池】放水规则未启用，等待1秒")
                    await asyncio.sleep(1)
                    continue
                
                # 强制检查回调
                if self.water_callback is None:
                    logger.error("🚨【数据池】致命错误：water_callback 为 None！放水系统停止！")
                    self.flowing = False
                    break
                
                if not callable(self.water_callback):
                    logger.error(f"🚨【数据池】致命错误：water_callback 不可调用！类型: {type(self.water_callback)}")
                    self.flowing = False
                    break
                
                # 收集数据
                water = await self._collect_water_by_rules()
                logger.debug(f"💧【数据池】本次收集到 {len(water)} 条数据")
                
                # 放水
                if water:
                    logger.info(f"🌊【数据池】正在放水！条数: {len(water)}")
                    logger.debug(f"🌊【数据池】回调函数: {self.water_callback}")
                    
                    try:
                        await self.water_callback(water)
                        logger.info("✅【数据池】回调执行成功")
                        
                        # 记录
                        async with self.locks['execution_records']:
                            self.execution_records["total_flows"] += 1
                            self.execution_records["last_flow_time"] = time.time()
                    except Exception as e:
                        logger.error(f"❌【数据池】回调执行失败: {e}")
                        logger.error(traceback.format_exc())
                else:
                    logger.debug("⏳【数据池】本次无数据可放")
                
                # 等待间隔
                interval = self.rules.get("flow", {}).get("interval_seconds", 5)
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                logger.info("🛑【数据池】放水循环被取消")
                break
            except Exception as e:
                logger.error(f"❌【数据池】放水循环错误: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    async def _collect_water_by_rules(self) -> List[Dict[str, Any]]:
        """按规则收集水 - 修复版"""
        if not self.rules:
            logger.warning("⚠️【数据池】无规则，无法收集水")
            return []
        
        water = []
        controller = self._binance_funding_controller
        
        async with self.locks['market_data']:
            # ===== 币安费率数据预处理 =====
            funding_ready = False
            if controller["enabled"]:
                # 探测是否有费率数据
                for symbol, data_dict in self.market_data.get("binance", {}).items():
                    if "funding_settlement" in data_dict:
                        funding_ready = True
                        break
                
                # 统计合约数（仅一次）
                if funding_ready and not controller["init_done"]:
                    valid_symbols = {
                        sym for sym, data_dict in self.market_data["binance"].items()
                        if "funding_settlement" in data_dict
                    }
                    controller["total_contracts"] = len(valid_symbols)
                    controller["init_done"] = True
                    logger.info(f"📊【数据池】统计到币安历史费率合约数: {len(valid_symbols)}")
            
            # ===== 统一收集所有数据 =====
            for exchange in ["binance", "okx"]:
                if exchange not in self.market_data:
                    continue
                
                for symbol, data_dict in self.market_data[exchange].items():
                    for data_type, data in data_dict.items():
                        # 跳过内部字段
                        if data_type in ['latest', 'store_timestamp']:
                            continue
                        
                        # 币安费率数据特殊处理
                        is_funding = (exchange == "binance" and data_type == "funding_settlement")
                        
                        if is_funding:
                            if not controller["enabled"]:
                                continue
                            if symbol in controller["flowed_contracts"]:
                                continue
                            
                            controller["flowed_contracts"].add(symbol)
                            flowed = len(controller["flowed_contracts"])
                            total = controller["total_contracts"]
                            logger.info(f"📤【数据池】币安费率数据流出: {symbol} ({flowed}/{total})")
                        
                        # ===== 核心修复：确保在循环内执行 =====
                        try:
                            water_item = {
                                'exchange': exchange,
                                'symbol': symbol,
                                'data_type': data_type,
                                'data': data,
                                'timestamp': data.get('timestamp'),
                                'priority': 5
                            }
                            water.append(water_item)
                        except Exception as e:
                            logger.error(f"❌【数据池】创建水项失败: {e}, exchange={exchange}, symbol={symbol}, data_type={data_type}")
            
            # ===== 检查是否全部流出 =====
            if controller["enabled"] and controller["init_done"]:
                if len(controller["flowed_contracts"]) >= controller["total_contracts"]:
                    controller["enabled"] = False
                    logger.info(f"🛑【数据池】币安费率数据全部流出，关闭控制器")
        
        logger.debug(f"💧【数据池】收集完成，共 {len(water)} 条数据")
        return water
    
    # ==================== 数据接收接口 ====================
    
    async def update_market_data(self, exchange: str, symbol: str, data: Dict[str, Any]):
        """接收市场数据"""
        async with self.locks['market_data']:
            if exchange not in self.market_data:
                self.market_data[exchange] = defaultdict(dict)
            
            data_type = data.get("data_type", "unknown")
            source = data.get("source", "websocket")
            
            self.market_data[exchange][symbol][data_type] = {
                **data,
                'store_timestamp': datetime.now().isoformat(),
                'source': source
            }
            self.market_data[exchange][symbol]['latest'] = data_type
            
            if exchange == "binance" and data_type == "funding_settlement":
                logger.info(f"📥【数据池】收到币安历史费率数据: {symbol}")
    
    async def update_account_data(self, exchange: str, data: Dict[str, Any]):
        """接收账户数据（立即自动流出）"""
        async with self.locks['account_data']:
            self.account_data[exchange] = {
                **data,
                'timestamp': datetime.now().isoformat()
            }
        
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
                
                async with self.locks['execution_records']:
                    self.execution_records["private_flows"]["account_updates"] += 1
                    self.execution_records["private_flows"]["last_account_update"] = time.time()
            except Exception as e:
                logger.error(f"❌【数据池】私人数据(账户)流出失败: {e}")
    
    async def update_order_data(self, exchange: str, order_id: str, data: Dict[str, Any]):
        """接收交易数据（立即自动流出）"""
        async with self.locks['order_data']:
            if exchange not in self.order_data:
                self.order_data[exchange] = {}
            self.order_data[exchange][order_id] = {
                **data,
                'update_time': datetime.now().isoformat()
            }
        
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
            "binance_funding_controller": self._get_binance_funding_stats(),
            "timestamp": datetime.now().isoformat()
        }
    
    def _get_binance_funding_stats(self) -> Dict[str, Any]:
        """获取币安费率数据状态"""
        controller = self._binance_funding_controller
        funding_contracts = [
            sym for sym, data_dict in self.market_data.get("binance", {}).items()
            if "funding_settlement" in data_dict
        ]
        
        return {
            "enabled": controller["enabled"],
            "total_contracts": controller["total_contracts"],
            "current_actual_contracts": len(funding_contracts),
            "flowed_count": len(controller["flowed_contracts"]),
            "init_done": controller["init_done"],
            "remaining": max(0, controller["total_contracts"] - len(controller["flowed_contracts"])),
            "contracts_list": funding_contracts[:10],
            "flowed_contracts_list": list(controller["flowed_contracts"])[:10]
        }
    
    async def get_binance_funding_status(self) -> Dict[str, Any]:
        """获取币安费率数据流出状态"""
        return self._get_binance_funding_stats()
    
    async def reset_binance_funding_controller(self):
        """重置币安费率数据流出控制器"""
        self._binance_funding_controller = {
            "enabled": True,
            "total_contracts": 0,
            "flowed_contracts": set(),
            "init_done": False
        }
        logger.info("🔄【数据池】重置币安费率数据流出控制器")
    
    async def force_one_flow(self):
        """强制放水一次（测试用）"""
        if not self.flowing:
            logger.warning("⚠️【数据池】放水系统未启动")
            return
        
        water = await self._collect_water_by_rules()
        logger.info(f"🧪【数据池】强制放水，收集到 {len(water)} 条数据")
        if water and self.water_callback:
            await self.water_callback(water)
    
    async def clear_market_data(self, exchange: str = None):
        """清空市场数据（谨慎使用）"""
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
        """健康检查"""
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
            "binance_funding_controller": {
                "enabled": self._binance_funding_controller["enabled"],
                "progress": f"{len(self._binance_funding_controller['flowed_contracts'])}/{self._binance_funding_controller['total_contracts']}",
                "remaining": max(0, self._binance_funding_controller["total_contracts"] - len(self._binance_funding_controller["flowed_contracts"]))
            }
        }


# 全局实例
data_store = DataStore()
