"""
私人数据处理器 - 最简版本
只接收、存储、查看私人数据
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

from .binance_classifier import classify_binance_order, is_closing_event


class PrivateDataProcessor:
    """私人数据处理器（单例模式）"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            # 🔴 仿制大脑的存储结构
            self.memory_store = {'private_data': {}}
            self._initialized = True
            logger.info("✅ [私人数据处理] 模块已初始化")
    
    async def _delayed_delete(self, keys: List[str], symbol: str):
        """5分钟后删除该symbol所有当前存在的key（包括后来新增的）"""
        try:
            await asyncio.sleep(300)  # 5分钟 = 300秒
            
            # 检查并获取分类存储
            if 'binance_order_update' not in self.memory_store['private_data']:
                return
                
            classified = self.memory_store['private_data']['binance_order_update'].get('classified', {})
            
            # 🔴 重新获取该symbol当前的所有key（包括5分钟内新增的过期数据）
            current_keys = [k for k in classified.keys() if k.startswith(f"{symbol}_")]
            
            for k in current_keys:
                del classified[k]
            
            if current_keys:
                logger.info(f"🧹 [币安订单] 延迟清理完成: {symbol} 已删除 {len(current_keys)}类")
            else:
                logger.debug(f"⏭️ [币安订单] 延迟清理: {symbol} 已无数据可删")
                
        except Exception as e:
            logger.error(f"❌ [币安订单] 延迟清理失败: {e}")
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据（仿制大脑接口）
        格式：{'exchange': 'binance', 'data_type': 'account_update', 'data': {...}, 'timestamp': '...'}
        """
        try:
            exchange = private_data.get('exchange', 'unknown')
            raw_data = private_data.get('data', {})
            source = private_data.get('source', '')
            
            # 🔴 === 币安订单更新专用处理（分类+去重+覆盖+清理）===
            if exchange == 'binance' and raw_data.get('e') == 'ORDER_TRADE_UPDATE':
                
                o = raw_data['o']
                
                # 🚫 只过滤市价单的未成交中间状态（开仓/平仓的NEW状态）
                if o.get('o') == 'MARKET' and o.get('ot') == 'MARKET' and o.get('X') == 'NEW' and o.get('l') == '0' and o.get('z') == '0':
                    logger.debug(f"⏭️ [币安订单] 过滤市价单未成交中间状态: {o.get('i')}")
                    return
                
                # 1. 分类
                category = classify_binance_order(private_data)
                logger.debug(f"🔍 [币安订单] 分类结果: {category}")
                
                symbol = raw_data['o']['s']
                classified_key = f"{symbol}_{category}"
                
                # 2. 初始化/获取分类存储结构
                if 'binance_order_update' not in self.memory_store['private_data']:
                    self.memory_store['private_data']['binance_order_update'] = {
                        'exchange': 'binance',
                        'data_type': 'order_update',
                        'classified': {}
                    }
                
                classified = self.memory_store['private_data']['binance_order_update']['classified']
                
                # 3. 按分类key存储
                if classified_key not in classified:
                    classified[classified_key] = []
                
                # 🔴 止盈止损的设置和取消 → 同一个合约只能保留最新一条
                # 更新分类名称
                if category in ['03_设置止损', '04_设置止盈', '08_取消止损', '09_取消止盈']:
                    # 直接清空该合约下这类事件的所有历史记录
                    classified[classified_key] = []
                    logger.debug(f"🔄 [币安订单] {symbol} {category} 已清空旧记录")
                
                # 4. 去重检查并追加新记录（按订单ID去重）
                order_id = raw_data['o'].get('i')
                if order_id:
                    # 检查是否已存在相同订单ID
                    existing = False
                    for item in classified[classified_key]:
                        if item['data']['o'].get('i') == order_id:
                            existing = True
                            logger.debug(f"🔄 [币安订单] 跳过重复订单: {order_id}")
                            break
                    
                    if not existing:
                        classified[classified_key].append({
                            'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                            'received_at': private_data.get('received_at', datetime.now().isoformat()),
                            'data': raw_data
                        })
                        logger.debug(f"📦 [币安订单] {symbol} {category} 已追加，当前总数: {len(classified[classified_key])}")
                else:
                    classified[classified_key].append({
                        'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                        'received_at': private_data.get('received_at', datetime.now().isoformat()),
                        'data': raw_data
                    })
                    logger.debug(f"📦 [币安订单] {symbol} {category} 已追加，当前总数: {len(classified[classified_key])}")
                
                # 5. 平仓处理：延迟5分钟清理该合约所有分类缓存
                # is_closing_event 已经返回 ['05_触发止损', '06_触发止盈', '07_主动平仓']
                if is_closing_event(category):
                    # 只获取该symbol相关的keys（不影响其他持仓合约）
                    keys_to_delayed_delete = [k for k in classified.keys() if k.startswith(f"{symbol}_")]
                    
                    # 启动异步延迟删除任务
                    asyncio.create_task(self._delayed_delete(keys_to_delayed_delete, symbol))
                    
                    logger.info(f"⏰ [币安订单] 平仓标记: {symbol} 将在5分钟后清理 ({len(keys_to_delayed_delete)}类)")
                
                # 🔴 币安订单处理完毕，直接返回（不走老逻辑）
                return
            
            # === 以下为原有代码（完全不变）===
            # 🔴 【关键修复】判断数据来源：HTTP获取器 vs WebSocket
            if source == 'http_fetcher':
                # HTTP获取器的数据：直接使用传入的 data_type
                final_data_type = private_data.get('data_type', 'unknown')
                logger.debug(f"📨 [私人数据处理] HTTP数据: {exchange}.{final_data_type}")
                
            else:
                # WebSocket数据：原有逻辑，通过 'e' 字段映射
                event_type = raw_data.get('e', 'unknown')
                
                if exchange == 'binance':
                    # 🚫 1. 过滤掉 TRADE_LITE 事件
                    if event_type == 'TRADE_LITE':
                        logger.debug(f"📨 [私人数据处理] 过滤掉 TRADE_LITE 事件: {raw_data.get('i')}")
                        return
                    
                    # 🗺️ 2. 币安事件类型映射
                    binance_mapping = {
                        'ACCOUNT_UPDATE': 'account_update',
                        'ACCOUNT_CONFIG_UPDATE': 'account_config_update',
                        'MARGIN_CALL': 'risk_event',
                        'listenKeyExpired': 'system_event',
                        'balanceUpdate': 'balance_update',
                        'outboundAccountPosition': 'account_update',
                        'executionReport': 'order_update'
                    }
                    
                    # 使用映射后的data_type
                    if event_type in binance_mapping:
                        final_data_type = binance_mapping[event_type]
                        logger.debug(f"📨 [私人数据处理] 币安事件映射: {event_type} -> {final_data_type}")
                    else:
                        # 对于未映射的事件，使用原生事件名的小写
                        final_data_type = event_type.lower()
                        
                else:
                    # 其他交易所（如OKX）保持原有的data_type
                    final_data_type = private_data.get('data_type', 'unknown')
            
            # 🔴 【新增】记录完整信息便于调试
            logger.debug(f"📨 [私人数据处理] 收到{exchange}.{final_data_type}数据")
            
            # 存储数据
            storage_key = f"{exchange}_{final_data_type}"
            
            self.memory_store['private_data'][storage_key] = {
                'exchange': exchange,
                'data_type': final_data_type,
                'data': raw_data,
                'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                'received_at': private_data.get('received_at', datetime.now().isoformat())
            }
            
            logger.debug(f"✅ [私人数据处理] 已保存: {storage_key}")
            
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 接收数据失败: {e}")
    
    async def get_all_data(self) -> Dict[str, Any]:
        """获取所有私人数据概览（仿制大脑接口）"""
        try:
            formatted_data = {}
            for key, data in self.memory_store['private_data'].items():
                formatted_data[key] = {
                    "exchange": data.get('exchange'),
                    "data_type": data.get('data_type'),
                    "received_at": data.get('received_at'),
                    "timestamp": data.get('timestamp'),
                    "data_keys": list(data.get('data', {}).keys()) if isinstance(data.get('data'), dict) else type(data.get('data')).__name__
                }
            
            return {
                "timestamp": datetime.now().isoformat(),
                "total_count": len(self.memory_store['private_data']),
                "private_data": formatted_data,
                "note": "私人数据处理模块 - 只存储最新一份数据"
            }
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 获取所有数据失败: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "private_data": {}
            }
    
    async def get_data_by_exchange(self, exchange: str) -> Dict[str, Any]:
        """按交易所获取私人数据（仿制大脑接口）"""
        try:
            exchange_data = {}
            for key, data in self.memory_store['private_data'].items():
                if key.startswith(f"{exchange.lower()}_"):
                    # 🔴 特殊处理：币安订单更新，返回分类统计摘要
                    if key == 'binance_order_update':
                        classified = data.get('classified', {})
                        summary = {}
                        for k, v in classified.items():
                            summary[k] = len(v)
                        
                        exchange_data[key] = {
                            "exchange": data.get('exchange'),
                            "data_type": data.get('data_type'),
                            "timestamp": data.get('timestamp'),
                            "received_at": data.get('received_at'),
                            "summary": summary,
                            "note": "各类别事件数量统计，详情请查询具体data_type"
                        }
                    else:
                        # 其他数据类型保持原样
                        exchange_data[key] = {
                            "exchange": data.get('exchange'),
                            "data_type": data.get('data_type'),
                            "timestamp": data.get('timestamp'),
                            "received_at": data.get('received_at'),
                            "data": data.get('data')
                        }
            
            return {
                "exchange": exchange,
                "timestamp": datetime.now().isoformat(),
                "count": len(exchange_data),
                "data": exchange_data,
                "note": f"{exchange}私人数据（最新一份）"
            }
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 按交易所获取数据失败: {e}")
            return {
                "exchange": exchange,
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "data": {}
            }
    
    async def get_data_detail(self, exchange: str, data_type: str) -> Dict[str, Any]:
        """获取特定私人数据详情（仿制大脑接口）"""
        try:
            key = f"{exchange.lower()}_{data_type.lower()}"
            
            # 🔴 特殊处理：币安订单更新，返回分类结构
            if key == 'binance_order_update':
                if key in self.memory_store['private_data']:
                    return self.memory_store['private_data'][key]
                else:
                    # 还没有任何订单数据时，返回空分类结构
                    return {
                        "exchange": "binance",
                        "data_type": "order_update",
                        "classified": {},
                        "note": "暂无订单数据"
                    }
            
            # 其他数据类型：保持原样返回
            if key in self.memory_store['private_data']:
                data = self.memory_store['private_data'][key]
                return {
                    "key": key,
                    "exchange": exchange,
                    "data_type": data_type,
                    "timestamp": data.get('timestamp'),
                    "received_at": data.get('received_at'),
                    "data": data.get('data'),
                    "note": "最新一份数据，新数据会覆盖旧数据"
                }
            else:
                return {
                    "error": f"未找到数据: {key}",
                    "available_keys": list(self.memory_store['private_data'].keys()),
                    "timestamp": datetime.now().isoformat()
                }
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 获取数据详情失败: {e}")
            return {
                "error": str(e),
                "exchange": exchange,
                "data_type": data_type,
                "timestamp": datetime.now().isoformat()
            }


# 全局单例实例
_global_processor = PrivateDataProcessor()

def get_processor():
    """获取处理器单例"""
    return _global_processor

async def receive_private_data(private_data):
    """
    供连接池调用的函数接口
    使用全局单例
    """
    return await _global_processor.receive_private_data(private_data)