"""
私人数据处理器 - 最简版本
只接收、存储、查看私人数据
"""
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

from .binance_classifier import classify_binance_order, is_closing_event as is_binance_closing
from .okx_classifier import classify_okx_order, is_closing_event as is_okx_closing
from .scheduler import get_scheduler


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
            self.memory_store = {'private_data': {}}
            self._initialized = True
            logger.info("✅ [私人数据处理] 模块已初始化")
            
            # ===== 初始化并启动调度器 =====
            self.scheduler = get_scheduler()
            self._start_scheduler()
    
    def _start_scheduler(self):
        """启动调度器（在模块初始化时立即启动）"""
        try:
            # 尝试获取当前事件循环
            loop = asyncio.get_running_loop()
            # 如果有事件循环，创建任务启动
            asyncio.create_task(self.scheduler.start())
            logger.info("🚀 [私人数据处理] 调度器已异步启动")
        except RuntimeError:
            # 没有事件循环，延后启动
            logger.warning("⚠️ [私人数据处理] 无事件循环，调度器将在首次数据到达时启动")
            self._scheduler_delayed_start = True
    
    async def _ensure_scheduler_started(self):
        """确保调度器已启动（用于延迟启动的情况）"""
        if hasattr(self, '_scheduler_delayed_start') and self._scheduler_delayed_start:
            await self.scheduler.start()
            self._scheduler_delayed_start = False
            logger.info("🚀 [私人数据处理] 调度器延迟启动完成")
    
    async def _binance_delayed_delete(self, keys: List[str], symbol: str):
        """30秒后删除该symbol所有当前存在的key（币安使用）"""
        try:
            await asyncio.sleep(30)
            
            if 'binance_order_update' not in self.memory_store['private_data']:
                return
                
            classified = self.memory_store['private_data']['binance_order_update'].get('classified', {})
            current_keys = [k for k in classified.keys() if k.startswith(f"{symbol}_")]
            
            for k in current_keys:
                del classified[k]
            
            if current_keys:
                logger.info(f"🧹 [币安订单] 延迟清理完成: {symbol} 已删除 {len(current_keys)}类")
                
        except Exception as e:
            logger.error(f"❌ [币安订单] 延迟清理失败: {e}")
    
    async def _okx_delayed_delete(self, symbol: str):
        """
        30秒后清理该symbol的所有相关数据
        包括：订单数据和持仓数据
        """
        try:
            await asyncio.sleep(30)
            
            # ===== 清理订单数据 =====
            if 'okx_order_update' in self.memory_store['private_data']:
                classified = self.memory_store['private_data']['okx_order_update'].get('classified', {})
                order_keys = [k for k in classified.keys() if k.startswith(f"{symbol}_")]
                
                for k in order_keys:
                    del classified[k]
                
                if order_keys:
                    logger.info(f"🧹 [OKX订单] 延迟清理完成: {symbol} 已删除 {len(order_keys)}个订单分类")
            
            # ===== 清理持仓数据 - 只清理 okx_position_update =====
            pos_key = 'okx_position_update'
            if pos_key in self.memory_store['private_data']:
                # 检查这个持仓数据是否属于要清理的symbol
                pos_data = self.memory_store['private_data'][pos_key]
                data_field = pos_data.get('data', {})
                
                # 从原始数据格式中提取instId
                if 'data' in data_field and isinstance(data_field['data'], list) and len(data_field['data']) > 0:
                    inst_id = data_field['data'][0].get('instId', '')
                    pos_symbol = inst_id.replace('-SWAP', '').replace('-USDT', '')
                    
                    # 只有属于这个symbol的才清理
                    if pos_symbol == symbol:
                        del self.memory_store['private_data'][pos_key]
                        logger.info(f"🧹 [OKX持仓] 延迟清理完成: 已删除 {pos_key} (symbol: {symbol})")
            
            logger.info(f"✅ [OKX清理] {symbol} 所有相关数据清理完成")
                
        except Exception as e:
            logger.error(f"❌ [OKX订单] 延迟清理失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    # ===== 新增：将完整存储区喂给Step1 =====
    async def _feed_full_storage_to_step1(self):
        """将整个存储区喂给Step1"""
        try:
            full_storage_item = {
                'full_storage': self.memory_store['private_data'].copy()
            }
            self.scheduler.feed_step1(full_storage_item)
            logger.info(f"📤【Manager】已将完整存储区喂给Step1，包含 {len(self.memory_store['private_data'])} 个数据项")
        except Exception as e:
            logger.error(f"❌【Manager】喂给Step1完整存储区失败: {e}")
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据
        格式：{'exchange': 'binance', 'data_type': 'account_update', 'data': {...}, 'timestamp': '...'}
        """
        try:
            # ===== 确保调度器已启动（延迟启动的情况）=====
            await self._ensure_scheduler_started()
            
            # ===== 【新增】等待调度器就绪 =====
            await self.scheduler.wait_until_ready()
            
            # ===== 调试日志1：确认收到数据 =====
            exchange = private_data.get('exchange', 'unknown')
            raw_data = private_data.get('data', {})
            source = private_data.get('source', '')
            data_type = private_data.get('data_type', 'unknown')
            event_type = raw_data.get('e', 'unknown') if isinstance(raw_data, dict) else 'unknown'
            logger.info(f"🎯【Manager】收到数据！exchange={exchange}, data_type={data_type}, event={event_type}")
            
            # 初始化存储格式数据（用于后续喂给Step1）
            stored_item_base = {
                'exchange': exchange,
                'data': raw_data,
                'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                'received_at': private_data.get('received_at', datetime.now().isoformat())
            }
            
            # ========== 币安订单更新处理 ==========
            if exchange == 'binance' and raw_data.get('e') == 'ORDER_TRADE_UPDATE':
                
                o = raw_data['o']
                
                # 过滤市价单未成交中间状态
                if o.get('o') == 'MARKET' and o.get('ot') == 'MARKET' and o.get('X') == 'NEW' and o.get('l') == '0' and o.get('z') == '0':
                    logger.debug(f"⏭️ [币安订单] 过滤市价单未成交中间状态: {o.get('i')}")
                    return
                
                # 分类
                category = classify_binance_order(private_data)
                logger.debug(f"🔍 [币安订单] 分类结果: {category}")
                
                symbol = raw_data['o']['s']
                classified_key = f"{symbol}_{category}"
                
                # 初始化存储
                if 'binance_order_update' not in self.memory_store['private_data']:
                    self.memory_store['private_data']['binance_order_update'] = {
                                                  'key': 'binance_order_update',  # ✅ 加上 key
                        'exchange': 'binance',
                        'data_type': 'order_update',
                        'classified': {}
                    }
                
                classified = self.memory_store['private_data']['binance_order_update']['classified']
                
                # 取消止损/止盈的立即清除
                if category == '11_取消止损':
                    stop_loss_key = f"{symbol}_03_设置止损"
                    if stop_loss_key in classified:
                        del classified[stop_loss_key]
                        logger.info(f"🗑️ [币安订单] {symbol} 取消止损，已删除设置止损记录")
                    # 触发完整存储区更新
                    await self._feed_full_storage_to_step1()
                    return
                
                if category == '12_取消止盈':
                    take_profit_key = f"{symbol}_04_设置止盈"
                    if take_profit_key in classified:
                        del classified[take_profit_key]
                        logger.info(f"🗑️ [币安订单] {symbol} 取消止盈，已删除设置止盈记录")
                    # 触发完整存储区更新
                    await self._feed_full_storage_to_step1()
                    return
                
                # 过期事件不保存
                if category in ['13_止损过期(被触发)', '14_止损过期(被取消)', 
                                '15_止盈过期(被触发)', '16_止盈过期(被取消)']:
                    logger.debug(f"⏭️ [币安订单] 过期事件不缓存: {category}")
                    # 触发完整存储区更新
                    await self._feed_full_storage_to_step1()
                    return
                
                # 按分类存储
                if classified_key not in classified:
                    classified[classified_key] = []
                
                # 止盈止损的设置事件只保留最新一条
                if category in ['03_设置止损', '04_设置止盈']:
                    classified[classified_key] = []
                    logger.debug(f"🔄 [币安订单] {symbol} {category} 已清空旧记录")
                
                # 去重追加
                order_id = raw_data['o'].get('i')
                if order_id:
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
                else:
                    classified[classified_key].append({
                        'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                        'received_at': private_data.get('received_at', datetime.now().isoformat()),
                        'data': raw_data
                    })
                
                # 平仓处理：延迟30秒清理
                if is_binance_closing(category):
                    keys_to_delayed_delete = [k for k in classified.keys() if k.startswith(f"{symbol}_")]
                    asyncio.create_task(self._binance_delayed_delete(keys_to_delayed_delete, symbol))
                    logger.info(f"⏰ [币安订单] 平仓标记: {symbol} 将在30秒后清理")
                
                # ===== 保存后，将完整存储区喂给Step1 =====
                await self._feed_full_storage_to_step1()
                
                return
            
            # ========== OKX订单更新处理（细分版本）==========
            if exchange == 'okx' and private_data.get('data_type') == 'order_update':
                
                logger.info(f"📥 [OKX订单] 收到订单更新")
                
                try:
                    # 提取订单数据 - OKX的数据结构: raw_data['data'] 是一个数组
                    if 'data' not in raw_data:
                        logger.error(f"❌ [OKX订单] 缺少data字段: {list(raw_data.keys())}")
                        return
                    
                    if not isinstance(raw_data['data'], list):
                        logger.error(f"❌ [OKX订单] data不是数组: {type(raw_data['data'])}")
                        return
                    
                    if len(raw_data['data']) == 0:
                        logger.error(f"❌ [OKX订单] data数组为空")
                        return
                    
                    # 获取第一个订单数据
                    order_data = raw_data['data'][0]
                    
                    if not isinstance(order_data, dict):
                        logger.error(f"❌ [OKX订单] 订单数据不是字典: {type(order_data)}")
                        return
                    
                    order_id = order_data.get('ordId', 'unknown')
                    state = order_data.get('state', 'unknown')
                    logger.info(f"✅ [OKX订单] 成功提取订单数据: {order_id}, 状态: {state}")
                    
                    # 分类 - 传入raw_data['data']
                    category = classify_okx_order(raw_data['data'])
                    logger.info(f"🔍 [OKX订单] 分类结果: {category}")
                    
                    # 过滤不需要保存的分类
                    filtered_categories = [
                        '01_挂单',              # 挂单过滤
                        '02_开仓(部分成交)',     # 部分成交过滤
                        '04_平仓(部分成交)'      # 部分成交过滤
                    ]
                    
                    if category in filtered_categories:
                        logger.info(f"⏭️ [OKX订单] 过滤 {category}: {order_id}, 不保存")
                        return
                    
                    # 获取交易对
                    symbol = order_data.get('instId', 'unknown')
                    if '-SWAP' in symbol:
                        symbol = symbol.replace('-SWAP', '')
                    if '-USDT' in symbol:
                        symbol = symbol.replace('-USDT', '')
                    
                    classified_key = f"{symbol}_{category}"
                    
                    # 初始化存储
                    if 'okx_order_update' not in self.memory_store['private_data']:
                        self.memory_store['private_data']['okx_order_update'] = {                            'key': 'okx_order_update',  # ✅ 加上 key
                            'exchange': 'okx',
                            'data_type': 'order_update',
                            'classified': {}
                        }
                    
                    classified = self.memory_store['private_data']['okx_order_update']['classified']
                    
                    # 取消订单处理
                    if category == '06_已取消':
                        logger.info(f"🗑️ [OKX订单] {symbol} 订单取消，等待后续清理")
                        return
                    
                    # 按分类存储
                    if classified_key not in classified:
                        classified[classified_key] = []
                    
                    # 开仓/平仓全部成交事件只保留最新一条
                    if category in ['03_开仓(全部成交)', '05_平仓(全部成交)']:
                        classified[classified_key] = []
                        logger.debug(f"🔄 [OKX订单] {symbol} {category} 已清空旧记录")
                    
                    # 去重追加
                    if order_id and order_id != 'unknown':
                        existing = False
                        for item in classified[classified_key]:
                            item_data = item.get('data', {})
                            if 'data' in item_data and isinstance(item_data['data'], list) and len(item_data['data']) > 0:
                                if item_data['data'][0].get('ordId') == order_id:
                                    existing = True
                                    logger.debug(f"🔄 [OKX订单] 跳过重复订单: {order_id}")
                                    break
                        
                        if not existing:
                            classified[classified_key].append({
                                'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                                'received_at': private_data.get('received_at', datetime.now().isoformat()),
                                'data': raw_data
                            })
                            logger.info(f"📦 [OKX订单] {symbol} {category} 已保存")
                    else:
                        classified[classified_key].append({
                            'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                            'received_at': private_data.get('received_at', datetime.now().isoformat()),
                            'data': raw_data
                        })
                        logger.info(f"📦 [OKX订单] {symbol} {category} 已保存")
                    
                    # ===== 平仓全部成交：延迟30秒清理所有相关数据 =====
                    if is_okx_closing(category):
                        asyncio.create_task(self._okx_delayed_delete(symbol))
                        logger.info(f"⏰ [OKX订单] 平仓全部成交标记: {symbol} 将在30秒后清理所有相关数据（订单+持仓）")
                    
                    # ===== 保存后，将完整存储区喂给Step1 =====
                    await self._feed_full_storage_to_step1()
                    
                    return
                    
                except Exception as e:
                    logger.error(f"❌ [OKX订单] 处理失败: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    return
            
            # ========== OKX持仓更新处理 ==========
            if exchange == 'okx' and private_data.get('data_type') == 'position_update':
                
                logger.debug(f"📥 [OKX持仓] 收到持仓更新")
                
                try:
                    # 直接存储原始数据 - 使用你提供的格式
                    storage_key = f"{exchange}_position_update"
                    
                    self.memory_store['private_data'][storage_key] = {
                        'exchange': exchange,
                        'data_type': 'position_update',
                        'data': raw_data,  # 直接存储原始数据
                        'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                        'received_at': private_data.get('received_at', datetime.now().isoformat())
                    }
                    
                    logger.debug(f"✅ [OKX持仓] 已保存: {storage_key}")
                    
                    # ===== 保存后，将完整存储区喂给Step1 =====
                    await self._feed_full_storage_to_step1()
                    
                except Exception as e:
                    logger.error(f"❌ [OKX持仓] 处理失败: {e}")
                
                return
            
            # ========== 其他数据类型 ==========
            if source == 'http_fetcher':
                final_data_type = private_data.get('data_type', 'unknown')
            else:
                event_type = raw_data.get('e', 'unknown')
                
                if exchange == 'binance':
                    if event_type == 'TRADE_LITE':
                        logger.debug(f"📨 [私人数据处理] 过滤掉 TRADE_LITE 事件")
                        return
                    
                    binance_mapping = {
                        'ACCOUNT_UPDATE': 'account_update',
                        'ACCOUNT_CONFIG_UPDATE': 'account_config_update',
                        'MARGIN_CALL': 'risk_event',
                        'listenKeyExpired': 'system_event',
                        'balanceUpdate': 'balance_update',
                        'outboundAccountPosition': 'account_update',
                        'executionReport': 'order_update'
                    }
                    
                    if event_type in binance_mapping:
                        final_data_type = binance_mapping[event_type]
                    else:
                        final_data_type = event_type.lower()
                else:
                    final_data_type = private_data.get('data_type', 'unknown')
            
            # 存储其他数据
            storage_key = f"{exchange}_{final_data_type}"
            self.memory_store['private_data'][storage_key] = {
                'exchange': exchange,
                'data_type': final_data_type,
                'data': raw_data,
                'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                'received_at': private_data.get('received_at', datetime.now().isoformat())
            }
            
            logger.debug(f"✅ [私人数据处理] 已保存: {storage_key}")
            
            # ===== 保存后，将完整存储区喂给Step1 =====
            await self._feed_full_storage_to_step1()
            
        except Exception as e:
            logger.error(f"❌【Manager】接收数据失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def get_all_data(self) -> Dict[str, Any]:
        """获取所有私人数据概览（修复订单更新时间显示问题）"""
        try:
            formatted_data = {}
            for key, data in self.memory_store['private_data'].items():
                # ===== 修复：订单类型从classified里取最新时间 =====
                if key in ['binance_order_update', 'okx_order_update']:
                    classified = data.get('classified', {})
                    summary = {}
                    latest_timestamp = None
                    latest_received_at = None
                    
                    for k, v in classified.items():
                        summary[k] = len(v)
                        # 取该分类下最新一条的时间
                        if v and isinstance(v, list) and len(v) > 0:
                            # 最后一条是最新的
                            latest_item = v[-1]
                            item_ts = latest_item.get('timestamp')
                            item_ra = latest_item.get('received_at')
                            
                            # 比较并更新最新时间
                            if latest_timestamp is None or (item_ts and item_ts > latest_timestamp):
                                latest_timestamp = item_ts
                                latest_received_at = item_ra
                    
                    formatted_data[key] = {
                        "exchange": data.get('exchange'),
                        "data_type": data.get('data_type'),
                        "received_at": latest_received_at,  # 从classified里取最新时间
                        "timestamp": latest_timestamp,      # 从classified里取最新时间
                        "data_keys": list(classified.keys()) if classified else "No classified data",  # 显示有哪些分类
                        "note": "订单数据的时间取自classified中的最新记录"
                    }
                else:
                    # 其他类型：返回数据键名（data_keys），不返回完整数据
                    raw_data = data.get('data', {})
                    
                    formatted_data[key] = {
                        "exchange": data.get('exchange'),
                        "data_type": data.get('data_type'),
                        "timestamp": data.get('timestamp'),
                        "received_at": data.get('received_at'),
                        "data_keys": list(raw_data.keys()) if isinstance(raw_data, dict) else str(type(raw_data)),
                        "note": f"{data.get('data_type')}数据大纲，详情请访问 /private/{data.get('exchange')}/{data.get('data_type')}"
                    }
            
            return {
                "timestamp": datetime.now().isoformat(),
                "total_count": len(self.memory_store['private_data']),
                "private_data": formatted_data,
                "note": "私人数据处理模块 - 只存储最新一份数据"
            }
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 获取所有数据失败: {e}")
            return {"timestamp": datetime.now().isoformat(), "error": str(e), "private_data": {}}
    
    async def get_data_by_exchange(self, exchange: str) -> Dict[str, Any]:
        """按交易所获取私人数据（统一返回大纲格式）"""
        try:
            exchange_data = {}
            for key, data in self.memory_store['private_data'].items():
                if key.startswith(f"{exchange.lower()}_"):
                    
                    # ===== 修复：订单类型从classified里取最新时间 =====
                    if key in ['binance_order_update', 'okx_order_update']:
                        classified = data.get('classified', {})
                        summary = {}
                        latest_timestamp = None
                        latest_received_at = None
                        
                        for k, v in classified.items():
                            summary[k] = len(v)
                            # 取该分类下最新一条的时间
                            if v and isinstance(v, list) and len(v) > 0:
                                # 最后一条是最新的
                                latest_item = v[-1]
                                item_ts = latest_item.get('timestamp')
                                item_ra = latest_item.get('received_at')
                                
                                # 比较并更新最新时间
                                if latest_timestamp is None or (item_ts and item_ts > latest_timestamp):
                                    latest_timestamp = item_ts
                                    latest_received_at = item_ra
                        
                        exchange_data[key] = {
                            "exchange": data.get('exchange'),
                            "data_type": data.get('data_type'),
                            "timestamp": latest_timestamp,  # 从classified里取
                            "received_at": latest_received_at,  # 从classified里取
                            "summary": summary,
                            "note": "各类别事件数量统计，详情请查询具体data_type"
                        }
                    else:
                        # 其他类型：返回数据键名（data_keys），不返回完整数据
                        raw_data = data.get('data', {})
                        
                        exchange_data[key] = {
                            "exchange": data.get('exchange'),
                            "data_type": data.get('data_type'),
                            "timestamp": data.get('timestamp'),
                            "received_at": data.get('received_at'),
                            "data_keys": list(raw_data.keys()) if isinstance(raw_data, dict) else str(type(raw_data)),
                            "note": f"{data.get('data_type')}数据大纲，详情请访问 /private/{exchange}/{data.get('data_type')}"
                        }
            
            return {
                "exchange": exchange,
                "timestamp": datetime.now().isoformat(),
                "count": len(exchange_data),
                "private_data": exchange_data,
                "note": f"{exchange}交易所数据大纲"
            }
        except Exception as e:
            logger.error(f"❌ [私人数据处理] 按交易所获取数据失败: {e}")
            return {"exchange": exchange, "timestamp": datetime.now().isoformat(), "error": str(e), "private_data": {}}
    
    async def get_data_detail(self, exchange: str, data_type: str) -> Dict[str, Any]:
        """获取特定私人数据详情"""
        try:
            key = f"{exchange.lower()}_{data_type.lower()}"
            
            if key in ['binance_order_update', 'okx_order_update']:
                if key in self.memory_store['private_data']:
                    return self.memory_store['private_data'][key]
                else:
                    return {
                        "exchange": exchange,
                        "data_type": data_type,
                        "classified": {},
                        "note": "暂无订单数据"
                    }
            
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
            return {"error": str(e), "exchange": exchange, "data_type": data_type, "timestamp": datetime.now().isoformat()}


# 全局单例实例
_global_processor = PrivateDataProcessor()

def get_processor():
    """获取处理器单例"""
    return _global_processor

async def receive_private_data(private_data):
    """供连接池调用的函数接口"""
    return await _global_processor.receive_private_data(private_data)