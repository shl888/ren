"""
私人数据处理器 - 最简版本
只接收、存储、查看私人数据
"""
import logging
import asyncio
import threading
import time
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
            self._lock = threading.Lock()  # 添加线程锁
            self._initialized = True
            logger.info("✅ [私人数据处理] 模块已初始化")
            
            # ===== 初始化并启动调度器 =====
            self.scheduler = get_scheduler()
            self._start_scheduler()
    
    def _start_scheduler(self):
        """启动调度器（在模块初始化时立即启动）"""
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(self.scheduler.start())
            logger.info("🚀 [私人数据处理] 调度器已异步启动")
        except RuntimeError:
            logger.warning("⚠️ [私人数据处理] 无事件循环，调度器将在首次数据到达时启动")
            self._scheduler_delayed_start = True
    
    async def _ensure_scheduler_started(self):
        """确保调度器已启动（用于延迟启动的情况）"""
        if hasattr(self, '_scheduler_delayed_start') and self._scheduler_delayed_start:
            await self.scheduler.start()
            self._scheduler_delayed_start = False
            logger.info("🚀 [私人数据处理] 调度器延迟启动完成")
    
    # ========== 币安清理（同步线程版）==========
    def _binance_delayed_delete_sync(self, symbol: str):
        """
        同步版：5秒后删除该symbol所有订单数据（币安使用）
        
        修复说明：
        - 原逻辑：两次持锁，锁内循环删除 + time.sleep(0)，死锁风险高
        - 新逻辑：一次持锁，按合约名批量删除，锁内无 sleep，持锁时间极短
        - 只删除当前合约的数据，不影响其他合约
        """
        try:
            time.sleep(5)
            
            # ===== 一次持锁，完成所有操作 =====
            with self._lock:
                if 'binance_order_update' not in self.memory_store['private_data']:
                    return
                
                classified = self.memory_store['private_data']['binance_order_update'].get('classified', {})
                
                # 找出该合约的所有分类key
                keys_to_delete = [k for k in list(classified.keys()) if k.startswith(f"{symbol}_")]
                
                # 批量删除（无 sleep，持锁时间极短）
                for k in keys_to_delete:
                    del classified[k]
                
                if keys_to_delete:
                    logger.debug(f"🧹【私人数据处理】 [币安订单] 已删除 {symbol} 的 {len(keys_to_delete)} 个分类")
            
            # ===== 锁外写日志 =====
            if keys_to_delete:
                logger.info(f"🧹【私人数据处理】 [币安订单] 清理完成: {symbol}")
            
        except Exception as e:
            # 异常也在锁外记录
            logger.error(f"❌ 【私人数据处理】[币安订单] 延迟清理失败: {e}")
    
    def _binance_delayed_delete(self, symbol: str):
        """启动独立线程执行清理"""
        thread = threading.Thread(target=self._binance_delayed_delete_sync, args=(symbol,))
        thread.daemon = True
        thread.start()
        logger.info(f"⏰【私人数据处理】 [币安订单] 清理线程已启动: {symbol} 将在5秒后清理")
    
    # ========== OKX清理（同步线程版）==========
    def _okx_delayed_delete_sync(self, symbol: str):
        """
        同步版：5秒后删除该symbol的所有相关数据
        包括：订单数据和持仓数据
        
        修复说明：
        - 原逻辑：两次持锁，锁内循环删除 + time.sleep(0)，死锁风险高
        - 新逻辑：一次持锁，按合约名批量删除订单 + 检查并删除持仓
        - 锁内无 sleep，持锁时间极短
        - 只删除当前合约的数据，不影响其他合约
        """
        try:
            time.sleep(5)
            
            # ===== 一次持锁，完成所有操作 =====
            with self._lock:
                # 1. 清理订单数据中的该合约
                if 'okx_order_update' in self.memory_store['private_data']:
                    classified = self.memory_store['private_data']['okx_order_update'].get('classified', {})
                    order_keys_to_delete = [k for k in list(classified.keys()) if k.startswith(f"{symbol}_")]
                    
                    for k in order_keys_to_delete:
                        del classified[k]
                    
                    if order_keys_to_delete:
                        logger.debug(f"🧹【私人数据处理】 [OKX订单] 已删除 {symbol} 的 {len(order_keys_to_delete)} 个订单分类")
                
                # 2. 清理持仓数据（如果是该合约）
                pos_key = 'okx_position_update'
                if pos_key in self.memory_store['private_data']:
                    pos_data = self.memory_store['private_data'][pos_key]
                    raw_data = pos_data.get('data', {})
                    
                    if 'data' in raw_data and isinstance(raw_data['data'], list) and len(raw_data['data']) > 0:
                        inst_id = raw_data['data'][0].get('instId', '')
                        pos_symbol = inst_id.replace('-SWAP', '').replace('-USDT', '')
                        
                        if pos_symbol == symbol:
                            del self.memory_store['private_data'][pos_key]
                            logger.debug(f"🧹【私人数据处理】 [OKX持仓] 已删除 {symbol} 的持仓数据")
            
            # ===== 锁外写日志 =====
            logger.info(f"🧹【私人数据处理】 [OKX] 清理完成: {symbol}")
            
        except Exception as e:
            # 异常也在锁外记录
            logger.error(f"❌【私人数据处理】 [OKX订单] 延迟清理失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _okx_delayed_delete(self, symbol: str):
        """启动独立线程执行清理"""
        thread = threading.Thread(target=self._okx_delayed_delete_sync, args=(symbol,))
        thread.daemon = True
        thread.start()
        logger.info(f"⏰ 【私人数据处理】[OKX订单] 清理线程已启动: {symbol} 将在5秒后清理")
    
    # ===== 将完整存储区喂给Step1 =====
    async def _feed_full_storage_to_step1(self):
        """将整个存储区喂给Step1"""
        try:
            full_storage_item = {
                'full_storage': self.memory_store['private_data'].copy()
            }
            await self.scheduler.feed_step1(full_storage_item) # ✅ 加上 await
            logger.debug(f"📤【私人数据处理】【Manager】已将完整存储区喂给Step1，包含 {len(self.memory_store['private_data'])} 个数据项")
        except Exception as e:
            logger.error(f"❌【私人数据处理】【Manager】喂给Step1完整存储区失败: {e}")
    
    async def receive_private_data(self, private_data):
        """
        接收私人数据
        格式：{'exchange': 'binance', 'data_type': 'account_update', 'data': {...}, 'timestamp': '...'}
        """
        try:
            # ===== 确保调度器已启动 =====
            await self._ensure_scheduler_started()
            
            # ===== 等待调度器就绪 =====
            await self.scheduler.wait_until_ready()
            
            # ===== 调试日志 =====
            exchange = private_data.get('exchange', 'unknown')
            raw_data = private_data.get('data', {})
            source = private_data.get('source', '')
            data_type = private_data.get('data_type', 'unknown')
            event_type = raw_data.get('e', 'unknown') if isinstance(raw_data, dict) else 'unknown'
            logger.debug(f"🎯【私人数据处理】【Manager】收到数据！exchange={exchange}, data_type={data_type}, event={event_type}")
            
            # 初始化存储格式数据
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
                    logger.debug(f"⏭️【私人数据处理】 [币安订单] 过滤市价单未成交中间状态: {o.get('i')}")
                    return
                
                # 分类
                category = classify_binance_order(private_data)
                logger.debug(f"🔍【私人数据处理】 [币安订单] 分类结果: {category}")
                
                symbol = raw_data['o']['s']
                classified_key = f"{symbol}_{category}"
                order_id = raw_data['o'].get('i')
                
                # ========== 修复：将去重检查移到锁外，避免锁内 await ==========
                # 第1步：锁内快速获取现有订单ID列表
                existing_ids = []
                with self._lock:
                    if 'binance_order_update' not in self.memory_store['private_data']:
                        self.memory_store['private_data']['binance_order_update'] = {
                            'key': 'binance_order_update',
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
                            logger.debug(f"🗑️【私人数据处理】 [币安订单] {symbol} 取消止损，已删除设置止损记录")
                        return
                    
                    if category == '12_取消止盈':
                        take_profit_key = f"{symbol}_04_设置止盈"
                        if take_profit_key in classified:
                            del classified[take_profit_key]
                            logger.debug(f"🗑️【私人数据处理】 [币安订单] {symbol} 取消止盈，已删除设置止盈记录")
                        await self._feed_full_storage_to_step1()
                        return
                    
                    # 过期事件不保存
                    if category in ['13_止损过期(被触发)', '14_止损过期(被取消)', 
                                    '15_止盈过期(被触发)', '16_止盈过期(被取消)']:
                        logger.debug(f"⏭️【私人数据处理】 [币安订单] 过期事件不缓存: {category}")
                        return
                    
                    # 按分类存储
                    if classified_key not in classified:
                        classified[classified_key] = []
                    
                    # 止盈止损的设置事件只保留最新一条
                    if category in ['03_设置止损', '04_设置止盈']:
                        classified[classified_key] = []
                        logger.debug(f"🔄【私人数据处理】 [币安订单] {symbol} {category} 已清空旧记录")
                    
                    # 如果有订单ID，获取现有订单ID列表用于去重（锁内快速读取）
                    if order_id:
                        existing_ids = [item['data']['o'].get('i') for item in classified[classified_key]]
                
                # 第2步：锁外检查重复（无锁，安全）
                if order_id and order_id in existing_ids:
                    logger.debug(f"🔄【私人数据处理】 [币安订单] 跳过重复订单: {order_id}")
                    return
                
                # 第3步：锁内写入数据
                with self._lock:
                    # 重新获取 classified（可能已被其他线程修改）
                    classified = self.memory_store['private_data']['binance_order_update']['classified']
                    
                    # 再次检查分类是否存在（可能被其他线程创建）
                    if classified_key not in classified:
                        classified[classified_key] = []
                    
                    # 再次检查重复（双重检查，防止在锁外检查后到锁内写入前被其他线程写入）
                    if order_id:
                        for item in classified[classified_key]:
                            if item['data']['o'].get('i') == order_id:
                                logger.debug(f"🔄【私人数据处理】 [币安订单] 跳过重复订单（二次检查）: {order_id}")
                                return
                    
                    # 写入数据
                    if order_id:
                        classified[classified_key].append({
                            'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                            'received_at': private_data.get('received_at', datetime.now().isoformat()),
                            'data': raw_data
                        })
                        logger.debug(f"📦【私人数据处理】 [币安订单] {symbol} {category} 订单 {order_id} 已保存")
                    else:
                        classified[classified_key].append({
                            'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                            'received_at': private_data.get('received_at', datetime.now().isoformat()),
                            'data': raw_data
                        })
                        logger.debug(f"📦【私人数据处理】 [币安订单] {symbol} {category} 已保存")
                    
                    # 平仓处理：启动独立线程清理
                    if is_binance_closing(category):
                        self._binance_delayed_delete(symbol)
                        logger.info(f"⏰【私人数据处理】 [币安订单] 平仓全部成交标记: {symbol} 清理线程已启动")
                
                # 保存后，将完整存储区喂给Step1
                await self._feed_full_storage_to_step1()
                return
            
            # ========== OKX订单更新处理 ==========
            if exchange == 'okx' and private_data.get('data_type') == 'order_update':
                
                logger.debug(f"📥【私人数据处理】 [OKX订单] 收到订单更新")
                
                try:
                    if 'data' not in raw_data:
                        logger.error(f"❌【私人数据处理】 [OKX订单] 缺少data字段: {list(raw_data.keys())}")
                        return
                    
                    if not isinstance(raw_data['data'], list):
                        logger.error(f"❌【私人数据处理】 [OKX订单] data不是数组: {type(raw_data['data'])}")
                        return
                    
                    if len(raw_data['data']) == 0:
                        logger.error(f"❌【私人数据处理】 [OKX订单] data数组为空")
                        return
                    
                    order_data = raw_data['data'][0]
                    
                    if not isinstance(order_data, dict):
                        logger.error(f"❌【私人数据处理】 [OKX订单] 订单数据不是字典: {type(order_data)}")
                        return
                    
                    order_id = order_data.get('ordId', 'unknown')
                    state = order_data.get('state', 'unknown')
                    logger.debug(f"✅【私人数据处理】 [OKX订单] 成功提取订单数据: {order_id}, 状态: {state}")
                    
                    category = classify_okx_order(raw_data['data'])
                    logger.debug(f"🔍【私人数据处理】 [OKX订单] 分类结果: {category}")
                    
                    # 过滤不需要保存的分类
                    filtered_categories = [
                        '01_挂单',
                        '02_开仓(部分成交)',
                        '04_平仓(部分成交)'
                    ]
                    
                    if category in filtered_categories:
                        logger.debug(f"⏭️【私人数据处理】 [OKX订单] 过滤 {category}: {order_id}, 不保存")
                        return
                    
                    symbol = order_data.get('instId', 'unknown')
                    if '-SWAP' in symbol:
                        symbol = symbol.replace('-SWAP', '')
                    if '-USDT' in symbol:
                        symbol = symbol.replace('-USDT', '')
                    
                    classified_key = f"{symbol}_{category}"
                    
                    # ========== 修复：将去重检查移到锁外，避免锁内 await ==========
                    # 第1步：锁内快速获取现有订单ID列表
                    existing_ids = []
                    with self._lock:
                        if 'okx_order_update' not in self.memory_store['private_data']:
                            self.memory_store['private_data']['okx_order_update'] = {
                                'key': 'okx_order_update',
                                'exchange': 'okx',
                                'data_type': 'order_update',
                                'classified': {}
                            }
                        
                        classified = self.memory_store['private_data']['okx_order_update']['classified']
                        
                        if category == '06_已取消':
                            logger.debug(f"🗑️【私人数据处理】 [OKX订单] {symbol} 订单取消，等待后续清理")
                            return
                        
                        if classified_key not in classified:
                            classified[classified_key] = []
                        
                        if category in ['03_开仓(全部成交)', '05_平仓(全部成交)']:
                            classified[classified_key] = []
                            logger.debug(f"🔄【私人数据处理】 [OKX订单] {symbol} {category} 已清空旧记录")
                        
                        # 获取现有订单ID列表用于去重（锁内快速读取）
                        if order_id and order_id != 'unknown':
                            existing_ids = []
                            for item in classified[classified_key]:
                                item_data = item.get('data', {})
                                if 'data' in item_data and isinstance(item_data['data'], list) and len(item_data['data']) > 0:
                                    existing_ids.append(item_data['data'][0].get('ordId'))
                    
                    # 第2步：锁外检查重复（无锁，安全）
                    if order_id and order_id != 'unknown' and order_id in existing_ids:
                        logger.debug(f"🔄【私人数据处理】 [OKX订单] 跳过重复订单: {order_id}")
                        return
                    
                    # 第3步：锁内写入数据
                    with self._lock:
                        classified = self.memory_store['private_data']['okx_order_update']['classified']
                        
                        # 再次检查分类是否存在
                        if classified_key not in classified:
                            classified[classified_key] = []
                        
                        # 再次检查重复（双重检查）
                        if order_id and order_id != 'unknown':
                            for item in classified[classified_key]:
                                item_data = item.get('data', {})
                                if 'data' in item_data and isinstance(item_data['data'], list) and len(item_data['data']) > 0:
                                    if item_data['data'][0].get('ordId') == order_id:
                                        logger.debug(f"🔄【私人数据处理】 [OKX订单] 跳过重复订单（二次检查）: {order_id}")
                                        return
                        
                        # 写入数据
                        if order_id and order_id != 'unknown':
                            classified[classified_key].append({
                                'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                                'received_at': private_data.get('received_at', datetime.now().isoformat()),
                                'data': raw_data
                            })
                            logger.debug(f"📦【私人数据处理】 [OKX订单] {symbol} {category} 订单 {order_id} 已保存")
                        else:
                            classified[classified_key].append({
                                'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                                'received_at': private_data.get('received_at', datetime.now().isoformat()),
                                'data': raw_data
                            })
                            logger.debug(f"📦【私人数据处理】 [OKX订单] {symbol} {category} 已保存")
                        
                        # ===== 平仓全部成交：启动独立线程清理 =====
                        if is_okx_closing(category):
                            self._okx_delayed_delete(symbol)
                            logger.info(f"⏰【私人数据处理】 [OKX订单] 平仓全部成交标记: {symbol} 清理线程已启动")
                    
                    await self._feed_full_storage_to_step1()
                    return
                    
                except Exception as e:
                    logger.error(f"❌【私人数据处理】 [OKX订单] 处理失败: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    return
            
            # ========== OKX持仓更新处理 ==========
            if exchange == 'okx' and private_data.get('data_type') == 'position_update':
                
                logger.debug(f"📥【私人数据处理】 [OKX持仓] 收到持仓更新")
                
                try:
                    storage_key = f"{exchange}_position_update"
                    
                    with self._lock:
                        self.memory_store['private_data'][storage_key] = {
                            'exchange': exchange,
                            'data_type': 'position_update',
                            'data': raw_data,
                            'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                            'received_at': private_data.get('received_at', datetime.now().isoformat())
                        }
                    
                    logger.debug(f"📦【私人数据处理】 [OKX持仓] 已保存: {storage_key}")
                    await self._feed_full_storage_to_step1()
                    
                except Exception as e:
                    logger.error(f"❌【私人数据处理】 [OKX持仓] 处理失败: {e}")
                
                return
            
            # ========== 其他数据类型 ==========
            if source == 'http_fetcher':
                final_data_type = private_data.get('data_type', 'unknown')
            else:
                event_type = raw_data.get('e', 'unknown')
                
                if exchange == 'binance':
                    if event_type == 'TRADE_LITE':
                        logger.debug(f"📨【私人数据处理】 [私人数据处理] 过滤掉 TRADE_LITE 事件")
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
            with self._lock:
                self.memory_store['private_data'][storage_key] = {
                    'exchange': exchange,
                    'data_type': final_data_type,
                    'data': raw_data,
                    'timestamp': private_data.get('timestamp', datetime.now().isoformat()),
                    'received_at': private_data.get('received_at', datetime.now().isoformat())
                }
            
            logger.debug(f"📦【私人数据处理】 已保存: {storage_key}")
            await self._feed_full_storage_to_step1()
            
        except Exception as e:
            logger.error(f"❌【私人数据处理】【Manager】接收数据失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def get_all_data(self) -> Dict[str, Any]:
        """获取所有私人数据概览"""
        try:
            formatted_data = {}
            for key, data in self.memory_store['private_data'].items():
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 异步方法中的循环让出CPU，避免数据量大时阻塞
                if key in ['binance_order_update', 'okx_order_update']:
                    classified = data.get('classified', {})
                    summary = {}
                    latest_timestamp = None
                    latest_received_at = None
                    
                    for k, v in classified.items():
                        summary[k] = len(v)
                        if v and isinstance(v, list) and len(v) > 0:
                            latest_item = v[-1]
                            item_ts = latest_item.get('timestamp')
                            item_ra = latest_item.get('received_at')
                            
                            if latest_timestamp is None or (item_ts and item_ts > latest_timestamp):
                                latest_timestamp = item_ts
                                latest_received_at = item_ra
                    
                    formatted_data[key] = {
                        "exchange": data.get('exchange'),
                        "data_type": data.get('data_type'),
                        "received_at": latest_received_at,
                        "timestamp": latest_timestamp,
                        "data_keys": list(classified.keys()) if classified else "No classified data",
                        "note": "订单数据的时间取自classified中的最新记录"
                    }
                else:
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
        """按交易所获取私人数据"""
        try:
            exchange_data = {}
            for key, data in self.memory_store['private_data'].items():
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 异步方法中的循环让出CPU，避免数据量大时阻塞
                if key.startswith(f"{exchange.lower()}_"):
                    
                    if key in ['binance_order_update', 'okx_order_update']:
                        classified = data.get('classified', {})
                        summary = {}
                        latest_timestamp = None
                        latest_received_at = None
                        
                        for k, v in classified.items():
                            summary[k] = len(v)
                            if v and isinstance(v, list) and len(v) > 0:
                                latest_item = v[-1]
                                item_ts = latest_item.get('timestamp')
                                item_ra = latest_item.get('received_at')
                                
                                if latest_timestamp is None or (item_ts and item_ts > latest_timestamp):
                                    latest_timestamp = item_ts
                                    latest_received_at = item_ra
                        
                        exchange_data[key] = {
                            "exchange": data.get('exchange'),
                            "data_type": data.get('data_type'),
                            "timestamp": latest_timestamp,
                            "received_at": latest_received_at,
                            "summary": summary,
                            "note": "各类别事件数量统计，详情请查询具体data_type"
                        }
                    else:
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