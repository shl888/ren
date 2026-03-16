"""
第一步：从已存储的原始数据中提取字段
==================================================
【文件职责】
1. 接收Manager的完整存储区
2. 并行提取所有key的数据字段
3. 返回提取结果列表给调度器

【重要变更 - 2026.03.12】
==================================================
1. 彻底改造为并行处理：
   - 为每个key创建独立异步任务
   - 同步提取函数放在线程池执行，不阻塞事件循环
   - 所有key同时处理，互不影响

2. 移除队列依赖：
   - 不再接收output_queue参数
   - receive()直接返回结果列表
   - 调度器负责后续处理

3. 性能优化：
   - 串行 → 并行，速度提升N倍（N=key数量）
   - 同步阻塞 → 线程池，事件循环畅通无阻
   - 整体延迟从"所有key处理时间之和"降为"最慢key的处理时间"
==================================================
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class Step1Extract:
    """第一步：字段提取器（并行优化版）"""

    # ===== 币安订单数据白名单（只提取这些分类）=====
    BINANCE_VALID_ORDER_EVENTS = {
        '_02_开仓(全部成交)',
        '_03_设置止损',
        '_04_设置止盈',
        '_06_触发止损(全部成交)',
        '_08_触发止盈(全部成交)',
        '_10_主动平仓(全部成交)'
    }

    def __init__(self):
        """初始化提取器（不再需要队列参数）"""
        # ===== 欧易相关缓存 =====
        self.okx_contract_cache = {}  # 面值缓存 { "BTCUSDT": 0.01, ... }
        self.okx_contract_loaded = False
        
        # 线程池执行器（用于同步提取函数）
        self._executor = None
        
        logger.info("✅【私人step1】字段提取器已创建（并行优化版）")
        logger.debug(f"📋【私人step1】币安有效订单事件: {self.BINANCE_VALID_ORDER_EVENTS}")

    def _get_executor(self):
        """懒加载线程池执行器"""
        if self._executor is None:
            import concurrent.futures
            # 创建线程池，默认使用CPU核心数*5
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=20,
                thread_name_prefix="step1_extract"
            )
        return self._executor

    # ===== 时间戳转换函数 =====
    def _convert_timestamp(self, timestamp_ms: Optional[Any]) -> Optional[str]:
        """将毫秒级时间戳转换为北京时间 (2026.03.16 08:00:03)"""
        if timestamp_ms is None or timestamp_ms == '':
            return None
        try:
            # 转换为毫秒整数
            ts = int(float(timestamp_ms))
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            beijing_tz = timezone(timedelta(hours=8))
            beijing_time = dt.astimezone(beijing_tz)
            return beijing_time.strftime("%Y.%m.%d %H:%M:%S")
        except (ValueError, TypeError, OverflowError):
            logger.debug(f"时间戳转换失败: {timestamp_ms}")
            return None

    async def receive(self, full_storage_item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        接收Manager塞进来的完整存储区数据
        ==================================================
        并行处理所有key，返回提取结果列表
        
        :param full_storage_item: 格式 {'full_storage': {...}}
        :return: 提取结果列表，每个元素是一条提取的数据
        ==================================================
        """
        logger.info(f"🎯【私人step1】收到完整存储区数据")
        
        try:
            # 获取完整存储区
            full_storage = full_storage_item.get('full_storage', {})
            if not full_storage:
                logger.warning(f"⚠️【私人step1】收到空存储区")
                return []
                
            logger.debug(f"📦【私人step1】存储区包含 {len(full_storage)} 个数据项: {list(full_storage.keys())}")
            
            # ===== 并行处理所有key =====
            # 为每个key创建独立异步任务
            tasks = []
            for key, data_item in full_storage.items():
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU，避免大量key阻塞事件循环
                task = asyncio.create_task(self._process_key_parallel(key, data_item))
                tasks.append(task)
            
            # 等待所有key处理完成（使用return_exceptions避免单个失败影响整体）
            results_lists = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 合并结果
            all_results = []
            for i, results in enumerate(results_lists):
                if isinstance(results, Exception):
                    # 记录异常但继续处理其他结果
                    logger.error(f"❌【私人step1】处理key失败: {results}")
                elif results:
                    all_results.extend(results)
            
            logger.debug(f"📊【私人step1】并行处理完成，共提取 {len(all_results)} 条结果")
            
            # 如果有大量结果，可以抽样日志
            if len(all_results) > 10:
                logger.debug(f"📊【私人step1】结果预览: {all_results[:3]} ... (共{len(all_results)}条)")
            
            return all_results
            
        except Exception as e:
            logger.error(f"❌【私人step1】处理失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    async def _process_key_parallel(self, key: str, data_item: Dict) -> List[Dict[str, Any]]:
        """
        并行处理单个key的数据
        ==================================================
        根据key路由到对应的提取函数，所有提取函数都在线程池执行
        
        :param key: 数据键名，如 'binance_order_update'
        :param data_item: 该key对应的数据
        :return: 提取结果列表
        ==================================================
        """
        try:
            logger.debug(f"🔍【私人step1】开始处理key: {key}")
            
            # 获取线程池执行器
            executor = self._get_executor()
            loop = asyncio.get_event_loop()
            
            # ========== 币安订单 ==========
            if key == 'binance_order_update':
                pseudo_item = {
                    'exchange': 'binance',
                    'data_type': 'order_update',
                    'classified': data_item.get('classified', {})
                }
                # 在线程池执行同步提取函数
                results = await loop.run_in_executor(
                    executor,
                    self._extract_binance_orders,
                    pseudo_item
                )
                if results:
                    logger.debug(f"✅【私人step1】从 {key} 提取了 {len(results)} 条订单结果")
                return results or []
            
            # ========== 币安HTTP账户 ==========
            elif key == 'binance_http_account':
                pseudo_item = {
                    'exchange': 'binance',
                    'data_type': 'http_account',
                    'data': data_item.get('data', {})
                }
                result = await loop.run_in_executor(
                    executor,
                    self._extract_binance_http,
                    pseudo_item
                )
                return [result] if result else []
            
            # ========== 币安WebSocket账户更新 ==========
            elif key == 'binance_account_update':
                pseudo_item = {
                    'exchange': 'binance',
                    'data_type': 'account_update',
                    'data': data_item.get('data', {})
                }
                result = await loop.run_in_executor(
                    executor,
                    self._extract_binance_account,
                    pseudo_item
                )
                return [result] if result else []
            
            # ========== 欧易合约信息（只缓存，不输出）==========
            elif key == 'okx_contract_info':
                await loop.run_in_executor(
                    executor,
                    self._extract_okx_contract,
                    data_item
                )
                return []
            
            # ========== 欧易账户 ==========
            elif key == 'okx_account_update':
                result = await loop.run_in_executor(
                    executor,
                    self._extract_okx_account,
                    data_item
                )
                return [result] if result else []
            
            # ========== 欧易订单 ==========
            elif key == 'okx_order_update':
                results = await loop.run_in_executor(
                    executor,
                    self._extract_okx_orders,
                    data_item
                )
                
                # 小融合：给订单加上面值（在异步上下文中执行）
                if results:
                    for r in results:
                        symbol = r.get('开仓合约名')
                        if symbol:
                            r['合约面值'] = self.okx_contract_cache.get(symbol)
                    logger.debug(f"✅【私人step1】从 {key} 提取了 {len(results)} 条订单结果，并添加面值")
                
                return results or []
            
            # ========== 欧易持仓 ==========
            elif key == 'okx_position_update':
                result = await loop.run_in_executor(
                    executor,
                    self._extract_okx_position,
                    data_item
                )
                return [result] if result else []
            
            # ========== 其他未处理的key ==========
            else:
                logger.debug(f"⏭️【私人step1】跳过未处理的key: {key}")
                return []
                
        except Exception as e:
            logger.error(f"❌【私人step1】处理key {key} 异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    # ========== 币安提取函数（原样保留，都是同步函数）==========
    def _extract_binance_http(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取币安HTTP账户数据"""
        data = item.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "http_account",
            "event_type": "http_account"
        }

        assets = data.get('assets', [])
        for asset in assets:
            if asset.get('asset') == 'USDT':
                if asset.get('marginBalance') is not None:
                    result["账户资产额"] = asset['marginBalance']
                result["资产币种"] = "USDT"
                break

        positions = data.get('positions', [])
        if positions:
            pos = positions[0]
            if pos.get('initialMargin') is not None:
                result["标记价保证金"] = pos['initialMargin']
            if pos.get('notional') is not None:
                result["标记价仓位价值"] = pos['notional']
            if pos.get('unrealizedProfit') is not None:
                result["标记价浮盈"] = pos['unrealizedProfit']

        return result

    def _extract_binance_account(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取币安账户更新数据"""
        data = item.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "account_update"
        }

        m_type = data.get('a', {}).get('m', '')

        if m_type == 'ORDER':
            p_data = data.get('a', {}).get('P', [])
            if p_data:
                result["event_type"] = "account_order"
                if p_data[0].get('mt') is not None:
                    result["保证金模式"] = p_data[0]['mt']
                if p_data[0].get('ma') is not None:
                    result["保证金币种"] = p_data[0]['ma']
                return result

        elif m_type == 'FUNDING_FEE':
            b_data = data.get('a', {}).get('B', [])
            if b_data:
                result["event_type"] = "account_funding"
                if data.get('T') is not None:
                    result["本次资金费结算时间"] = self._convert_timestamp(data['T'])
                if b_data[0].get('bc') is not None:
                    result["本次资金费"] = b_data[0]['bc']
                return result

        return None

    def _extract_binance_orders(self, item: Dict) -> List[Dict[str, Any]]:
        """提取币安订单数据"""
        classified = item.get('classified', {})
        if not classified:
            logger.warning(f"⚠️【私人step1】币安order_update无classified数据")
            return []

        logger.debug(f"🔍【私人step1】币安classified keys: {list(classified.keys())}")

        results = []
        for event_key, event_list in classified.items():
            parts = event_key.split('_', 1)
            if len(parts) < 2:
                continue
            
            event_type = f"_{parts[1]}"
            
            if event_type not in self.BINANCE_VALID_ORDER_EVENTS:
                logger.debug(f"⏭️【私人step1】跳过非白名单事件: {event_type}")
                continue

            for event in event_list:
                data = event.get('data', {})
                o_data = data.get('o', {})

                result = {
                    "交易所": "binance",
                    "data_type": "order_update",
                    "event_type": event_key
                }

                if '开仓' in parts[1]:
                    if o_data.get('s') is not None:
                        result["开仓合约名"] = o_data['s']
                    if o_data.get('ps') is not None:
                        result["开仓方向"] = o_data['ps']
                    if o_data.get('ot') is not None:
                        result["开仓执行方式"] = o_data['ot']
                    if o_data.get('ap') is not None:
                        result["开仓价"] = o_data['ap']
                    if o_data.get('z') is not None:
                        result["持仓币数"] = o_data['z']
                    if o_data.get('n') is not None:
                        result["开仓手续费"] = o_data['n']
                    if o_data.get('N') is not None:
                        result["开仓手续费币种"] = o_data['N']
                    if o_data.get('T') is not None:
                        result["开仓时间"] = self._convert_timestamp(o_data['T'])

                elif '设置止损' in parts[1]:
                    if o_data.get('wt') is not None:
                        result["止损触发方式"] = o_data['wt']
                    if o_data.get('sp') is not None:
                        result["止损触发价"] = o_data['sp']

                elif '设置止盈' in parts[1]:
                    if o_data.get('wt') is not None:
                        result["止盈触发方式"] = o_data['wt']
                    if o_data.get('sp') is not None:
                        result["止盈触发价"] = o_data['sp']

                elif any(x in parts[1] for x in ['触发止损', '触发止盈', '主动平仓']):
                    if o_data.get('ot') is not None:
                        result["平仓执行方式"] = o_data['ot']
                    if o_data.get('ap') is not None:
                        result["平仓价"] = o_data['ap']
                    if o_data.get('n') is not None:
                        result["平仓手续费"] = o_data['n']
                    if o_data.get('N') is not None:
                        result["平仓手续费币种"] = o_data['N']
                    if o_data.get('T') is not None:
                        result["平仓时间"] = self._convert_timestamp(o_data['T'])

                results.append(result)

        logger.debug(f"📊【私人step1】币安订单提取完成，共 {len(results)} 条")
        return results

    # ========== 欧易工具函数 ==========
    def _normalize_okx_symbol(self, symbol: str) -> str:
        """标准化欧易合约名：BTC-USDT-SWAP -> BTCUSDT"""
        if not symbol:
            return symbol
        # 先去掉 -SWAP，再去掉所有 -
        return symbol.replace('-SWAP', '').replace('-', '')

    # ========== 欧易提取函数（原样保留，都是同步函数）==========
    def _extract_okx_contract(self, item: Dict):
        """提取欧易合约面值数据 - 只缓存，不输出"""
        if self.okx_contract_loaded:
            return
        
        try:
            data = item.get('data', {})
            contracts = data.get('contracts', [])
            
            for c in contracts:
                raw_symbol = c.get('instId')
                if raw_symbol:
                    symbol = self._normalize_okx_symbol(raw_symbol)
                    ct_val = c.get('ctVal')
                    if ct_val is not None:
                        self.okx_contract_cache[symbol] = ct_val
            
            self.okx_contract_loaded = True
            logger.info(f"📦【私人step1】缓存了 {len(self.okx_contract_cache)} 个欧易合约面值")
            logger.debug(f"📦【私人step1】缓存内容: {self.okx_contract_cache}")
        except Exception as e:
            logger.error(f"❌【私人step1】提取欧易合约面值失败: {e}")

    def _extract_okx_account(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取欧易账户数据"""
        try:
            data = item.get('data', {})
            logger.debug(f"🔍【私人step1-欧易账户】原始数据: {data}")
            
            if not data.get('data'):
                logger.debug(f"⚠️【私人step1-欧易账户】data.data 不存在")
                return None
            
            first_level = data['data']
            if not isinstance(first_level, list) or len(first_level) == 0:
                logger.debug(f"⚠️【私人step1-欧易账户】data.data 不是数组或为空")
                return None
            
            details_list = first_level[0].get('details', [])
            if not isinstance(details_list, list) or len(details_list) == 0:
                logger.debug(f"⚠️【私人step1-欧易账户】details 不存在或为空")
                return None
            
            details = details_list[0]
            logger.debug(f"🔍【私人step1-欧易账户】details内容: {details}")
            
            result = {"交易所": "okx", "data_type": "account_update"}
            
            eq = details.get('eq')
            if eq is not None and eq != '':
                result["账户资产额"] = eq
                logger.debug(f"🔍【私人step1-欧易账户】提取到账户资产额: {eq}")
            
            ccy = details.get('ccy')
            if ccy:
                result["资产币种"] = ccy
                logger.debug(f"🔍【私人step1-欧易账户】提取到资产币种: {ccy}")
            
            logger.debug(f"📤【私人step1-欧易账户】提取结果: {result}")
            return result if len(result) > 2 else None
            
        except Exception as e:
            logger.debug(f"⚠️【私人step1】提取欧易账户数据异常: {e}")
            return None

    def _extract_okx_orders(self, item: Dict) -> List[Dict[str, Any]]:
        """提取欧易订单数据"""
        try:
            classified = item.get('classified', {})
            logger.debug(f"🔍【私人step1-欧易订单】classified内容: {classified}")
            
            if not classified:
                logger.debug(f"⚠️【私人step1-欧易订单】classified为空")
                return []
            
            results = []
            for event_key, event_list in classified.items():
                logger.debug(f"🔍【私人step1-欧易订单】处理事件: {event_key}, 数量: {len(event_list)}")
                
                if '03_开仓(全部成交)' in event_key:
                    for i, event in enumerate(event_list):
                        result = self._extract_okx_open_order(event)
                        if result:
                            results.append(result)
                            logger.debug(f"✅【私人step1-欧易订单】开仓事件提取成功")
                            
                elif '05_平仓(全部成交)' in event_key:
                    for i, event in enumerate(event_list):
                        result = self._extract_okx_close_order(event)
                        if result:
                            results.append(result)
                            logger.debug(f"✅【私人step1-欧易订单】平仓事件提取成功")
            
            logger.debug(f"📊【私人step1-欧易订单】共提取 {len(results)} 条订单")
            return results
            
        except Exception as e:
            logger.debug(f"⚠️【私人step1】提取欧易订单数据异常: {e}")
            return []

    def _extract_okx_open_order(self, event: Dict) -> Optional[Dict[str, Any]]:
        """提取欧易开仓订单字段"""
        try:
            data = event.get('data', {})
            logger.debug(f"🔍【私人step1-欧易开仓】event data: {data}")
            
            if not data.get('data'):
                logger.debug(f"⚠️【私人step1-欧易开仓】data.data 不存在")
                return None
            
            data_list = data['data']
            if not isinstance(data_list, list) or len(data_list) == 0:
                logger.debug(f"⚠️【私人step1-欧易开仓】data.data 不是数组或为空")
                return None
            
            order_data = data_list[0]
            logger.debug(f"🔍【私人step1-欧易开仓】order_data: {order_data}")
            
            result = {"交易所": "okx", "data_type": "order_update"}
            
            td_mode = order_data.get('tdMode')
            if td_mode:
                result["保证金模式"] = td_mode
            
            ccy = order_data.get('ccy')
            if ccy:
                result["保证金币种"] = ccy
            
            inst_id = order_data.get('instId')
            if inst_id:
                result["开仓合约名"] = self._normalize_okx_symbol(inst_id)
            
            pos_side = order_data.get('posSide')
            if pos_side:
                if pos_side == "long":
                    result["开仓方向"] = "LONG"
                elif pos_side == "short":
                    result["开仓方向"] = "SHORT"
                else:
                    result["开仓方向"] = pos_side
            
            ord_type = order_data.get('ordType')
            if ord_type:
                if ord_type == "market":
                    result["开仓执行方式"] = "MARKET"
                elif ord_type == "limit":
                    result["开仓执行方式"] = "LIMIT"
                else:
                    result["开仓执行方式"] = ord_type
            
            avg_px = order_data.get('avgPx')
            if avg_px is not None and avg_px != '':
                result["开仓价"] = avg_px
            
            acc_fill_sz = order_data.get('accFillSz')
            if acc_fill_sz is not None and acc_fill_sz != '':
                result["持仓张数"] = acc_fill_sz
            
            lever = order_data.get('lever')
            if lever is not None and lever != '':
                result["杠杆"] = lever
            
            fee = order_data.get('fee')
            if fee is not None and fee != '':
                result["开仓手续费"] = fee
            
            fee_ccy = order_data.get('feeCcy')
            if fee_ccy:
                result["开仓手续费币种"] = fee_ccy
            
            c_time = order_data.get('cTime')
            if c_time is not None and c_time != '':
                result["开仓时间"] = self._convert_timestamp(c_time)
            
            logger.debug(f"📤【私人step1-欧易开仓】提取结果: {result}")
            return result if len(result) > 2 else None
            
        except Exception as e:
            logger.debug(f"⚠️【私人step1】提取欧易开仓订单异常: {e}")
            return None

    def _extract_okx_close_order(self, event: Dict) -> Optional[Dict[str, Any]]:
        """提取欧易平仓订单字段"""
        try:
            data = event.get('data', {})
            logger.debug(f"🔍【私人step1-欧易平仓】event data: {data}")
            
            if not data.get('data'):
                logger.debug(f"⚠️【私人step1-欧易平仓】data.data 不存在")
                return None
            
            data_list = data['data']
            if not isinstance(data_list, list) or len(data_list) == 0:
                logger.debug(f"⚠️【私人step1-欧易平仓】data.data 不是数组或为空")
                return None
            
            order_data = data_list[0]
            logger.debug(f"🔍【私人step1-欧易平仓】order_data: {order_data}")
            
            result = {"交易所": "okx", "data_type": "order_update"}
            
            ord_type = order_data.get('ordType')
            if ord_type:
                if ord_type == "market":
                    result["平仓执行方式"] = "MARKET"
                elif ord_type == "limit":
                    result["平仓执行方式"] = "LIMIT"
                else:
                    result["平仓执行方式"] = ord_type
            
            avg_px = order_data.get('avgPx')
            if avg_px is not None and avg_px != '':
                result["平仓价"] = avg_px
            
            fee = order_data.get('fee')
            if fee is not None and fee != '':
                result["平仓手续费"] = fee
            
            fee_ccy = order_data.get('feeCcy')
            if fee_ccy:
                result["平仓手续费币种"] = fee_ccy
            
            u_time = order_data.get('uTime')
            if u_time is not None and u_time != '':
                result["平仓时间"] = self._convert_timestamp(u_time)
            
            # 从instId提取合约名（用于关联）
            if order_data.get('instId'):
                result["开仓合约名"] = self._normalize_okx_symbol(order_data['instId'])
            
            logger.debug(f"📤【私人step1-欧易平仓】提取结果: {result}")
            return result if len(result) > 2 else None
            
        except Exception as e:
            logger.debug(f"⚠️【私人step1】提取欧易平仓订单异常: {e}")
            return None

    def _extract_okx_position(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取欧易持仓数据"""
        try:
            data = item.get('data', {})
            logger.debug(f"🔍【私人step1-欧易持仓】原始数据: {data}")
            
            if not data.get('data'):
                logger.debug(f"⚠️【私人step1-欧易持仓】data.data 不存在")
                return None
            
            data_list = data['data']
            if not isinstance(data_list, list) or len(data_list) == 0:
                logger.debug(f"⚠️【私人step1-欧易持仓】data.data 不是数组或为空")
                return None
            
            pos_data = data_list[0]
            logger.debug(f"🔍【私人step1-欧易持仓】pos_data: {pos_data}")
            
            pos = pos_data.get('pos')
            logger.debug(f"🔍【私人step1-欧易持仓】pos值: {pos}")
            
            if pos is None or pos == '' or float(pos) == 0:
                logger.debug(f"⏭️【私人step1-欧易持仓】空仓，跳过提取")
                return None
            
            result = {"交易所": "okx", "data_type": "position_update"}
            
            # 标记价和最新价
            mark_px = pos_data.get('markPx')
            if mark_px is not None and mark_px != '':
                result["标记价"] = mark_px
            
            last = pos_data.get('last')
            if last is not None and last != '':
                result["最新价"] = last
            
            # 持仓字段
            imr = pos_data.get('imr')
            if imr is not None and imr != '':
                result["标记价保证金"] = imr
            
            upl = pos_data.get('upl')
            if upl is not None and upl != '':
                result["标记价浮盈"] = upl
            
            upl_ratio = pos_data.get('uplRatio')
            if upl_ratio is not None and upl_ratio != '':
                result["标记价浮盈百分比"] = upl_ratio
            
            upl_last_px = pos_data.get('uplLastPx')
            if upl_last_px is not None and upl_last_px != '':
                result["最新价浮盈"] = upl_last_px
            
            upl_ratio_last_px = pos_data.get('uplRatioLastPx')
            if upl_ratio_last_px is not None and upl_ratio_last_px != '':
                result["最新价浮盈百分比"] = upl_ratio_last_px
            
            # 从 closeOrderAlgo 数组中提取止损止盈
            close_order_algo = pos_data.get('closeOrderAlgo', [])
            if close_order_algo and len(close_order_algo) > 0:
                algo = close_order_algo[0]
                
                sl_trigger_px_type = algo.get('slTriggerPxType')
                if sl_trigger_px_type:
                    result["止损触发方式"] = sl_trigger_px_type
                
                sl_trigger_px = algo.get('slTriggerPx')
                if sl_trigger_px is not None and sl_trigger_px != '':
                    result["止损触发价"] = sl_trigger_px
                
                tp_trigger_px_type = algo.get('tpTriggerPxType')
                if tp_trigger_px_type:
                    result["止盈触发方式"] = tp_trigger_px_type
                
                tp_trigger_px = algo.get('tpTriggerPx')
                if tp_trigger_px is not None and tp_trigger_px != '':
                    result["止盈触发价"] = tp_trigger_px
            
            funding_fee = pos_data.get('fundingFee')
            if funding_fee is not None and funding_fee != '':
                result["累计资金费"] = funding_fee
            
            logger.debug(f"📤【私人step1-欧易持仓】提取结果: {result}")
            return result if len(result) > 2 else None
            
        except Exception as e:
            logger.debug(f"⚠️【私人step1】提取欧易持仓数据异常: {e}")
            return None