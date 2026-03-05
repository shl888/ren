"""
第一步：从已存储的原始数据中提取字段
"""
import logging
import asyncio
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class Step1Extract:
    """第一步：字段提取"""

    # ===== 币安订单数据白名单（只提取这些分类）=====
    BINANCE_VALID_ORDER_EVENTS = {
        '_02_开仓(全部成交)',
        '_03_设置止损',
        '_04_设置止盈',
        '_06_触发止损(全部成交)',
        '_08_触发止盈(全部成交)',
        '_10_主动平仓(全部成交)'
    }

    def __init__(self, output_queue: asyncio.Queue):
        self.output_queue = output_queue
        
        # ===== 欧易相关缓存 =====
        self.okx_contract_cache = {}  # 面值缓存 { "BTCUSDT": 0.01, ... }
        self.okx_contract_loaded = False
        
        logger.info("✅【Step1】字段提取器已创建")
        logger.info(f"📋【Step1】币安有效订单事件: {self.BINANCE_VALID_ORDER_EVENTS}")

    async def receive(self, full_storage_item: Dict[str, Any]):
        """
        接收Manager塞进来的完整存储区数据
        格式: {'full_storage': {...}}
        用key精确路由到对应的处理函数
        """
        logger.info(f"🎯【Step1】收到完整存储区数据")
        
        try:
            # 获取完整存储区
            full_storage = full_storage_item.get('full_storage', {})
            if not full_storage:
                logger.warning(f"⚠️【Step1】收到空存储区")
                return
                
            logger.info(f"📦【Step1】存储区包含 {len(full_storage)} 个数据项")
            
            # 遍历存储区中的所有数据，用key精确路由
            all_results = []
            for key, data_item in full_storage.items():
                
                # ========== 币安房间 ==========
                if key == 'binance_order_update':
                    logger.info(f"🔍【Step1】处理币安订单数据")
                    pseudo_item = {
                        'exchange': 'binance',
                        'data_type': 'order_update',
                        'classified': data_item.get('classified', {})
                    }
                    results = self._extract_binance_orders(pseudo_item)
                    if results:
                        all_results.extend(results)
                        logger.info(f"✅【Step1】从 {key} 提取了 {len(results)} 条订单结果")
                
                elif key == 'binance_http_account':
                    logger.info(f"🔍【Step1】处理币安HTTP账户数据")
                    pseudo_item = {
                        'exchange': 'binance',
                        'data_type': 'http_account',
                        'data': data_item.get('data', {})
                    }
                    result = self._extract_binance_http(pseudo_item)
                    if result:
                        all_results.append(result)
                        logger.info(f"✅【Step1】从 {key} 提取了HTTP账户数据")
                
                elif key == 'binance_account_update':
                    logger.info(f"🔍【Step1】处理币安WebSocket账户更新")
                    pseudo_item = {
                        'exchange': 'binance',
                        'data_type': 'account_update',
                        'data': data_item.get('data', {})
                    }
                    result = self._extract_binance_account(pseudo_item)
                    if result:
                        all_results.append(result)
                        logger.info(f"✅【Step1】从 {key} 提取了账户更新数据")
                
                # ========== 欧易房间 ==========
                elif key == 'okx_contract_info':
                    logger.info(f"🔍【Step1】处理欧易合约面值数据")
                    self._extract_okx_contract(data_item)  # 只缓存，不输出
                
                elif key == 'okx_account_update':
                    logger.info(f"🔍【Step1】处理欧易账户数据")
                    result = self._extract_okx_account(data_item)
                    if result:
                        all_results.append(result)
                        logger.info(f"✅【Step1】从 {key} 提取了账户数据")
                    else:
                        logger.warning(f"⚠️【Step1】从 {key} 提取账户数据失败")
                
                elif key == 'okx_order_update':
                    logger.info(f"🔍【Step1】处理欧易订单数据")
                    results = self._extract_okx_orders(data_item)
                    if results:
                        # 小融合：给订单加上面值
                        for r in results:
                            symbol = r.get('开仓合约名')
                            if symbol:
                                r['合约面值'] = self.okx_contract_cache.get(symbol)
                        all_results.extend(results)
                        logger.info(f"✅【Step1】从 {key} 提取了 {len(results)} 条订单结果")
                    else:
                        logger.warning(f"⚠️【Step1】从 {key} 提取订单数据失败")
                
                elif key == 'okx_position_update':
                    logger.info(f"🔍【Step1】处理欧易持仓数据")
                    result = self._extract_okx_position(data_item)
                    if result:
                        all_results.append(result)
                        logger.info(f"✅【Step1】从 {key} 提取了持仓数据")
                    else:
                        logger.warning(f"⚠️【Step1】从 {key} 提取持仓数据失败")
                
                # ===== 其他key：暂不处理 =====
                else:
                    logger.debug(f"⏭️【Step1】跳过未处理的key: {key}")
            
            # 将所有结果推入队列
            if all_results:
                for i, result in enumerate(all_results):
                    await self.output_queue.put(result)
                    logger.info(f"📤【Step1】第{i+1}条结果已推入队列: {result.get('event_type', result.get('data_type', 'unknown'))}，队列大小: {self.output_queue.qsize()}")
                    logger.info(f"📤【Step1】第{i+1}条结果内容: {result}")
            else:
                logger.warning(f"⚠️【Step1】完整存储区未提取到任何结果")
                    
        except Exception as e:
            logger.error(f"❌【Step1】处理失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # ========== 币安提取函数（已有，保持不变）==========
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
                if b_data[0].get('bc') is not None:
                    result["本次资金费"] = b_data[0]['bc']
                if data.get('T') is not None:
                    result["本次资金费结算时间"] = data['T']
                return result

        return None

    def _extract_binance_orders(self, item: Dict) -> List[Dict[str, Any]]:
        """提取币安订单数据"""
        classified = item.get('classified', {})
        if not classified:
            logger.warning(f"⚠️【Step1】币安order_update无classified数据")
            return []

        logger.info(f"🔍【Step1】币安classified keys: {list(classified.keys())}")

        results = []
        for event_key, event_list in classified.items():
            parts = event_key.split('_', 1)
            if len(parts) < 2:
                continue
            
            event_type = f"_{parts[1]}"
            
            if event_type not in self.BINANCE_VALID_ORDER_EVENTS:
                logger.debug(f"⏭️【Step1】跳过非白名单事件: {event_type}")
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
                        result["开仓时间"] = o_data['T']

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
                        result["平仓时间"] = o_data['T']

                results.append(result)

        logger.info(f"📊【Step1】币安订单提取完成，共 {len(results)} 条")
        return results

    # ========== 欧易提取函数 ==========
    def _normalize_okx_symbol(self, symbol: str) -> str:
        """标准化欧易合约名：BTC-USDT-SWAP -> BTCUSDT"""
        if not symbol:
            return symbol
        without_swap = symbol.replace('-SWAP', '')
        return without_swap.replace('-USDT', '')

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
            logger.info(f"📦【Step1】缓存了 {len(self.okx_contract_cache)} 个欧易合约面值")
            logger.info(f"📦【Step1】缓存内容: {self.okx_contract_cache}")
        except Exception as e:
            logger.error(f"❌【Step1】提取欧易合约面值失败: {e}")

    def _extract_okx_account(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取欧易账户数据"""
        try:
            data = item.get('data', {})
            logger.info(f"🔍【Step1-欧易账户】原始数据: {data}")
            
            if not data.get('data'):
                logger.warning(f"⚠️【Step1-欧易账户】data.data 不存在")
                return None
            
            first_level = data['data']
            if not isinstance(first_level, list) or len(first_level) == 0:
                logger.warning(f"⚠️【Step1-欧易账户】data.data 不是数组或为空")
                return None
            
            details_list = first_level[0].get('details', [])
            if not isinstance(details_list, list) or len(details_list) == 0:
                logger.warning(f"⚠️【Step1-欧易账户】details 不存在或为空")
                return None
            
            details = details_list[0]
            logger.info(f"🔍【Step1-欧易账户】details内容: {details}")
            
            result = {"交易所": "okx", "data_type": "account_update"}
            
            eq = details.get('eq')
            if eq is not None and eq != '':
                result["账户资产额"] = eq
                logger.info(f"🔍【Step1-欧易账户】提取到账户资产额: {eq}")
            
            ccy = details.get('ccy')
            if ccy:
                result["资产币种"] = ccy
                logger.info(f"🔍【Step1-欧易账户】提取到资产币种: {ccy}")
            
            logger.info(f"📤【Step1-欧易账户】提取结果: {result}")
            return result if len(result) > 2 else None
            
        except Exception as e:
            logger.debug(f"⚠️【Step1】提取欧易账户数据异常: {e}")
            return None

    def _extract_okx_orders(self, item: Dict) -> List[Dict[str, Any]]:
        """提取欧易订单数据"""
        try:
            classified = item.get('classified', {})
            logger.info(f"🔍【Step1-欧易订单】classified内容: {classified}")
            
            if not classified:
                logger.warning(f"⚠️【Step1-欧易订单】classified为空")
                return []
            
            results = []
            for event_key, event_list in classified.items():
                logger.info(f"🔍【Step1-欧易订单】处理事件: {event_key}, 数量: {len(event_list)}")
                
                if '03_开仓(全部成交)' in event_key:
                    for i, event in enumerate(event_list):
                        logger.info(f"🔍【Step1-欧易订单】处理开仓事件 {i+1}")
                        result = self._extract_okx_open_order(event)
                        if result:
                            results.append(result)
                            logger.info(f"✅【Step1-欧易订单】开仓事件提取成功: {result}")
                        else:
                            logger.warning(f"⚠️【Step1-欧易订单】开仓事件提取失败")
                            
                elif '05_平仓(全部成交)' in event_key:
                    for i, event in enumerate(event_list):
                        logger.info(f"🔍【Step1-欧易订单】处理平仓事件 {i+1}")
                        result = self._extract_okx_close_order(event)
                        if result:
                            results.append(result)
                            logger.info(f"✅【Step1-欧易订单】平仓事件提取成功: {result}")
                        else:
                            logger.warning(f"⚠️【Step1-欧易订单】平仓事件提取失败")
            
            logger.info(f"📊【Step1-欧易订单】共提取 {len(results)} 条订单")
            return results
            
        except Exception as e:
            logger.debug(f"⚠️【Step1】提取欧易订单数据异常: {e}")
            return []

    def _extract_okx_open_order(self, event: Dict) -> Optional[Dict[str, Any]]:
        """提取欧易开仓订单字段"""
        try:
            data = event.get('data', {})
            logger.info(f"🔍【Step1-欧易开仓】event data: {data}")
            
            if not data.get('data'):
                logger.warning(f"⚠️【Step1-欧易开仓】data.data 不存在")
                return None
            
            data_list = data['data']
            if not isinstance(data_list, list) or len(data_list) == 0:
                logger.warning(f"⚠️【Step1-欧易开仓】data.data 不是数组或为空")
                return None
            
            order_data = data_list[0]
            logger.info(f"🔍【Step1-欧易开仓】order_data: {order_data}")
            
            result = {"交易所": "okx", "data_type": "order_update"}
            
            td_mode = order_data.get('tdMode')
            if td_mode:
                result["保证金模式"] = td_mode
                logger.info(f"🔍【Step1-欧易开仓】提取到保证金模式: {td_mode}")
            
            ccy = order_data.get('ccy')
            if ccy:
                result["保证金币种"] = ccy
                logger.info(f"🔍【Step1-欧易开仓】提取到保证金币种: {ccy}")
            
            inst_id = order_data.get('instId')
            if inst_id:
                result["开仓合约名"] = self._normalize_okx_symbol(inst_id)
                logger.info(f"🔍【Step1-欧易开仓】提取到开仓合约名: {result['开仓合约名']}")
            
            pos_side = order_data.get('posSide')
            if pos_side:
                result["开仓方向"] = pos_side
                logger.info(f"🔍【Step1-欧易开仓】提取到开仓方向: {pos_side}")
            
            ord_type = order_data.get('ordType')
            if ord_type:
                result["开仓执行方式"] = ord_type
                logger.info(f"🔍【Step1-欧易开仓】提取到开仓执行方式: {ord_type}")
            
            avg_px = order_data.get('avgPx')
            if avg_px is not None and avg_px != '':
                result["开仓价"] = avg_px
                logger.info(f"🔍【Step1-欧易开仓】提取到开仓价: {avg_px}")
            
            acc_fill_sz = order_data.get('accFillSz')
            if acc_fill_sz is not None and acc_fill_sz != '':
                result["持仓张数"] = acc_fill_sz
                logger.info(f"🔍【Step1-欧易开仓】提取到持仓张数: {acc_fill_sz}")
            
            lever = order_data.get('lever')
            if lever is not None and lever != '':
                result["杠杆"] = lever
                logger.info(f"🔍【Step1-欧易开仓】提取到杠杆: {lever}")
            
            fee = order_data.get('fee')
            if fee is not None and fee != '':
                result["开仓手续费"] = fee
                logger.info(f"🔍【Step1-欧易开仓】提取到开仓手续费: {fee}")
            
            fee_ccy = order_data.get('feeCcy')
            if fee_ccy:
                result["开仓手续费币种"] = fee_ccy
                logger.info(f"🔍【Step1-欧易开仓】提取到开仓手续费币种: {fee_ccy}")
            
            c_time = order_data.get('cTime')
            if c_time is not None and c_time != '':
                result["开仓时间"] = c_time
                logger.info(f"🔍【Step1-欧易开仓】提取到开仓时间: {c_time}")
            
            logger.info(f"📤【Step1-欧易开仓】提取结果: {result}")
            return result if len(result) > 2 else None
            
        except Exception as e:
            logger.debug(f"⚠️【Step1】提取欧易开仓订单异常: {e}")
            return None

    def _extract_okx_close_order(self, event: Dict) -> Optional[Dict[str, Any]]:
        """提取欧易平仓订单字段"""
        try:
            data = event.get('data', {})
            logger.info(f"🔍【Step1-欧易平仓】event data: {data}")
            
            if not data.get('data'):
                logger.warning(f"⚠️【Step1-欧易平仓】data.data 不存在")
                return None
            
            data_list = data['data']
            if not isinstance(data_list, list) or len(data_list) == 0:
                logger.warning(f"⚠️【Step1-欧易平仓】data.data 不是数组或为空")
                return None
            
            order_data = data_list[0]
            logger.info(f"🔍【Step1-欧易平仓】order_data: {order_data}")
            
            result = {"交易所": "okx", "data_type": "order_update"}
            
            ord_type = order_data.get('ordType')
            if ord_type:
                result["平仓执行方式"] = ord_type
                logger.info(f"🔍【Step1-欧易平仓】提取到平仓执行方式: {ord_type}")
            
            avg_px = order_data.get('avgPx')
            if avg_px is not None and avg_px != '':
                result["平仓价"] = avg_px
                logger.info(f"🔍【Step1-欧易平仓】提取到平仓价: {avg_px}")
            
            fee = order_data.get('fee')
            if fee is not None and fee != '':
                result["平仓手续费"] = fee
                logger.info(f"🔍【Step1-欧易平仓】提取到平仓手续费: {fee}")
            
            fee_ccy = order_data.get('feeCcy')
            if fee_ccy:
                result["平仓手续费币种"] = fee_ccy
                logger.info(f"🔍【Step1-欧易平仓】提取到平仓手续费币种: {fee_ccy}")
            
            u_time = order_data.get('uTime')
            if u_time is not None and u_time != '':
                result["平仓时间"] = u_time
                logger.info(f"🔍【Step1-欧易平仓】提取到平仓时间: {u_time}")
            
            logger.info(f"📤【Step1-欧易平仓】提取结果: {result}")
            return result if len(result) > 2 else None
            
        except Exception as e:
            logger.debug(f"⚠️【Step1】提取欧易平仓订单异常: {e}")
            return None

    def _extract_okx_position(self, item: Dict) -> Optional[Dict[str, Any]]:
        """提取欧易持仓数据"""
        try:
            data = item.get('data', {})
            logger.info(f"🔍【Step1-欧易持仓】原始数据: {data}")
            
            if not data.get('data'):
                logger.warning(f"⚠️【Step1-欧易持仓】data.data 不存在")
                return None
            
            data_list = data['data']
            if not isinstance(data_list, list) or len(data_list) == 0:
                logger.warning(f"⚠️【Step1-欧易持仓】data.data 不是数组或为空")
                return None
            
            pos_data = data_list[0]
            logger.info(f"🔍【Step1-欧易持仓】pos_data: {pos_data}")
            
            pos = pos_data.get('pos')
            logger.info(f"🔍【Step1-欧易持仓】pos值: {pos}")
            
            if pos is None or pos == '' or float(pos) == 0:
                logger.info(f"⏭️【Step1-欧易持仓】空仓，跳过提取")
                return None
            
            result = {"交易所": "okx", "data_type": "position_update"}
            
            # 标记价和最新价
            mark_px = pos_data.get('markPx')
            if mark_px is not None and mark_px != '':
                result["标记价"] = mark_px
                logger.info(f"🔍【Step1-欧易持仓】提取到标记价: {mark_px}")
            
            last = pos_data.get('last')
            if last is not None and last != '':
                result["最新价"] = last
                logger.info(f"🔍【Step1-欧易持仓】提取到最新价: {last}")
            
            # 持仓字段
            imr = pos_data.get('imr')
            if imr is not None and imr != '':
                result["标记价保证金"] = imr
                logger.info(f"🔍【Step1-欧易持仓】提取到标记价保证金: {imr}")
            
            upl = pos_data.get('upl')
            if upl is not None and upl != '':
                result["标记价浮盈"] = upl
                logger.info(f"🔍【Step1-欧易持仓】提取到标记价浮盈: {upl}")
            
            upl_ratio = pos_data.get('uplRatio')
            if upl_ratio is not None and upl_ratio != '':
                result["标记价浮盈百分比"] = upl_ratio
                logger.info(f"🔍【Step1-欧易持仓】提取到标记价浮盈百分比: {upl_ratio}")
            
            upl_last_px = pos_data.get('uplLastPx')
            if upl_last_px is not None and upl_last_px != '':
                result["最新价浮盈"] = upl_last_px
                logger.info(f"🔍【Step1-欧易持仓】提取到最新价浮盈: {upl_last_px}")
            
            upl_ratio_last_px = pos_data.get('uplRatioLastPx')
            if upl_ratio_last_px is not None and upl_ratio_last_px != '':
                result["最新价浮盈百分比"] = upl_ratio_last_px
                logger.info(f"🔍【Step1-欧易持仓】提取到最新价浮盈百分比: {upl_ratio_last_px}")
            
            # ===== 修正：从 closeOrderAlgo 数组中提取止损止盈 =====
            close_order_algo = pos_data.get('closeOrderAlgo', [])
            if close_order_algo and len(close_order_algo) > 0:
                algo = close_order_algo[0]
                
                sl_trigger_px_type = algo.get('slTriggerPxType')
                if sl_trigger_px_type:
                    result["止损触发方式"] = sl_trigger_px_type
                    logger.info(f"🔍【Step1-欧易持仓】提取到止损触发方式: {sl_trigger_px_type}")
                
                sl_trigger_px = algo.get('slTriggerPx')
                if sl_trigger_px is not None and sl_trigger_px != '':
                    result["止损触发价"] = sl_trigger_px
                    logger.info(f"🔍【Step1-欧易持仓】提取到止损触发价: {sl_trigger_px}")
                
                tp_trigger_px_type = algo.get('tpTriggerPxType')
                if tp_trigger_px_type:
                    result["止盈触发方式"] = tp_trigger_px_type
                    logger.info(f"🔍【Step1-欧易持仓】提取到止盈触发方式: {tp_trigger_px_type}")
                
                tp_trigger_px = algo.get('tpTriggerPx')
                if tp_trigger_px is not None and tp_trigger_px != '':
                    result["止盈触发价"] = tp_trigger_px
                    logger.info(f"🔍【Step1-欧易持仓】提取到止盈触发价: {tp_trigger_px}")
            
            funding_fee = pos_data.get('fundingFee')
            if funding_fee is not None and funding_fee != '':
                result["累计资金费"] = funding_fee
                logger.info(f"🔍【Step1-欧易持仓】提取到累计资金费: {funding_fee}")
            
            logger.info(f"📤【Step1-欧易持仓】提取结果: {result}")
            return result if len(result) > 2 else None
            
        except Exception as e:
            logger.debug(f"⚠️【Step1】提取欧易持仓数据异常: {e}")
            return None