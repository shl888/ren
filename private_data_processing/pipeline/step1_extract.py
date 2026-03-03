"""
第一步：字段提取
按你写的9种数据类型提取：
1. http_account
2. account_update(ORDER)
3. account_update(FUNDING_FEE)
4. order_update(_02_开仓(全部成交))
5. order_update(_03_设置止损)
6. order_update(_04_设置止盈)
7. order_update(_06_触发止损(全部成交))
8. order_update(_08_触发止盈(全部成交))
9. order_update(_10_主动平仓(全部成交))
"""
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class Step1Extract:
    """第一步：字段提取"""
    
    def extract(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        提取字段
        
        Args:
            raw_data: 已经分类好的原始数据
            
        Returns:
            提取到的字段字典
        """
        exchange = raw_data.get('exchange', '')
        data_type = raw_data.get('data_type', '')
        
        # 只处理币安
        if exchange != 'binance':
            return None
        
        # ===== 1. http_account =====
        if data_type == 'http_account':
            return self._extract_http(raw_data)
        
        # ===== 2. account_update =====
        if data_type == 'account_update':
            return self._extract_account(raw_data)
        
        # ===== 3. order_update =====
        if data_type == 'order_update':
            return self._extract_order(raw_data)
        
        return None
    
    def _extract_http(self, raw_data: Dict) -> Dict[str, Any]:
        """提取 http_account 数据"""
        data = raw_data.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "http_account",
            "event_type": "http_account"
        }
        
        # 找 USDT
        assets = data.get('assets', [])
        for asset in assets:
            if asset.get('asset') == 'USDT':
                if asset.get('marginBalance') is not None:
                    result["账户资产额"] = asset['marginBalance']
                result["资产币种"] = "USDT"
                break
        
        # positions
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
    
    def _extract_account(self, raw_data: Dict) -> Optional[Dict[str, Any]]:
        """提取 account_update 数据"""
        data = raw_data.get('data', {})
        result = {
            "交易所": "binance",
            "data_type": "account_update"
        }
        
        m_type = data.get('a', {}).get('m', '')
        
        # ORDER
        if m_type == 'ORDER':
            p_data = data.get('a', {}).get('P', [])
            if p_data:
                result["event_type"] = "account_order"
                if p_data[0].get('mt') is not None:
                    result["保证金模式"] = p_data[0]['mt']
                if p_data[0].get('ma') is not None:
                    result["保证金币种"] = p_data[0]['ma']
                return result
        
        # FUNDING_FEE
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
    
    def _extract_order(self, raw_data: Dict) -> Optional[Dict[str, Any]]:
        """提取 order_update 数据"""
        classified = raw_data.get('classified', {})
        if not classified:
            return None
        
        # 遍历所有事件类型
        for event_key, event_list in classified.items():
            if not event_list:
                continue
            
            event = event_list[0]
            data = event.get('data', {})
            o_data = data.get('o', {})
            
            result = {
                "交易所": "binance",
                "data_type": "order_update",
                "event_type": event_key
            }
            
            # ===== 4. _02_开仓(全部成交) =====
            if '_02_开仓(全部成交)' in event_key:
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
                return result
            
            # ===== 5. _03_设置止损 =====
            elif '_03_设置止损' in event_key:
                if o_data.get('wt') is not None:
                    result["止损触发方式"] = o_data['wt']
                if o_data.get('sp') is not None:
                    result["止损触发价"] = o_data['sp']
                return result
            
            # ===== 6. _04_设置止盈 =====
            elif '_04_设置止盈' in event_key:
                if o_data.get('wt') is not None:
                    result["止盈触发方式"] = o_data['wt']
                if o_data.get('sp') is not None:
                    result["止盈触发价"] = o_data['sp']
                return result
            
            # ===== 7. _06_触发止损(全部成交) =====
            elif '_06_触发止损(全部成交)' in event_key:
                if o_data.get('ot') is not None:
                    result["平仓执行方式"] = o_data['ot']
                if o_data.get('ap') is not None:
                    result["平仓价"] = o_data['ap']
                if o_data.get('n') is not None:
                    result["平仓手续费"] = o_data['n']
                if o_data.get('N') is not None:
                    result["平仓手续费币种"] = o_data['N']
                if o_data.get('rp') is not None:
                    result["平仓收益"] = o_data['rp']
                if o_data.get('T') is not None:
                    result["平仓时间"] = o_data['T']
                return result
            
            # ===== 8. _08_触发止盈(全部成交) =====
            elif '_08_触发止盈(全部成交)' in event_key:
                if o_data.get('ot') is not None:
                    result["平仓执行方式"] = o_data['ot']
                if o_data.get('ap') is not None:
                    result["平仓价"] = o_data['ap']
                if o_data.get('n') is not None:
                    result["平仓手续费"] = o_data['n']
                if o_data.get('N') is not None:
                    result["平仓手续费币种"] = o_data['N']
                if o_data.get('rp') is not None:
                    result["平仓收益"] = o_data['rp']
                if o_data.get('T') is not None:
                    result["平仓时间"] = o_data['T']
                return result
            
            # ===== 9. _10_主动平仓(全部成交) =====
            elif '_10_主动平仓(全部成交)' in event_key:
                if o_data.get('ot') is not None:
                    result["平仓执行方式"] = o_data['ot']
                if o_data.get('ap') is not None:
                    result["平仓价"] = o_data['ap']
                if o_data.get('n') is not None:
                    result["平仓手续费"] = o_data['n']
                if o_data.get('N') is not None:
                    result["平仓手续费币种"] = o_data['N']
                if o_data.get('rp') is not None:
                    result["平仓收益"] = o_data['rp']
                if o_data.get('T') is not None:
                    result["平仓时间"] = o_data['T']
                return result
        
        return None