"""
第四步：资金费特殊处理
逻辑：
1. 收到container
2. 检查缓存
3. 按4种场景更新缓存
4. 用更新后的缓存覆盖container
5. 返回container（给调度器）
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class Step4Funding:
    """第四步：资金费处理"""
    
    def __init__(self):
        # 缓存整个container
        self.cache = {
            "binance": None,
            "okx": None
        }
        logger.info("✅【step4】资金费缓存已创建")
    
    def _round_4(self, value):
        if value is None:
            return None
        try:
            return round(float(value), 4)
        except (ValueError, TypeError):
            return None
    
    def _should_reset_funding(self, cached: Dict, new_data: Dict) -> bool:
        """
        判断是否需要重置资金费数据
        条件：
        1. 缓存中有结算时间（说明曾经有过持仓）
        2. 新数据中标记价仓位价值为空（说明当前无持仓）
        """
        if cached is None:
            return False
        
        # 缓存中有结算时间吗？
        cache_time = cached.get("本次资金费结算时间")
        if cache_time is None:
            return False
        
        # 新数据中标记价仓位价值为空吗？
        position_value = new_data.get("标记价仓位价值")
        if position_value is None:
            return True
        
        return False
    
    def _reset_funding_fields(self, cache_entry: Dict):
        """重置资金费相关字段"""
        cache_entry["本次资金费"] = 0
        cache_entry["累计资金费"] = 0
        cache_entry["资金费结算次数"] = 0
        cache_entry["平均资金费率"] = None
        cache_entry["本次资金费结算时间"] = None
        logger.debug(f"💰 资金费数据已重置（无持仓）")
    
    def process(self, container: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理资金费数据
        1. 先更新缓存
        2. 用缓存覆盖container
        3. 返回container
        """
        exchange = container.get("交易所")
        if not exchange or exchange not in self.cache:
            return container
        
        # 获取缓存的container
        cached = self.cache[exchange]
        
        # ===== 新增：检查是否需要重置资金费 =====
        if cached is not None and self._should_reset_funding(cached, container):
            logger.info(f"💰【{exchange}】检测到平仓，重置资金费数据")
            self._reset_funding_fields(cached)
            # 重置后，用重置后的缓存覆盖container
            for key, value in cached.items():
                container[key] = value  # 直接覆盖，不检查value是否为None
            return container
        
        # 获取本次的结算时间
        new_time = container.get("本次资金费结算时间")
        
        # ===== 第一次收到数据 =====
        if cached is None:
            logger.debug(f"💰【{exchange}】首次收到数据，直接缓存")
            self.cache[exchange] = container.copy()
            # 用缓存覆盖container
            for key, value in self.cache[exchange].items():
                container[key] = value  # 直接覆盖，不检查value是否为None
            return container
        
        # 获取缓存的结算时间
        cache_time = cached.get("本次资金费结算时间")
        
        # ===== 场景1：缓存时间为空 =====
        if cache_time is None:
            # 场景1a：新时间也为空
            if new_time is None:
                logger.debug(f"💰【{exchange}】场景1a：无结算，全字段覆盖缓存")
                self.cache[exchange] = container.copy()
                
            # 场景1b：新时间有值（首次结算）
            else:
                logger.debug(f"💰【{exchange}】场景1b：首次结算，时间={new_time}")
                # 先缓存新数据
                self.cache[exchange] = container.copy()
                # 更新5个资金费字段（累加）
                self._update_funding_fields(self.cache[exchange], cached, is_first=True)
        
        # ===== 场景2：缓存时间有值 =====
        else:
            # 场景2c：时间相同
            if cache_time == new_time:
                logger.debug(f"💰【{exchange}】场景2c：同次结算，资金费字段保持")
                # 其他字段覆盖缓存
                self._update_other_fields(cached, container)
                self.cache[exchange] = cached  # 确保缓存更新
            
            # 场景2d：时间不同（新结算）
            elif new_time is not None:
                logger.debug(f"💰【{exchange}】场景2d：新结算，时间={new_time}")
                # 先缓存新数据
                self.cache[exchange] = container.copy()
                # 更新5个资金费字段（累加）
                self._update_funding_fields(self.cache[exchange], cached, is_first=False)
        
        # ===== 用更新后的缓存覆盖传入的container =====
        for key, value in self.cache[exchange].items():
            container[key] = value  # 直接覆盖，不检查value是否为None
        
        return container
    
    def _update_funding_fields(self, new_cache: Dict, old_cache: Dict, is_first: bool):
        """更新5个资金费字段"""
        new_fee = new_cache.get("本次资金费")
        
        # 旧累计
        if is_first:
            old_total = 0
        else:
            old_total = float(old_cache.get("累计资金费") or 0)
        
        try:
            new_fee_float = float(new_fee) if new_fee is not None else 0
            new_total = old_total + new_fee_float
            new_cache["累计资金费"] = self._round_4(new_total)
            
            # 结算次数
            if is_first:
                new_cache["资金费结算次数"] = 1
            else:
                old_count = int(old_cache.get("资金费结算次数") or 0)
                new_cache["资金费结算次数"] = old_count + 1
            
            # 平均资金费率
            position_value = new_cache.get("开仓价仓位价值")
            if position_value is not None:
                try:
                    pv = float(position_value)
                    if pv > 0:
                        avg_rate = (new_total * 100) / pv
                        new_cache["平均资金费率"] = self._round_4(avg_rate)
                except (ValueError, TypeError, ZeroDivisionError):
                    pass
        except (ValueError, TypeError):
            pass
    
    def _update_other_fields(self, cached: Dict, new_data: Dict):
        """更新非资金费字段 - 直接覆盖，空值也覆盖"""
        funding_fields = [
            "本次资金费", "累计资金费", "资金费结算次数",
            "平均资金费率", "本次资金费结算时间"
        ]
        
        for key, value in new_data.items():
            if key not in funding_fields:  # 去掉 value is not None 的判断
                cached[key] = value