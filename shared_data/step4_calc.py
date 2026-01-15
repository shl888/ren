"""
第四步：单平台计算（统一缓存版 - 智能覆盖方案）
功能：统一缓存所有平台数据，所有计算基于缓存数据
原则：1. 先缓存后计算 2. 缓存为唯一数据源 3. 智能覆盖（有值覆盖，无值保留）
特点：智能覆盖+滚动更新，防止空数据覆盖历史缓存
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import time

logger = logging.getLogger(__name__)

@dataclass
class PlatformData:
    """单平台计算后的数据结构"""
    symbol: str
    exchange: str
    contract_name: str
    
    # 价格和费率
    latest_price: Optional[str] = None
    funding_rate: Optional[str] = None
    
    # 时间字段
    last_settlement_time: Optional[str] = None
    current_settlement_time: Optional[str] = None
    next_settlement_time: Optional[str] = None
    
    # 时间戳
    last_settlement_ts: Optional[int] = None
    current_settlement_ts: Optional[int] = None
    next_settlement_ts: Optional[int] = None
    
    # 计算结果
    period_seconds: Optional[int] = None
    countdown_seconds: Optional[int] = None

class Step4Calc:
    """第四步：单平台计算（统一缓存+智能覆盖方案）"""
    
    def __init__(self):
        # 统一缓存结构：symbol -> exchange -> 数据
        self.platform_cache = {}
        self.last_log_time = 0
        self.log_interval = 60  # 1分钟
        self.process_count = 0
        
    def process(self, aligned_results: List) -> List[PlatformData]:
        """
        统一处理流程：1.智能更新缓存 2.从缓存计算
        """
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        # 处理前日志 - 只在频率控制时打印
        if should_log:
            logger.info(f"🔄【流水线步骤4】开始处理 {len(aligned_results)} 个合约，采用智能缓存方案...")
        
        # 批次统计
        batch_stats = {
            "total_contracts": len(aligned_results),
            "okx_updated": 0,
            "binance_updated": 0,
            "binance_cache_protected": 0,  # 新增：缓存保护次数
            "okx_calculated": 0,
            "binance_calculated": 0,
            "calculation_errors": 0,
            "binance_rollover_symbols": set(),  # 触发滚动的合约集合
            "binance_with_history": 0,  # 有历史时间戳的币安合约
            
            # 成功率统计
            "okx_period_success": 0,
            "okx_period_fail": 0,
            "okx_countdown_success": 0,
            "okx_countdown_fail": 0,
            "binance_period_success": 0,
            "binance_period_fail": 0,
            "binance_countdown_success": 0,
            "binance_countdown_fail": 0,
        }
        
        all_results = []
        
        for item in aligned_results:
            try:
                symbol = item.symbol
                
                # 🔄 第一步：智能更新缓存（有值覆盖，无值保留）
                self._update_cache_smart(item, batch_stats)
                
                # 🔢 第二步：从缓存统一计算
                # OKX计算
                okx_data = self._calc_from_cache(symbol, "okx", batch_stats)
                if okx_data:
                    all_results.append(okx_data)
                    batch_stats["okx_calculated"] += 1
                
                # 币安计算
                binance_data = self._calc_from_cache(symbol, "binance", batch_stats)
                if binance_data:
                    all_results.append(binance_data)
                    batch_stats["binance_calculated"] += 1
                    
                    # 统计有历史数据的币安合约
                    if binance_data.last_settlement_ts:
                        batch_stats["binance_with_history"] += 1
                
            except Exception as e:
                batch_stats["calculation_errors"] += 1
                continue
        
        # 处理后日志 - 只在频率控制时打印
        if should_log:
            logger.info(f"✅【流水线步骤4】完成，共生成 {len(all_results)} 条数据")
            
            self._log_cache_status(batch_stats)
            self._log_calculation_report(batch_stats)
            
            self.last_log_time = current_time
            self.process_count = 0
        
        self.process_count += 1
        
        return all_results
    
    def _update_cache_smart(self, aligned_item, batch_stats: Dict[str, int]):
        """
        智能更新所有平台缓存
        原则：有值的项覆盖，无值的项保留原缓存
        """
        symbol = aligned_item.symbol
        
        # 初始化缓存结构
        if symbol not in self.platform_cache:
            self.platform_cache[symbol] = {}
        
        # 📥 更新OKX缓存（直接覆盖）
        if aligned_item.okx_current_ts:
            self.platform_cache[symbol]["okx"] = {
                "contract_name": aligned_item.okx_contract_name or "",
                "latest_price": aligned_item.okx_price,
                "funding_rate": aligned_item.okx_funding_rate,
                "last_settlement_time": None,
                "current_settlement_time": aligned_item.okx_current_settlement,
                "next_settlement_time": aligned_item.okx_next_settlement,
                "last_settlement_ts": None,
                "current_settlement_ts": aligned_item.okx_current_ts,
                "next_settlement_ts": aligned_item.okx_next_ts,
                "update_timestamp": time.time()
            }
            batch_stats["okx_updated"] += 1
        
        # 🔥 关键：智能更新币安缓存
        self._update_binance_smart(symbol, aligned_item, batch_stats)
    
    def _update_binance_smart(self, symbol: str, aligned_item, batch_stats: Dict[str, int]):
        """
        智能更新币安缓存：
        1. 第一次只要有任意数据就创建缓存
        2. 后续更新：有值的项覆盖，无值的项保留原缓存
        3. 检查本次结算时间变化 → 触发滚动更新
        """
        # 获取或初始化缓存
        if symbol not in self.platform_cache:
            self.platform_cache[symbol] = {}
        
        # 获取缓存状态（如果不存在则创建空缓存）
        if "binance" not in self.platform_cache[symbol]:
            self.platform_cache[symbol]["binance"] = {
                "contract_name": "",
                "latest_price": "",
                "funding_rate": "",
                "last_settlement_time": "",
                "current_settlement_time": "",
                "next_settlement_time": None,
                "last_settlement_ts": None,
                "current_settlement_ts": None,
                "next_settlement_ts": None,
                "has_rollover": False,
                "update_timestamp": 0
            }
        
        cache = self.platform_cache[symbol]["binance"]
        
        # 1. 检查滚动更新
        new_current_ts = aligned_item.binance_current_ts
        old_current_ts = cache.get("current_settlement_ts")
        
        if new_current_ts and old_current_ts and new_current_ts != old_current_ts:
            # 🔄 触发滚动：旧的本次 → 新的上次
            cache["last_settlement_ts"] = old_current_ts
            cache["last_settlement_time"] = cache.get("current_settlement_time", "")
            cache["has_rollover"] = True
            batch_stats["binance_rollover_symbols"].add(symbol)
        
        # 2. 智能覆盖：有值就覆盖，无值保留
        cache["update_timestamp"] = time.time()
        
        # 覆盖逻辑（只有有值时才覆盖）
        if aligned_item.binance_contract_name:
            cache["contract_name"] = aligned_item.binance_contract_name
        
        if aligned_item.binance_price:
            cache["latest_price"] = aligned_item.binance_price
        
        if aligned_item.binance_funding_rate:
            cache["funding_rate"] = aligned_item.binance_funding_rate
        
        if aligned_item.binance_last_settlement:
            cache["last_settlement_time"] = aligned_item.binance_last_settlement
        
        if aligned_item.binance_current_settlement:
            cache["current_settlement_time"] = aligned_item.binance_current_settlement
        
        # 🎯 关键：时间戳特殊处理
        if aligned_item.binance_last_ts:
            cache["last_settlement_ts"] = aligned_item.binance_last_ts
        
        if aligned_item.binance_current_ts:
            cache["current_settlement_ts"] = aligned_item.binance_current_ts
        
        # 3. 统计更新
        has_effective_update = any([
            aligned_item.binance_contract_name,
            aligned_item.binance_price,
            aligned_item.binance_funding_rate,
            aligned_item.binance_current_ts,
            aligned_item.binance_last_ts
        ])
        
        if has_effective_update:
            batch_stats["binance_updated"] += 1
        else:
            batch_stats["binance_cache_protected"] += 1
    
    def _calc_from_cache(self, symbol: str, exchange: str, batch_stats: Dict[str, int]) -> Optional[PlatformData]:
        """从缓存计算数据（唯一数据源）"""
        if symbol not in self.platform_cache:
            return None
        
        cache_data = self.platform_cache[symbol].get(exchange)
        if not cache_data:
            return None
        
        # 📊 从缓存构建数据对象
        if exchange == "okx":
            data = PlatformData(
                symbol=symbol,
                exchange="okx",
                contract_name=cache_data["contract_name"],
                latest_price=cache_data["latest_price"],
                funding_rate=cache_data["funding_rate"],
                last_settlement_time=cache_data["last_settlement_time"],
                current_settlement_time=cache_data["current_settlement_time"],
                next_settlement_time=cache_data["next_settlement_time"],
                last_settlement_ts=cache_data["last_settlement_ts"],
                current_settlement_ts=cache_data["current_settlement_ts"],
                next_settlement_ts=cache_data["next_settlement_ts"],
            )
            
            # 计算OKX费率周期（下次→上次）
            if data.current_settlement_ts and data.next_settlement_ts:
                data.period_seconds = (data.next_settlement_ts - data.current_settlement_ts) // 1000
                batch_stats["okx_period_success"] += 1
            else:
                batch_stats["okx_period_fail"] += 1
            
            # 计算倒计时
            data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
            if data.countdown_seconds is not None:
                batch_stats["okx_countdown_success"] += 1
            else:
                batch_stats["okx_countdown_fail"] += 1
            
        elif exchange == "binance":
            data = PlatformData(
                symbol=symbol,
                exchange="binance",
                contract_name=cache_data["contract_name"],
                latest_price=cache_data["latest_price"],
                funding_rate=cache_data["funding_rate"],
                last_settlement_time=cache_data["last_settlement_time"],
                current_settlement_time=cache_data["current_settlement_time"],
                next_settlement_time=cache_data["next_settlement_time"],
                last_settlement_ts=cache_data["last_settlement_ts"],
                current_settlement_ts=cache_data["current_settlement_ts"],
                next_settlement_ts=cache_data["next_settlement_ts"],
            )
            
            # 计算币安费率周期（本次→上次）- 有历史数据才计算
            if data.current_settlement_ts and data.last_settlement_ts:
                data.period_seconds = (data.current_settlement_ts - data.last_settlement_ts) // 1000
                batch_stats["binance_period_success"] += 1
            else:
                batch_stats["binance_period_fail"] += 1
            
            # 计算倒计时
            data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
            if data.countdown_seconds is not None:
                batch_stats["binance_countdown_success"] += 1
            else:
                batch_stats["binance_countdown_fail"] += 1
        
        else:
            return None
        
        return data
    
    def _calc_countdown(self, settlement_ts: Optional[int]) -> Optional[int]:
        """计算倒计时"""
        if not settlement_ts:
            return None
        
        try:
            now_ms = int(time.time() * 1000)
            countdown = max(0, (settlement_ts - now_ms) // 1000)
            return countdown
        except Exception:
            return None
    
    def _log_cache_status(self, batch_stats: Dict[str, int]):
        """打印缓存状态"""
        total_symbols = len(self.platform_cache)
        if total_symbols == 0:
            return
        
        # 统计缓存数据
        okx_count = 0
        binance_count = 0
        binance_with_history = 0
        
        for symbol, exchanges in self.platform_cache.items():
            if "okx" in exchanges:
                okx_count += 1
            if "binance" in exchanges:
                binance_count += 1
                if exchanges["binance"].get("last_settlement_ts"):
                    binance_with_history += 1
        
        logger.info("🗃️【流水线步骤4】缓存状态:")
        logger.info(f"  • 总缓存合约数: {total_symbols} 条")
        logger.info(f"  • OKX数据缓存: {okx_count} 条")
        logger.info(f"  • 币安数据缓存: {binance_count} 条")
        logger.info(f"  • 币安上次结算时间: 有{binance_with_history}条，无{binance_count - binance_with_history}条")
        
        # 保护统计
        protected_count = batch_stats.get("binance_cache_protected", 0)
        if protected_count > 0:
            logger.info(f"  • 缓存保护次数: {protected_count}次")
    
    def _log_calculation_report(self, batch_stats: Dict[str, int]):
        """打印计算报告"""
        logger.info("📊【流水线步骤4】计算报告:")
        
        # 费率周期计算统计
        logger.info(f"  • 费率周期计算:")
        logger.info(f"     - OKX: 成功{batch_stats['okx_period_success']}个，失败{batch_stats['okx_period_fail']}个")
        logger.info(f"     - 币安: 成功{batch_stats['binance_period_success']}个，失败{batch_stats['binance_period_fail']}个")
        
        # 倒计时计算统计
        logger.info(f"  • 倒计时计算:")
        logger.info(f"     - OKX: 成功{batch_stats['okx_countdown_success']}个，失败{batch_stats['okx_countdown_fail']}个")
        logger.info(f"     - 币安: 成功{batch_stats['binance_countdown_success']}个，失败{batch_stats['binance_countdown_fail']}个")
        
        # 滚动更新统计
        rollover_count = len(batch_stats["binance_rollover_symbols"])
        if rollover_count > 0:
            logger.info(f"  • 滚动更新合约数: {rollover_count}个")
    
    def get_cache_report(self) -> Dict[str, Any]:
        """获取完整缓存报告"""
        report = {
            "total_symbols": len(self.platform_cache),
            "okx_contracts": 0,
            "binance_contracts": 0,
            "binance_with_history": 0,
            "binance_without_history": 0,
            "symbols": {}
        }
        
        for symbol, exchanges in self.platform_cache.items():
            symbol_report = {}
            
            if "okx" in exchanges:
                report["okx_contracts"] += 1
                okx_cache = exchanges["okx"]
                symbol_report["okx"] = {
                    "last_time": okx_cache.get("last_settlement_time"),
                    "last_ts": okx_cache.get("last_settlement_ts"),
                    "current_time": okx_cache.get("current_settlement_time"),
                    "current_ts": okx_cache.get("current_settlement_ts"),
                    "next_time": okx_cache.get("next_settlement_time"),
                    "next_ts": okx_cache.get("next_settlement_ts"),
                }
            
            if "binance" in exchanges:
                report["binance_contracts"] += 1
                binance_cache = exchanges["binance"]
                has_history = bool(binance_cache.get("last_settlement_ts"))
                
                if has_history:
                    report["binance_with_history"] += 1
                else:
                    report["binance_without_history"] += 1
                
                symbol_report["binance"] = {
                    "last_time": binance_cache.get("last_settlement_time"),
                    "last_ts": binance_cache.get("last_settlement_ts"),
                    "current_time": binance_cache.get("current_settlement_time"),
                    "current_ts": binance_cache.get("current_settlement_ts"),
                    "next_time": binance_cache.get("next_settlement_time"),
                    "next_ts": binance_cache.get("next_settlement_ts"),
                    "has_history": has_history,
                    "has_rollover": binance_cache.get("has_rollover", False),
                }
            
            report["symbols"][symbol] = symbol_report
        
        return report
    
    def clear_cache(self):
        """清空缓存"""
        self.platform_cache.clear()
        logger.info("🗑️【流水线步骤4】缓存已清空")