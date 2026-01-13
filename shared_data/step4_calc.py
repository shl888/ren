"""
第四步：单平台计算（修复版）
功能：1. 币安时间滚动 2. 费率周期 3. 倒计时
修正：时间字段直接保留Step3的字符串，不再重复转换
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from collections import defaultdict
import time

logger = logging.getLogger(__name__)

@dataclass
class PlatformData:
    symbol: str
    exchange: str
    contract_name: str
    latest_price: Optional[str] = None
    funding_rate: Optional[str] = None
    last_settlement_time: Optional[str] = None
    current_settlement_time: Optional[str] = None
    next_settlement_time: Optional[str] = None
    last_settlement_ts: Optional[int] = None
    current_settlement_ts: Optional[int] = None
    next_settlement_ts: Optional[int] = None
    period_seconds: Optional[int] = None
    countdown_seconds: Optional[int] = None

class Step4Calc:
    def __init__(self):
        self.binance_cache = {}
        self.last_log_time = 0
        self.log_interval = 60
        self.process_count = 0
        # ✅ DEBUG: 打印计数器
        self.debug_print_counter = 0
    
    def process(self, aligned_results: List) -> List[PlatformData]:
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        if should_log:
            logger.info(f"🔄【流水线步骤4】开始单平台计算Step3输出的 {len(aligned_results)} 个双平台合约的对齐数据...")
        
        batch_stats = {
            "total_contracts": len(aligned_results),
            "okx_complete_contracts": 0,
            "binance_complete_contracts": 0,
            "both_platform_contracts": 0,
            "calculation_errors": 0,
            "binance_rollovers": 0,
            "okx_period_success": 0,
            "okx_countdown_success": 0,
            "binance_period_success": 0,
            "binance_countdown_success": 0,
        }
        
        all_results = []
        
        for item in aligned_results:
            try:
                okx_data = self._calc_okx(item, batch_stats)
                binance_data = self._calc_binance(item, batch_stats)
                
                has_okx = okx_data is not None
                has_binance = binance_data is not None
                
                if has_okx:
                    all_results.append(okx_data)
                    batch_stats["okx_complete_contracts"] += 1
                    if okx_data.period_seconds is not None:
                        batch_stats["okx_period_success"] += 1
                    if okx_data.countdown_seconds is not None:
                        batch_stats["okx_countdown_success"] += 1
                
                if has_binance:
                    all_results.append(binance_data)
                    batch_stats["binance_complete_contracts"] += 1
                    if binance_data.period_seconds is not None:
                        batch_stats["binance_period_success"] += 1
                    if binance_data.countdown_seconds is not None:
                        batch_stats["binance_countdown_success"] += 1
                
                if has_okx and has_binance:
                    batch_stats["both_platform_contracts"] += 1
                
            except Exception as e:
                batch_stats["calculation_errors"] += 1
                if should_log:
                    logger.error(f"❌【流水线步骤4】合约计算失败: {item.symbol} - {e}")
                continue
        
        if should_log:
            self._log_batch_statistics(batch_stats)
            logger.info(f"✅【流水线步骤4】Step4计算完成，共生成 {len(all_results)} 条单平台数据")
            self._log_cache_report(batch_stats["binance_complete_contracts"])
            self.last_log_time = current_time
            self.process_count = 0
        
        self.process_count += 1
        return all_results
    
    def _log_batch_statistics(self, batch_stats: Dict[str, int]):
        logger.info("📝【流水线步骤4】当前批次合约统计:")
        logger.info(f"  • 总合约数: {batch_stats['total_contracts']} 个")
        logger.info(f"  • 双平台完整: {batch_stats['both_platform_contracts']} 个")
        logger.info(f"  • 计算失败: {batch_stats['calculation_errors']} 个")
        
        if batch_stats['total_contracts'] > 0:
            both_rate = (batch_stats['both_platform_contracts'] / batch_stats['total_contracts']) * 100
            logger.info(f"✅【流水线步骤4】双平台完整率: {both_rate:.1f}%")
            
            if batch_stats['both_platform_contracts'] == batch_stats['total_contracts']:
                logger.info("🎉【流水线步骤4】所有合约双平台数据完整！")
            else:
                incomplete = batch_stats['total_contracts'] - batch_stats['both_platform_contracts']
                logger.warning(f"⚠️【流水线步骤4】 {incomplete} 个合约数据不完整")
        
        logger.info("⏱️【流水线步骤4】费率周期和倒计时计算统计:")
        
        if batch_stats["okx_complete_contracts"] > 0:
            period_rate = (batch_stats["okx_period_success"] / batch_stats["okx_complete_contracts"]) * 100
            countdown_rate = (batch_stats["okx_countdown_success"] / batch_stats["okx_complete_contracts"]) * 100
            logger.info(f"  • OKX周期计算: {batch_stats['okx_period_success']}/{batch_stats['okx_complete_contracts']} ({period_rate:.1f}%)")
            logger.info(f"  • OKX倒计时: {batch_stats['okx_countdown_success']}/{batch_stats['okx_complete_contracts']} ({countdown_rate:.1f}%)")
        
        if batch_stats["binance_complete_contracts"] > 0:
            period_rate = (batch_stats["binance_period_success"] / batch_stats["binance_complete_contracts"]) * 100
            countdown_rate = (batch_stats["binance_countdown_success"] / batch_stats["binance_complete_contracts"]) * 100
            logger.info(f"  • 币安周期计算: {batch_stats['binance_period_success']}/{batch_stats['binance_complete_contracts']} ({period_rate:.1f}%)")
            logger.info(f"  • 币安倒计时: {batch_stats['binance_countdown_success']}/{batch_stats['binance_complete_contracts']} ({countdown_rate:.1f}%)")
        
        if batch_stats["binance_rollovers"] > 0:
            logger.info(f"🔄【流水线步骤4】币安时间滚动: {batch_stats['binance_rollovers']} 次")
        else:
            logger.info(f"🔵【流水线步骤4】币安时间滚动: 0 次（或未发生）")
    
    def _log_cache_report(self, binance_contracts: int):
        cache_size = len(self.binance_cache)
        logger.info("🗃️【流水线步骤4】币安缓存详细报告:")
        logger.info(f"  • 缓存合约数: {cache_size} 个")
        logger.info(f"  • 当前批次币安合约: {binance_contracts} 个")
        
        if binance_contracts > 0:
            cache_coverage = (cache_size / binance_contracts) * 100
            logger.info(f"  • 缓存覆盖率: {cache_coverage:.1f}%")
        
        if cache_size > 0:
            with_history = 0
            without_history = 0
            
            for symbol, cache in self.binance_cache.items():
                if cache.get("last_ts"):
                    with_history += 1
                else:
                    without_history += 1
            
            logger.info(f"  • 有历史数据: {with_history} 个合约")
            logger.info(f"  • 无历史数据: {without_history} 个合约")
            
            if without_history > 0:
                logger.warning(f"⚠️【流水线步骤4】 {without_history} 个合约缺少历史结算时间，等待首次滚动")
    
    def _calc_okx(self, aligned_item, batch_stats: Dict) -> Optional[PlatformData]:
        if not aligned_item.okx_current_ts:
            return None
        
        data = PlatformData(
            symbol=aligned_item.symbol,
            exchange="okx",
            contract_name=aligned_item.okx_contract_name or "",
            latest_price=aligned_item.okx_price,
            funding_rate=aligned_item.okx_funding_rate,
            current_settlement_time=aligned_item.okx_current_settlement,
            next_settlement_time=aligned_item.okx_next_settlement,
            current_settlement_ts=aligned_item.okx_current_ts,
            next_settlement_ts=aligned_item.okx_next_ts
        )
        
        if data.current_settlement_ts and data.next_settlement_ts:
            data.period_seconds = (data.next_settlement_ts - data.current_settlement_ts) // 1000
        
        data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        return data
    
    def _calc_binance(self, aligned_item, batch_stats: Dict) -> Optional[PlatformData]:
        if not aligned_item.binance_current_ts:
            return None
        
        symbol = aligned_item.symbol
        
        if symbol not in self.binance_cache:
            self.binance_cache[symbol] = {
                "last_ts": aligned_item.binance_last_ts,
                "current_ts": aligned_item.binance_current_ts
            }
        
        cache = self.binance_cache[symbol]
        T1 = cache["last_ts"]
        T2 = cache["current_ts"]
        T3 = aligned_item.binance_current_ts
        
        if T2 and T3 != T2:
            batch_stats["binance_rollovers"] += 1
            T1 = T2
            T2 = T3
            cache["last_ts"] = T1
            cache["current_ts"] = T2
        
        data = PlatformData(
            symbol=symbol,
            exchange="binance",
            contract_name=aligned_item.binance_contract_name or "",
            latest_price=aligned_item.binance_price,
            funding_rate=aligned_item.binance_funding_rate,
            last_settlement_time=aligned_item.binance_last_settlement,
            current_settlement_time=aligned_item.binance_current_settlement,
            next_settlement_time=aligned_item.binance_next_settlement,
            last_settlement_ts=T1,
            current_settlement_ts=T2
        )
        
        # ✅ DEBUG: 打印前2条币安计算数据
        if self.debug_print_counter < 2:
            logger.warning(f"【DEBUG-Step4-币安】{symbol} last_ts={T1} current_ts={T2} last_settlement_str={data.last_settlement_time}")
            self.debug_print_counter += 1
        
        if data.current_settlement_ts and data.last_settlement_ts:
            data.period_seconds = (data.current_settlement_ts - data.last_settlement_ts) // 1000
        
        data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        return data
    
    def _calc_countdown(self, settlement_ts: Optional[int]) -> Optional[int]:
        if not settlement_ts:
            return None
        
        try:
            now_ms = int(time.time() * 1000)
            return max(0, (settlement_ts - now_ms) // 1000)
        except Exception:
            return None
    
    def get_cache_status(self, symbol: str) -> Dict[str, Any]:
        cache = self.binance_cache.get(symbol, {})
        return {
            "has_last_ts": cache.get("last_ts") is not None,
            "has_current_ts": cache.get("current_ts") is not None,
            "last_ts": cache.get("last_ts"),
            "current_ts": cache.get("current_ts"),
            "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
            "current_settlement_time": self._ts_to_str(cache.get("current_ts"))
        }
    
    def get_cache_report(self) -> Dict[str, Any]:
        report = {
            "total_cached": len(self.binance_cache),
            "with_last_ts": 0,
            "without_last_ts": 0,
            "symbols_without_history": [],
            "symbol_details": {}
        }
        
        for symbol, cache in self.binance_cache.items():
            if cache.get("last_ts"):
                report["with_last_ts"] += 1
            else:
                report["without_last_ts"] += 1
                report["symbols_without_history"].append(symbol)
            
            report["symbol_details"][symbol] = {
                "last_ts": cache.get("last_ts"),
                "current_ts": cache.get("current_ts"),
                "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
                "current_settlement_time": self._ts_to_str(cache.get("current_ts")),
                "status": "complete" if cache.get("last_ts") else "pending_history"
            }
        
        return report
    
    def _ts_to_str(self, ts: Optional[int]) -> Optional[str]:
        if ts is None or ts <= 0:
            return None
        
        try:
            from datetime import datetime, timedelta
            dt_utc = datetime.utcfromtimestamp(ts / 1000)
            dt_bj = dt_utc + timedelta(hours=8)
            return dt_bj.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return None
