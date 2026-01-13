"""
第二步：数据融合与统一规格
功能：将Step1提取的5种数据源，按交易所+合约名合并成一条
输出：每个交易所每个合约一条完整数据
"""

import logging
from typing import Dict, List, Any, Optional, TYPE_CHECKING
from collections import defaultdict
from dataclasses import dataclass
import time

if TYPE_CHECKING:
    from step1_filter import ExtractedData

logger = logging.getLogger(__name__)

@dataclass 
class FusedData:
    exchange: str
    symbol: str
    contract_name: str
    latest_price: Optional[str] = None
    funding_rate: Optional[str] = None
    last_settlement_time: Optional[int] = None
    current_settlement_time: Optional[int] = None
    next_settlement_time: Optional[int] = None

class Step2Fusion:
    def __init__(self):
        self.stats = defaultdict(int)
        self.fusion_stats = {
            "total_groups": 0,
            "success_groups": 0,
            "failed_groups": 0
        }
        self.last_log_time = 0
        self.log_interval = 60
        self.process_count = 0
        # ✅ DEBUG: 打印计数器
        self.debug_print_counters = {"okx": 0, "binance": 0}
    
    def process(self, step1_results: List["ExtractedData"]) -> List[FusedData]:
        self.fusion_stats = {
            "total_groups": 0,
            "success_groups": 0,
            "failed_groups": 0
        }
        self.stats.clear()
        
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        grouped = defaultdict(list)
        for item in step1_results:
            key = f"{item.exchange}_{item.symbol}"
            grouped[key].append(item)
        
        self.fusion_stats["total_groups"] = len(grouped)
        
        if should_log:
            logger.info(f"🔄【流水线步骤2】开始融合Step1输出的 {len(step1_results)} 条精简数据...")
            logger.info(f"【流水线步骤2】检测到 {len(grouped)} 个不同的交易所合约")
        
        results = []
        exchange_contracts = defaultdict(set)
        fusion_stats_detail = {
            "total_groups": 0,
            "has_required_fields": 0,
            "missing_ticker": 0,
            "missing_mark_price": 0,
            "missing_funding_rate": 0,
            "missing_history": 0,
            "has_history": 0
        }
        
        for key, items in grouped.items():
            fusion_stats_detail["total_groups"] += 1
            
            try:
                fused = self._merge_group(items, fusion_stats_detail)
                if fused:
                    results.append(fused)
                    exchange_contracts[fused.exchange].add(fused.symbol)
                    self.stats[fused.exchange] += 1
                    self.fusion_stats["success_groups"] += 1
                else:
                    self.fusion_stats["failed_groups"] += 1
            except Exception as e:
                self.fusion_stats["failed_groups"] += 1
                if should_log:
                    logger.error(f"❌【流水线步骤2】融合失败: {key} - {e}")
                continue
        
        if should_log:
            logger.info(f"✅【流水线步骤2】Step2融合完成，共生成 {len(results)} 条融合数据")
            
            okx_contracts = len(exchange_contracts.get("okx", set()))
            binance_contracts = len(exchange_contracts.get("binance", set()))
            total_contracts = okx_contracts + binance_contracts
            
            logger.info("📊【流水线步骤2】融合结果合约统计:")
            if okx_contracts > 0:
                logger.info(f"  • OKX合约数: {okx_contracts} 个")
            if binance_contracts > 0:
                logger.info(f"  • 币安合约数: {binance_contracts} 个")
            logger.info(f"  • 总计: {total_contracts} 个合约")
            
            logger.info("📊【流水线步骤2】融合详细统计:")
            logger.info(f"  • 总合约组数: {fusion_stats_detail['total_groups']}")
            logger.info(f"  • 符合要求组数: {fusion_stats_detail['has_required_fields']}")
            logger.info(f"  • 缺少ticker数据: {fusion_stats_detail['missing_ticker']}")
            logger.info(f"  • 缺少mark_price数据: {fusion_stats_detail['missing_mark_price']}")
            logger.info(f"  • 缺少funding_rate数据: {fusion_stats_detail['missing_funding_rate']}")
            logger.info(f"  • 有历史费率数据: {fusion_stats_detail['has_history']}")
            logger.info(f"  • 无历史费率数据: {fusion_stats_detail['missing_history']}")
            
            if results:
                self._validate_fields(results)
            
            self.last_log_time = current_time
            self.process_count = 0
        
        self.process_count += 1
        # ✅ DEBUG: 重置计数器
        self.debug_print_counters = {"okx": 0, "binance": 0}
        return results
    
    def _validate_fields(self, results: List[FusedData]):
        okx_valid = 0
        binance_valid = 0
        binance_with_history = 0
        
        for item in results:
            if item.exchange == "okx":
                required = [item.latest_price, item.funding_rate, item.next_settlement_time]
                if all(field is not None for field in required):
                    okx_valid += 1
            elif item.exchange == "binance":
                required = [item.latest_price, item.funding_rate, item.current_settlement_time]
                if all(field is not None for field in required):
                    binance_valid += 1
                    if item.last_settlement_time is not None:
                        binance_with_history += 1
        
        okx_count = len([r for r in results if r.exchange == "okx"])
        binance_count = len([r for r in results if r.exchange == "binance"])
        
        if okx_count > 0:
            validation_rate = (okx_valid / okx_count) * 100
            logger.info(f"📊【流水线步骤2】OKX合约验证:")
            logger.info(f"  • 验证通过: {okx_valid}/{okx_count} ({validation_rate:.1f}%)")
        
        if binance_count > 0:
            validation_rate = (binance_valid / binance_count) * 100
            history_rate = (binance_with_history / binance_count) * 100
            logger.info(f"📊【流水线步骤2】币安合约验证:")
            logger.info(f"  • 验证通过: {binance_valid}/{binance_count} ({validation_rate:.1f}%)")
            logger.info(f"  • 有历史数据: {binance_with_history}/{binance_count} ({history_rate:.1f}%)")
    
    def _merge_group(self, items: List["ExtractedData"], stats: Dict) -> Optional[FusedData]:
        if not items:
            return None
        
        first = items[0]
        exchange = first.exchange
        symbol = first.symbol
        
        fused = FusedData(
            exchange=exchange,
            symbol=symbol,
            contract_name=""
        )
        
        if exchange == "okx":
            return self._merge_okx(items, fused, stats)
        elif exchange == "binance":
            return self._merge_binance(items, fused, stats)
        else:
            return None
    
    def _merge_okx(self, items: List["ExtractedData"], fused: FusedData, stats: Dict) -> Optional[FusedData]:
        ticker_item = None
        funding_item = None
        
        for item in items:
            if item.data_type == "okx_ticker":
                ticker_item = item
            elif item.data_type == "okx_funding_rate":
                funding_item = item
        
        if not ticker_item:
            stats["missing_ticker"] += 1
            return None
        
        if not funding_item:
            stats["missing_funding_rate"] += 1
            return None
        
        ticker_payload = ticker_item.payload
        funding_payload = funding_item.payload
        
        fused.contract_name = ticker_payload.get("contract_name") or funding_payload.get("contract_name") or fused.symbol
        fused.latest_price = ticker_payload.get("latest_price")
        fused.funding_rate = funding_payload.get("funding_rate")
        fused.current_settlement_time = self._to_int(funding_payload.get("current_settlement_time"))
        fused.next_settlement_time = self._to_int(funding_payload.get("next_settlement_time"))
        
        required_fields = [fused.latest_price, fused.funding_rate, fused.next_settlement_time]
        
        if any(field is None for field in required_fields):
            return None
        
        stats["has_required_fields"] += 1
        
        # ✅ DEBUG: 打印前2条OKX融合数据
        if self.debug_print_counters["okx"] < 2:
            logger.warning(f"【DEBUG-Step2-OKX】{fused.symbol} latest_price={fused.latest_price} funding_rate={fused.funding_rate} next_settlement={fused.next_settlement_time}")
            self.debug_print_counters["okx"] += 1
        
        return fused
    
    def _merge_binance(self, items: List["ExtractedData"], fused: FusedData, stats: Dict) -> Optional[FusedData]:
        ticker_item = None
        mark_price_item = None
        history_item = None
        
        for item in items:
            if item.data_type == "binance_ticker":
                ticker_item = item
            elif item.data_type == "binance_mark_price":
                mark_price_item = item
            elif item.data_type == "binance_funding_settlement":
                history_item = item
        
        if not ticker_item:
            stats["missing_ticker"] += 1
            return None
        
        if not mark_price_item:
            stats["missing_mark_price"] += 1
            return None
        
        ticker_payload = ticker_item.payload
        fused.latest_price = ticker_payload.get("latest_price")
        
        mark_payload = mark_price_item.payload
        fused.contract_name = mark_payload.get("contract_name", fused.symbol)
        fused.funding_rate = mark_payload.get("funding_rate")
        fused.current_settlement_time = self._to_int(mark_payload.get("current_settlement_time"))
        
        if history_item:
            fused.last_settlement_time = self._to_int(history_item.payload.get("last_settlement_time"))
            stats["has_history"] += 1
        else:
            stats["missing_history"] += 1
        
        # ✅ 关键调试：打印历史数据提取结果
        if self.debug_print_counters["binance"] < 2:
            logger.warning(f"【DEBUG-Step2-币安】{fused.symbol} has_history={history_item is not None} last_settlement_time={fused.last_settlement_time} (raw={history_item.payload if history_item else None})")
            self.debug_print_counters["binance"] += 1
        
        required_fields = [fused.latest_price, fused.funding_rate, fused.current_settlement_time]
        
        if any(field is None for field in required_fields):
            return None
        
        stats["has_required_fields"] += 1
        return fused
    
    def _to_int(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
