"""
第三步：筛选双平台合约 + 时间转换（修正版）
功能：1. 只保留OKX和币安都有的合约 2. UTC时间戳转UTC+8 3. 转24小时制字符串
修正：时间戳是纯UTC，必须先utcfromtimestamp() + 8小时
"""

import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

@dataclass
class AlignedData:
    symbol: str
    okx_contract_name: Optional[str] = None
    binance_contract_name: Optional[str] = None
    
    okx_price: Optional[str] = None
    okx_funding_rate: Optional[str] = None
    okx_last_settlement: Optional[str] = None
    okx_current_settlement: Optional[str] = None
    okx_next_settlement: Optional[str] = None
    
    binance_price: Optional[str] = None
    binance_funding_rate: Optional[str] = None
    binance_last_settlement: Optional[str] = None
    binance_current_settlement: Optional[str] = None
    binance_next_settlement: Optional[str] = None
    
    okx_current_ts: Optional[int] = None
    okx_next_ts: Optional[int] = None
    binance_current_ts: Optional[int] = None
    binance_last_ts: Optional[int] = None

class Step3Align:
    def __init__(self):
        self.last_log_time = 0
        self.log_interval = 60
        self.process_count = 0
        # ✅ DEBUG: 打印计数器
        self.debug_print_counter = 0
    
    def process(self, fused_results: List) -> List[AlignedData]:
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        grouped = {}
        for item in fused_results:
            symbol = item.symbol
            if symbol not in grouped:
                grouped[symbol] = {"okx": None, "binance": None}
            
            if item.exchange == "okx":
                grouped[symbol]["okx"] = item
            elif item.exchange == "binance":
                grouped[symbol]["binance"] = item
        
        okx_only_contracts = 0
        binance_only_contracts = 0
        both_platform_contracts = 0
        total_contracts = len(grouped)
        
        for symbol, data in grouped.items():
            if data["okx"] and data["binance"]:
                both_platform_contracts += 1
            elif data["okx"]:
                okx_only_contracts += 1
            elif data["binance"]:
                binance_only_contracts += 1
        
        expected_total = okx_only_contracts + binance_only_contracts + both_platform_contracts
        if expected_total != total_contracts:
            logger.error(f"❌【流水线步骤3】统计错误: 总合约数 {total_contracts} != 各部分之和 {expected_total}")
        
        align_results = []
        
        for symbol, data in grouped.items():
            if data["okx"] and data["binance"]:
                try:
                    aligned = self._align_item(symbol, data["okx"], data["binance"])
                    if aligned:
                        align_results.append(aligned)
                except Exception as e:
                    if should_log:
                        logger.error(f"❌【流水线步骤3】对齐失败: {symbol} - {e}")
                    continue
        
        if should_log:
            logger.info(f"🔄【流水线步骤3】开始对齐step2输出的 {len(fused_results)} 条融合数据...")
            logger.info(f"📊【流水线步骤3】合约分布统计:")
            logger.info(f"  • 总合约数: {total_contracts} 个")
            logger.info(f"  • 仅OKX: {okx_only_contracts} 个")
            logger.info(f"  • 仅币安: {binance_only_contracts} 个")
            logger.info(f"  • 双平台: {both_platform_contracts} 个")
            logger.info(f"✅【流水线步骤3】Step3对齐完成，共生成 {len(align_results)} 条双平台合约的对齐数据")
            self.last_log_time = current_time
            self.process_count = 0
        
        self.process_count += 1
        return align_results
    
    def _align_item(self, symbol: str, okx_item, binance_item) -> Optional[AlignedData]:
        aligned = AlignedData(symbol=symbol)
        
        if okx_item:
            aligned.okx_contract_name = okx_item.contract_name
            aligned.okx_price = okx_item.latest_price
            aligned.okx_funding_rate = okx_item.funding_rate
            aligned.okx_current_ts = okx_item.current_settlement_time
            aligned.okx_next_ts = okx_item.next_settlement_time
            aligned.okx_current_settlement = self._ts_to_str(okx_item.current_settlement_time, symbol, "okx_current")
            aligned.okx_next_settlement = self._ts_to_str(okx_item.next_settlement_time, symbol, "okx_next")
        
        if binance_item:
            aligned.binance_contract_name = binance_item.contract_name
            aligned.binance_price = binance_item.latest_price
            aligned.binance_funding_rate = binance_item.funding_rate
            aligned.binance_last_ts = binance_item.last_settlement_time
            aligned.binance_current_ts = binance_item.current_settlement_time
            aligned.binance_last_settlement = self._ts_to_str(binance_item.last_settlement_time, symbol, "binance_last")
            aligned.binance_current_settlement = self._ts_to_str(binance_item.current_settlement_time, symbol, "binance_current")
            
            # ✅ DEBUG: 打印前2条币安对齐数据
            if self.debug_print_counter < 2:
                logger.error(f"【DEBUG-Step3】{symbol} last_settlement_ts={aligned.binance_last_ts} -> last_settlement_str={aligned.binance_last_settlement}")
                self.debug_print_counter += 1
        
        return aligned
    
    def _ts_to_str(self, ts: Optional[int], symbol: str = None, field: str = None) -> Optional[str]:
        if ts is None or ts <= 0:
            return None
        
        try:
            dt_utc = datetime.utcfromtimestamp(ts / 1000)
            dt_bj = dt_utc + timedelta(hours=8)
            return dt_bj.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            return None
