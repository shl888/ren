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
    """对齐后的数据结构"""
    symbol: str
    okx_contract_name: Optional[str] = None
    binance_contract_name: Optional[str] = None
    
    # OKX数据
    okx_price: Optional[str] = None
    okx_funding_rate: Optional[str] = None
    okx_last_settlement: Optional[str] = None
    okx_current_settlement: Optional[str] = None
    okx_next_settlement: Optional[str] = None
    
    # 币安数据
    binance_price: Optional[str] = None
    binance_funding_rate: Optional[str] = None
    binance_last_settlement: Optional[str] = None
    binance_current_settlement: Optional[str] = None
    binance_next_settlement: Optional[str] = None
    
    # 时间戳备份（用于后续计算）
    okx_current_ts: Optional[int] = None
    okx_next_ts: Optional[int] = None
    binance_current_ts: Optional[int] = None
    binance_last_ts: Optional[int] = None

class Step3Align:
    """第三步：双平台对齐 + 时间转换（修正版）"""
    
    def __init__(self):
        self.last_log_time = 0
        self.log_interval = 360  # 6分钟，单位：秒
        self.process_count = 0
    
    def process(self, fused_results: List) -> List[AlignedData]:
        """处理Step2的融合结果"""
        # 频率控制：只偶尔显示处理日志
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        # 按symbol分组
        grouped = {}
        for item in fused_results:
            symbol = item.symbol
            if symbol not in grouped:
                grouped[symbol] = {"okx": None, "binance": None}
            
            if item.exchange == "okx":
                grouped[symbol]["okx"] = item
            elif item.exchange == "binance":
                grouped[symbol]["binance"] = item
        
        # 正确统计合约分布
        okx_only_contracts = 0     # 仅OKX有的合约
        binance_only_contracts = 0 # 仅币安有的合约
        both_platform_contracts = 0  # 双平台都有的合约
        total_contracts = len(grouped)  # 总合约数
        
        for symbol, data in grouped.items():
            if data["okx"] and data["binance"]:
                both_platform_contracts += 1
            elif data["okx"]:
                okx_only_contracts += 1
            elif data["binance"]:
                binance_only_contracts += 1
        
        # 验证统计正确性
        expected_total = okx_only_contracts + binance_only_contracts + both_platform_contracts
        if expected_total != total_contracts:
            logger.error(f"❌【流水线步骤3】统计错误: 总合约数 {total_contracts} != 各部分之和 {expected_total}")
        
        # 只保留双平台都有的合约
        align_results = []
        time_conversion_errors = 0
        
        for symbol, data in grouped.items():
            if data["okx"] and data["binance"]:
                try:
                    aligned = self._align_item(symbol, data["okx"], data["binance"])
                    if aligned:
                        align_results.append(aligned)
                        # 统计时间转换错误
                        if (data["okx"].current_settlement_time and not aligned.okx_current_settlement) or \
                           (data["okx"].next_settlement_time and not aligned.okx_next_settlement) or \
                           (data["binance"].last_settlement_time and not aligned.binance_last_settlement) or \
                           (data["binance"].current_settlement_time and not aligned.binance_current_settlement):
                            time_conversion_errors += 1
                except Exception as e:
                    # 只在频率控制时打印错误
                    if should_log:
                        logger.error(f"❌【流水线步骤3】对齐失败: {symbol} - {e}")
                    continue
        
        if should_log:
            logger.info(f"🔄【流水线步骤3】开始对齐step2输出的 {len(fused_results)} 条融合数据...")
            
            # 正确的合约分布统计
            logger.info(f"📊【流水线步骤3】合约分布统计:")
            logger.info(f"  • 总合约数: {total_contracts} 个")
            logger.info(f"  • 仅OKX: {okx_only_contracts} 个")
            logger.info(f"  • 仅币安: {binance_only_contracts} 个")
            logger.info(f"  • 双平台: {both_platform_contracts} 个")
            
            logger.info(f"✅【流水线步骤3】Step3对齐完成，共生成 {len(align_results)} 条双平台合约的对齐数据")
            
            # 时间转换统计
            if time_conversion_errors == 0:
                logger.info(f"✅【流水线步骤3】时间转换: 全部 {len(align_results)} 个合约转换成功")
            else:
                logger.warning(f"⚠️【流水线步骤3】时间转换: {time_conversion_errors} 个合约存在转换错误")
            
            # 验证时间格式
            if align_results:
                self._validate_time_formats(align_results)
            
            self.last_log_time = current_time
            # 重置计数（仅用于频率控制）
            self.process_count = 0
        
        self.process_count += 1
        
        return align_results
    
    def _validate_time_formats(self, align_results: List[AlignedData]):
        """验证时间格式是否正确"""
        if not align_results:
            return
        
        # 检查前几个时间格式作为样本
        sample_count = min(3, len(align_results))
        valid_count = 0
        
        for i in range(sample_count):
            item = align_results[i]
            
            # 检查OKX时间格式
            if item.okx_current_settlement:
                if self._is_valid_time_format(item.okx_current_settlement):
                    valid_count += 1
            
            # 检查币安时间格式
            if item.binance_current_settlement:
                if self._is_valid_time_format(item.binance_current_settlement):
                    valid_count += 1
        
        if sample_count > 0:
            total_checks = sample_count * 2
            logger.info(f"✅【流水线步骤3】时间格式验证: {valid_count}/{total_checks} 个时间字段格式正确")
    
    def _is_valid_time_format(self, time_str: str) -> bool:
        """验证时间字符串格式"""
        try:
            datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            return True
        except (ValueError, TypeError):
            return False
    
    def _align_item(self, symbol: str, okx_item, binance_item) -> Optional[AlignedData]:
        """对齐单个合约"""
        
        aligned = AlignedData(symbol=symbol)
        
        # OKX数据
        if okx_item:
            aligned.okx_contract_name = okx_item.contract_name
            aligned.okx_price = okx_item.latest_price
            aligned.okx_funding_rate = okx_item.funding_rate
            aligned.okx_current_ts = okx_item.current_settlement_time
            aligned.okx_next_ts = okx_item.next_settlement_time
            
            # 时间转换：UTC时间戳 -> UTC+8 -> 24小时字符串
            aligned.okx_current_settlement = self._ts_to_str(okx_item.current_settlement_time, symbol, "okx_current")
            aligned.okx_next_settlement = self._ts_to_str(okx_item.next_settlement_time, symbol, "okx_next")
            aligned.okx_last_settlement = None
        
        # 币安数据
        if binance_item:
            aligned.binance_contract_name = binance_item.contract_name
            aligned.binance_price = binance_item.latest_price
            aligned.binance_funding_rate = binance_item.funding_rate
            aligned.binance_last_ts = binance_item.last_settlement_time
            aligned.binance_current_ts = binance_item.current_settlement_time
            
            # 时间转换
            aligned.binance_last_settlement = self._ts_to_str(binance_item.last_settlement_time, symbol, "binance_last")
            aligned.binance_current_settlement = self._ts_to_str(binance_item.current_settlement_time, symbol, "binance_current")
            aligned.binance_next_settlement = None
        
        return aligned
    
    def _ts_to_str(self, ts: Optional[int], symbol: str = None, field: str = None) -> Optional[str]:
        """时间戳转换：UTC毫秒 -> UTC+8 -> 24小时制字符串"""
        # 增加无效值检查
        if ts is None or ts <= 0:  # 无效或负值时间戳
            return None
        
        try:
            # 1. 先拿到纯UTC时间（关键！用utcfromtimestamp）
            dt_utc = datetime.utcfromtimestamp(ts / 1000)
            
            # 2. 加8小时到北京
            dt_bj = dt_utc + timedelta(hours=8)
            
            # 3. 转24小时字符串
            return dt_bj.strftime("%Y-%m-%d %H:%M:%S")
        
        except Exception as e:
            return None