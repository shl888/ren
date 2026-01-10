"""
第三步：筛选双平台合约 + 时间转换（修正版）
功能：1. 只保留OKX和币安都有的合约 2. UTC时间戳转UTC+8 3. 转24小时制字符串
修正：时间戳是纯UTC，必须先utcfromtimestamp() + 8小时
"""

import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

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
        self.stats = {
            "total_symbols": 0, 
            "okx_only": 0, 
            "binance_only": 0, 
            "both_platforms": 0,
            "time_conversion_errors": 0
        }
        self.align_results = []
    
    def process(self, fused_results: List) -> List[AlignedData]:
        """处理Step2的融合结果"""
        logger.info(f"✅【流水线步骤3】开始对齐 {len(fused_results)} 条融合数据...")
        
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
        
        # 统计
        self.stats["total_symbols"] = len(grouped)
        for symbol, data in grouped.items():
            if data["okx"] and data["binance"]:
                self.stats["both_platforms"] += 1
            elif data["okx"]:
                self.stats["okx_only"] += 1
            elif data["binance"]:
                self.stats["binance_only"] += 1
        
        # 只保留双平台都有的合约
        for symbol, data in grouped.items():
            if data["okx"] and data["binance"]:
                try:
                    aligned = self._align_item(symbol, data["okx"], data["binance"])
                    if aligned:
                        self.align_results.append(aligned)
                except Exception as e:
                    logger.error(f"❌【流水线步骤3】对齐失败: {symbol} - {e}")
                    continue
        
        # 处理完成后，打印统计结果
        self._log_statistics()
        
        logger.info(f"✅【流水线步骤3】Step3对齐完成，共生成 {len(self.align_results)} 条对齐数据")
        return self.align_results
    
    def _log_statistics(self):
        """打印统计结果"""
        logger.info("📝【流水线步骤3】对齐结果统计:")
        
        total_symbols = self.stats["total_symbols"]
        okx_only = self.stats["okx_only"]
        binance_only = self.stats["binance_only"]
        both_platforms = self.stats["both_platforms"]
        
        logger.info(f"   总合约数: {total_symbols}")
        logger.info(f"   仅OKX: {okx_only}")
        logger.info(f"   仅币安: {binance_only}")
        logger.info(f"   双平台: {both_platforms}")
        
        # 时间格式验证
        self._validate_time_formats()
        
        # 过滤统计
        filtered_count = okx_only + binance_only
        logger.info(f"ℹ️【流水线步骤3】过滤掉 {filtered_count} 个单平台合约")
        logger.info(f"✅【流水线步骤3】最终保留 {both_platforms} 个双平台合约")
        
        # 总结
        if both_platforms > 0:
            success_rate = (len(self.align_results) / both_platforms) * 100
            logger.info(f"🎉 **恭喜！【流水线步骤3】Step3对齐功能{success_rate:.1f}%正常！**")
            
            if self.stats["time_conversion_errors"] == 0:
                logger.info("✅【流水线步骤3】 时间已全部转为北京时间24小时制")
            else:
                logger.warning(f"⚠️【流水线步骤3】  时间转换存在 {self.stats['time_conversion_errors']} 个错误")
            
            logger.info("✅【流水线步骤3】 单平台合约已全部过滤")
    
    def _validate_time_formats(self):
        """验证时间格式是否正确"""
        if not self.align_results:
            return
        
        # 检查前几个时间格式作为样本
        sample_count = min(5, len(self.align_results))
        valid_formats = 0
        
        for i in range(sample_count):
            item = self.align_results[i]
            
            # 检查OKX时间格式
            if item.okx_current_settlement:
                if self._is_valid_time_format(item.okx_current_settlement):
                    valid_formats += 1
            
            # 检查币安时间格式
            if item.binance_current_settlement:
                if self._is_valid_time_format(item.binance_current_settlement):
                    valid_formats += 1
        
        if sample_count > 0:
            logger.info(f"✅【流水线步骤3】 时间格式正确样本: {valid_formats}/{sample_count*2}")
    
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
            self.stats["time_conversion_errors"] += 1
            return None