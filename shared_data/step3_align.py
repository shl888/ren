"""
第三步：筛选双平台合约 + 时间转换（修正版）
功能：1. 只保留OKX和币安都有的合约 2. UTC时间戳转UTC+8 3. 转24小时制字符串
修正：时间戳是纯UTC，必须先utcfromtimestamp() + 8小时
"""

import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import time  # ✅ 添加time模块

logger = logging.getLogger(__name__)

# ✅ 添加：统一的日志工具函数
def log_data_process(module: str, action: str, message: str, level: str = "INFO"):
    """统一的数据处理日志格式"""
    prefix = f"[数据处理][{module}][{action}]"
    full_message = f"{prefix} {message}"
    
    if level == "INFO":
        logger.info(full_message)
    elif level == "ERROR":
        logger.error(full_message)
    elif level == "WARNING":
        logger.warning(full_message)
    elif level == "DEBUG":
        logger.debug(full_message)

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
        self.stats = {"total_symbols": 0, "okx_only": 0, "binance_only": 0, "both_platforms": 0}
        # ✅ 添加：5分钟统计计时器
        self.last_report_time = time.time()
        self.total_input = 0
        self.total_output = 0
        self.platform_stats = {"both": 0, "okx_only": 0, "binance_only": 0}
        
        log_data_process("步骤3", "启动", "Step3Align初始化完成（双平台对齐+时间转换）")
    
    def process(self, fused_results: List) -> List[AlignedData]:
        """处理Step2的融合结果"""
        # ✅ 修改：移除开始处理日志，改为计数
        input_count = len(fused_results)
        self.total_input += input_count
        
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
                self.platform_stats["both"] += 1
            elif data["okx"]:
                self.stats["okx_only"] += 1
                self.platform_stats["okx_only"] += 1
            elif data["binance"]:
                self.stats["binance_only"] += 1
                self.platform_stats["binance_only"] += 1
        
        # 只保留双平台都有的合约
        results = []
        for symbol, data in grouped.items():
            if data["okx"] and data["binance"]:
                try:
                    aligned = self._align_item(symbol, data["okx"], data["binance"])
                    if aligned:
                        results.append(aligned)
                        self.total_output += 1
                except Exception as e:
                    log_data_process("步骤3", "错误", f"对齐失败: {symbol} - {e}", "ERROR")
                    continue
        
        # ✅ 添加：5分钟统计
        current_time = time.time()
        if current_time - self.last_report_time >= 300:  # 5分钟
            both_count = self.platform_stats["both"]
            okx_only = self.platform_stats["okx_only"]
            binance_only = self.platform_stats["binance_only"]
            total_symbols = both_count + okx_only + binance_only
            
            if total_symbols > 0:
                both_rate = (both_count / total_symbols * 100)
            else:
                both_rate = 0
                
            conversion_rate = (self.total_output / self.total_input * 100) if self.total_input > 0 else 0
            
            log_data_process("步骤3", "统计", 
                           f"5分钟: 接收{self.total_input}合约 → 双平台对齐{self.total_output}合约 "
                           f"({conversion_rate:.1f}%)")
            log_data_process("步骤3", "完成", 
                           f"平台分布: 双平台{both_count}({both_rate:.1f}%), "
                           f"仅OKX{okx_only}, 仅币安{binance_only}")
            
            # 重置统计
            self.last_report_time = current_time
            self.total_input = 0
            self.total_output = 0
            self.platform_stats = {"both": 0, "okx_only": 0, "binance_only": 0}
        
        return results
    
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
            aligned.okx_current_settlement = self._ts_to_str(okx_item.current_settlement_time)
            aligned.okx_next_settlement = self._ts_to_str(okx_item.next_settlement_time)
            aligned.okx_last_settlement = None
        
        # 币安数据
        if binance_item:
            aligned.binance_contract_name = binance_item.contract_name
            aligned.binance_price = binance_item.latest_price
            aligned.binance_funding_rate = binance_item.funding_rate
            aligned.binance_last_ts = binance_item.last_settlement_time
            aligned.binance_current_ts = binance_item.current_settlement_time
            
            # 时间转换
            aligned.binance_last_settlement = self._ts_to_str(binance_item.last_settlement_time)
            aligned.binance_current_settlement = self._ts_to_str(binance_item.current_settlement_time)
            aligned.binance_next_settlement = None
        
        return aligned
    
    def _ts_to_str(self, ts: Optional[int]) -> Optional[str]:
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
            log_data_process("步骤3", "警告", f"时间戳转换失败: {ts} - {e}", "WARNING")
            return None