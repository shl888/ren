import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

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
    
    # 时间戳备份
    okx_current_ts: Optional[int] = None
    okx_next_ts: Optional[int] = None
    binance_current_ts: Optional[int] = None
    binance_last_ts: Optional[int] = None

class Step3Align:
    """第三步：双平台对齐（状态机版）"""
    
    def __init__(self):
        # ✅ 状态缓存：按symbol缓存双平台数据
        self.state_cache = {}  # symbol -> {"okx": item, "binance": item, "last_update": timestamp}
        
        # ✅ 按合约统计
        self.aligned_contracts = set()  # 成功对齐的合约
        self.current_batch_aligned = set()  # 当前批次对齐的合约
        
        # ✅ 5分钟统计
        self.last_report_time = time.time()
        
        log_data_process("步骤3", "启动", "Step3Align初始化完成（状态机版）")
    
    def process(self, fused_results: List) -> List[AlignedData]:
        # 重置当前批次
        self.current_batch_aligned.clear()
        
        results = []
        for item in fused_results:
            try:
                symbol = item.symbol
                
                # 初始化或获取状态
                if symbol not in self.state_cache:
                    self.state_cache[symbol] = {
                        "okx": None,
                        "binance": None,
                        "last_update": time.time()
                    }
                
                state = self.state_cache[symbol]
                state["last_update"] = time.time()
                
                # 存入数据
                if item.exchange == "okx":
                    state["okx"] = item
                elif item.exchange == "binance":
                    state["binance"] = item
                else:
                    continue
                
                # 检查是否可以对齐
                if state["okx"] and state["binance"]:
                    # 对齐输出
                    aligned = self._align_pair(symbol, state["okx"], state["binance"])
                    if aligned:
                        results.append(aligned)
                        self.aligned_contracts.add(symbol)
                        self.current_batch_aligned.add(symbol)
                        
                        # ✅ 对齐成功后清理状态
                        del self.state_cache[symbol]
                        log_data_process("步骤3", "对齐", f"{symbol} ✓ 双平台对齐完成")
                
            except Exception as e:
                log_data_process("步骤3", "错误", f"对齐失败: {item.symbol} - {e}", "ERROR")
                continue
        
        # ✅ 检查过期状态
        current_time = time.time()
        expired_symbols = []
        
        for symbol, state in self.state_cache.items():
            if current_time - state["last_update"] > 10:  # 超过10秒未更新
                expired_symbols.append(symbol)
        
        # 清理过期状态
        for symbol in expired_symbols:
            del self.state_cache[symbol]
            log_data_process("步骤3", "清理", f"清理过期状态: {symbol}", "DEBUG")
        
        # ✅ 5分钟统计
        current_time = time.time()
        if current_time - self.last_report_time >= 300:
            aligned_count = len(self.aligned_contracts)
            waiting_count = len(self.state_cache)
            current_count = len(self.current_batch_aligned)
            
            log_data_process("步骤3", "统计", f"5分钟对齐 {aligned_count} 个合约")
            log_data_process("步骤3", "状态", f"等待配对: {waiting_count}合约 | 当前批次: {current_count}合约")
            
            # 重置统计
            self.last_report_time = current_time
            self.aligned_contracts.clear()
        
        return results
    
    def _align_pair(self, symbol: str, okx_item, binance_item) -> Optional[AlignedData]:
        """对齐一对数据"""
        try:
            aligned = AlignedData(symbol=symbol)
            
            # OKX数据
            if okx_item:
                aligned.okx_contract_name = okx_item.contract_name
                aligned.okx_price = okx_item.latest_price
                aligned.okx_funding_rate = okx_item.funding_rate
                aligned.okx_current_ts = okx_item.current_settlement_time
                aligned.okx_next_ts = okx_item.next_settlement_time
                
                # 时间转换
                aligned.okx_current_settlement = self._ts_to_str(okx_item.current_settlement_time)
                aligned.okx_next_settlement = self._ts_to_str(okx_item.next_settlement_time)
            
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
            
            return aligned
            
        except Exception as e:
            log_data_process("步骤3", "错误", f"对齐处理失败: {symbol} - {e}", "ERROR")
            return None
    
    def _ts_to_str(self, ts: Optional[int]) -> Optional[str]:
        """时间戳转换：UTC毫秒 -> UTC+8 -> 24小时制字符串"""
        if ts is None or ts <= 0:
            return None
        
        try:
            # 1. 先拿到纯UTC时间
            dt_utc = datetime.utcfromtimestamp(ts / 1000)
            
            # 2. 加8小时到北京
            dt_bj = dt_utc + timedelta(hours=8)
            
            # 3. 转24小时字符串
            return dt_bj.strftime("%Y-%m-%d %H:%M:%S")
        
        except Exception as e:
            log_data_process("步骤3", "警告", f"时间戳转换失败: {ts} - {e}", "WARNING")
            return None
    
    def get_status(self) -> Dict[str, Any]:
        """获取状态信息"""
        return {
            "waiting_contracts": len(self.state_cache),
            "aligned_contracts": len(self.aligned_contracts),
            "current_batch_aligned": len(self.current_batch_aligned)
        }