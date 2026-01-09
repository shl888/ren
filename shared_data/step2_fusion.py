"""
第二步：数据融合（状态机版）
功能：将Step1提取的数据，按交易所+合约名合并成一条
核心：历史费率由步骤1控制，这里只做正常融合
修复：正确处理历史费率配对逻辑
"""

import logging
from typing import Dict, List, Any, Optional, TYPE_CHECKING
from collections import defaultdict
from dataclasses import dataclass
import time

if TYPE_CHECKING:
    from step1_filter import ExtractedData

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
class FusedData:
    """融合后的统一数据结构"""
    exchange: str
    symbol: str
    contract_name: str
    latest_price: Optional[str] = None
    funding_rate: Optional[str] = None
    last_settlement_time: Optional[int] = None      # 币安历史数据提供
    current_settlement_time: Optional[int] = None   # 实时数据提供
    next_settlement_time: Optional[int] = None      # OKX提供

class Step2Fusion:
    """第二步：数据融合（状态机版）"""
    
    def __init__(self):
        # ✅ 状态缓存：按交易所+symbol缓存不同类型的数据
        self.state_cache = {}  # key: f"{exchange}_{symbol}" -> {"ticker": item, "funding": item, "last_update": timestamp}
        
        # ✅ 按合约统计
        self.fused_contracts = set()  # 成功融合的合约
        self.current_batch_fused = set()  # 当前批次融合的合约
        self.okx_fused = set()  # OKX融合的合约
        self.binance_fused = set()  # 币安融合的合约
        self.binance_with_history = set()  # 使用历史费率的合约
        
        # ✅ 5分钟统计
        self.last_report_time = time.time()
        
        log_data_process("步骤2", "启动", "Step2Fusion初始化完成（状态机版）")
    
    def process(self, step1_results: List["ExtractedData"]) -> List[FusedData]:
        """处理Step1的提取结果（状态机版）"""
        # 重置当前批次
        self.current_batch_fused.clear()
        
        results = []
        for item in step1_results:
            try:
                # 生成状态键
                state_key = f"{item.exchange}_{item.symbol}"
                
                # 初始化状态
                if state_key not in self.state_cache:
                    self.state_cache[state_key] = {
                        "ticker": None,
                        "funding": None,
                        "last_update": time.time()
                    }
                
                state = self.state_cache[state_key]
                state["last_update"] = time.time()
                
                # 根据数据类型存入状态
                data_type = item.data_type
                if "ticker" in data_type:
                    state["ticker"] = item
                elif "funding" in data_type:  # 包括funding_rate, mark_price, funding_settlement
                    state["funding"] = item
                
                # 检查是否可以融合
                fused_result = self._check_and_fuse(state_key, state)
                if fused_result:
                    results.append(fused_result)
                    
                    # ✅ 记录融合的合约
                    symbol = item.symbol
                    self.fused_contracts.add(symbol)
                    self.current_batch_fused.add(symbol)
                    
                    if item.exchange == "okx":
                        self.okx_fused.add(symbol)
                    elif item.exchange == "binance":
                        self.binance_fused.add(symbol)
                        # ✅ 记录是否使用了历史费率
                        if "funding_settlement" in item.data_type:
                            self.binance_with_history.add(symbol)
                    
                    # ✅ 融合成功后清理状态
                    del self.state_cache[state_key]
                    
                    log_data_process("步骤2", "融合", f"{item.exchange} {symbol} ✓ 数据融合完成")
                    
            except Exception as e:
                log_data_process("步骤2", "错误", f"处理失败: {item.exchange}.{item.symbol} - {e}", "ERROR")
                continue
        
        # ✅ 清理过期状态（防内存泄漏）
        self._cleanup_stale_states()
        
        # ✅ 5分钟统计
        current_time = time.time()
        if current_time - self.last_report_time >= 300:
            okx_count = len(self.okx_fused)
            binance_count = len(self.binance_fused)
            history_count = len(self.binance_with_history)
            current_count = len(self.current_batch_fused)
            
            log_data_process("步骤2", "统计", f"5分钟融合 {okx_count + binance_count} 个合约")
            log_data_process("步骤2", "详情", f"OKX: {okx_count}合约 | 币安: {binance_count}合约（含历史费率: {history_count}）")
            log_data_process("步骤2", "当前", f"当前批次: {current_count}合约")
            
            # 重置统计
            self.last_report_time = current_time
            self.okx_fused.clear()
            self.binance_fused.clear()
            self.binance_with_history.clear()
        
        return results
    
    def _check_and_fuse(self, state_key: str, state: Dict) -> Optional[FusedData]:
        """检查状态并融合 - 修复版"""
        exchange_symbol = state_key.split("_")
        if len(exchange_symbol) != 2:
            return None
        
        exchange, symbol = exchange_symbol
        
        if exchange == "okx":
            # OKX需要ticker和funding_rate
            if state["ticker"] and state["funding"]:
                return self._merge_okx_pair(state["ticker"], state["funding"])
        
        elif exchange == "binance":
            # ✅ 修复：币安需要mark_price（不能是历史费率）
            if state["funding"] and state["ticker"]:
                data_type = state["funding"].data_type
                # 只有mark_price可以立即融合（包含历史费率信息）
                if "mark_price" in data_type:
                    return self._merge_binance_pair(symbol, state["funding"], state["ticker"])
                # 历史费率需要等待mark_price
                elif "funding_settlement" in data_type:
                    # 历史费率单独不能融合，需要等待mark_price
                    return None
        
        return None
    
    def _merge_binance_pair(self, symbol: str, mark_price_item, ticker_item) -> Optional[FusedData]:
        """合并币安数据：mark_price + (历史费率，如果有的话)"""
        try:
            # 获取历史费率结算时间（如果存在）
            last_settlement_time = None
            if mark_price_item.payload.get("last_settlement_time"):
                last_settlement_time = self._to_int(mark_price_item.payload.get("last_settlement_time"))
            
            # 构建融合数据
            fused = FusedData(
                exchange="binance",
                symbol=symbol,
                contract_name=mark_price_item.payload.get("contract_name", ""),
                latest_price=ticker_item.payload.get("latest_price") if ticker_item else None,
                funding_rate=mark_price_item.payload.get("funding_rate"),
                current_settlement_time=self._to_int(mark_price_item.payload.get("current_settlement_time")),
                last_settlement_time=last_settlement_time  # 历史费率提供
            )
            
            return fused
            
        except Exception as e:
            log_data_process("步骤2", "错误", f"币安合并失败: {symbol} - {e}", "ERROR")
            return None
    
    def _merge_okx_pair(self, ticker_item, funding_item) -> Optional[FusedData]:
        """合并OKX的ticker和funding_rate数据"""
        try:
            symbol = ticker_item.symbol
            
            fused = FusedData(
                exchange="okx",
                symbol=symbol,
                contract_name=ticker_item.payload.get("contract_name", ""),
                latest_price=ticker_item.payload.get("latest_price"),
                funding_rate=funding_item.payload.get("funding_rate"),
                current_settlement_time=self._to_int(funding_item.payload.get("current_settlement_time")),
                next_settlement_time=self._to_int(funding_item.payload.get("next_settlement_time"))
            )
            
            return fused
            
        except Exception as e:
            log_data_process("步骤2", "错误", f"OKX合并失败: {e}", "ERROR")
            return None
    
    def _cleanup_stale_states(self):
        """清理过期状态（超过30秒未更新）"""
        current_time = time.time()
        stale_keys = []
        
        for key, state in self.state_cache.items():
            if current_time - state["last_update"] > 30:  # 30秒未更新
                stale_keys.append(key)
        
        for key in stale_keys:
            del self.state_cache[key]
            log_data_process("步骤2", "清理", f"清理过期状态: {key}", "DEBUG")
    
    def _to_int(self, value: Any) -> Optional[int]:
        """安全转换为int"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError) as e:
            log_data_process("步骤2", "警告", f"时间戳转换失败: {value} - {e}", "WARNING")
            return None
    
    def get_status(self) -> Dict[str, Any]:
        """获取状态信息"""
        return {
            "active_states": len(self.state_cache),
            "fused_contracts": len(self.fused_contracts),
            "current_batch_fused": len(self.current_batch_fused),
            "okx_fused": len(self.okx_fused),
            "binance_fused": len(self.binance_fused),
            "binance_with_history": len(self.binance_with_history)
        }