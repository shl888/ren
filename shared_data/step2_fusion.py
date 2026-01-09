"""
第二步：数据融合与统一规格
功能：将Step1提取的5种数据源，按交易所+合约名合并成一条
输出：每个交易所每个合约一条完整数据
"""

import logging
from typing import Dict, List, Any, Optional, TYPE_CHECKING
from collections import defaultdict
from dataclasses import dataclass
import time  # ✅ 添加time模块

# 类型检查时导入，避免循环依赖
if TYPE_CHECKING:
    from step1_filter import ExtractedData

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
    """第二步：数据融合"""
    
    def __init__(self):
        self.stats = defaultdict(int)
        # ✅ 添加：5分钟统计计时器
        self.last_report_time = time.time()
        self.total_input = 0
        self.total_output = 0
        self.exchange_stats = defaultdict(int)
        
        log_data_process("步骤2", "启动", "Step2Fusion初始化完成（按交易所+合约合并）")
    
    def process(self, step1_results: List["ExtractedData"]) -> List[FusedData]:
        """
        处理Step1的提取结果，按交易所+合约名合并
        """
        # ✅ 修改：移除开始处理日志，改为计数
        input_count = len(step1_results)
        self.total_input += input_count
        
        # 按 exchange + symbol 分组
        grouped = defaultdict(list)
        for item in step1_results:
            key = f"{item.exchange}_{item.symbol}"
            grouped[key].append(item)
        
        # 合并每组数据
        results = []
        for key, items in grouped.items():
            try:
                fused = self._merge_group(items)
                if fused:
                    results.append(fused)
                    self.stats[fused.exchange] += 1
                    self.exchange_stats[fused.exchange] += 1
                    self.total_output += 1
                else:
                    # ✅ 修改：恢复到debug级别，避免警告刷屏
                    logger.debug(f"{key}: 融合返回空结果")
            except Exception as e:
                logger.error(f"融合失败: {key} - {e}", exc_info=True)
                continue
        
        # ✅ 添加：5分钟统计
        current_time = time.time()
        if current_time - self.last_report_time >= 300:  # 5分钟
            # 准备交易所分布字符串
            exchange_dist = []
            for exchange, count in self.exchange_stats.items():
                if count > 0:
                    exchange_dist.append(f"{exchange}:{count}")
            exchange_str = ", ".join(exchange_dist) if exchange_dist else "无"
            
            conversion_rate = (self.total_output / self.total_input * 100) if self.total_input > 0 else 0
            
            log_data_process("步骤2", "统计", 
                           f"5分钟: 接收{self.total_input}条 → 融合{self.total_output}合约 "
                           f"({conversion_rate:.1f}%)")
            log_data_process("步骤2", "完成", f"交易所分布: {exchange_str}")
            
            # 重置统计
            self.last_report_time = current_time
            self.total_input = 0
            self.total_output = 0
            self.exchange_stats.clear()
        
        return results
    
    def _merge_group(self, items: List["ExtractedData"]) -> Optional[FusedData]:
        """合并同一组内的所有数据"""
        if not items:
            logger.debug("合并组接收空列表")
            return None
        
        # 取第一条的基础信息
        first = items[0]
        exchange = first.exchange
        symbol = first.symbol
        
        # 初始化融合结果
        fused = FusedData(
            exchange=exchange,
            symbol=symbol,
            contract_name=""
        )
        
        # 按交易所分发处理
        if exchange == "okx":
            return self._merge_okx(items, fused)
        elif exchange == "binance":
            return self._merge_binance(items, fused)
        else:
            logger.warning(f"未知交易所: {exchange}，跳过")
            return None
    
    def _merge_okx(self, items: List["ExtractedData"], fused: FusedData) -> Optional[FusedData]:
        """合并OKX数据：ticker + funding_rate"""
        
        for item in items:
            payload = item.payload
            
            # 提取合约名（OKX数据里都有）
            if not fused.contract_name and "contract_name" in payload:
                fused.contract_name = payload["contract_name"]
            
            # ticker数据：提取价格
            if item.data_type == "okx_ticker":
                fused.latest_price = payload.get("latest_price")
                logger.debug(f"OKX {fused.symbol} ✓ 提取价格: {fused.latest_price}")
            
            # funding_rate数据：提取费率和时间
            elif item.data_type == "okx_funding_rate":
                fused.funding_rate = payload.get("funding_rate")
                fused.current_settlement_time = self._to_int(payload.get("current_settlement_time"))
                fused.next_settlement_time = self._to_int(payload.get("next_settlement_time"))
                logger.debug(f"OKX {fused.symbol} ✓ 提取费率: {fused.funding_rate}")
        
        # 验证：至少要有价格或费率之一
        if not any([fused.latest_price, fused.funding_rate]):
            logger.debug(f"OKX {fused.symbol} 跳过：无有效数据")
            return None
        
        return fused
    
    def _merge_binance(self, items: List["ExtractedData"], fused: FusedData) -> Optional[FusedData]:
        """合并币安数据：核心是以mark_price为准"""
        
        # 第一步：找mark_price数据（必须有）
        mark_price_item = None
        for item in items:
            if item.data_type == "binance_mark_price":
                mark_price_item = item
                break
        
        if not mark_price_item:
            # ✅ 恢复到原文件的debug级别，避免警告刷屏
            logger.debug(f"币安 {fused.symbol} 跳过：无mark_price数据（必须有实时费率）")
            return None
        
        logger.debug(f"币安 {fused.symbol} ✓ 找到mark_price")
        
        # 从mark_price提取核心数据
        mark_payload = mark_price_item.payload
        fused.contract_name = mark_payload.get("contract_name", fused.symbol)
        fused.funding_rate = mark_payload.get("funding_rate")
        fused.current_settlement_time = self._to_int(mark_payload.get("current_settlement_time"))
        
        # 验证：mark_price必须有费率
        if fused.funding_rate is None:
            logger.warning(f"币安 {fused.symbol} 跳过：mark_price中无费率数据")
            return None
        
        # ticker数据：提取价格
        for item in items:
            if item.data_type == "binance_ticker":
                fused.latest_price = item.payload.get("latest_price")
                logger.debug(f"币安 {fused.symbol} ✓ 提取价格: {fused.latest_price}")
                break
        
        # funding_settlement数据：填充上次结算时间
        for item in items:
            if item.data_type == "binance_funding_settlement":
                fused.last_settlement_time = self._to_int(item.payload.get("last_settlement_time"))
                logger.debug(f"币安 {fused.symbol} ✓ 提取上次结算时间")
                break  # 只取第一个
        
        return fused
    
    def _to_int(self, value: Any) -> Optional[int]:
        """安全转换为int"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError) as e:
            logger.warning(f"时间戳转换失败: {value} - {e}")
            return None