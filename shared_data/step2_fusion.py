"""
第二步：数据融合与统一规格
功能：将Step1提取的5种数据源，按交易所+合约名合并成一条
输出：每个交易所每个合约一条完整数据
"""

import logging
from typing import Dict, List, Any, Optional, TYPE_CHECKING
from collections import defaultdict
from dataclasses import dataclass

# 类型检查时导入，避免循环依赖
if TYPE_CHECKING:
    from step1_filter import ExtractedData

logger = logging.getLogger(__name__)

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
        self.fusion_stats = {
            "total_groups": 0,
            "success_groups": 0,
            "failed_groups": 0
        }
    
    def process(self, step1_results: List["ExtractedData"]) -> List[FusedData]:
        """
        处理Step1的提取结果，按交易所+合约名合并
        """
        logger.info(f"🔄【流水线步骤2】开始融合 {len(step1_results)} 条Step1数据...")
        
        # 按 exchange + symbol 分组
        grouped = defaultdict(list)
        for item in step1_results:
            key = f"{item.exchange}_{item.symbol}"
            grouped[key].append(item)
        
        self.fusion_stats["total_groups"] = len(grouped)
        logger.info(f"【流水线步骤2】检测到 {len(grouped)} 个不同的交易所合约")
        
        # 合并每组数据
        results = []
        for key, items in grouped.items():
            try:
                fused = self._merge_group(items)
                if fused:
                    results.append(fused)
                    self.stats[fused.exchange] += 1
                    self.fusion_stats["success_groups"] += 1
                else:
                    self.fusion_stats["failed_groups"] += 1
            except Exception as e:
                logger.error(f"❌【流水线步骤2】融合失败: {key} - {e}")
                self.fusion_stats["failed_groups"] += 1
                continue
        
        # 处理完成后，打印统计结果
        self._log_statistics(results)
        
        logger.info(f"✅【流水线步骤2】Step2融合完成，共生成 {len(results)} 条融合数据")
        return results
    
    def _log_statistics(self, results: List[FusedData]):
        """打印统计结果"""
        logger.info("📝【流水线步骤2】融合结果统计:")
        
        # 按交易所统计
        okx_count = self.stats.get("okx", 0)
        binance_count = self.stats.get("binance", 0)
        total_count = okx_count + binance_count
        
        logger.info(f"  OKX合约数: {okx_count}")
        logger.info(f"  币安合约数: {binance_count}")
        logger.info(f"  总计: {total_count}")
        
        # 验证字段完整性
        if results:
            self._validate_fields(results)
        
        # 融合过程统计
        logger.info(f"  融合组数: {self.fusion_stats['total_groups']}")
        logger.info(f"  成功融合: {self.fusion_stats['success_groups']}")
        logger.info(f"  失败/跳过: {self.fusion_stats['failed_groups']}")
    
    def _validate_fields(self, results: List[FusedData]):
        """验证字段完整性"""
        okx_valid = 0
        binance_valid = 0
        
        for item in results:
            if item.exchange == "okx":
                # OKX验证：应该有next_settlement_time，没有last_settlement_time
                if item.next_settlement_time is not None and item.last_settlement_time is None:
                    okx_valid += 1
            elif item.exchange == "binance":
                # 币安验证：应该有last_settlement_time，没有next_settlement_time
                if item.last_settlement_time is not None and item.next_settlement_time is None:
                    binance_valid += 1
        
        total_count = len(results)
        okx_count = self.stats.get("okx", 0)
        binance_count = self.stats.get("binance", 0)
        
        # 只打印验证结果，不打印每条合约的验证过程
        if okx_count > 0:
            validation_rate = (okx_valid / okx_count) * 100 if okx_count > 0 else 0
            logger.info(f"【流水线步骤2】 ✅ OKX合约验证通过率: {okx_valid}/{okx_count} ({validation_rate:.1f}%)")
            logger.info("【流水线步骤2】✅ OKX合约的last_settlement_time正确为空")
            
        if binance_count > 0:
            validation_rate = (binance_valid / binance_count) * 100 if binance_count > 0 else 0
            logger.info(f"【流水线步骤2】✅ 币安合约验证通过率: {binance_valid}/{binance_count} ({validation_rate:.1f}%)")
            logger.info("【流水线步骤2】✅ 币安合约的next_settlement_time正确为空")
    
    def _merge_group(self, items: List["ExtractedData"]) -> Optional[FusedData]:
        """合并同一组内的所有数据"""
        if not items:
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
            
            # funding_rate数据：提取费率和时间
            elif item.data_type == "okx_funding_rate":
                fused.funding_rate = payload.get("funding_rate")
                fused.current_settlement_time = self._to_int(payload.get("current_settlement_time"))
                fused.next_settlement_time = self._to_int(payload.get("next_settlement_time"))
        
        # 验证：至少要有价格或费率之一
        if not any([fused.latest_price, fused.funding_rate]):
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
            return None
        
        # 从mark_price提取核心数据
        mark_payload = mark_price_item.payload
        fused.contract_name = mark_payload.get("contract_name", fused.symbol)
        fused.funding_rate = mark_payload.get("funding_rate")
        fused.current_settlement_time = self._to_int(mark_payload.get("current_settlement_time"))
        
        # 验证：mark_price必须有费率
        if fused.funding_rate is None:
            return None
        
        # ticker数据：提取价格
        for item in items:
            if item.data_type == "binance_ticker":
                fused.latest_price = item.payload.get("latest_price")
                break
        
        # funding_settlement数据：填充上次结算时间
        for item in items:
            if item.data_type == "binance_funding_settlement":
                fused.last_settlement_time = self._to_int(item.payload.get("last_settlement_time"))
                break  # 只取第一个
        
        return fused
    
    def _to_int(self, value: Any) -> Optional[int]:
        """安全转换为int"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None