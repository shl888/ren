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
        self.last_log_time = 0
        self.log_interval = 60  # 1分钟，单位：秒
        self.process_count = 0
    
    def process(self, step1_results: List["ExtractedData"]) -> List[FusedData]:
        """
        处理Step1的提取结果，按交易所+合约名合并
        """
        # 重置统计，避免累积
        self.fusion_stats = {
            "total_groups": 0,
            "success_groups": 0,
            "failed_groups": 0
        }
        self.stats.clear()
        
        # 频率控制：只偶尔显示处理日志
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        # 按 exchange + symbol 分组
        grouped = defaultdict(list)
        for item in step1_results:
            key = f"{item.exchange}_{item.symbol}"
            grouped[key].append(item)
        
        self.fusion_stats["total_groups"] = len(grouped)
        
        if should_log:
            logger.info(f"🔄【流水线步骤2】开始融合Step1输出的 {len(step1_results)} 条精简数据...")
            logger.info(f"【流水线步骤2】检测到 {len(grouped)} 个不同的交易所合约")
        
        # 合并每组数据
        results = []
        exchange_contracts = defaultdict(set)  # 统计成功融合的合约
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
                # 只在日志频率控制时打印错误
                if should_log:
                    logger.error(f"❌【流水线步骤2】融合失败: {key} - {e}")
                continue
        
        if should_log:
            # 处理完成后，打印统计结果
            logger.info(f"✅【流水线步骤2】Step2融合完成，共生成 {len(results)} 条融合数据")
            
            # 按交易所统计合约数
            okx_contracts = len(exchange_contracts.get("okx", set()))
            binance_contracts = len(exchange_contracts.get("binance", set()))
            total_contracts = okx_contracts + binance_contracts
            
            logger.info("📊【流水线步骤2】融合结果合约统计:")
            if okx_contracts > 0:
                logger.info(f"  • OKX合约数: {okx_contracts} 个")
            if binance_contracts > 0:
                logger.info(f"  • 币安合约数: {binance_contracts} 个")
            logger.info(f"  • 总计: {total_contracts} 个合约")
            
            # 详细统计信息
            logger.info("📊【流水线步骤2】融合详细统计:")
            logger.info(f"  • 总合约组数: {fusion_stats_detail['total_groups']}")
            logger.info(f"  • 符合要求组数: {fusion_stats_detail['has_required_fields']}")
            logger.info(f"  • 缺少ticker数据: {fusion_stats_detail['missing_ticker']}")
            logger.info(f"  • 缺少mark_price数据: {fusion_stats_detail['missing_mark_price']}")
            logger.info(f"  • 缺少funding_rate数据: {fusion_stats_detail['missing_funding_rate']}")
            logger.info(f"  • 有历史费率数据: {fusion_stats_detail['has_history']}")
            logger.info(f"  • 无历史费率数据: {fusion_stats_detail['missing_history']}")
            
            # 验证字段完整性（只针对成功融合的结果）
            if results:
                self._validate_fields(results)
            
            self.last_log_time = current_time
            # 重置计数（仅用于频率控制）
            self.process_count = 0
        
        self.process_count += 1
        
        return results
    
    def _validate_fields(self, results: List[FusedData]):
        """验证字段完整性（严格的验证规则）"""
        okx_valid = 0
        binance_valid = 0
        binance_with_history = 0
        
        for item in results:
            if item.exchange == "okx":
                # OKX验证：必须有价格、费率、下次结算时间
                required = [
                    item.latest_price,           # 实时行情数据特有
                    item.funding_rate,           # 实时费率数据特有
                    item.next_settlement_time    # 实时费率数据特有
                ]
                if all(field is not None for field in required):
                    okx_valid += 1
            
            elif item.exchange == "binance":
                # 币安验证：必须有价格、费率、本次结算时间
                required = [
                    item.latest_price,           # 实时行情数据特有
                    item.funding_rate,           # 实时费率数据特有
                    item.current_settlement_time # 实时费率数据特有
                ]
                if all(field is not None for field in required):
                    binance_valid += 1
                    # 统计有历史数据的合约
                    if item.last_settlement_time is not None:
                        binance_with_history += 1
        
        # 输出统计
        okx_count = len([r for r in results if r.exchange == "okx"])
        binance_count = len([r for r in results if r.exchange == "binance"])
        
        if okx_count > 0:
            validation_rate = (okx_valid / okx_count) * 100
            logger.info(f"📊【流水线步骤2】OKX合约验证:")
            logger.info(f"  • 验证通过: {okx_valid}/{okx_count} ({validation_rate:.1f}%)")
            if okx_valid < okx_count:
                logger.info(f"  ⚠️  {okx_count - okx_valid} 个合约缺少必要字段")
        
        if binance_count > 0:
            validation_rate = (binance_valid / binance_count) * 100
            history_rate = (binance_with_history / binance_count) * 100
            logger.info(f"📊【流水线步骤2】币安合约验证:")
            logger.info(f"  • 验证通过: {binance_valid}/{binance_count} ({validation_rate:.1f}%)")
            logger.info(f"  • 有历史数据: {binance_with_history}/{binance_count} ({history_rate:.1f}%)")
            if binance_valid < binance_count:
                logger.info(f"  ⚠️  {binance_count - binance_valid} 个合约缺少必要字段")
    
    def _merge_group(self, items: List["ExtractedData"], stats: Dict) -> Optional[FusedData]:
        """合并同一组内的所有数据（带统计）"""
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
            return self._merge_okx(items, fused, stats)
        elif exchange == "binance":
            return self._merge_binance(items, fused, stats)
        else:
            return None
    
    def _merge_okx(self, items: List["ExtractedData"], fused: FusedData, stats: Dict) -> Optional[FusedData]:
        """合并OKX数据：必须有ticker + funding_rate"""
        
        # 必须找到这两种数据
        ticker_item = None
        funding_item = None
        
        for item in items:
            if item.data_type == "okx_ticker":
                ticker_item = item
            elif item.data_type == "okx_funding_rate":
                funding_item = item
        
        # ✅ 规则1：必须有实时行情数据（ticker）
        if not ticker_item:
            stats["missing_ticker"] += 1
            return None
        
        # ✅ 规则2：必须有实时费率数据（funding_rate）
        if not funding_item:
            stats["missing_funding_rate"] += 1
            return None
        
        # 提取合约名
        ticker_payload = ticker_item.payload
        funding_payload = funding_item.payload
        
        fused.contract_name = (
            ticker_payload.get("contract_name") or 
            funding_payload.get("contract_name") or 
            fused.symbol
        )
        
        # 从ticker获取价格（实时行情数据特有）
        fused.latest_price = ticker_payload.get("latest_price")
        
        # 从funding_rate获取费率和时间（实时费率数据特有）
        fused.funding_rate = funding_payload.get("funding_rate")
        fused.current_settlement_time = self._to_int(funding_payload.get("current_settlement_time"))
        fused.next_settlement_time = self._to_int(funding_payload.get("next_settlement_time"))
        
        # ✅ 最终验证：必须有的核心字段都不能为空
        required_fields = [
            fused.latest_price,          # 实时价格（必须）
            fused.funding_rate,          # 实时费率（必须）
            fused.next_settlement_time   # 下次结算时间（必须）
        ]
        
        if any(field is None for field in required_fields):
            return None
        
        stats["has_required_fields"] += 1
        return fused
    
    def _merge_binance(self, items: List["ExtractedData"], fused: FusedData, stats: Dict) -> Optional[FusedData]:
        """合并币安数据：必须有ticker + mark_price"""
        
        # 必须找到这三种数据
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
        
        # ✅ 规则1：必须有实时行情数据（ticker）
        if not ticker_item:
            stats["missing_ticker"] += 1
            return None
        
        # ✅ 规则2：必须有实时费率数据（mark_price）
        if not mark_price_item:
            stats["missing_mark_price"] += 1
            return None
        
        # 从实时行情数据获取价格
        ticker_payload = ticker_item.payload
        fused.latest_price = ticker_payload.get("latest_price")
        
        # 从实时费率数据获取核心信息
        mark_payload = mark_price_item.payload
        fused.contract_name = mark_payload.get("contract_name", fused.symbol)
        fused.funding_rate = mark_payload.get("funding_rate")
        fused.current_settlement_time = self._to_int(mark_payload.get("current_settlement_time"))
        
        # 从历史费率数据获取上次结算时间（可有可无）
        if history_item:
            fused.last_settlement_time = self._to_int(history_item.payload.get("last_settlement_time"))
            stats["has_history"] += 1
        else:
            stats["missing_history"] += 1
        
        # ✅ 最终验证：必须有的核心字段都不能为空
        required_fields = [
            fused.latest_price,           # 实时价格（必须）
            fused.funding_rate,           # 实时费率（必须）
            fused.current_settlement_time # 本次结算时间（必须）
        ]
        
        if any(field is None for field in required_fields):
            return None
        
        stats["has_required_fields"] += 1
        return fused
    
    def _to_int(self, value: Any) -> Optional[int]:
        """安全转换为int"""
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None