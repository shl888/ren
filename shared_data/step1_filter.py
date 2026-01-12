"""
第一步：提取5种原始数据中的指定数据
功能：精炼5种原始数据
输出：精炼后的5种原始数据
"""
import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict
from dataclasses import dataclass
import time

logger = logging.getLogger(__name__)

@dataclass
class ExtractedData:
    data_type: str
    exchange: str
    symbol: str
    payload: Dict

class Step1Filter:
    FIELD_MAP = {
        "okx_ticker": {"path": ["raw_data", "data", 0], "fields": {"contract_name": "instId", "latest_price": "last"}},
        "okx_funding_rate": {"path": ["raw_data", "data", 0], "fields": {"contract_name": "instId", "funding_rate": "fundingRate", "current_settlement_time": "fundingTime", "next_settlement_time": "nextFundingTime"}},
        "binance_ticker": {"path": ["raw_data"], "fields": {"contract_name": "s", "latest_price": "c"}},
        "binance_mark_price": {"path": ["raw_data"], "fields": {"contract_name": "s", "funding_rate": "r", "current_settlement_time": "T"}},
        "binance_funding_settlement": {"path": [], "fields": {"contract_name": "symbol", "funding_rate": "funding_rate", "last_settlement_time": "funding_time"}}
    }
    
    def __init__(self):
        self.stats = defaultdict(int)
        self.last_log_time = 0
        self.log_interval = 120  # 2分钟，单位：秒
        self.process_count = 0
    
    def process(self, raw_items: List[Dict[str, Any]]) -> List[ExtractedData]:
        # 频率控制：只偶尔显示处理日志
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        # 统计原始数据的类型和对应合约数（只统计5种数据类型）
        raw_contract_stats = defaultdict(set)
        for item in raw_items:
            exchange = item.get("exchange", "unknown")
            data_type = item.get("data_type", "unknown")
            symbol = item.get("symbol", "")
            
            # 构建标准类型键
            if exchange == "binance" and data_type == "funding_settlement":
                type_key = "binance_funding_settlement"
            else:
                type_key = f"{exchange}_{data_type}"
            
            # 只统计已知的5种数据类型
            if type_key in self.FIELD_MAP:
                # 添加symbol（即使为空也会添加，但我们需要知道有数据存在）
                raw_contract_stats[type_key].add(symbol if symbol else "empty")
        
        if should_log:
            logger.info(f"🔄【流水线步骤1】开始处理 {len(raw_items)} 条原始数据...")
            
            # ✅ 修复：将所有统计信息收集到一个字符串中一次性输出
            stats_lines = []
            stats_lines.append("📊【流水线步骤1】原始数据合约统计:")
            
            # ✅ 固定显示所有5种数据类型
            type_order = [
                "binance_ticker",
                "binance_mark_price", 
                "binance_funding_settlement",
                "okx_ticker",
                "okx_funding_rate"
            ]
            
            for type_key in type_order:
                # 获取实际合约数（排除空symbol）
                symbol_set = raw_contract_stats.get(type_key, set())
                # 计算实际合约数（排除空字符串）
                actual_count = len([s for s in symbol_set if s and s != "empty"])
                stats_lines.append(f"  • {type_key}: {actual_count} 个合约")
            
            # ✅ 一次性输出所有统计信息
            logger.info("\n".join(stats_lines))
            
            self.last_log_time = current_time
        
        results = []
        extracted_contract_stats = defaultdict(set)
        
        # 批量处理，不打印每条数据的处理日志
        for item in raw_items:
            try:
                extracted = self._extract_item(item)
                if extracted:
                    results.append(extracted)
                    self.stats[extracted.data_type] += 1
                    extracted_contract_stats[extracted.data_type].add(extracted.symbol)
            except Exception as e:
                # 只打印错误日志，正常处理过程不打印
                logger.error(f"❌【流水线步骤1】提取失败: {item.get('exchange')}.{item.get('symbol')} - {e}")
                continue
        
        if should_log:
#            logger.info(f"✅【流水线步骤1】Step1过滤完成，共提取 {len(results)} 条数据")
            
            # ✅ 同样修复提取后的统计信息
#            extracted_stats_lines = []
#            extracted_stats_lines.append("📊【流水线步骤1】提取数据合约统计:")
            
#            for type_key in type_order:
                # 计算实际提取到的合约数
#                symbol_set = extracted_contract_stats.get(type_key, set())
#                actual_count = len([s for s in symbol_set if s])  # 排除空字符串
#                extracted_stats_lines.append(f"  • {type_key}: {actual_count} 个合约")
            
            # ✅ 一次性输出所有提取统计信息
#            logger.info("\n".join(extracted_stats_lines))
            
            # 重置计数（仅用于频率控制）
            self.process_count = 0
        
        self.process_count += 1
        
        return results
    
    def _traverse_path(self, data: Any, path: List[Any]) -> Any:
        """遍历路径获取数据"""
        result = data
        for key in path:
            if isinstance(key, int) and isinstance(result, list):
                result = result[key] if key < len(result) else None
            elif isinstance(result, dict):
                result = result.get(key)
            else:
                result = None
                break
            if result is None:
                break
        return result
    
    def _extract_item(self, raw_item: Dict[str, Any]) -> Optional[ExtractedData]:
        """提取单个数据项"""
        exchange = raw_item.get("exchange")
        data_type = raw_item.get("data_type")
        type_key = "binance_funding_settlement" if data_type == "funding_settlement" else f"{exchange}_{data_type}"
        
        if type_key not in self.FIELD_MAP:
            # 只在遇到未知类型时打印警告，而不是每条数据都打印
            logger.warning(f"⚠️【流水线步骤1】未知数据类型: {type_key}")
            return None
        
        config = self.FIELD_MAP[type_key]
        path = config["path"]
        fields = config["fields"]
        
        # 统一提取逻辑，增加类型注解和验证
        data_source = raw_item if type_key == "binance_funding_settlement" else self._traverse_path(raw_item, path)
        
        # 增加空值检查
        if data_source is None:
            # 不再为每条空值数据打印警告，减少日志刷屏
            return None
        
        # 统一的字段提取逻辑
        extracted_payload = {}
        for output_key, input_key in fields.items():
            # 统一从 data_source 提取
            value = data_source.get(input_key) if isinstance(data_source, dict) else None
            extracted_payload[output_key] = value
        
        # 获取 symbol
        symbol = raw_item.get("symbol", "")
        if exchange == "okx":
            inst_id = extracted_payload.get("contract_name", "")
            if inst_id:
                symbol = inst_id.replace("-SWAP", "").replace("-", "")
        
        if exchange == "binance" and not symbol:
            symbol = extracted_payload.get("contract_name", "")
        
        return ExtractedData(
            data_type=type_key,
            exchange=exchange,
            symbol=symbol,
            payload=extracted_payload
        )
        