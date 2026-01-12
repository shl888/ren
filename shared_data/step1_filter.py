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
        "binance_funding_settlement": {
            "path": [],  # 直接使用raw_item
            "fields": {
                "contract_name": "symbol", 
                "funding_rate": "funding_rate", 
                "last_settlement_time": "funding_time"
            }
        }
    }
    
    def __init__(self):
        self.stats = defaultdict(int)
        self.last_log_time = 0
        self.log_interval = 120  # 2分钟
        self.process_count = 0
    
    def process(self, raw_items: List[Dict[str, Any]]) -> List[ExtractedData]:
        """处理原始数据"""
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        if should_log:
            logger.info(f"🔄【流水线步骤1】开始处理 data_store流入的{len(raw_items)} 条原始数据...")
        
        # 统计原始数据中各数据类型的合约数量
        raw_stats = defaultdict(set)
        for item in raw_items:
            exchange = item.get("exchange", "unknown")
            data_type = item.get("data_type", "unknown")
            symbol = item.get("symbol", "")
            type_key = f"{exchange}_{data_type}"
            
            if symbol:
                raw_stats[type_key].add(symbol)
        
        # 输出原始数据合约统计
        if should_log and raw_stats:
            log_lines = ["📊【流水线步骤1】原始数据合约统计:"]
            for type_key in sorted(raw_stats.keys()):
                contract_count = len(raw_stats[type_key])
                log_lines.append(f"  • {type_key}: {contract_count} 个合约")
            logger.info("\n".join(log_lines))
        
        results = []
        
        for item in raw_items:
            try:
                extracted = self._extract_item(item)
                if extracted:
                    results.append(extracted)
                    self.stats[extracted.data_type] += 1
            except Exception as e:
                logger.error(f"❌【流水线步骤1】提取失败: {item.get('exchange')}.{item.get('symbol')} - {e}")
                continue
        
        if should_log:
            logger.info(f"✅【流水线步骤1】Step1过滤完成，共提取 {len(results)} 条精简数据")
            self.last_log_time = current_time
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
        symbol = raw_item.get("symbol", "")
        
        # 处理币安历史费率数据类型
        if exchange == "binance" and data_type == "funding_settlement":
            type_key = "binance_funding_settlement"
        else:
            type_key = f"{exchange}_{data_type}"
        
        if type_key not in self.FIELD_MAP:
            logger.warning(f"⚠️【流水线步骤1】未知数据类型: {type_key}")
            return None
        
        config = self.FIELD_MAP[type_key]
        path = config["path"]
        fields = config["fields"]
        
        # 统一处理逻辑
        if path and len(path) > 0:
            data_source = self._traverse_path(raw_item, path)
        else:
            data_source = raw_item
        
        if data_source is None:
            return None
        
        extracted_payload = {}
        for output_key, input_key in fields.items():
            value = data_source.get(input_key) if isinstance(data_source, dict) else None
            extracted_payload[output_key] = value
        
        # 获取 symbol
        if not symbol and "contract_name" in extracted_payload:
            symbol = extracted_payload["contract_name"]
        
        # 币安历史费率数据验证
        if type_key == "binance_funding_settlement" and extracted_payload.get('funding_rate') is None:
            return None
        
        return ExtractedData(
            data_type=type_key,
            exchange=exchange,
            symbol=symbol,
            payload=extracted_payload
        )