import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict
from dataclasses import dataclass

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
    
    def process(self, raw_items: List[Dict[str, Any]]) -> List[ExtractedData]:
        logger.info(f"🔄【流水线步骤1】开始处理 {len(raw_items)} 条原始数据...")
        results = []
        
        # 批量处理，不打印每条数据的处理日志
        for item in raw_items:
            try:
                extracted = self._extract_item(item)
                if extracted:
                    results.append(extracted)
                    self.stats[extracted.data_type] += 1
            except Exception as e:
                # 只打印错误日志，正常处理过程不打印
                logger.error(f"❌【流水线步骤1】提取失败: {item.get('exchange')}.{item.get('symbol')} - {e}")
                continue
        
        # 处理完成后，打印统计结果
        self._log_statistics()
        
        logger.info(f"✅【流水线步骤1】Step1过滤完成，共提取 {len(results)} 条数据")
        return results
    
    def _log_statistics(self):
        """打印统计结果"""
        if not self.stats:
            logger.info("❌【流水线步骤1】未提取到任何数据")
            return
        
        logger.info("📝【流水线步骤1】提取结果统计:")
        for data_type, count in sorted(self.stats.items()):
            # 使用缩进显示，类似示例格式
            logger.info(f"  {data_type}: {count} 条")
    
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