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
        "okx_ticker": {
            "path": ["data", "raw_data", "data", 0], 
            "fields": {"contract_name": "instId", "latest_price": "last"}
        },
        "okx_funding_rate": {
            "path": ["data", "raw_data", "data", 0], 
            "fields": {
                "contract_name": "instId", 
                "funding_rate": "fundingRate", 
                "current_settlement_time": "fundingTime", 
                "next_settlement_time": "nextFundingTime"
            }
        },
        "binance_ticker": {
            "path": ["data", "raw_data"], 
            "fields": {"contract_name": "s", "latest_price": "c"}
        },
        "binance_mark_price": {
            "path": ["data", "raw_data"], 
            "fields": {"contract_name": "s", "funding_rate": "r", "current_settlement_time": "T"}
        },
        # ✅ 修正：币安历史费率数据
        "binance_funding_settlement": {
            "path": ["data"],  # 直接从data字段获取
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
        
        # 统计原始数据
        raw_contract_stats = defaultdict(set)
        binance_history_count = 0
        
        for item in raw_items:
            exchange = item.get("exchange", "unknown")
            data_type = item.get("data_type", "unknown")
            symbol = item.get("symbol", "")
            
            if exchange == "binance" and data_type == "funding_settlement":
                type_key = "binance_funding_settlement"
                binance_history_count += 1
            else:
                type_key = f"{exchange}_{data_type}"
            
            if type_key in self.FIELD_MAP:
                raw_contract_stats[type_key].add(symbol if symbol else "empty")
        
        # ✅ 添加：记录币安历史费率数据数量
        if binance_history_count > 0:
            logger.info(f"📥【流水线步骤1】收到 {binance_history_count} 条币安历史费率数据")
        
        if should_log:
            logger.info(f"🔄【流水线步骤1】开始处理data_store流入的 {len(raw_items)} 条原始数据...")
            
            stats_lines = []
            stats_lines.append("📊【流水线步骤1】原始数据合约统计:")
            
            type_order = [
                "binance_ticker",
                "binance_mark_price", 
                "binance_funding_settlement",
                "okx_ticker",
                "okx_funding_rate"
            ]
            
            for type_key in type_order:
                symbol_set = raw_contract_stats.get(type_key, set())
                actual_count = len([s for s in symbol_set if s and s != "empty"])
                stats_lines.append(f"  • {type_key}: {actual_count} 个合约")
            
            logger.info("\n".join(stats_lines))
            self.last_log_time = current_time
        
        results = []
        extracted_contract_stats = defaultdict(set)
        binance_history_extracted = 0
        binance_history_failed = 0
        
        for item in raw_items:
            try:
                extracted = self._extract_item(item)
                if extracted:
                    results.append(extracted)
                    self.stats[extracted.data_type] += 1
                    extracted_contract_stats[extracted.data_type].add(extracted.symbol)
                    
                    # ✅ 记录币安历史费率提取成功
                    if extracted.data_type == "binance_funding_settlement":
                        binance_history_extracted += 1
                        # 检查提取的字段是否完整
                        if not extracted.payload.get('funding_rate'):
                            logger.warning(f"⚠️【步骤1调试】币安历史费率数据funding_rate为空: {extracted.symbol}")
                else:
                    # ✅ 记录提取失败
                    exchange = item.get("exchange")
                    data_type = item.get("data_type")
                    if exchange == "binance" and data_type == "funding_settlement":
                        binance_history_failed += 1
            except Exception as e:
                logger.error(f"❌【流水线步骤1】提取失败: {item.get('exchange')}.{item.get('symbol')} - {e}")
                continue
        
        # ✅ 添加：记录提取结果
        if binance_history_count > 0:
            logger.info(f"✅【流水线步骤1】币安历史费率数据提取结果:")
            logger.info(f"  • 接收总数: {binance_history_count} 条")
            logger.info(f"  • 成功提取: {binance_history_extracted} 条")
            logger.info(f"  • 提取失败: {binance_history_failed} 条")
            
            # 显示前几条提取结果
            if binance_history_extracted > 0:
                history_results = [r for r in results if r.data_type == "binance_funding_settlement"]
                for i, result in enumerate(history_results[:3]):  # 显示前3条
                    logger.info(f"🔍【步骤1调试】币安历史费率示例 {i+1}:")
                    logger.info(f"  • 合约: {result.symbol}")
                    logger.info(f"  • 费率: {result.payload.get('funding_rate')}")
                    logger.info(f"  • 历史时间: {result.payload.get('last_settlement_time')}")
        
        if should_log:
            logger.info(f"✅【流水线步骤1】Step1过滤完成，共提取 {len(results)} 条精简数据")
            
            # 统计每种数据类型的提取数量
            logger.info("📊【流水线步骤1】提取数据统计:")
            for data_type, count in self.stats.items():
                logger.info(f"  • {data_type}: {count} 条")
            
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
        
        # ✅ 添加详细调试：只针对币安历史费率数据
        is_binance_history = (exchange == "binance" and data_type == "funding_settlement")
        
        if is_binance_history:
            logger.debug(f"🔍【步骤1详细调试】开始处理币安历史费率: {exchange}.{symbol}.{data_type}")
            logger.debug(f"🔍【步骤1详细调试】raw_item keys: {list(raw_item.keys())}")
        
        # ✅ 修复：正确处理币安历史费率数据类型
        if exchange == "binance" and data_type == "funding_settlement":
            type_key = "binance_funding_settlement"
        else:
            type_key = f"{exchange}_{data_type}"
        
        if is_binance_history:
            logger.debug(f"🔍【步骤1详细调试】生成的type_key: {type_key}")
        
        if type_key not in self.FIELD_MAP:
            logger.warning(f"⚠️【流水线步骤1】未知数据类型: {type_key}")
            if is_binance_history:
                logger.warning(f"⚠️【步骤1调试】可用类型: {list(self.FIELD_MAP.keys())}")
            return None
        
        config = self.FIELD_MAP[type_key]
        path = config["path"]
        fields = config["fields"]
        
        if is_binance_history:
            logger.debug(f"🔍【步骤1详细调试】配置信息:")
            logger.debug(f"  • path: {path}")
            logger.debug(f"  • fields: {fields}")
        
        # ✅ 修复：统一处理逻辑
        if path and len(path) > 0:
            # 有路径配置：遍历路径获取数据
            data_source = self._traverse_path(raw_item, path)
        else:
            # 无路径配置：直接使用原始数据
            data_source = raw_item
        
        if is_binance_history:
            logger.debug(f"🔍【步骤1详细调试】data_source 类型: {type(data_source)}")
            if isinstance(data_source, dict):
                logger.debug(f"🔍【步骤1详细调试】data_source keys: {list(data_source.keys())}")
            else:
                logger.debug(f"🔍【步骤1详细调试】data_source: {data_source}")
        
        if data_source is None:
            if is_binance_history:
                logger.warning(f"⚠️【步骤1调试】数据源为空: {type_key}")
                logger.warning(f"⚠️【步骤1调试】路径遍历失败: path={path}")
            return None
        
        extracted_payload = {}
        
        # ✅ 提取字段并记录
        for output_key, input_key in fields.items():
            value = data_source.get(input_key) if isinstance(data_source, dict) else None
            extracted_payload[output_key] = value
            
            if is_binance_history and output_key == "funding_rate":
                logger.debug(f"🔍【步骤1详细调试】提取funding_rate字段:")
                logger.debug(f"  • 输入字段名: {input_key}")
                logger.debug(f"  • 实际值: {value}")
                logger.debug(f"  • 值类型: {type(value)}")
        
        # ✅ 获取 symbol
        if not symbol and "contract_name" in extracted_payload:
            symbol = extracted_payload["contract_name"]
        
        # ✅ 验证提取结果
        if is_binance_history:
            logger.debug(f"🔍【步骤1详细调试】提取完成:")
            logger.debug(f"  • 提取字段数: {len(extracted_payload)}")
            logger.debug(f"  • symbol: {symbol}")
            logger.debug(f"  • 包含字段: {list(extracted_payload.keys())}")
            
            # 检查关键字段是否存在
            required_fields = ["contract_name", "funding_rate", "last_settlement_time"]
            missing_fields = []
            for field in required_fields:
                value = extracted_payload.get(field)
                if value is None:
                    missing_fields.append(field)
            
            if missing_fields:
                logger.warning(f"⚠️【步骤1调试】币安历史费率数据缺失字段: {missing_fields}")
                
                # 深度调试：显示data_source中实际存在的字段
                if isinstance(data_source, dict):
                    available_keys = list(data_source.keys())
                    logger.warning(f"⚠️【步骤1调试】data_source中可用字段: {available_keys}")
            else:
                logger.info(f"✅【步骤1调试】币安历史费率数据提取成功: {symbol}")
                logger.info(f"  • 费率: {extracted_payload.get('funding_rate')}")
                logger.info(f"  • 历史时间: {extracted_payload.get('last_settlement_time')}")
        
        # 如果funding_rate为空，返回None（表示提取失败）
        if is_binance_history and extracted_payload.get('funding_rate') is None:
            logger.warning(f"⚠️【步骤1调试】币安历史费率数据funding_rate为空，提取失败: {symbol}")
            return None
        
        return ExtractedData(
            data_type=type_key,
            exchange=exchange,
            symbol=symbol,
            payload=extracted_payload
        )