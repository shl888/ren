"""
第一步：提取6种原始数据中的指定数据
功能：精炼6种原始数据
输出：精炼后的6种原始数据
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
            "fields": {
                "contract_name": "instId", 
                "trade_price": "last"  # ✅ renamed: latest_price → trade_price
            }
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
        # ✅ NEW: OKX mark_price独立通道
        "okx_mark_price": {
            "path": ["data", "raw_data", "data", 0],
            "fields": {
                "contract_name": "instId",
                "mark_price": "markPx"
            }
        },
        "binance_ticker": {
            "path": ["data", "raw_data"], 
            "fields": {
                "contract_name": "s", 
                "trade_price": "c"  # ✅ renamed: latest_price → trade_price
            }
        },
        "binance_mark_price": {
            "path": ["data", "raw_data"], 
            "fields": {
                "contract_name": "s", 
                "funding_rate": "r", 
                "current_settlement_time": "T",
                "mark_price": "p"  # ✅ 确保mark_price被提取
            }
        },
        # ✅ 币安历史费率数据（统一格式）
        "binance_funding_settlement": {
            "path": ["data", "raw_data"],  # 与其他数据格式统一
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
        self.log_detail_counter = 0  # 用于记录详细日志的计数器
        
        # 每小时重置统计计数相关
        self._last_hourly_reset = time.time()
        self._hourly_reset_interval = 3600  # 1小时 = 3600秒
    
    def process(self, raw_items: List[Dict[str, Any]]) -> List[ExtractedData]:
        """处理原始数据"""
        # 检查是否需要每小时重置统计
        self._check_hourly_reset()
        
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        # 处理前日志 - 只在频率控制时打印
        if should_log:
            logger.info(f"🔄【流水线步骤1】开始处理 {len(raw_items)} 条原始数据...")
        
        # 统计原始数据
        raw_contract_stats = defaultdict(set)
        
        for item in raw_items:
            exchange = item.get("exchange", "unknown")
            data_type = item.get("data_type", "unknown")
            symbol = item.get("symbol", "")
            
            # 生成类型键
            if exchange == "binance" and data_type == "funding_settlement":
                type_key = "binance_funding_settlement"
            elif exchange == "binance" and data_type == "mark_price":
                type_key = "binance_mark_price"
            elif exchange == "okx" and data_type == "mark_price":  # ✅ NEW
                type_key = "okx_mark_price"
            else:
                type_key = f"{exchange}_{data_type}"
            
            if type_key in self.FIELD_MAP:
                raw_contract_stats[type_key].add(symbol if symbol else "empty")
        
        # 提取数据
        results = []
        self.log_detail_counter = 0  # 重置详细日志计数器
        
        for item in raw_items:
            try:
                extracted = self._extract_item(item)
                if extracted:
                    results.append(extracted)
                    self.stats[extracted.data_type] += 1
                        
            except Exception as e:
                # 错误日志仍然保留
                logger.error(f"❌【流水线步骤1】提取失败: {item.get('exchange')}.{item.get('symbol')} - {e}")
                continue
        
        # 处理后日志 - 只在频率控制时打印
        if should_log:
            logger.info(f"✅【流水线步骤1】过滤完成，共提取 {len(results)} 条精简数据")
            
            # 统计每种数据类型的提取数量（当前小时的统计）
            if self.stats:
                logger.info("📊【流水线步骤1】提取数据统计（当前小时）:")
                for data_type, count in sorted(self.stats.items()):
                    logger.info(f"  • {data_type}: {count} 条")
            
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
        
        # 生成类型键
        if exchange == "binance" and data_type == "funding_settlement":
            type_key = "binance_funding_settlement"
        elif exchange == "binance" and data_type == "mark_price":
            type_key = "binance_mark_price"
        elif exchange == "okx" and data_type == "mark_price":  # ✅ NEW
            type_key = "okx_mark_price"
        else:
            type_key = f"{exchange}_{data_type}"
        
        if type_key not in self.FIELD_MAP:
            return None
        
        config = self.FIELD_MAP[type_key]
        path = config["path"]
        fields = config["fields"]
        
        # 遍历路径获取数据源
        if path and len(path) > 0:
            data_source = self._traverse_path(raw_item, path)
        else:
            data_source = raw_item
        
        if data_source is None:
            return None
        
        # 提取字段
        extracted_payload = {}
        for output_key, input_key in fields.items():
            value = data_source.get(input_key) if isinstance(data_source, dict) else None
            extracted_payload[output_key] = value
        
        # 获取 symbol
        if not symbol and "contract_name" in extracted_payload:
            symbol = extracted_payload["contract_name"]
        
        # 对于币安历史费率，检查必要字段
        if type_key == "binance_funding_settlement":
            if extracted_payload.get('funding_rate') is None:
                return None
        
        return ExtractedData(
            data_type=type_key,
            exchange=exchange,
            symbol=symbol,
            payload=extracted_payload
        )
    
    # ==================== 每小时重置方法 ====================
    
    def _check_hourly_reset(self):
        """检查并执行每小时统计重置"""
        current_time = time.time()
        time_since_reset = current_time - self._last_hourly_reset
        
        if time_since_reset >= self._hourly_reset_interval:
            self._reset_hourly_stats()
            self._last_hourly_reset = current_time
    
    def _reset_hourly_stats(self):
        """每小时重置统计计数"""
        logger.info("🕐【流水线步骤1】每小时统计重置开始")
        
        # 记录重置前的统计快照（可选，仅日志）
        if self.stats:
            total_before = sum(self.stats.values())
            logger.info(f"📊【流水线步骤1】重置前统计: 总共提取 {total_before} 条数据")
        
        # 重置统计计数
        self.stats.clear()
        
        logger.info("✅【流水线步骤1】每小时统计重置完成")
    
    def get_status(self) -> Dict[str, Any]:
        """获取步骤状态（包含每小时重置检查）"""
        self._check_hourly_reset()
        
        return {
            "stats": dict(self.stats),
            "process_count": self.process_count,
            "next_reset_in": max(0, self._hourly_reset_interval - (time.time() - self._last_hourly_reset)),
            "last_reset_time": self._last_hourly_reset,
            "timestamp": time.time()
        }