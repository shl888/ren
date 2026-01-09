import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict
from dataclasses import dataclass
import time  # ✅ 添加time模块

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
        # ✅ 添加：5分钟统计计时器
        self.last_report_time = time.time()
        self.batch_count = 0
        self.total_processed = 0
        self.total_success = 0
        self.type_stats = defaultdict(int)
        
        log_data_process("步骤1", "启动", "Step1Filter初始化完成（5种数据源提取）")
    
    def process(self, raw_items: List[Dict[str, Any]]) -> List[ExtractedData]:
        # ✅ 修改：移除开始处理日志，改为计数
        self.batch_count += 1
        batch_size = len(raw_items)
        self.total_processed += batch_size
        
        results = []
        for item in raw_items:
            try:
                extracted = self._extract_item(item)
                if extracted:
                    results.append(extracted)
                    self.stats[extracted.data_type] += 1
                    self.type_stats[extracted.data_type] += 1
                    self.total_success += 1
            except Exception as e:
                exchange = item.get('exchange', 'unknown')
                symbol = item.get('symbol', 'unknown')
                log_data_process("步骤1", "错误", f"数据提取失败: {exchange}.{symbol} - {e}", "ERROR")
                continue
        
        # ✅ 添加：5分钟统计
        current_time = time.time()
        if current_time - self.last_report_time >= 300:  # 5分钟
            success_rate = (self.total_success / self.total_processed * 100) if self.total_processed > 0 else 0
            
            # 准备类型分布字符串
            type_dist = []
            for data_type, count in self.type_stats.items():
                if count > 0:
                    type_dist.append(f"{data_type}:{count}")
            type_str = ", ".join(type_dist) if type_dist else "无"
            
            log_data_process("步骤1", "统计", 
                           f"5分钟: 处理{self.total_processed}条 → 提取{self.total_success}条 "
                           f"({success_rate:.1f}%)")
            log_data_process("步骤1", "完成", f"类型分布: {type_str}")
            
            # 重置统计
            self.last_report_time = current_time
            self.batch_count = 0
            self.total_processed = 0
            self.total_success = 0
            self.type_stats.clear()
        
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
            log_data_process("步骤1", "警告", f"未知数据类型: {type_key}", "WARNING")
            return None
        
        config = self.FIELD_MAP[type_key]
        path = config["path"]
        fields = config["fields"]
        
        # 统一提取逻辑，增加类型注解和验证
        data_source = raw_item if type_key == "binance_funding_settlement" else self._traverse_path(raw_item, path)
        
        # 增加空值检查
        if data_source is None:
            log_data_process("步骤1", "跳过", f"{type_key} 数据路径失败: {path}", "WARNING")
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