import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict
from dataclasses import dataclass
import time

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
        # ✅ 历史费率读取控制（每个合约只读取1次）
        self.history_read_contracts = set()  # 已读取历史费率的合约
        
        # ✅ 按合约统计
        self.processed_contracts = set()  # 所有处理过的合约
        self.contract_data_types = defaultdict(set)  # 每个合约的数据类型
        
        # 当前批次统计
        self.current_batch_contracts = set()
        self.current_batch_types = defaultdict(int)
        
        # 5分钟统计
        self.last_report_time = time.time()
        
        log_data_process("步骤1", "启动", "Step1Filter初始化完成（历史费率每个合约只读取1次）")
    
    def process(self, raw_items: List[Dict[str, Any]]) -> List[ExtractedData]:
        """处理原始数据，提取关键信息"""
        # 重置当前批次统计
        self.current_batch_contracts.clear()
        self.current_batch_types.clear()
        
        results = []
        
        for item in raw_items:
            try:
                # 获取基本信息
                exchange = item.get("exchange", "")
                symbol = item.get("symbol", "")
                data_type = item.get("data_type", "")
                
                # ✅ 关键修复：历史费率读取控制
                # 如果是币安历史费率且已读取过，跳过这个数据
                if data_type == "funding_settlement" and exchange == "binance":
                    if symbol in self.history_read_contracts:
                        log_data_process("步骤1", "跳过", f"币安历史费率已读取过，跳过 {symbol}", "DEBUG")
                        continue  # ✅ 正确：跳过这个历史费率数据，不处理
                
                # ✅ 提取数据
                extracted = self._extract_item(item)
                if extracted:
                    results.append(extracted)
                    
                    # ✅ 如果是币安历史费率，标记为已读取
                    if extracted.data_type == "binance_funding_settlement":
                        if symbol not in self.history_read_contracts:
                            self.history_read_contracts.add(symbol)
                            log_data_process("步骤1", "读取", f"币安历史费率首次读取: {symbol}")
                    
                    # ✅ 按合约记录统计信息
                    symbol = extracted.symbol
                    data_type = extracted.data_type
                    
                    self.processed_contracts.add(symbol)
                    self.contract_data_types[symbol].add(data_type)
                    self.current_batch_contracts.add(symbol)
                    self.current_batch_types[data_type] += 1
                    
            except Exception as e:
                exchange = item.get('exchange', 'unknown')
                symbol = item.get('symbol', 'unknown')
                log_data_process("步骤1", "错误", f"数据提取失败: {exchange}.{symbol} - {e}", "ERROR")
                continue
        
        # ✅ 5分钟统计报告
        current_time = time.time()
        if current_time - self.last_report_time >= 300:
            # 统计历史费率读取情况
            history_count = len(self.history_read_contracts)
            
            # 统计数据类型分布
            type_details = []
            for data_type, count in sorted(self.current_batch_types.items()):
                if count > 0:
                    # 美化类型名称
                    type_name = self._get_data_type_name(data_type)
                    
                    # 统计有这个数据类型的合约数
                    contract_count = len([s for s in self.current_batch_contracts 
                                         if data_type in self.contract_data_types[s]])
                    type_details.append(f"{type_name}: {contract_count}合约")
            
            type_str = " | ".join(type_details) if type_details else "无"
            
            # 输出统计日志
            log_data_process("步骤1", "统计", f"5分钟处理 {len(self.current_batch_contracts)} 个合约")
            log_data_process("步骤1", "历史", f"历史费率已读取: {history_count}个合约")
            log_data_process("步骤1", "类型", f"数据类型: {type_str}")
            
            # 重置统计（不重置history_read_contracts，要永久记录）
            self.last_report_time = current_time
            self.processed_contracts.clear()
            self.contract_data_types.clear()
            self.current_batch_types.clear()
        
        return results
    
    def _get_data_type_name(self, data_type: str) -> str:
        """获取数据类型的美化名称"""
        type_map = {
            "okx_ticker": "OKX行情",
            "okx_funding_rate": "OKX费率",
            "binance_ticker": "币安行情",
            "binance_mark_price": "币安实时费率",
            "binance_funding_settlement": "币安历史费率"
        }
        return type_map.get(data_type, data_type)
    
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
        
        # 构建类型键
        if data_type == "funding_settlement" and exchange == "binance":
            type_key = "binance_funding_settlement"
        else:
            type_key = f"{exchange}_{data_type}"
        
        # 检查是否支持的数据类型
        if type_key not in self.FIELD_MAP:
            log_data_process("步骤1", "警告", f"未知数据类型: {type_key}", "WARNING")
            return None
        
        config = self.FIELD_MAP[type_key]
        path = config["path"]
        fields = config["fields"]
        
        # 提取数据源
        data_source = raw_item if type_key == "binance_funding_settlement" else self._traverse_path(raw_item, path)
        
        # 空值检查
        if data_source is None:
            log_data_process("步骤1", "跳过", f"{type_key} 数据路径失败: {path}", "WARNING")
            return None
        
        # 提取字段
        extracted_payload = {}
        for output_key, input_key in fields.items():
            value = data_source.get(input_key) if isinstance(data_source, dict) else None
            extracted_payload[output_key] = value
        
        # 获取symbol
        symbol = raw_item.get("symbol", "")
        if exchange == "okx":
            inst_id = extracted_payload.get("contract_name", "")
            if inst_id:
                symbol = inst_id.replace("-SWAP", "").replace("-", "")
        
        if exchange == "binance" and not symbol:
            symbol = extracted_payload.get("contract_name", "")
        
        # 返回提取的数据
        return ExtractedData(
            data_type=type_key,
            exchange=exchange,
            symbol=symbol,
            payload=extracted_payload
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """获取当前统计信息"""
        return {
            "history_read_contracts": len(self.history_read_contracts),
            "processed_contracts": len(self.processed_contracts),
            "current_batch_contracts": len(self.current_batch_contracts),
            "data_types": dict(self.current_batch_types)
        }
    
    def reset_history_read(self):
        """重置历史费率读取记录（用于测试或特殊情况）"""
        old_count = len(self.history_read_contracts)
        self.history_read_contracts.clear()
        log_data_process("步骤1", "重置", f"重置历史费率读取记录，原记录 {old_count} 个合约")