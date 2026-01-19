"""
Step1过滤测试 - 暴力版
功能：不管什么格式，暴力提取币安历史费率
运行：python test_step1_暴力版.py
"""

import sys
sys.path.append("./shared_data")

import requests
import logging
from typing import Dict, List, Any, Optional
from collections import defaultdict
from dataclasses import dataclass

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ExtractedData:
    data_type: str
    exchange: str
    symbol: str
    payload: Dict

class Step1Filter暴力版:
    """不管格式，暴力提取"""
    
    def __init__(self):
        self.stats = defaultdict(int)
        self.process_count = 0
    
    def process(self, raw_items: List[Dict[str, Any]]) -> List[ExtractedData]:
        """暴力处理所有数据"""
        results = []
        
        for raw_item in raw_items:
            try:
                extracted = self._暴力提取(raw_item)
                if extracted:
                    results.append(extracted)
                    self.stats[extracted.data_type] += 1
            except Exception as e:
                logger.error(f"❌ 提取失败: {e}")
                continue
        
        logger.info(f"✅ 暴力提取完成，共 {len(results)} 条数据")
        return results
    
    def _暴力提取(self, raw_item: Dict[str, Any]) -> Optional[ExtractedData]:
        """暴力提取单个数据项"""
        exchange = raw_item.get("exchange")
        data_type = raw_item.get("data_type")
        
        # 1. 币安历史费率特殊处理
        if exchange == "binance" and data_type == "funding_settlement":
            logger.info(f"🔍【暴力提取】处理币安历史费率: {raw_item.get('symbol')}")
            
            # 尝试所有可能的位置
            possible_sources = [
                raw_item,  # 直接
                raw_item.get('data', {}),  # 新格式
                raw_item.get('raw_data', {}),  # 旧格式
                raw_item.get('payload', {}),  # 其他格式
            ]
            
            for i, source in enumerate(possible_sources):
                if not isinstance(source, dict):
                    continue
                    
                logger.info(f"🔍【暴力提取】尝试第{i+1}种格式: {list(source.keys())[:5]}...")
                
                # 查找funding_rate字段
                for key, value in source.items():
                    if 'funding' in key.lower() and 'rate' in key.lower():
                        funding_rate = value
                        funding_time_key = None
                        
                        # 找对应的时间字段
                        for time_key in source.keys():
                            if ('funding' in time_key.lower() and 'time' in time_key.lower()) or \
                               ('settlement' in time_key.lower() and 'time' in time_key.lower()):
                                funding_time_key = time_key
                                break
                        
                        funding_time = source.get(funding_time_key) if funding_time_key else None
                        
                        # 获取symbol
                        symbol = source.get('symbol') or raw_item.get('symbol', '')
                        
                        if funding_rate is not None:
                            logger.info(f"🎉【暴力提取】成功！从第{i+1}种格式提取: {symbol}")
                            logger.info(f"🎉【暴力提取】字段名: funding_rate={key}, funding_time={funding_time_key}")
                            
                            return ExtractedData(
                                data_type="binance_funding_settlement",
                                exchange=exchange,
                                symbol=symbol,
                                payload={
                                    "contract_name": symbol,
                                    "funding_rate": funding_rate,
                                    "last_settlement_time": funding_time
                                }
                            )
            
            # 所有尝试都失败
            logger.warning(f"❌【暴力提取】所有格式都失败: {raw_item}")
            return None
        
        # 2. 其他数据类型（简化版）
        elif exchange and data_type:
            # 生成标准type_key
            type_key = f"{exchange}_{data_type}"
            
            # 简单提取
            symbol = raw_item.get("symbol", "")
            payload = {}
            
            # 根据数据类型提取不同字段
            if "funding_rate" in data_type:
                # 找费率数据
                for source in [raw_item.get('data', {}), raw_item.get('raw_data', {}), raw_item]:
                    if isinstance(source, dict):
                        for key in source:
                            if 'funding' in key.lower() and 'rate' in key.lower():
                                payload['funding_rate'] = source[key]
                                break
            elif "ticker" in data_type or "mark_price" in data_type:
                # 找价格数据
                for source in [raw_item.get('data', {}), raw_item.get('raw_data', {}), raw_item]:
                    if isinstance(source, dict):
                        for key in source:
                            if any(word in key.lower() for word in ['price', 'last', 'c']):
                                payload['latest_price'] = source[key]
                                break
            
            if payload:
                payload['contract_name'] = symbol
                return ExtractedData(
                    data_type=type_key,
                    exchange=exchange,
                    symbol=symbol,
                    payload=payload
                )
        
        return None

class RealDataFetcher:
    """真实数据获取器"""
    
    def __init__(self):
        self.websocket_api = "https://ren-7gar.onrender.com/api/debug/all_websocket_data"
        self.history_api = "https://ren-7gar.onrender.com/api/funding/settlement/public"
    
    def fetch_all_formats(self) -> List[Dict[str, Any]]:
        """获取所有数据，并尝试多种格式"""
        try:
            all_items = []
            
            # 1. 获取WebSocket实时数据
            logger.info("正在获取WebSocket实时数据...")
            response = requests.get(f"{self.websocket_api}?show_all=true", timeout=10)
            response.raise_for_status()
            
            ws_data = response.json()
            ws_items = self._format_websocket_data(ws_data)
            all_items.extend(ws_items)
            logger.info(f"✅ WebSocket数据: {len(ws_items)} 条")
            
            # 2. 获取币安历史费率数据
            logger.info("正在获取币安历史费率数据...")
            response = requests.get(self.history_api, timeout=10)
            response.raise_for_status()
            
            history_data = response.json().get("data", [])
            
            # ⚠️ 关键：生成多种格式的历史费率数据
            history_items = []
            for item in history_data:
                # 格式1：直接格式（API原始格式）
                history_items.append({
                    "exchange": "binance",
                    "symbol": item.get("symbol", ""),
                    "data_type": "funding_settlement",
                    "funding_rate": item.get("funding_rate"),
                    "funding_time": item.get("funding_time"),
                    "timestamp": item.get("timestamp"),
                    "source": "api"
                })
                
                # 格式2：新DataStore格式（带data字段）
                history_items.append({
                    "exchange": "binance",
                    "symbol": item.get("symbol", ""),
                    "data_type": "funding_settlement",
                    "data": {
                        "symbol": item.get("symbol", ""),
                        "funding_rate": item.get("funding_rate"),
                        "funding_time": item.get("funding_time"),
                        "timestamp": item.get("timestamp"),
                        "source": "api"
                    },
                    "timestamp": item.get("timestamp"),
                    "priority": 5
                })
                
                # 格式3：旧DataStore格式（带raw_data字段）
                history_items.append({
                    "exchange": "binance",
                    "symbol": item.get("symbol", ""),
                    "data_type": "funding_settlement",
                    "raw_data": {
                        "symbol": item.get("symbol", ""),
                        "funding_rate": item.get("funding_rate"),
                        "funding_time": item.get("funding_time"),
                        "timestamp": item.get("timestamp"),
                        "source": "api"
                    },
                    "timestamp": item.get("timestamp"),
                    "priority": 5
                })
            
            all_items.extend(history_items)
            logger.info(f"✅ 历史费率数据（3种格式）: {len(history_items)} 条")
            logger.info(f"✅ 总共数据: {len(all_items)} 条")
            
            return all_items
            
        except Exception as e:
            logger.error(f"获取数据失败: {e}")
            return []
    
    def _format_websocket_data(self, ws_data: Dict) -> List[Dict]:
        """格式化WebSocket数据为多种格式"""
        items = []
        
        for exchange, symbols in ws_data.get("data", {}).items():
            for symbol, data_types in symbols.items():
                for data_type, payload in data_types.items():
                    if data_type in ['latest', 'store_timestamp']:
                        continue
                    
                    # 格式1：直接格式
                    items.append({
                        "exchange": exchange,
                        "symbol": symbol,
                        "data_type": data_type,
                        **payload
                    })
                    
                    # 格式2：新DataStore格式
                    items.append({
                        "exchange": exchange,
                        "symbol": symbol,
                        "data_type": data_type,
                        "data": payload,
                        "timestamp": payload.get("timestamp"),
                        "priority": 5
                    })
        
        return items

def main():
    print("=" * 90)
    print("Step1过滤测试 - 暴力版（尝试所有可能格式）")
    print("=" * 90 + "\n")
    
    # 1. 获取真实原始数据（多种格式）
    print("1. 获取真实原始数据（尝试3种格式）...")
    fetcher = RealDataFetcher()
    raw_data = fetcher.fetch_all_formats()
    
    if not raw_data:
        logger.error("❌ 没有获取到数据，测试终止")
        return
    
    print(f"   原始数据: {len(raw_data)} 条（包含多种格式）\n")
    
    # 2. 统计不同类型数据
    print("2. 数据格式分析:\n")
    
    format_stats = defaultdict(int)
    for item in raw_data:
        if "data" in item:
            format_stats["带data字段"] += 1
        if "raw_data" in item:
            format_stats["带raw_data字段"] += 1
        if "funding_rate" in item:
            format_stats["直接字段"] += 1
    
    for fmt, count in format_stats.items():
        print(f"   {fmt}: {count} 条")
    
    # 统计币安历史费率
    binance_history = [
        item for item in raw_data 
        if item.get("exchange") == "binance" 
        and item.get("data_type") == "funding_settlement"
    ]
    print(f"\n   币安历史费率: {len(binance_history)} 条")
    
    if binance_history:
        print(f"   示例（第1条）:")
        sample = binance_history[0]
        for key, value in list(sample.items())[:6]:  # 显示前6个字段
            print(f"     {key}: {value}")
    
    print()
    
    # 3. 运行暴力提取
    print("3. 运行暴力提取...")
    step1 = Step1Filter暴力版()
    step1_results = step1.process(raw_data)
    print(f"   暴力提取结果: {len(step1_results)} 条\n")
    
    # 4. 按数据类型展示
    print("4. 提取结果按类型统计:\n")
    
    grouped = defaultdict(list)
    for item in step1_results:
        grouped[item.data_type].append(item)
    
    for data_type, items in sorted(grouped.items()):
        print(f"   {data_type}: {len(items)} 条")
        if items:
            first = items[0]
            print(f"     示例: {first.symbol}")
            for key, value in first.payload.items():
                print(f"       {key}: {value}")
        print()
    
    # 5. 重点检查币安历史费率
    print("5. 币安历史费率提取详细结果:\n")
    
    binance_history_results = grouped.get("binance_funding_settlement", [])
    if binance_history_results:
        print(f"   ✅ 成功提取 {len(binance_history_results)} 条币安历史费率")
        print("   前5条示例:")
        for i, item in enumerate(binance_history_results[:5], 1):
            print(f"     [{i}] {item.symbol}")
            print(f"         费率: {item.payload.get('funding_rate')}")
            print(f"         时间: {item.payload.get('last_settlement_time')}")
    else:
        print("   ❌ 没有提取到币安历史费率")
        
        # 深度分析为什么失败
        print("\n   🔍 深度分析失败原因:")
        history_items = [
            item for item in raw_data 
            if item.get("exchange") == "binance" 
            and item.get("data_type") == "funding_settlement"
        ]
        
        if history_items:
            sample = history_items[0]
            print(f"   原始数据示例:")
            for key, value in sample.items():
                print(f"     {key}: {type(value).__name__} = {value}")
            
            # 检查可能的字段名
            print(f"\n   字段名分析:")
            all_keys = set()
            for item in history_items:
                all_keys.update(item.keys())
                if isinstance(item.get('data'), dict):
                    all_keys.update(item['data'].keys())
                if isinstance(item.get('raw_data'), dict):
                    all_keys.update(item['raw_data'].keys())
            
            print(f"   所有可能的字段: {sorted(all_keys)}")
    
    # 6. 最终结论
    print("\n" + "=" * 90)
    
    if binance_history_results:
        print("🎉 **暴力提取成功！**")
        print(f"✅ 成功提取 {len(binance_history_results)} 条币安历史费率")
        
        # 检查实际字段名
        if binance_history_results:
            first_result = binance_history_results[0]
            print(f"✅ 实际提取的字段名:")
            for key in first_result.payload.keys():
                print(f"   • {key}")
    else:
        print("❌ **暴力提取失败**")
        print("   需要检查服务器上实际的字段名")
    
    print("=" * 90)

if __name__ == "__main__":
    main()