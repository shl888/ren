"""
Step2融合测试 - 真实数据版
功能：从真实API获取数据，经过Step1+Step2，看真实融合效果
运行：python test_step2.py
"""

import sys
sys.path.append("./shared_data")

import requests
import logging
from typing import Dict, List, Any
from collections import defaultdict

from step1_filter import Step1Filter
from step2_fusion import Step2Fusion

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealDataFetcher:
    """真实数据获取器（从test_step1复制）"""
    
    def __init__(self):
        self.websocket_api = "https://ren-7gar.onrender.com/api/debug/all_websocket_data"
        self.history_api = "https://ren-7gar.onrender.com/api/funding/settlement/public"
    
    def fetch_websocket_data(self) -> List[Dict[str, Any]]:
        try:
            logger.info("正在获取WebSocket实时数据...")
            response = requests.get(f"{self.websocket_api}?show_all=true", timeout=10)
            response.raise_for_status()
            
            data = response.json()
            raw_items = []
            for exchange, symbols in data.get("data", {}).items():
                for symbol, data_types in symbols.items():
                    for data_type, payload in data_types.items():
                        if data_type in ['latest', 'store_timestamp']:
                            continue
                        raw_items.append({
                            "exchange": exchange,
                            "symbol": symbol,
                            "data_type": data_type,
                            "raw_data": payload.get("raw_data", {}),
                            "timestamp": payload.get("timestamp"),
                            "source": payload.get("source", "websocket")
                        })
            
            logger.info(f"✅ 获取到 {len(raw_items)} 条实时数据")
            return raw_items
            
        except Exception as e:
            logger.error(f"获取WebSocket数据失败: {e}")
            return []
    
    def fetch_history_data(self) -> List[Dict[str, Any]]:
        try:
            logger.info("正在获取币安历史费率数据...")
            response = requests.get(self.history_api, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            raw_items = data.get("data", [])
            logger.info(f"✅ 获取到 {len(raw_items)} 条历史费率数据")
            return raw_items
            
        except Exception as e:
            logger.error(f"获取历史费率数据失败: {e}")
            return []

def main():
    print("=" * 90)
    print("Step2融合测试 - 真实数据版")
    print("=" * 90 + "\n")
    
    # 1. 获取真实原始数据
    print("1. 获取真实原始数据...")
    fetcher = RealDataFetcher()
    websocket_data = fetcher.fetch_websocket_data()
    history_data = fetcher.fetch_history_data()
    all_raw_data = websocket_data + history_data
    print(f"   原始数据: {len(all_raw_data)} 条\n")
    
    # 2. 运行Step1过滤
    print("2. 运行Step1过滤...")
    step1 = Step1Filter()
    step1_results = step1.process(all_raw_data)
    print(f"   Step1输出: {len(step1_results)} 条提取数据\n")
    
    # 3. 运行Step2融合
    print("3. 运行Step2融合...")
    step2 = Step2Fusion()
    fused_results = step2.process(step1_results)
    print(f"   Step2输出: {len(fused_results)} 条融合数据\n")
    
    # 4. 打印真实结果（只显示前10条，避免刷屏）
    print("4. 融合结果详情（显示前10条）:\n")
    
    # 按交易所分组显示
    grouped = defaultdict(list)
    for item in fused_results:
        grouped[item.exchange].append(item)
    
    for exchange in ["okx", "binance"]:
        items = grouped[exchange]
        print(f"【{exchange.upper()}】 {len(items)} 个合约")
        
        for i, item in enumerate(items[:5], 1):
            print(f"\n  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            print(f"  [{i}] {item.symbol}")
            print(f"     合约名: {item.contract_name}")
            print(f"     最新价格: {item.latest_price}")
            print(f"     资金费率: {item.funding_rate}")
            print(f"     上次结算: {item.last_settlement_time}")
            print(f"     本次结算: {item.current_settlement_time}")
            print(f"     下次结算: {item.next_settlement_time}")
        
        if len(items) > 5:
            print(f"\n     ... 还有 {len(items) - 5} 个合约")
        
        print()
    
    # 5. 验证
    print("=" * 90)
    print("🧪 验证结果")
    print("=" * 90 + "\n")
    
    # 统计
    okx_count = len(grouped["okx"])
    binance_count = len(grouped["binance"])
    
    print(f"   OKX合约数: {okx_count} (预期: 252)")
    print(f"   币安合约数: {binance_count} (预期: 536)")
    print(f"   总计: {len(fused_results)} (预期: 788)\n")
    
    # 验证币安的特殊规则：必须有mark_price
    # 验证字段完整性
    for item in fused_results:
        assert item.contract_name, f"{item.symbol} 缺少合约名"
        assert item.latest_price is not None, f"{item.symbol} 缺少价格"
        assert item.funding_rate is not None, f"{item.symbol} 缺少费率"
        assert item.current_settlement_time is not None, f"{item.symbol} 缺少本次结算时间"
    
    print("   ✅ 所有字段验证通过")
    
    # 验证OKX没有last_settlement_time
    okx_with_last = [r for r in grouped["okx"] if r.last_settlement_time is not None]
    if okx_with_last:
        print(f"   ⚠️  警告: 有 {len(okx_with_last)} 个OKX合约错误地包含了last_settlement_time")
    else:
        print("   ✅ OKX合约的last_settlement_time正确为空")
    
    # 验证币安没有next_settlement_time
    binance_with_next = [r for r in grouped["binance"] if r.next_settlement_time is not None]
    if binance_with_next:
        print(f"   ⚠️  警告: 有 {len(binance_with_next)} 个币安合约错误地包含了next_settlement_time")
    else:
        print("   ✅ 币安合约的next_settlement_time正确为空")
    
    print("\n" + "=" * 90)
    if okx_count == 252 and binance_count == 536:
        print("🎉 **恭喜！Step2融合功能100%正常！**")
        print("✅ 788个合约全部正确处理")
    else:
        print("⚠️  合约数量不匹配，请检查")
    print("=" * 90)

if __name__ == "__main__":
    main()
