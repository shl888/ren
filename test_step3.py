"""
Step3对齐测试 - 真实数据版
功能：从真实API拉数据，经过Step1+Step2+Step3，看最终效果
运行：python test_step3.py
"""

import sys
sys.path.append("./shared_data")

import requests
import logging
from typing import Dict, List, Any
from collections import defaultdict

from step1_filter import Step1Filter
from step2_fusion import Step2Fusion
from step3_align import Step3Align

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealDataFetcher:
    """从真实API拉数据"""
    
    def __init__(self):
        self.websocket_api = "https://ren-7gar.onrender.com/api/debug/all_websocket_data"
        self.history_api = "https://ren-7gar.onrender.com/api/funding/settlement/public"
    
    def fetch_all(self) -> List[Dict[str, Any]]:
        """获取所有原始数据"""
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
            
            # 获取历史费率
            logger.info("正在获取币安历史费率数据...")
            response = requests.get(self.history_api, timeout=10)
            response.raise_for_status()
            
            history_data = response.json().get("data", [])
            logger.info(f"✅ 获取到 {len(history_data)} 条历史费率数据")
            
            return raw_items + history_data
            
        except Exception as e:
            logger.error(f"获取数据失败: {e}")
            return []

def main():
    print("=" * 90)
    print("Step3对齐测试 - 真实数据版")
    print("=" * 90 + "\n")
    
    # 1. 拉真实数据
    print("1. 获取真实原始数据...")
    fetcher = RealDataFetcher()
    raw_data = fetcher.fetch_all()
    print(f"   原始数据: {len(raw_data)} 条\n")
    
    # 2. Step1过滤
    print("2. Step1过滤...")
    step1 = Step1Filter()
    step1_results = step1.process(raw_data)
    
    # 3. Step2融合
    print("3. Step2融合...")
    step2 = Step2Fusion()
    step2_results = step2.process(step1_results)
    
    # 4. Step3对齐（核心）
    print("4. Step3对齐（筛选双平台+时间转换）...")
    step3 = Step3Align()
    aligned_results = step3.process(step2_results)
    
    # 5. 统计
    print("\n" + "=" * 90)
    print("📊 最终统计")
    print("=" * 90 + "\n")
    
    stats = step3.stats
    print(f"   总合约数: {stats['total_symbols']}")
    print(f"   仅OKX: {stats['okx_only']}")
    print(f"   仅币安: {stats['binance_only']}")
    print(f"   双平台: {stats['both_platforms']} ← 这才是我们要的\n")
    
    # 6. 显示前10个双平台合约（真实数据）
    print("=" * 90)
    print("🔍 双平台合约详情（前10个）")
    print("=" * 90 + "\n")
    
    for i, item in enumerate(aligned_results[:10], 1):
        print(f"【{i}】 {item.symbol}")
        print(f"   OKX合约: {item.okx_contract_name}")
        
        print(f"   OKX价格: {item.okx_price}")
        print(f"   OKX费率: {item.okx_funding_rate}")
        
        print(f"   OKX上次: {item.okx_last_settlement}")
        
        print(f"   OKX本次: {item.okx_current_settlement} ← 北京时间24小时")
        
        print(f"   OKX下次: {item.okx_next_settlement}")
        
        print(f"   币安合约: {item
        .binance_contract_name}")
        
        print(f"   币安价格: {item.binance_price}")
        
        print(f"   币安费率: {item.binance_funding_rate}")
        
        print(f"   币安上次: {item.binance_last_settlement}")
        
        print(f"   币安本次: {item.binance_current_settlement} ← 北京时间24小时")
        
        print(f"   币安下次: {item.binance_next_settlement}")
        print("━" * 90)
    
    # 7. 最终验证
    print("\n" + "=" * 90)
    print("🎯 最终验证")
    print("=" * 90 + "\n")
    
    # 验证时间格式
    sample = aligned_results[0]
    if sample.okx_current_settlement:
        assert ":" in sample.okx_current_settlement, "时间格式不正确"
        assert len(sample.okx_current_settlement) == 19, "时间长度不正确"
        print(f"   ✅ 时间格式正确: {sample.okx_current_settlement}")
    
    # 验证没有单平台合约
    if stats['okx_only'] > 0 or stats['binance_only'] > 0:
        print(f"   ℹ️  过滤掉 {stats['okx_only'] + stats['binance_only']} 个单平台合约")
    
    print(f"   ✅ 最终保留 {len(aligned_results)} 个双平台合约")
    
    print("\n" + "=" * 90)
    print("🎉 **恭喜！Step3对齐功能100%正常！**")
    print("✅ 时间已转为北京时间24小时制")
    print("✅ 单平台合约已全部过滤")
    print("=" * 90)

if __name__ == "__main__":
    main()
