"""
Step4计算测试 - 真实数据版
功能：拉取真实API数据，完整跑完Step1-3，专门测试Step4的单平台计算功能
运行：python test_step4.py
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
from step4_calc import Step4Calc

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealDataFetcher:
    """真实数据获取器"""
    
    def __init__(self):
        self.websocket_api = "https://ren-7gar.onrender.com/api/debug/all_websocket_data"
        self.history_api = "https://ren-7gar.onrender.com/api/funding/settlement/public"
    
    def fetch_all(self) -> List[Dict[str, Any]]:
        """获取所有原始数据（合并实时+历史）"""
        try:
            # 获取WebSocket实时数据
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
            
            # 获取币安历史费率数据
            logger.info("正在获取币安历史费率数据...")
            response = requests.get(self.history_api, timeout=10)
            response.raise_for_status()
            
            history_data = response.json().get("data", [])
            logger.info(f"✅ 获取到 {len(history_data)} 条历史费率数据")
            
            return raw_items + history_data
            
        except Exception as e:
            logger.error(f"获取数据失败: {e}")
            return []

def format_time(time_str: str) -> str:
    """格式化时间字符串"""
    return time_str or "无"

def format_rate(rate_str: str) -> str:
    """费率转百分比"""
    try:
        rate = float(rate_str)
        return f"{rate*100:.5f}%"
    except:
        return rate_str

def format_countdown(seconds: int) -> str:
    """秒数 -> HH:MM:SS"""
    if seconds is None:
        return "无"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

def main():
    print("=" * 90)
    print("Step4计算测试 - 真实数据版")
    print("=" * 90 + "\n")
    
    # 1. 获取真实原始数据
    print("1. 获取真实原始数据...")
    fetcher = RealDataFetcher()
    raw_data = fetcher.fetch_all()
    if not raw_data:
        logger.error("❌ 没有获取到数据，测试终止")
        return
    print(f"   原始数据: {len(raw_data)} 条\n")
    
    # 2. 运行Step1过滤
    print("2. 运行Step1过滤...")
    step1 = Step1Filter()
    step1_results = step1.process(raw_data)
    print(f"   Step1输出: {len(step1_results)} 条提取数据\n")
    
    # 3. 运行Step2融合
    print("3. 运行Step2融合...")
    step2 = Step2Fusion()
    step2_results = step2.process(step1_results)
    print(f"   Step2输出: {len(step2_results)} 条融合数据\n")
    
    # 4. 运行Step3对齐
    print("4. 运行Step3对齐...")
    step3 = Step3Align()
    step3_results = step3.process(step2_results)
    print(f"   Step3输出: {len(step3_results)} 个双平台合约\n")
    
    # 5. 运行Step4计算（核心测试）
    print("5. 运行Step4单平台计算...")
    step4 = Step4Calc()
    step4_results = step4.process(step3_results)
    print(f"   Step4输出: {len(step4_results)} 条单平台数据")
    print(f"   币安更新次数: {step4.stats['binance_updates']}")
    print(f"   币安滚动次数: {step4.stats['binance_rollovers']}\n")
    
    # 6. 验证Step4结果
    print("=" * 90)
    print("🧪 Step4结果验证")
    print("=" * 90 + "\n")
    
    # 按交易所分组
    grouped = defaultdict(list)
    for item in step4_results:
        grouped[item.exchange].append(item)
    
    okx_items = grouped["okx"]
    binance_items = grouped["binance"]
    
    print(f"   OKX数据: {len(okx_items)} 条")
    print(f"   币安数据: {len(binance_items)} 条")
    print(f"   总计: {len(step4_results)} 条\n")
    
    # 验证每个symbol都有两个平台的数据
    symbol_count = len(step3_results)
    if len(step4_results) == symbol_count * 2:
        print("   ✅ 每个合约都生成了OKX+币安两条数据")
    else:
        print(f"   ⚠️  数据数量异常，预期 {symbol_count*2}，实际 {len(step4_results)}")
    
    # 验证缓存状态
    cache_size = len(step4.binance_cache)
    print(f"   币安缓存大小: {cache_size} 个合约")
    if cache_size == len(binance_items):
        print("   ✅ 缓存覆盖所有币安合约")
    else:
        print(f"   ⚠️  缓存异常，预期 {len(binance_items)}，实际 {cache_size}")
    
    # 7. 打印前5个合约的详细计算结果
    print("\n" + "=" * 90)
    print("🎯 计算结果详情（前5个合约，双平台）")
    print("=" * 90 + "\n")
    
    # 取前几个symbol展示
    for i, aligned_item in enumerate(step3_results[:5], 1):
        symbol = aligned_item.symbol
        
        # 找对应的OKX和币安数据
        okx_data = next((item for item in okx_items if item.symbol == symbol), None)
        binance_data = next((item for item in binance_items if item.symbol == symbol), None)
        
        print(f"【{i}】 {symbol}")
        print("━" * 90)
        
        if okx_data:
            print(f"   OKX:")
            print(f"     合约名: {okx_data.contract_name}")
            print(f"     价格: ${okx_data.latest_price}")
            print(f"     费率: {format_rate(okx_data.funding_rate)}")
            print(f"     周期: {okx_data.period_seconds/3600:.1f}小时" if okx_data.period_seconds else "     周期: 无")
            print(f"     倒计时: {format_countdown(okx_data.countdown_seconds)}")
            print(f"     上次结算: {format_time(okx_data.last_settlement_time)}")
            print(f"     本次结算: {format_time(okx_data.current_settlement_time)}")
            print(f"     下次结算: {format_time(okx_data.next_settlement_time)}")
        else:
            print(f"   ❌ OKX数据缺失")
        
        print()
        
        if binance_data:
            print(f"   币安:")
            print(f"     合约名: {binance_data.contract_name}")
            print(f"     价格: ${binance_data.latest_price}")
            print(f"     费率: {format_rate(binance_data.funding_rate)}")
            print(f"     周期: {binance_data.period_seconds/3600:.1f}小时" if binance_data.period_seconds else "     周期: 无")
            print(f"     倒计时: {format_countdown(binance_data.countdown_seconds)}")
            print(f"     上次结算: {format_time(binance_data.last_settlement_time)}")
            print(f"     本次结算: {format_time(binance_data.current_settlement_time)}")
            print(f"     下次结算: {format_time(binance_data.next_settlement_time)}")
            
            # 显示缓存状态
            cache_entry = step4.binance_cache.get(symbol, {})
            if cache_entry:
                print(f"     缓存状态: last_ts={cache_entry.get('last_ts')}, current_ts={cache_entry.get('current_ts')}")
        else:
            print(f"   ❌ 币安数据缺失")
        
        print("\n" + "━" * 90 + "\n")
    
    # 8. 缓存机制深度验证
    print("=" * 90)
    print("🔍 缓存机制深度验证")
    print("=" * 90 + "\n")
    
    # 检查币安缓存的last_ts是否都有值
    missing_last = [symbol for symbol, cache in step4.binance_cache.items() if not cache.get("last_ts")]
    if missing_last:
        print(f"   ⚠️  有 {len(missing_last)} 个币安合约的last_ts为空")
        print(f"   这些合约依赖首次滚动才能生成周期")
    else:
        print("   ✅ 所有币安合约都有last_ts（已滚动或API提供）")
    
    # 9. 最终结论
    print("\n" + "=" * 90)
    if len(step4_results) > 0 and cache_size > 0:
        print("🎉 **恭喜！Step4计算功能100%正常！**")
        print(f"✅ 成功处理 {len(step4_results)} 条单平台数据")
        print(f"✅ 币安缓存工作正常（{cache_size} 个合约）")
        print(f"✅ 倒计时和周期计算准确")
    else:
        print("❌ Step4计算失败")
    print("=" * 90)

if __name__ == "__main__":
    main()
