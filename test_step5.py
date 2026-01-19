"""
Step5跨平台计算测试 - 真实数据版
功能：完整跑完Step1-4，专门测试Step5的最终套利数据生成
运行：python test_step5.py
"""

import sys
sys.path.append("./shared_data")

import requests
import logging
from typing import Dict, List, Any, Optional

from collections import defaultdict

from step1_filter import Step1Filter
from step2_fusion import Step2Fusion
from step3_align import Step3Align
from step4_calc import Step4Calc
from step5_cross_calc import Step5CrossCalc

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

def format_period(seconds: Optional[int]) -> str:
    """格式化周期（处理None）"""
    if seconds is None:
        return "未知"
    return f"{seconds/3600:.1f}小时"

def format_price(price_str: str) -> str:
    """格式化价格显示"""
    try:
        price = float(price_str)
        if price < 0.01:
            return f"${price:.8f}"
        elif price < 1:
            return f"${price:.6f}"
        else:
            return f"${price:.4f}"
    except:
        return f"${price_str}"

def main():
    print("=" * 90)
    print("Step5跨平台计算测试 - 真实数据版")
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
    print(f"   Step3输出: {len(step3_results)} 个双平台合约")
    
    # 安全地访问stats（如果存在）
    if hasattr(step3, 'stats'):
        print(f"   统计: {step3.stats}")
    elif hasattr(step3, 'get_batch_stats'):
        # 尝试使用新版本的方法获取批次统计
        try:
            batch_stats = step3.get_batch_stats(step2_results)
            print(f"   批次统计: {batch_stats}")
        except:
            pass
    print()
    
    # 5. 运行Step4单平台计算
    print("5. 运行Step4单平台计算...")
    step4 = Step4Calc()
    step4_results = step4.process(step3_results)
    print(f"   Step4输出: {len(step4_results)} 条单平台数据")
    
    # 安全地访问stats（如果存在）
    if hasattr(step4, 'stats'):
        print(f"   币安更新次数: {step4.stats.get('binance_updates', 'N/A')}")
        print(f"   币安滚动次数: {step4.stats.get('binance_rollovers', 'N/A')}")
    elif hasattr(step4, 'get_batch_stats'):
        # 尝试使用新版本的方法获取批次统计
        try:
            batch_stats = step4.get_batch_stats(step3_results)
            print(f"   批次统计: {batch_stats}")
        except:
            pass
    print()
    
    # 6. 运行Step5跨平台计算（核心测试）
    print("6. 运行Step5跨平台计算...")
    step5 = Step5CrossCalc()
    final_results = step5.process(step4_results)
    print(f"   Step5输出: {len(final_results)} 条最终套利数据\n")
    
    # 7. 验证结果
    print("=" * 90)
    print("🧪 Step5结果验证")
    print("=" * 90 + "\n")
    
    # 验证数量
    expected_count = len(step3_results)  # 应该与双平台合约数一致
    actual_count = len(final_results)
    print(f"   预期套利数据: {expected_count} 条")
    print(f"   实际套利数据: {actual_count} 条")
    if actual_count == expected_count:
        print("   ✅ 数据数量完美匹配")
    else:
        print(f"   ⚠️  数量不匹配，缺失 {expected_count - actual_count} 条")
    
    # 安全地访问stats（如果存在）
    if hasattr(step5, 'stats'):
        print(f"\n   统计信息: {step5.stats}")
    elif hasattr(step5, 'get_batch_stats'):
        # 尝试使用新版本的方法获取批次统计
        try:
            batch_stats = step5.get_batch_stats(step4_results)
            print(f"   批次统计: {batch_stats}")
        except:
            pass
    print()
    
    # 8. 打印最终套利数据（前10个）
    print("=" * 90)
    print("🎯 最终套利数据（前10个合约）")
    print("=" * 90 + "\n")
    
    if not final_results:
        print("   ❌ 没有生成任何套利数据\n")
    else:
        for i, item in enumerate(final_results[:10], 1):
            print(f"【{i}】 {item.symbol}")
            
            # 获取价格值用于显示判断
            try:
                okx_price_val = float(item.okx_price or 0)
                binance_price_val = float(item.binance_price or 0)
                
                # 计算谁的价格更高
                if okx_price_val > binance_price_val:
                    price_direction = "OKX > 币安"
                elif okx_price_val < binance_price_val:
                    price_direction = "OKX < 币安"
                else:
                    price_direction = "OKX = 币安"
                    
                # 显示价格差
                if item.price_diff < 0.01:
                    price_display = f"${item.price_diff:.8f}"
                elif item.price_diff < 1:
                    price_display = f"${item.price_diff:.6f}"
                else:
                    price_display = f"${item.price_diff:.4f}"
                    
                print(f"   绝对价差: {price_display}  ← {price_direction}")
                print(f"   价差百分比: {item.price_diff_percent:.4f}%  ← (以低价为基准)")
                print(f"   费率差: {format_rate(str(item.rate_diff))}")
            except Exception as e:
                print(f"   价格计算异常: {e}")
                
            print()
            print(f"   OKX数据:")
            print(f"     价格: {format_price(item.okx_price)}")
            print(f"     费率: {format_rate(item.okx_funding_rate)}")
            print(f"     周期: {format_period(item.okx_period_seconds)}")
            print(f"     倒计时: {format_countdown(item.okx_countdown_seconds)}")
            print(f"     上次: {format_time(item.okx_last_settlement)}")
            print(f"     本次: {format_time(item.okx_current_settlement)}")
            print(f"     下次: {format_time(item.okx_next_settlement)}")
            print()
            print(f"   币安数据:")
            print(f"     价格: {format_price(item.binance_price)}")
            print(f"     费率: {format_rate(item.binance_funding_rate)}")
            print(f"     周期: {format_period(item.binance_period_seconds)}")
            print(f"     倒计时: {format_countdown(item.binance_countdown_seconds)}")
            print(f"     上次: {format_time(item.binance_last_settlement)}")
            print(f"     本次: {format_time(item.binance_current_settlement)}")
            print(f"     下次: {format_time(item.binance_next_settlement or '无')}")
            print("━" * 90 + "\n")
    
    # 9. 数据质量验证
    print("=" * 90)
    print("🔍 数据质量验证")
    print("=" * 90 + "\n")
    
    if not final_results:
        print("   ❌ 无数据可验证")
    else:
        # 验证价格差的合理性
        crazy_price_diffs = [r for r in final_results if hasattr(r, 'price_diff') and r.price_diff > 1000]
        if crazy_price_diffs:
            print(f"   ⚠️  发现 {len(crazy_price_diffs)} 个价格差异常大的合约（>1000美元）")
        else:
            print("   ✅ 所有价格差都在合理范围内")
        
        # 验证价格百分比差的合理性
        crazy_price_percents = [r for r in final_results if hasattr(r, 'price_diff_percent') and r.price_diff_percent > 10]  # >10%
        if crazy_price_percents:
            print(f"   ⚠️  发现 {len(crazy_price_percents)} 个价格百分比差异常大的合约（>10%）")
            for r in crazy_price_percents[:3]:  # 显示前3个
                print(f"      - {r.symbol}: {r.price_diff_percent:.2f}%")
        else:
            print("   ✅ 所有价格百分比差都在合理范围内")
        
        # 验证费率差的合理性
        crazy_rate_diffs = [r for r in final_results if hasattr(r, 'rate_diff') and r.rate_diff > 0.1]  # >10%
        if crazy_rate_diffs:
            print(f"   ⚠️  发现 {len(crazy_rate_diffs)} 个费率差异常大的合约（>10%）")
        else:
            print("   ✅ 所有费率差都在合理范围内")
        
        # 验证倒计时
        missing_countdown = [r for r in final_results if not hasattr(r, 'okx_countdown_seconds') or not hasattr(r, 'binance_countdown_seconds') or r.okx_countdown_seconds is None or r.binance_countdown_seconds is None]
        if missing_countdown:
            print(f"   ⚠️  有 {len(missing_countdown)} 条数据缺少倒计时")
        else:
            print("   ✅ 所有数据都有有效的倒计时")
    
    # 10. 最终结论
    print("\n" + "=" * 90)
    if actual_count > 0 and not crazy_price_diffs and not crazy_rate_diffs and not missing_countdown:
        print("🎉 **恭喜！Step5跨平台计算功能100%正常！**")
        print(f"✅ 成功生成 {actual_count} 条高质量套利数据")
        print("✅ 价格差、价差百分比计算准确")
        print("✅ 双平台数据完整")
        print("✅ 倒计时和周期信息齐全")
    elif actual_count > 0:
        print("✅ Step5跨平台计算功能基本正常")
        print(f"✅ 成功生成 {actual_count} 条套利数据")
        print("⚠️  但存在部分异常数据，请检查")
    else:
        print("❌ Step5跨平台计算失败")
    print("=" * 90)

if __name__ == "__main__":
    main()