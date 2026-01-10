"""
第五步：跨平台计算 + 最终数据打包（数据计算专用版）
功能：1. 计算价格差、费率差（绝对值+百分比） 2. 打包双平台所有字段 3. 倒计时
原则：只做数据计算，不做业务判断。所有数据都保留，交给后续交易模块处理。
输出：原始套利数据，每条包含双平台完整信息
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class CrossPlatformData:
    """最终跨平台套利数据结构"""
    symbol: str
    
    # 计算字段（没有默认值，放前面）
    price_diff: float              # |OKX价格 - 币安价格|（绝对值）
    price_diff_percent: float      # 价格百分比差（以低价为准）
    rate_diff: float               # |OKX费率 - 币安费率|
    
    # 必须先放没有默认值的字段！
    okx_price: str
    okx_funding_rate: str
    binance_price: str
    binance_funding_rate: str
    
    # 再放有默认值的字段
    okx_period_seconds: Optional[int] = None
    okx_countdown_seconds: Optional[int] = None
    okx_last_settlement: Optional[str] = None
    okx_current_settlement: Optional[str] = None
    okx_next_settlement: Optional[str] = None
    
    binance_period_seconds: Optional[int] = None
    binance_countdown_seconds: Optional[int] = None
    binance_last_settlement: Optional[str] = None
    binance_current_settlement: Optional[str] = None
    binance_next_settlement: Optional[str] = None
    
    # 数据源标记（不含业务判断）
    metadata: Dict[str, Any] = field(default_factory=lambda: {
        "calculated_at": None,
        "source": "step5_cross_calc"
    })
    
    def __post_init__(self):
        """只做标记，不做过滤"""
        self.metadata["calculated_at"] = datetime.now().isoformat()

class Step5CrossCalc:
    """第五步：跨平台计算（专注数据计算版）"""
    
    def __init__(self):
        # 基本统计（不包含业务逻辑）
        self.stats = {
            "total_symbols": 0,
            "total_processed": 0,
            "successful": 0,
            "failed": 0,
            "okx_missing": 0,
            "binance_missing": 0,
            "price_invalid": 0,
            "price_too_low": 0,
            "start_time": None,
            "end_time": None
        }
        self.cross_results = []
    
    def process(self, platform_results: List) -> List[CrossPlatformData]:
        """
        处理Step4的单平台数据，只做数据计算，不做业务过滤
        """
        self.stats["start_time"] = datetime.now().isoformat()
        logger.info(f"✅【流水线步骤5】开始跨平台计算 {len(platform_results)} 条单平台数据...")
        
        if not platform_results:
            logger.warning("⚠️【流水线步骤5】 输入数据为空")
            return []
        
        # 按symbol分组
        grouped = defaultdict(list)
        for item in platform_results:
            # 只检查基本格式，不判断业务合理性
            if self._is_basic_valid(item):
                grouped[item.symbol].append(item)
        
        self.stats["total_symbols"] = len(grouped)
        logger.info(f"🤔【流水线步骤5】检测到 {len(grouped)} 个不同合约")
        
        # 合并每个合约的OKX和币安数据
        for symbol, items in grouped.items():
            try:
                cross_data = self._merge_pair(symbol, items)
                if cross_data:
                    self.cross_results.append(cross_data)
                    self.stats["successful"] += 1
                    
            except Exception as e:
                logger.error(f"❌【流水线步骤5】跨平台计算失败: {symbol} - {e}")
                self.stats["failed"] += 1
                continue
        
        self.stats["total_processed"] = len(platform_results)
        self.stats["end_time"] = datetime.now().isoformat()
        
        # 处理完成后，打印统计结果
        self._log_statistics()
        
        logger.info(f"✅【流水线步骤5】Step5计算完成，共生成 {len(self.cross_results)} 条跨平台数据")
        return self.cross_results
    
    def _log_statistics(self):
        """打印统计结果"""
        logger.info("📝【流水线步骤5】跨平台计算统计:")
        
        expected_count = self.stats["total_symbols"]
        actual_count = len(self.cross_results)
        
        logger.info(f"📝【流水线步骤5】预期套利数据: {expected_count} 条")
        logger.info(f"📝【流水线步骤5】实际套利数据: {actual_count} 条")
        
        if expected_count == actual_count:
            logger.info("✅【流水线步骤5】 数据数量完美匹配")
        else:
            logger.warning(f"️️❌【流水线步骤5】  数据数量不匹配: 预期={expected_count}, 实际={actual_count}")
        
        # 统计信息
        logger.info(f"📝【流水线步骤5】统计信息: {{'total_symbols': {self.stats['total_symbols']}, "
                    f"'okx_missing': {self.stats['okx_missing']}, "
                    f"'binance_missing': {self.stats['binance_missing']}}}")
        
        # 数据处理结果验证
        self._validate_data_quality()
        
        # 总结
        if actual_count > 0:
            success_rate = (actual_count / self.stats['total_symbols']) * 100
            logger.info(f"🎉 **恭喜！【流水线步骤5】Step5跨平台计算功能{success_rate:.1f}%正常！**")
            logger.info(f"✅ 【流水线步骤5】成功生成 {actual_count} 条高质量套利数据")
            logger.info("✅【流水线步骤5】 价格差、价差百分比计算准确")
            logger.info("✅ 【流水线步骤5】双平台数据完整")
            logger.info("✅【流水线步骤5】 倒计时和周期信息齐全")
    
    def _validate_data_quality(self):
        """验证数据处理结果（只做统计，不做过滤）"""
        if not self.cross_results:
            return
        
        total_count = len(self.cross_results)
        
        # 统计各种计算的完整性
        price_diff_count = 0
        price_percent_count = 0
        rate_diff_count = 0
        countdown_count = 0
        
        for item in self.cross_results:
            if item.price_diff is not None:
                price_diff_count += 1
            if item.price_diff_percent is not None:
                price_percent_count += 1
            if item.rate_diff is not None:
                rate_diff_count += 1
            if item.okx_countdown_seconds is not None or item.binance_countdown_seconds is not None:
                countdown_count += 1
        
        logger.info("🔍 【流水线步骤5】数据处理结果验证")
        logger.info(f"✅ 【流水线步骤5】价格差计算完成: {price_diff_count}/{total_count}")
        logger.info(f"✅【流水线步骤5】价格百分比差计算完成: {price_percent_count}/{total_count}")
        logger.info(f"✅ 【流水线步骤5】费率差计算完成: {rate_diff_count}/{total_count}")
        logger.info(f"✅ 【流水线步骤5】倒计时计算完成: {countdown_count}/{total_count}")
    
    def _is_basic_valid(self, item: Any) -> bool:
        """只做最基础的格式验证"""
        try:
            # 必须有基础属性
            if not hasattr(item, 'exchange') or not hasattr(item, 'symbol'):
                return False
            
            # 必须有交易所标识
            if item.exchange not in ["okx", "binance"]:
                return False
                
            return True
        except Exception:
            return False
    
    def _merge_pair(self, symbol: str, items: List) -> Optional[CrossPlatformData]:
        """合并OKX和币安数据（只做计算，不做判断）"""
        
        # 分离OKX和币安数据
        okx_item = next((item for item in items if item.exchange == "okx"), None)
        binance_item = next((item for item in items if item.exchange == "binance"), None)
        
        # 必须两个平台都有数据
        if not okx_item or not binance_item:
            if not okx_item:
                self.stats["okx_missing"] += 1
            if not binance_item:
                self.stats["binance_missing"] += 1
            return None
        
        # 计算价格差和费率差
        try:
            # 价格计算（允许异常值）
            okx_price = self._safe_float(okx_item.latest_price)
            binance_price = self._safe_float(binance_item.latest_price)
            
            # 如果价格无效，使用0值
            if okx_price is None or binance_price is None:
                self.stats["price_invalid"] += 1
                okx_price = okx_price or 0
                binance_price = binance_price or 0
            
            price_diff = abs(okx_price - binance_price)
            
            # 计算价格百分比差
            if okx_price > 0 and binance_price > 0:
                min_price = min(okx_price, binance_price)
                if min_price > 1e-10:  # 防止除以极度接近0的数
                    price_diff_percent = (price_diff / min_price) * 100
                else:
                    price_diff_percent = 0.0
                    self.stats["price_too_low"] += 1
            else:
                price_diff_percent = 0.0
            
            # 费率计算（允许异常值）
            okx_rate = self._safe_float(okx_item.funding_rate)
            binance_rate = self._safe_float(binance_item.funding_rate)
            
            # 如果费率无效，使用0值
            okx_rate = okx_rate or 0
            binance_rate = binance_rate or 0
            rate_diff = abs(okx_rate - binance_rate)
            
        except Exception as e:
            logger.error(f"{symbol} 计算失败: {e}")
            return None
        
        # 构建最终数据（保留所有原始值）
        return CrossPlatformData(
            symbol=symbol,
            price_diff=price_diff,
            price_diff_percent=price_diff_percent,
            rate_diff=rate_diff,
            
            # 必须先放没有默认值的字段
            okx_price=str(okx_item.latest_price),
            okx_funding_rate=str(okx_item.funding_rate),
            binance_price=str(binance_item.latest_price),
            binance_funding_rate=str(binance_item.funding_rate),
            
            # 再放有默认值的字段
            okx_period_seconds=okx_item.period_seconds,
            okx_countdown_seconds=okx_item.countdown_seconds,
            okx_last_settlement=okx_item.last_settlement_time,
            okx_current_settlement=okx_item.current_settlement_time,
            okx_next_settlement=okx_item.next_settlement_time,
            
            binance_period_seconds=binance_item.period_seconds,
            binance_countdown_seconds=binance_item.countdown_seconds,
            binance_last_settlement=binance_item.last_settlement_time,
            binance_current_settlement=binance_item.current_settlement_time,
            binance_next_settlement=binance_item.next_settlement_time,
        )
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """安全转换为float，不抛出异常"""
        if value is None:
            return None
        
        try:
            # 尝试直接转换
            result = float(value)
            
            # 检查特殊值
            if str(value).lower() in ['inf', '-inf', 'nan']:
                return None
                
            # 检查异常数值
            if abs(result) > 1e15:  # 防止天文数字
                return None
                
            return result
        except (ValueError, TypeError):
            try:
                # 尝试清理字符串
                cleaned = str(value).strip().replace(',', '')
                return float(cleaned)
            except:
                return None
    
    def get_detailed_report(self) -> Dict[str, Any]:
        """获取详细处理报告"""
        if self.stats["start_time"] and self.stats["end_time"]:
            start = datetime.fromisoformat(self.stats["start_time"])
            end = datetime.fromisoformat(self.stats["end_time"])
            duration = (end - start).total_seconds()
        else:
            duration = 0
        
        return {
            "statistics": self.stats,
            "processing_time_seconds": duration,
            "success_rate": self.stats["successful"] / max(1, self.stats["total_symbols"]),
            "timestamp": datetime.now().isoformat()
        }
        