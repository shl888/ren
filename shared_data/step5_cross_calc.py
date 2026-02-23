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
import time
from decimal import Decimal, getcontext

# 设置Decimal精度
getcontext().prec = 28

logger = logging.getLogger(__name__)

@dataclass
class CrossPlatformData:
    """最终跨平台套利数据结构"""
    symbol: str
    
    # 计算字段（没有默认值，放前面）
    trade_price_diff: float              # |OKX成交价 - 币安成交价|（绝对值）
    trade_price_diff_percent: float      # 成交价百分比差（以低价为准）
    rate_diff: Optional[float] = None    # |OKX费率 - 币安费率|（百分化后的差值）
    
    # 必须先放没有默认值的字段！
    okx_trade_price: str                  # OKX成交价格
    okx_funding_rate: Optional[str] = None # OKX费率（百分化后）
    binance_trade_price: str               # 币安成交价格
    binance_funding_rate: Optional[str] = None # 币安费率（百分化后）
    
    # 标记价格字段
    okx_mark_price: str
    binance_mark_price: str
    
    # 成交与标记价差计算
    okx_price_to_mark_diff: Optional[float] = None
    okx_price_to_mark_diff_percent: Optional[float] = None
    binance_price_to_mark_diff: Optional[float] = None
    binance_price_to_mark_diff_percent: Optional[float] = None
    
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
        self.last_log_time = 0
        self.log_interval = 120  # 2分钟，单位：秒
        self.process_count = 0
    
    def process(self, platform_results: List) -> List[CrossPlatformData]:
        """
        处理Step4的单平台数据，只做数据计算，不做业务过滤
        """
        # 频率控制：只偶尔显示处理日志
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        if should_log:
            logger.info(f"✅【流水线步骤5】开始跨平台计算Step4输出的 {len(platform_results)} 条单平台数据...")
        
        if not platform_results:
            if should_log:
                logger.warning("⚠️【流水线步骤5】输入数据为空")
            return []
        
        # 按symbol分组
        grouped = defaultdict(list)
        for item in platform_results:
            # 只检查基本格式，不判断业务合理性
            if self._is_basic_valid(item):
                grouped[item.symbol].append(item)
        
        # 当前批次统计（基于合约数量）
        total_contracts = len(grouped)
        
        if should_log:
            logger.info(f"🤔【流水线步骤5】检测到 {total_contracts} 个合约")
        
        cross_results = []
        
        # 合并每个合约的OKX和币安数据
        for symbol, items in grouped.items():
            try:
                cross_data = self._merge_pair(symbol, items)
                if cross_data:
                    cross_results.append(cross_data)
            except Exception as e:
                if should_log:
                    logger.error(f"❌【流水线步骤5】跨平台计算失败: {symbol} - {e}")
                continue
        
        if should_log:
            # 处理完成后，打印统计结果
            actual_contracts = len(cross_results)
            
            logger.info(f"✅【流水线步骤5】Step5计算完成，共生成 {actual_contracts} 个合约的跨平台数据")
            
            # 只显示当前批次的统计
            self._log_batch_statistics(total_contracts, actual_contracts, cross_results)
            
            self.last_log_time = current_time
            self.process_count = 0
        
        self.process_count += 1
        
        return cross_results
    
    def _log_batch_statistics(self, total_contracts: int, actual_contracts: int, results: List[CrossPlatformData]):
        """打印当前批次的统计结果"""
        # 数据处理结果验证
        if results:
            self._validate_data_quality(results)
            
            # 总结
            if total_contracts > 0:
                success_rate = (actual_contracts / total_contracts) * 100
                logger.info(f"🎉【流水线步骤5】当前批次完成率: {success_rate:.1f}%")
            logger.info(f"✅【流水线步骤5】成功计算 {actual_contracts} 个合约的跨平台数据")
            logger.info(f"✅【流水线步骤5】成功生成 {actual_contracts} 个双平台合约的{actual_contracts}条成品数据")
    
    def _validate_data_quality(self, results: List[CrossPlatformData]):
        """验证数据处理结果（只做统计，不做过滤）"""
        total_count = len(results)
        
        # 统计各种计算的完整性
        trade_price_diff_count = 0
        trade_price_percent_count = 0
        rate_diff_count = 0
        countdown_count = 0
        price_to_mark_count = 0
        
        for item in results:
            if item.trade_price_diff is not None:
                trade_price_diff_count += 1
            if item.trade_price_diff_percent is not None:
                trade_price_percent_count += 1
            if item.rate_diff is not None:
                rate_diff_count += 1
            if item.okx_countdown_seconds is not None or item.binance_countdown_seconds is not None:
                countdown_count += 1
            if item.okx_price_to_mark_diff is not None or item.binance_price_to_mark_diff is not None:
                price_to_mark_count += 1
        
        logger.info("🔍【流水线步骤5】数据处理质量验证:")
        logger.info(f"  • 成交价差计算: {trade_price_diff_count}/{total_count} 个合约")
        logger.info(f"  • 成交价差百分比: {trade_price_percent_count}/{total_count} 个合约")
        logger.info(f"  • 费率差计算: {rate_diff_count}/{total_count} 个合约")
        logger.info(f"  • 倒计时计算: {countdown_count}/{total_count} 个合约")
        logger.info(f"  • 成交-标记价差: {price_to_mark_count}/{total_count} 个合约")
    
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
    
    # ==================== 辅助方法 ====================
    
    def _decimal_diff(self, a_str: Optional[str], b_str: Optional[str]) -> Optional[float]:
        """
        用Decimal精确计算两个字符串表示的数值的绝对差
        返回float，但已经是精确值
        """
        if not a_str or not b_str:
            return None
        
        try:
            a = Decimal(str(a_str).strip())
            b = Decimal(str(b_str).strip())
            diff = abs(a - b)
            return float(diff)
        except Exception:
            return None
    
    def _format_percent(self, value: Optional[float], decimals: int = 4) -> Optional[float]:
        """百分比保留指定位数小数，四舍五入"""
        if value is None:
            return None
        return round(value, decimals)
    
    def _funding_rate_to_percent(self, rate_str: Optional[str]) -> Optional[float]:
        """
        将原始费率转换为百分化后的值
        原始值 -0.0000226151 → -0.00226151
        """
        if not rate_str:
            return None
        try:
            rate = float(rate_str)
            return rate * 100  # 百分化
        except:
            return None
    
    # ==================== 修改：_merge_pair 方法 ====================
    
    def _merge_pair(self, symbol: str, items: List) -> Optional[CrossPlatformData]:
        """合并OKX和币安数据"""
        
        # 分离OKX和币安数据
        okx_item = next((item for item in items if item.exchange == "okx"), None)
        binance_item = next((item for item in items if item.exchange == "binance"), None)
        
        # 必须两个平台都有数据
        if not okx_item or not binance_item:
            return None
        
        try:
            # ========== 价差计算（用Decimal精确计算） ==========
            # 成交价差
            trade_price_diff = self._decimal_diff(
                okx_item.trade_price, 
                binance_item.trade_price
            ) or 0.0
            
            # OKX成交-标记价差
            okx_price_to_mark_diff = self._decimal_diff(
                okx_item.trade_price,
                okx_item.mark_price
            )
            
            # 币安成交-标记价差
            binance_price_to_mark_diff = self._decimal_diff(
                binance_item.trade_price,
                binance_item.mark_price
            )
            
            # ========== 价格相关百分比计算 ==========
            
            # 成交价百分比差
            trade_price_diff_percent = 0.0
            okx_trade_price = self._safe_float(okx_item.trade_price)
            binance_trade_price = self._safe_float(binance_item.trade_price)
            
            if okx_trade_price and binance_trade_price and okx_trade_price > 0 and binance_trade_price > 0:
                min_trade_price = min(okx_trade_price, binance_trade_price)
                if min_trade_price > 1e-10:
                    trade_price_diff_percent = (trade_price_diff / min_trade_price) * 100
                    trade_price_diff_percent = self._format_percent(trade_price_diff_percent)
            
            # ========== 费率相关计算（先百分化，再四舍五入，再计算差值） ==========
            
            # 1. 费率百分化
            okx_rate_pct = self._funding_rate_to_percent(okx_item.funding_rate)
            binance_rate_pct = self._funding_rate_to_percent(binance_item.funding_rate)
            
            # 2. 费率四舍五入到4位（显示优化）
            okx_rate_display = self._format_percent(okx_rate_pct) if okx_rate_pct is not None else None
            binance_rate_display = self._format_percent(binance_rate_pct) if binance_rate_pct is not None else None
            
            # 3. 费率差计算（基于四舍五入后的值）
            rate_diff = None
            if okx_rate_display is not None and binance_rate_display is not None:
                rate_diff = abs(okx_rate_display - binance_rate_display)
                # 费率差本身已经是百分比值，不再四舍五入
            
            # ========== OKX成交-标记价差百分比 ==========
            okx_price_to_mark_diff_percent = None
            if okx_price_to_mark_diff and okx_trade_price and okx_trade_price > 0:
                min_price = min(okx_trade_price, self._safe_float(okx_item.mark_price) or okx_trade_price)
                if min_price > 1e-10:
                    okx_price_to_mark_diff_percent = (okx_price_to_mark_diff / min_price) * 100
                    okx_price_to_mark_diff_percent = self._format_percent(okx_price_to_mark_diff_percent)
            
            # ========== 币安成交-标记价差百分比 ==========
            binance_price_to_mark_diff_percent = None
            if binance_price_to_mark_diff and binance_trade_price and binance_trade_price > 0:
                min_price = min(binance_trade_price, self._safe_float(binance_item.mark_price) or binance_trade_price)
                if min_price > 1e-10:
                    binance_price_to_mark_diff_percent = (binance_price_to_mark_diff / min_price) * 100
                    binance_price_to_mark_diff_percent = self._format_percent(binance_price_to_mark_diff_percent)
            
        except Exception as e:
            return None
        
        # 构建最终数据
        return CrossPlatformData(
            symbol=symbol,
            trade_price_diff=trade_price_diff,
            trade_price_diff_percent=trade_price_diff_percent,
            rate_diff=rate_diff,
            
            # 必须先放没有默认值的字段
            okx_trade_price=str(okx_item.trade_price) if okx_item.trade_price else "",
            okx_funding_rate=str(okx_rate_display) if okx_rate_display is not None else None,
            binance_trade_price=str(binance_item.trade_price) if binance_item.trade_price else "",
            binance_funding_rate=str(binance_rate_display) if binance_rate_display is not None else None,
            
            # 标记价格字段
            okx_mark_price=str(okx_item.mark_price) if okx_item.mark_price else "",
            binance_mark_price=str(binance_item.mark_price) if binance_item.mark_price else "",
            
            # 成交与标记价差（精确值，不四舍五入）
            okx_price_to_mark_diff=okx_price_to_mark_diff,
            okx_price_to_mark_diff_percent=okx_price_to_mark_diff_percent,
            binance_price_to_mark_diff=binance_price_to_mark_diff,
            binance_price_to_mark_diff_percent=binance_price_to_mark_diff_percent,
            
            # 其他字段保持不变
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