"""
第五步：跨平台计算 + 最终数据打包（按合约数统计版）
功能：1. 计算价格差、费率差（绝对值+百分比） 2. 打包双平台所有字段 3. 倒计时
原则：只做数据计算，不做业务判断。所有数据都保留，交给后续交易模块处理。
输出：原始套利数据，每条包含双平台完整信息
统计：按合约数统计
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime
import traceback
import time

logger = logging.getLogger(__name__)

# ✅ 修改：统一的日志工具函数
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
    
    # ✅ 数据源标记
    metadata: Dict[str, Any] = field(default_factory=lambda: {
        "calculated_at": None,
        "source": "step5_cross_calc",
        "version": "3.0.0-stable"
    })
    
    def __post_init__(self):
        """只记录时间戳"""
        self.metadata["calculated_at"] = datetime.now().isoformat()

class Step5CrossCalc:
    """第五步：跨平台计算（按合约数统计）"""
    
    def __init__(self):
        # ✅ 修改：详细统计
        self.stats = {
            "total_contracts_received": 0,      # 接收的合约数
            "successful_contracts": 0,          # 成功计算的合约数
            "failed_contracts": 0,              # 失败的合约数
            "okx_missing": 0,                   # 缺少OKX数据的合约数
            "binance_missing": 0,               # 缺少币安数据的合约数
            "price_invalid": 0,                 # 价格无效的合约数
            "calc_errors": 0,                   # 计算错误的合约数
            "price_diff_stats": {               # 价格差统计
                "total": 0,
                "sum": 0.0,
                "max": 0.0,
                "min": float('inf')
            },
            "rate_diff_stats": {                # 费率差统计
                "total": 0,
                "sum": 0.0,
                "max": 0.0,
                "min": float('inf')
            }
        }
        
        # ✅ 5分钟统计
        self.last_report_time = time.time()
        self.batch_count = 0
        
        # ✅ 成功计算的合约列表（用于去重统计）
        self.successful_symbols = set()
        
        log_data_process("步骤5", "启动", "Step5CrossCalc初始化完成（按合约数统计）")
    
    def process(self, platform_results: List) -> List[CrossPlatformData]:
        """
        处理Step4的单平台数据，按合约数统计
        """
        self.stats["start_time"] = datetime.now().isoformat()
        
        # ✅ 修改：按合约统计
        self.batch_count += 1
        
        # 按symbol分组
        grouped = defaultdict(list)
        for item in platform_results:
            # 只检查基本格式
            if self._is_basic_valid(item):
                grouped[item.symbol].append(item)
                self.stats["total_contracts_received"] += 1
        
        # 合并每个合约的OKX和币安数据
        results = []
        for symbol, items in grouped.items():
            try:
                cross_data = self._merge_pair(symbol, items)
                if cross_data:
                    results.append(cross_data)
                    self.stats["successful_contracts"] += 1
                    self.successful_symbols.add(symbol)
                    
                    # ✅ 更新统计信息
                    self._update_diff_stats(cross_data.price_diff, cross_data.rate_diff)
                    
            except Exception as e:
                log_data_process("步骤5", "错误", f"跨平台计算失败: {symbol} - {e}", "ERROR")
                self.stats["failed_contracts"] += 1
                self.stats["calc_errors"] += 1
                continue
        
        self.stats["end_time"] = datetime.now().isoformat()
        
        # ✅ 修改：5分钟详细统计
        current_time = time.time()
        if current_time - self.last_report_time >= 300:  # 5分钟
            unique_contracts = len(grouped)
            successful_unique = len(self.successful_symbols)
            
            if self.stats["total_contracts_received"] > 0:
                success_rate = (self.stats["successful_contracts"] / self.stats["total_contracts_received"] * 100)
            else:
                success_rate = 0
            
            # 价格差统计
            price_stats = self.stats["price_diff_stats"]
            if price_stats["total"] > 0:
                avg_price_diff = price_stats["sum"] / price_stats["total"]
            else:
                avg_price_diff = 0
            
            # 费率差统计
            rate_stats = self.stats["rate_diff_stats"]
            if rate_stats["total"] > 0:
                avg_rate_diff = rate_stats["sum"] / rate_stats["total"]
            else:
                avg_rate_diff = 0
            
            log_data_process("步骤5", "统计", 
                           f"5分钟: 接收{self.stats['total_contracts_received']}合约 → "
                           f"成功{self.stats['successful_contracts']}合约({success_rate:.1f}%)")
            log_data_process("步骤5", "详情", 
                           f"合约分布: 唯一{unique_contracts}个, 成功唯一{successful_unique}个, "
                           f"批次{self.batch_count}次")
            log_data_process("步骤5", "错误", 
                           f"错误统计: 失败{self.stats['failed_contracts']}个, "
                           f"缺OKX{self.stats['okx_missing']}个, "
                           f"缺币安{self.stats['binance_missing']}个, "
                           f"价格无效{self.stats['price_invalid']}个")
            log_data_process("步骤5", "差值", 
                           f"价格差: 平均{avg_price_diff:.6f}, 最大{price_stats['max']:.6f}, "
                           f"费率差: 平均{avg_rate_diff:.6f}, 最大{rate_stats['max']:.6f}")
            
            # 重置统计
            self.last_report_time = current_time
            self.batch_count = 0
            self.successful_symbols.clear()
            self._reset_stats()
        
        return results
    
    def _update_diff_stats(self, price_diff: float, rate_diff: float):
        """更新差值统计"""
        # 价格差统计
        price_stats = self.stats["price_diff_stats"]
        price_stats["total"] += 1
        price_stats["sum"] += price_diff
        price_stats["max"] = max(price_stats["max"], price_diff)
        price_stats["min"] = min(price_stats["min"], price_diff)
        
        # 费率差统计
        rate_stats = self.stats["rate_diff_stats"]
        rate_stats["total"] += 1
        rate_stats["sum"] += rate_diff
        rate_stats["max"] = max(rate_stats["max"], rate_diff)
        rate_stats["min"] = min(rate_stats["min"], rate_diff)
    
    def _is_basic_valid(self, item: Any) -> bool:
        """只做最基础的格式验证"""
        try:
            # 必须有基础属性
            if not hasattr(item, 'exchange') or not hasattr(item, 'symbol'):
                return False
            
            # 必须有价格属性（值可以任意）
            if not hasattr(item, 'latest_price'):
                return False
            
            # 必须有交易所标识
            if item.exchange not in ["okx", "binance"]:
                return False
                
            return True
        except Exception:
            return False
    
    def _merge_pair(self, symbol: str, items: List) -> Optional[CrossPlatformData]:
        """合并OKX和币安数据"""
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
            # 价格计算
            okx_price = self._safe_float(okx_item.latest_price)
            binance_price = self._safe_float(binance_item.latest_price)
            
            # 如果价格无效
            if okx_price is None or binance_price is None:
                self.stats["price_invalid"] += 1
                okx_price = okx_price or 0
                binance_price = binance_price or 0
            
            price_diff = abs(okx_price - binance_price)
            
            # 计算价格百分比差
            if okx_price > 0 and binance_price > 0:
                min_price = min(okx_price, binance_price)
                if min_price > 1e-10:
                    price_diff_percent = (price_diff / min_price) * 100
                else:
                    price_diff_percent = 0.0
            else:
                price_diff_percent = 0.0
            
            # 费率计算
            okx_rate = self._safe_float(okx_item.funding_rate)
            binance_rate = self._safe_float(binance_item.funding_rate)
            
            okx_rate = okx_rate or 0
            binance_rate = binance_rate or 0
            rate_diff = abs(okx_rate - binance_rate)
            
        except Exception as e:
            self.stats["calc_errors"] += 1
            return None
        
        # 构建最终数据
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
        """安全转换为float"""
        if value is None:
            return None
        
        try:
            result = float(value)
            
            # 检查特殊值
            if str(value).lower() in ['inf', '-inf', 'nan']:
                return None
                
            # 检查异常数值
            if abs(result) > 1e15:
                return None
                
            return result
        except (ValueError, TypeError):
            try:
                cleaned = str(value).strip().replace(',', '')
                return float(cleaned)
            except:
                return None
    
    def _reset_stats(self):
        """重置统计"""
        self.stats = {
            "total_contracts_received": 0,
            "successful_contracts": 0,
            "failed_contracts": 0,
            "okx_missing": 0,
            "binance_missing": 0,
            "price_invalid": 0,
            "calc_errors": 0,
            "price_diff_stats": {
                "total": 0,
                "sum": 0.0,
                "max": 0.0,
                "min": float('inf')
            },
            "rate_diff_stats": {
                "total": 0,
                "sum": 0.0,
                "max": 0.0,
                "min": float('inf')
            }
        }
    
    def get_detailed_report(self) -> Dict[str, Any]:
        """获取详细处理报告"""
        if "start_time" in self.stats and "end_time" in self.stats and self.stats["start_time"] and self.stats["end_time"]:
            start = datetime.fromisoformat(self.stats["start_time"])
            end = datetime.fromisoformat(self.stats["end_time"])
            duration = (end - start).total_seconds()
        else:
            duration = 0
        
        return {
            "statistics": self.stats,
            "processing_time_seconds": duration,
            "success_rate": self.stats["successful_contracts"] / max(1, self.stats["total_contracts_received"]),
            "timestamp": datetime.now().isoformat(),
            "unique_successful_symbols": len(self.successful_symbols)
        }