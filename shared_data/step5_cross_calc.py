"""
第五步：跨平台计算 + 最终数据打包（数据计算专用版）
功能：1. 计算价格差、费率差（绝对值+百分比） 2. 打包双平台所有字段 3. 倒计时
原则：只做数据计算，不做业务判断。所有数据都保留，交给后续交易模块处理。
输出：原始套利数据，每条包含双平台完整信息
稳定性增强：微调metadata字段命名，保持纯粹计算
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime
import traceback
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
    
    # ✅ 微调：数据源标记（不含业务判断，只记录来源）
    metadata: Dict[str, Any] = field(default_factory=lambda: {
        "calculated_at": None,
        "source": "step5_cross_calc",  # ✅ 只记录来源，不做验证标记
        "version": "3.0.0-stable"
    })
    
    def __post_init__(self):
        """只记录时间戳，不做任何验证"""
        self.metadata["calculated_at"] = datetime.now().isoformat()

class Step5CrossCalc:
    """第五步：跨平台计算（专注数据计算版）"""
    
    def __init__(self):
        # 基本统计（不包含业务逻辑）
        self.stats = {
            "total_processed": 0,
            "successful": 0,
            "failed": 0,
            "okx_missing": 0,
            "binance_missing": 0,
            "price_invalid": 0,
            "calc_errors": 0,
            # ✅ 删除：price_too_low统计（按你要求）
            "start_time": None,
            "end_time": None
        }
        
        # ✅ 添加：5分钟统计计时器
        self.last_report_time = time.time()
        self.batch_input = 0
        self.batch_output = 0
        
        log_data_process("步骤5", "启动", "Step5CrossCalc初始化完成（跨平台套利计算）")
    
    def process(self, platform_results: List) -> List[CrossPlatformData]:
        """
        处理Step4的单平台数据，只做数据计算，不做业务过滤
        """
        self.stats["start_time"] = datetime.now().isoformat()
        input_count = len(platform_results)
        self.stats["total_processed"] = input_count
        self.batch_input += input_count
        
        # ✅ 修改：移除开始处理日志
        if not platform_results:
            log_data_process("步骤5", "警告", "输入数据为空", "WARNING")
            return []
        
        # 按symbol分组
        grouped = defaultdict(list)
        for item in platform_results:
            # 只检查基本格式，不判断业务合理性
            if self._is_basic_valid(item):
                grouped[item.symbol].append(item)
        
        # 合并每个合约的OKX和币安数据
        results = []
        for symbol, items in grouped.items():
            try:
                cross_data = self._merge_pair(symbol, items)
                if cross_data:
                    results.append(cross_data)
                    self.stats["successful"] += 1
                    self.batch_output += 1
                    
            except Exception as e:
                log_data_process("步骤5", "错误", f"跨平台计算失败: {symbol} - {e}", "ERROR")
                log_data_process("步骤5", "调试", f"错误详情: {traceback.format_exc()}", "DEBUG")
                self.stats["failed"] += 1
                self.stats["calc_errors"] += 1
                continue
        
        self.stats["end_time"] = datetime.now().isoformat()
        
        # ✅ 添加：5分钟统计
        current_time = time.time()
        if current_time - self.last_report_time >= 300:  # 5分钟
            success_rate = (self.batch_output / self.batch_input * 100) if self.batch_input > 0 else 0
            
            log_data_process("步骤5", "统计", 
                           f"5分钟: 接收{self.batch_input}条 → 跨平台数据{self.batch_output}条 "
                           f"({success_rate:.1f}%)")
            log_data_process("步骤5", "完成", 
                           f"计算统计: 成功{self.stats['successful']}条, "
                           f"价格无效{self.stats['price_invalid']}条, "
                           f"计算错误{self.stats['calc_errors']}条")
            
            # 重置统计
            self.last_report_time = current_time
            self.batch_input = 0
            self.batch_output = 0
        
        return results
    
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
                log_data_process("步骤5", "警告", f"未知交易所: {item.exchange}", "WARNING")
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
            log_data_process("步骤5", "跳过", f"{symbol}: 缺少平台数据", "WARNING")
            return None
        
        # 计算价格差和费率差
        try:
            # 价格计算（允许异常值）
            okx_price = self._safe_float(okx_item.latest_price)
            binance_price = self._safe_float(binance_item.latest_price)
            
            # 如果价格无效，只标记，不过滤
            if okx_price is None or binance_price is None:
                self.stats["price_invalid"] += 1
                # 继续处理，使用0值
                okx_price = okx_price or 0
                binance_price = binance_price or 0
            
            price_diff = abs(okx_price - binance_price)
            
            # 优化：更安全地计算价格百分比差
            if okx_price > 0 and binance_price > 0:
                min_price = min(okx_price, binance_price)
                if min_price > 1e-10:  # 防止除以极度接近0的数
                    price_diff_percent = (price_diff / min_price) * 100
                else:
                    price_diff_percent = 0.0
                    # ✅ 删除：价格过低警告（按你要求）
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
            log_data_process("步骤5", "错误", f"{symbol}: 计算失败 - {e}", "ERROR")
            log_data_process("步骤5", "调试", 
                           f"原始数据 - OKX价格: {okx_item.latest_price}, 币安价格: {binance_item.latest_price}", 
                           "DEBUG")
            self.stats["calc_errors"] += 1
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
            "success_rate": self.stats["successful"] / max(1, self.stats["total_processed"]),
            "timestamp": datetime.now().isoformat()
        }