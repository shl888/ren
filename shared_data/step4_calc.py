"""
第四步：单平台计算（修复版）
功能：1. 币安时间滚动 2. 费率周期 3. 倒计时
修正：时间字段直接保留Step3的字符串，不再重复转换
稳定性增强：添加24小时TTL缓存清理
统计增强：按合约数统计
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from collections import defaultdict
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
class PlatformData:
    """单平台计算后的数据结构"""
    symbol: str
    exchange: str
    contract_name: str
    
    # 价格和费率
    latest_price: Optional[str] = None
    funding_rate: Optional[str] = None
    
    # 时间字段（直接保留Step3的字符串格式）
    last_settlement_time: Optional[str] = None      # 字符串格式
    current_settlement_time: Optional[str] = None
    next_settlement_time: Optional[str] = None
    
    # 时间戳备份（仅用于倒计时计算）
    last_settlement_ts: Optional[int] = None
    current_settlement_ts: Optional[int] = None
    next_settlement_ts: Optional[int] = None
    
    # 计算结果
    period_seconds: Optional[int] = None
    countdown_seconds: Optional[int] = None

class Step4Calc:
    """第四步：单平台计算（按合约数统计）"""
    
    def __init__(self):
        self.binance_cache = {}
        
        # ✅ 修改：详细统计
        self.stats = {
            "total_contracts_received": 0,      # 接收的合约数
            "okx_processed": 0,                 # OKX处理的合约数
            "binance_processed": 0,             # 币安处理的合约数
            "rollover_events": 0,               # 时间滚动次数
            "cache_updates": 0,                 # 缓存更新次数
            "period_calculated": 0,             # 周期计算次数
            "countdown_calculated": 0           # 倒计时计算次数
        }
        
        # ✅ 按交易所统计
        self.exchange_stats = defaultdict(int)
        
        # ✅ 5分钟统计
        self.last_report_time = time.time()
        self.batch_count = 0
        
        # ✅ 稳定性增强：TTL缓存清理
        self._cache_ttl_hours = 24
        self._last_cleanup_time = time.time()
        self._cleanup_interval_hours = 1  # 每小时检查一次
        
        log_data_process("步骤4", "启动", f"Step4Calc初始化完成（按合约数统计版）")
    
    def process(self, aligned_results: List) -> List[PlatformData]:
        """
        处理Step3的对齐数据
        """
        # ✅ 处理前检查是否需要清理缓存
        self._auto_cleanup_if_needed()
        
        # ✅ 修改：按合约统计
        self.batch_count += 1
        input_contracts = len(aligned_results)
        self.stats["total_contracts_received"] += input_contracts
        
        results = []
        for item in aligned_results:
            try:
                # ✅ 按交易所统计
                self.exchange_stats[item.symbol] += 1  # 记录每个合约的处理次数
                
                okx_data = self._calc_okx(item)
                binance_data = self._calc_binance(item)
                
                if okx_data:
                    results.append(okx_data)
                    self.stats["okx_processed"] += 1
                
                if binance_data:
                    results.append(binance_data)
                    self.stats["binance_processed"] += 1
                    self.stats["cache_updates"] += 1
                
            except Exception as e:
                log_data_process("步骤4", "错误", f"计算失败: {item.symbol} - {e}", "ERROR")
                continue
        
        # ✅ 修改：5分钟详细统计
        current_time = time.time()
        if current_time - self.last_report_time >= 300:  # 5分钟
            total_processed = self.stats["okx_processed"] + self.stats["binance_processed"]
            
            # ✅ 统计不同交易所的合约数量（去重）
            unique_contracts = len(self.exchange_stats)
            okx_contracts = sum(1 for item in aligned_results if hasattr(item, 'okx_price') and item.okx_price)
            binance_contracts = sum(1 for item in aligned_results if hasattr(item, 'binance_price') and item.binance_price)
            
            # 计算平均费率周期
            avg_period = self._calculate_average_period()
            
            log_data_process("步骤4", "统计", 
                           f"5分钟: 接收{self.stats['total_contracts_received']}合约, "
                           f"处理{total_processed}次(OKX{self.stats['okx_processed']}+币安{self.stats['binance_processed']})")
            log_data_process("步骤4", "详情", 
                           f"合约分布: 唯一合约{unique_contracts}个, OKX{okx_contracts}个, 币安{binance_contracts}个")
            log_data_process("步骤4", "计算", 
                           f"计算统计: 周期{self.stats['period_calculated']}次, "
                           f"倒计时{self.stats['countdown_calculated']}次, "
                           f"时间滚动{self.stats['rollover_events']}次")
            log_data_process("步骤4", "缓存", 
                           f"缓存状态: {len(self.binance_cache)}合约, "
                           f"平均周期{avg_period:.1f}小时, "
                           f"批次{self.batch_count}次")
            
            # 重置统计
            self.last_report_time = current_time
            self.batch_count = 0
            self._reset_stats()
            self.exchange_stats.clear()
        
        return results
    
    def _calc_okx(self, aligned_item) -> Optional[PlatformData]:
        """计算OKX数据"""
        if not aligned_item.okx_current_ts:
            return None
        
        # 直接保留Step3的字符串时间
        data = PlatformData(
            symbol=aligned_item.symbol,
            exchange="okx",
            contract_name=aligned_item.okx_contract_name or "",
            latest_price=aligned_item.okx_price,
            funding_rate=aligned_item.okx_funding_rate,
            current_settlement_time=aligned_item.okx_current_settlement,
            next_settlement_time=aligned_item.okx_next_settlement,
            # 保存时间戳用于倒计时计算
            current_settlement_ts=aligned_item.okx_current_ts,
            next_settlement_ts=aligned_item.okx_next_ts
        )
        
        # 计算费率周期
        if data.current_settlement_ts and data.next_settlement_ts:
            data.period_seconds = (data.next_settlement_ts - data.current_settlement_ts) // 1000
            self.stats["period_calculated"] += 1
        
        # 计算倒计时
        data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        if data.countdown_seconds is not None:
            self.stats["countdown_calculated"] += 1
        
        return data
    
    def _calc_binance(self, aligned_item) -> Optional[PlatformData]:
        """计算币安数据（时间滚动）"""
        if not aligned_item.binance_current_ts:
            return None
        
        symbol = aligned_item.symbol
        
        # 初始化缓存
        if symbol not in self.binance_cache:
            self.binance_cache[symbol] = {
                "last_ts": aligned_item.binance_last_ts,
                "current_ts": aligned_item.binance_current_ts,
                "last_update_time": time.time()
            }
        
        cache = self.binance_cache[symbol]
        T1 = cache["last_ts"]
        T2 = cache["current_ts"]
        T3 = aligned_item.binance_current_ts
        
        # 时间滚动逻辑
        if T2 and T3 != T2:
            # ✅ 修改：实时打印缓存更新
            t2_str = self._ts_to_str(T2)
            t3_str = self._ts_to_str(T3)
            log_data_process("步骤4", "滚动", f"币安{symbol}: 时间滚动 {t2_str}→{t3_str}")
            
            T1 = T2
            T2 = T3
            cache["last_ts"] = T1
            cache["current_ts"] = T2
            self.stats["rollover_events"] += 1
        
        # ✅ 更新缓存时间戳
        cache["last_update_time"] = time.time()
        
        # 构建数据
        data = PlatformData(
            symbol=symbol,
            exchange="binance",
            contract_name=aligned_item.binance_contract_name or "",
            latest_price=aligned_item.binance_price,
            funding_rate=aligned_item.binance_funding_rate,
            last_settlement_time=aligned_item.binance_last_settlement,
            current_settlement_time=aligned_item.binance_current_settlement,
            next_settlement_time=aligned_item.binance_next_settlement,
            last_settlement_ts=T1,
            current_settlement_ts=T2
        )
        
        # 计算费率周期
        if data.current_settlement_ts and data.last_settlement_ts:
            data.period_seconds = (data.current_settlement_ts - data.last_settlement_ts) // 1000
            self.stats["period_calculated"] += 1
        
        # 计算倒计时
        data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        if data.countdown_seconds is not None:
            self.stats["countdown_calculated"] += 1
        
        return data
    
    def _calc_countdown(self, settlement_ts: Optional[int]) -> Optional[int]:
        """计算倒计时"""
        if not settlement_ts:
            return None
        
        try:
            now_ms = int(time.time() * 1000)
            return max(0, (settlement_ts - now_ms) // 1000)
        except Exception as e:
            log_data_process("步骤4", "警告", f"倒计时计算失败: {settlement_ts} - {e}", "WARNING")
            return None
    
    def _auto_cleanup_if_needed(self):
        """自动清理过期缓存"""
        current_time = time.time()
        time_since_last_cleanup = current_time - self._last_cleanup_time
        
        # 每小时检查一次
        if time_since_last_cleanup >= (self._cleanup_interval_hours * 3600):
            cleaned = self._cleanup_expired_cache()
            if cleaned > 0:
                log_data_process("步骤4", "缓存", f"清理了 {cleaned} 个过期条目")
            self._last_cleanup_time = current_time
    
    def _cleanup_expired_cache(self) -> int:
        """清理过期缓存"""
        if not self.binance_cache:
            return 0
        
        current_time = time.time()
        expired_seconds = self._cache_ttl_hours * 3600
        expired_symbols = []
        
        for symbol, cache in self.binance_cache.items():
            last_update = cache.get("last_update_time", 0)
            if current_time - last_update > expired_seconds:
                expired_symbols.append(symbol)
        
        # 清理过期缓存
        cleaned = 0
        for symbol in expired_symbols:
            try:
                del self.binance_cache[symbol]
                cleaned += 1
                log_data_process("步骤4", "缓存", f"清理过期缓存: {symbol}", "DEBUG")
            except KeyError:
                pass
        
        return cleaned
    
    def _calculate_average_period(self) -> float:
        """计算平均费率周期（小时）"""
        total_period = 0
        count = 0
        
        for cache in self.binance_cache.values():
            last_ts = cache.get("last_ts")
            current_ts = cache.get("current_ts")
            
            if last_ts and current_ts and current_ts > last_ts:
                period_hours = (current_ts - last_ts) / 1000 / 3600
                if 0 < period_hours < 24:  # 合理范围
                    total_period += period_hours
                    count += 1
        
        return total_period / count if count > 0 else 0.0
    
    def _ts_to_str(self, ts: Optional[int]) -> str:
        """时间戳转字符串（供日志使用）"""
        if ts is None or ts <= 0:
            return "无"
        
        try:
            from datetime import datetime, timedelta
            dt_utc = datetime.utcfromtimestamp(ts / 1000)
            dt_bj = dt_utc + timedelta(hours=8)
            return dt_bj.strftime("%H:%M")
        except:
            return "无效"
    
    def _reset_stats(self):
        """重置统计"""
        self.stats = {
            "total_contracts_received": 0,
            "okx_processed": 0,
            "binance_processed": 0,
            "rollover_events": 0,
            "cache_updates": 0,
            "period_calculated": 0,
            "countdown_calculated": 0
        }
    
    # 以下方法保持原样...
    def manual_cleanup_cache(self) -> int:
        """手动清理缓存（用于监控或测试）"""
        return self._cleanup_expired_cache()
    
    def get_cache_status(self, symbol: str) -> Dict[str, Any]:
        """查询单个合约的币安缓存状态"""
        cache = self.binance_cache.get(symbol, {})
        
        status = {
            "has_last_ts": cache.get("last_ts") is not None,
            "has_current_ts": cache.get("current_ts") is not None,
            "last_ts": cache.get("last_ts"),
            "current_ts": cache.get("current_ts"),
            "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
            "current_settlement_time": self._ts_to_str(cache.get("current_ts"))
        }
        
        if cache:
            last_update = cache.get("last_update_time", 0)
            age_hours = (time.time() - last_update) / 3600
            status["cache_age_hours"] = age_hours
            status["will_expire_in_hours"] = max(0, self._cache_ttl_hours - age_hours)
            status["is_expired"] = age_hours > self._cache_ttl_hours
        
        return status
    
    def get_cache_report(self) -> Dict[str, Any]:
        """获取币安缓存状态完整报告"""
        current_time = time.time()
        
        report = {
            "total_cached": len(self.binance_cache),
            "with_last_ts": 0,
            "without_last_ts": 0,
            "symbols_without_history": [],
            "symbol_details": {},
            "ttl_hours": self._cache_ttl_hours,
            "expired_count": 0
        }
        
        for symbol, cache in self.binance_cache.items():
            # 计算缓存年龄
            last_update = cache.get("last_update_time", 0)
            age_hours = (current_time - last_update) / 3600
            is_expired = age_hours > self._cache_ttl_hours
            
            if cache.get("last_ts"):
                report["with_last_ts"] += 1
            else:
                report["without_last_ts"] += 1
                report["symbols_without_history"].append(symbol)
            
            if is_expired:
                report["expired_count"] += 1
            
            # 添加详细缓存信息
            report["symbol_details"][symbol] = {
                "last_ts": cache.get("last_ts"),
                "current_ts": cache.get("current_ts"),
                "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
                "current_settlement_time": self._ts_to_str(cache.get("current_ts")),
                "cache_age_hours": age_hours,
                "is_expired": is_expired,
                "status": "expired" if is_expired else "active",
                "history_status": "complete" if cache.get("last_ts") else "pending_history"
            }
        
        return report