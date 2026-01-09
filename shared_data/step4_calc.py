"""
第四步：单平台计算（修复版）
功能：1. 币安时间滚动 2. 费率周期 3. 倒计时
修正：时间字段直接保留Step3的字符串，不再重复转换
稳定性增强：添加24小时TTL缓存清理
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from collections import defaultdict
import time  # ✅ 添加time模块

logger = logging.getLogger(__name__)

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
    """第四步：单平台计算（添加TTL缓存清理）"""
    
    def __init__(self):
        self.binance_cache = {}
        self.stats = defaultdict(int)
        
        # ✅ 稳定性增强：TTL缓存清理
        self._cache_ttl_hours = 24
        self._last_cleanup_time = time.time()
        self._cleanup_interval_hours = 1  # 每小时检查一次
        
        logger.info(f"✅ Step4Calc初始化完成（缓存TTL: {self._cache_ttl_hours}小时）")
    
    def process(self, aligned_results: List) -> List[PlatformData]:
        """
        处理Step3的对齐数据
        """
        # ✅ 处理前检查是否需要清理缓存
        self._auto_cleanup_if_needed()
        
        logger.info(f"开始单平台计算 {len(aligned_results)} 个合约...")
        
        results = []
        for item in aligned_results:
            try:
                okx_data = self._calc_okx(item)
                binance_data = self._calc_binance(item)
                
                if okx_data:
                    results.append(okx_data)
                if binance_data:
                    results.append(binance_data)
                
            except Exception as e:
                logger.error(f"计算失败: {item.symbol} - {e}")
                continue
        
        logger.info(f"Step4计算完成: {len(results)} 条单平台数据")
        logger.info(f"币安时间滚动统计: {dict(self.stats)}")
        return results
    
    def _calc_okx(self, aligned_item) -> Optional[PlatformData]:
        """计算OKX数据（保持原样）"""
        
        if not aligned_item.okx_current_ts:
            logger.debug(f"OKX {aligned_item.symbol} 无有效时间戳，跳过")
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
        
        # 计算倒计时
        data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        
        return data
    
    def _calc_binance(self, aligned_item) -> Optional[PlatformData]:
        """计算币安数据（时间滚动 - 添加缓存时间戳）"""
        
        if not aligned_item.binance_current_ts:
            logger.debug(f"币安 {aligned_item.symbol} 无有效时间戳，跳过")
            return None
        
        symbol = aligned_item.symbol
        
        # 初始化缓存
        if symbol not in self.binance_cache:
            self.binance_cache[symbol] = {
                "last_ts": aligned_item.binance_last_ts,
                "current_ts": aligned_item.binance_current_ts,
                "last_update_time": time.time()  # ✅ 记录更新时间
            }
        
        cache = self.binance_cache[symbol]
        T1 = cache["last_ts"]
        T2 = cache["current_ts"]
        T3 = aligned_item.binance_current_ts
        
        # 时间滚动逻辑（保持原样）
        if T2 and T3 != T2:
            logger.info(f"币安 {symbol} 结算时间更新: T1={T2} → T2={T3}")
            T1 = T2
            T2 = T3
            cache["last_ts"] = T1
            cache["current_ts"] = T2
            self.stats["binance_rollovers"] += 1
        
        # ✅ 更新缓存时间戳
        cache["last_update_time"] = time.time()
        
        self.stats["binance_updates"] += 1
        
        # 构建数据（保留字符串，保存时间戳用于计算）
        data = PlatformData(
            symbol=symbol,
            exchange="binance",
            contract_name=aligned_item.binance_contract_name or "",
            latest_price=aligned_item.binance_price,
            funding_rate=aligned_item.binance_funding_rate,
            last_settlement_time=aligned_item.binance_last_settlement,  # 字符串！
            current_settlement_time=aligned_item.binance_current_settlement,
            next_settlement_time=aligned_item.binance_next_settlement,
            last_settlement_ts=T1,
            current_settlement_ts=T2
        )
        
        # 计算费率周期
        if data.current_settlement_ts and data.last_settlement_ts:
            data.period_seconds = (data.current_settlement_ts - data.last_settlement_ts) // 1000
        
        # 计算倒计时
        data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        
        return data
    
    def _calc_countdown(self, settlement_ts: Optional[int]) -> Optional[int]:
        """计算倒计时（保持原样）"""
        if not settlement_ts:
            return None
        
        try:
            now_ms = int(time.time() * 1000)
            return max(0, (settlement_ts - now_ms) // 1000)
        except Exception as e:
            logger.warning(f"倒计时计算失败: {settlement_ts} - {e}")
            return None
    
    def _auto_cleanup_if_needed(self):
        """自动清理过期缓存"""
        current_time = time.time()
        time_since_last_cleanup = current_time - self._last_cleanup_time
        
        # 每小时检查一次
        if time_since_last_cleanup >= (self._cleanup_interval_hours * 3600):
            cleaned = self._cleanup_expired_cache()
            if cleaned > 0:
                logger.info(f"Step4缓存清理: 清理了 {cleaned} 个过期条目")
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
                logger.debug(f"清理过期缓存: {symbol}")
            except KeyError:
                pass
        
        return cleaned
    
    def manual_cleanup_cache(self) -> int:
        """手动清理缓存（用于监控或测试）"""
        return self._cleanup_expired_cache()
    
    def get_cache_status(self, symbol: str) -> Dict[str, Any]:
        """查询单个合约的币安缓存状态（添加TTL信息）"""
        cache = self.binance_cache.get(symbol, {})
        
        status = {
            "has_last_ts": cache.get("last_ts") is not None,
            "has_current_ts": cache.get("current_ts") is not None,
            "last_ts": cache.get("last_ts"),
            "current_ts": cache.get("current_ts"),
            "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
            "current_settlement_time": self._ts_to_str(cache.get("current_ts"))
        }
        
        # ✅ 添加TTL信息
        if cache:
            last_update = cache.get("last_update_time", 0)
            age_hours = (time.time() - last_update) / 3600
            status["cache_age_hours"] = age_hours
            status["will_expire_in_hours"] = max(0, self._cache_ttl_hours - age_hours)
            status["is_expired"] = age_hours > self._cache_ttl_hours
        
        return status
    
    def get_cache_report(self) -> Dict[str, Any]:
        """获取币安缓存状态完整报告（添加TTL统计）"""
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
    
    def _ts_to_str(self, ts: Optional[int]) -> Optional[str]:
        """内部辅助方法：时间戳转字符串（仅供报告使用）"""
        if ts is None or ts <= 0:
            return None
        
        try:
            from datetime import datetime, timedelta
            dt_utc = datetime.utcfromtimestamp(ts / 1000)
            dt_bj = dt_utc + timedelta(hours=8)
            return dt_bj.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return None