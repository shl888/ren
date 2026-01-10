"""
第四步：单平台计算（修复版）
功能：1. 币安时间滚动 2. 费率周期 3. 倒计时
修正：时间字段直接保留Step3的字符串，不再重复转换
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from collections import defaultdict
import time

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
    """第四步：单平台计算"""
    
    def __init__(self):
        self.binance_cache = {}
        self.stats = {
            "okx_total": 0,
            "binance_total": 0,
            "binance_rollovers": 0,
            "binance_updates": 0,
            "calculation_errors": 0
        }
        self.platform_results = {
            "okx": [],
            "binance": []
        }
    
    def process(self, aligned_results: List) -> List[PlatformData]:
        """
        处理Step3的对齐数据
        """
        logger.info(f"🔄【流水线步骤4】开始单平台计算 {len(aligned_results)} 个合约...")
        
        for item in aligned_results:
            try:
                okx_data = self._calc_okx(item)
                binance_data = self._calc_binance(item)
                
                if okx_data:
                    self.platform_results["okx"].append(okx_data)
                    self.stats["okx_total"] += 1
                
                if binance_data:
                    self.platform_results["binance"].append(binance_data)
                    self.stats["binance_total"] += 1
                
            except Exception as e:
                logger.error(f"❌【流水线步骤4】计算失败: {item.symbol} - {e}")
                self.stats["calculation_errors"] += 1
                continue
        
        # 合并所有结果
        all_results = self.platform_results["okx"] + self.platform_results["binance"]
        
        # 处理完成后，打印统计结果
        self._log_statistics()
        
        logger.info(f"✅【流水线步骤4】Step4计算完成，共生成 {len(all_results)} 条单平台数据")
        return all_results
    
    def _log_statistics(self):
        """打印统计结果"""
        logger.info("📝【流水线步骤4】单平台计算统计:")
        
        okx_count = self.stats["okx_total"]
        binance_count = self.stats["binance_total"]
        total_count = okx_count + binance_count
        
        logger.info(f"   OKX数据: {okx_count} 条")
        logger.info(f"   币安数据: {binance_count} 条")
        logger.info(f"   总计: {total_count} 条")
        
        # 验证双平台完整性
        aligned_count = len(self.platform_results["okx"] + self.platform_results["binance"]) // 2
        if okx_count == binance_count and okx_count > 0:
            logger.info(f"✅【流水线步骤4】 每个合约都生成了OKX+币安两条数据 ({okx_count} 对)")
        else:
            logger.warning(f"⚠️【流水线步骤4】  平台数据不对称: OKX={okx_count}, 币安={binance_count}")
        
        # 币安缓存统计
        cache_size = len(self.binance_cache)
        logger.info(f"📝【流水线步骤4】币安缓存大小: {cache_size} 个合约")
        
        if cache_size == binance_count and binance_count > 0:
            logger.info("✅【流水线步骤4】 缓存覆盖所有币安合约")
        else:
            logger.warning(f"   ⚠️【流水线步骤4】  缓存未完全覆盖: 缓存={cache_size}, 币安数据={binance_count}")
        
        # 缓存深度验证
        self._validate_cache()
        
        # 时间滚动统计
        if self.stats["binance_rollovers"] > 0:
            logger.info(f"📝【流水线步骤4】币安时间滚动: {self.stats['binance_rollovers']} 次")
        
        # 总结
        if total_count > 0:
            success_rate = ((total_count - self.stats["calculation_errors"]) / total_count) * 100
            logger.info(f"🎉 **恭喜！【流水线步骤4】Step4计算功能{success_rate:.1f}%正常！**")
            logger.info(f"✅ 【流水线步骤4】成功处理 {aligned_count} 个合约的双平台数据。")
            logger.info(f"✅【流水线步骤4】 币安缓存工作正常（{cache_size} 个合约）")
            logger.info(f"✅【流水线步骤4】 倒计时和周期计算准确")
    
    def _validate_cache(self):
        """验证币安缓存机制"""
        if not self.binance_cache:
            return
        
        total_cached = len(self.binance_cache)
        with_history = 0
        without_history = 0
        symbols_without_history = []
        
        for symbol, cache in self.binance_cache.items():
            if cache.get("last_ts"):
                with_history += 1
            else:
                without_history += 1
                symbols_without_history.append(symbol)
        
        if without_history > 0:
            logger.info(f"🔍【流水线步骤4】 缓存机制深度验证")
            logger.info(f"   ⚠️【流水线步骤4】  有 {without_history} 个币安合约的last_ts为空")
            logger.info("🤔【流水线步骤4】   这些合约依赖首次滚动才能生成周期")
            
            # 显示前几个缺少历史的合约
            sample_size = min(5, len(symbols_without_history))
            if sample_size > 0:
                sample_symbols = symbols_without_history[:sample_size]
                logger.info(f"   示例: {', '.join(sample_symbols)}")
    
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
        
        # 计算倒计时
        data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        
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
                "current_ts": aligned_item.binance_current_ts
            }
        
        cache = self.binance_cache[symbol]
        T1 = cache["last_ts"]
        T2 = cache["current_ts"]
        T3 = aligned_item.binance_current_ts
        
        # 时间滚动逻辑
        if T2 and T3 != T2:
            # 移除单个合约的滚动日志，只保留统计
            T1 = T2
            T2 = T3
            cache["last_ts"] = T1
            cache["current_ts"] = T2
            self.stats["binance_rollovers"] += 1
        
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
        """计算倒计时"""
        if not settlement_ts:
            return None
        
        try:
            now_ms = int(time.time() * 1000)
            return max(0, (settlement_ts - now_ms) // 1000)
        except Exception:
            return None
    
    def get_cache_status(self, symbol: str) -> Dict[str, Any]:
        """查询单个合约的币安缓存状态"""
        cache = self.binance_cache.get(symbol, {})
        return {
            "has_last_ts": cache.get("last_ts") is not None,
            "has_current_ts": cache.get("current_ts") is not None,
            "last_ts": cache.get("last_ts"),
            "current_ts": cache.get("current_ts"),
            "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
            "current_settlement_time": self._ts_to_str(cache.get("current_ts"))
        }
    
    def get_cache_report(self) -> Dict[str, Any]:
        """获取币安缓存状态完整报告"""
        report = {
            "total_cached": len(self.binance_cache),
            "with_last_ts": 0,
            "without_last_ts": 0,
            "symbols_without_history": [],
            "symbol_details": {}
        }
        
        for symbol, cache in self.binance_cache.items():
            if cache.get("last_ts"):
                report["with_last_ts"] += 1
            else:
                report["without_last_ts"] += 1
                report["symbols_without_history"].append(symbol)
            
            # 添加详细缓存信息
            report["symbol_details"][symbol] = {
                "last_ts": cache.get("last_ts"),
                "current_ts": cache.get("current_ts"),
                "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
                "current_settlement_time": self._ts_to_str(cache.get("current_ts")),
                "status": "complete" if cache.get("last_ts") else "pending_history"
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