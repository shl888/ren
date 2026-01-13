"""
第四步：单平台计算（修复版）
功能：1. 币安时间滚动 2. 费率周期 3. 倒计时
修正：确保正确接收步骤3传递的历史时间戳
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
    
    # 时间戳备份（用于倒计时和周期计算）
    last_settlement_ts: Optional[int] = None
    current_settlement_ts: Optional[int] = None
    next_settlement_ts: Optional[int] = None
    
    # 计算结果
    period_seconds: Optional[int] = None
    countdown_seconds: Optional[int] = None

class Step4Calc:
    """第四步：单平台计算（修复T1接收问题）"""
    
    def __init__(self):
        self.binance_cache = {}  # 币安时间滚动缓存
        self.last_log_time = 0
        self.log_interval = 60  # 1分钟，单位：秒
        self.process_count = 0
        self.log_detail_counter = 0  # 用于记录详细日志的计数器
    
    def process(self, aligned_results: List) -> List[PlatformData]:
        """
        处理Step3的对齐数据
        """
        # 频率控制：只偶尔显示处理日志
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        if should_log:
            logger.info(f"🔄【流水线步骤4】开始单平台计算 {len(aligned_results)} 个合约...")
        
        # 当前批次统计
        batch_stats = {
            "total_contracts": len(aligned_results),
            "okx_complete_contracts": 0,
            "binance_complete_contracts": 0,
            "both_platform_contracts": 0,
            "calculation_errors": 0,
            "binance_rollovers": 0,
            
            # 计算成功率统计
            "okx_period_success": 0,
            "okx_countdown_success": 0,
            "binance_period_success": 0,
            "binance_countdown_success": 0,
            
            # 新增：币安历史数据统计（根据缓存判断）
            "binance_with_history": 0,      # 有历史时间戳的合约数
            "binance_without_history": 0,   # 无历史时间戳的合约数
        }
        
        all_results = []
        self.log_detail_counter = 0  # 重置详细日志计数器
        
        for item in aligned_results:
            try:
                # 处理OKX数据
                okx_data = self._calc_okx(item)
                
                # 处理币安数据（修复版）
                binance_data = self._calc_binance(item, batch_stats)
                
                # 统计每个合约的平台数据完整情况
                has_okx = okx_data is not None
                has_binance = binance_data is not None
                
                # 更新统计
                if has_okx:
                    all_results.append(okx_data)
                    batch_stats["okx_complete_contracts"] += 1
                    if okx_data.period_seconds is not None:
                        batch_stats["okx_period_success"] += 1
                    if okx_data.countdown_seconds is not None:
                        batch_stats["okx_countdown_success"] += 1
                
                if has_binance:
                    all_results.append(binance_data)
                    batch_stats["binance_complete_contracts"] += 1
                    
                    # 修正：根据缓存数据判断历史数据情况
                    symbol = item.symbol
                    if symbol in self.binance_cache and self.binance_cache[symbol].get("last_ts"):
                        batch_stats["binance_with_history"] += 1
                        if binance_data.period_seconds is not None:
                            batch_stats["binance_period_success"] += 1
                    else:
                        batch_stats["binance_without_history"] += 1
                    
                    if binance_data.countdown_seconds is not None:
                        batch_stats["binance_countdown_success"] += 1
                
                if has_okx and has_binance:
                    batch_stats["both_platform_contracts"] += 1
                
            except Exception as e:
                batch_stats["calculation_errors"] += 1
                if should_log:
                    logger.error(f"❌【流水线步骤4】合约计算失败: {item.symbol} - {e}")
                continue
        
        if should_log:
            # 处理完成后，打印统计结果
            self._log_batch_statistics(batch_stats)
            
            # 数据生成统计
            logger.info(f"✅【流水线步骤4】Step4计算完成，共生成 {len(all_results)} 条单平台数据")
            
            # 币安缓存报告
            self._log_cache_report(batch_stats["binance_complete_contracts"])
            
            self.last_log_time = current_time
            self.process_count = 0
        
        self.process_count += 1
        
        return all_results
    
    def _log_calc_result(self, data: PlatformData, exchange_name: str, rollover_count: int, source_item: Any):
        """记录计算结果的详细日志 - 暂时关闭"""
        pass
    
    def _log_batch_statistics(self, batch_stats: Dict[str, int]):
        """打印当前批次的合约统计结果"""
        logger.info("📝【流水线步骤4】当前批次合约统计:")
        
        total_contracts = batch_stats["total_contracts"]
        
        logger.info(f"  • 总合约数: {total_contracts} 个")
        logger.info(f"  • 双平台完整: {batch_stats['both_platform_contracts']} 个")
        logger.info(f"  • 计算失败: {batch_stats['calculation_errors']} 个")
        
        # 计算双平台完整率
        if total_contracts > 0:
            complete_rate = (batch_stats['both_platform_contracts'] / total_contracts) * 100
            logger.info(f"✅【流水线步骤4】双平台完整率: {complete_rate:.1f}%")
            
            if batch_stats['both_platform_contracts'] == total_contracts:
                logger.info(f"🎉【流水线步骤4】所有合约双平台数据完整！")
        
        # 费率周期和倒计时统计
        logger.info(f"⏱️【流水线步骤4】费率周期和倒计时计算统计:")
        
        okx_total = batch_stats["okx_complete_contracts"]
        if okx_total > 0:
            period_rate = (batch_stats["okx_period_success"] / okx_total) * 100
            countdown_rate = (batch_stats["okx_countdown_success"] / okx_total) * 100
            logger.info(f"  • OKX周期计算: {batch_stats['okx_period_success']}/{okx_total} ({period_rate:.1f}%)")
            logger.info(f"  • OKX倒计时: {batch_stats['okx_countdown_success']}/{okx_total} ({countdown_rate:.1f}%)")
        
        binance_total = batch_stats["binance_complete_contracts"]
        if binance_total > 0:
            period_success = batch_stats["binance_period_success"]
            period_rate = (period_success / binance_total) * 100 if binance_total > 0 else 0
            countdown_rate = (batch_stats["binance_countdown_success"] / binance_total) * 100
            logger.info(f"  • 币安周期计算: {period_success}/{binance_total} ({period_rate:.1f}%)")
            logger.info(f"  • 币安倒计时: {batch_stats['binance_countdown_success']}/{binance_total} ({countdown_rate:.1f}%)")
    
    def _log_cache_report(self, binance_contracts: int):
        """打印币安缓存详细报告"""
        cache_size = len(self.binance_cache)
        
        if cache_size > 0:
            logger.info("🗃️【流水线步骤4】币安缓存详细报告:")
            logger.info(f"  • 缓存合约数: {cache_size} 个")
            logger.info(f"  • 当前批次币安合约: {binance_contracts} 个")
            
            # 计算缓存覆盖率
            if binance_contracts > 0:
                cache_coverage = (cache_size / binance_contracts) * 100
                logger.info(f"  • 缓存覆盖率: {cache_coverage:.1f}%")
            
            # 统计历史数据情况
            with_history = 0
            without_history = 0
            
            for symbol, cache in self.binance_cache.items():
                if cache.get("last_ts"):
                    with_history += 1
                else:
                    without_history += 1
            
            logger.info(f"  • 有历史数据: {with_history} 个合约")
            logger.info(f"  • 无历史数据: {without_history} 个合约")
    
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
    
    def _calc_binance(self, aligned_item, batch_stats: Dict[str, int]) -> Optional[PlatformData]:
        """计算币安数据（所有数据从缓存读取）"""
        
        if not aligned_item.binance_current_ts:
            return None
        
        symbol = aligned_item.symbol
        
        # 1. 先把步骤3的数据存入缓存
        current_ts = aligned_item.binance_current_ts
        
        if symbol not in self.binance_cache:
            # 第一次：初始化缓存，没有历史数据
            self.binance_cache[symbol] = {
                "last_ts": None,
                "current_ts": current_ts
            }
        else:
            # 检查是否需要时间滚动
            cache = self.binance_cache[symbol]
            cached_current_ts = cache["current_ts"]
            
            if cached_current_ts and current_ts != cached_current_ts:
                # 时间滚动：T2_current → T1_last, T3_new → T2_current
                cache["last_ts"] = cached_current_ts
                cache["current_ts"] = current_ts
                batch_stats["binance_rollovers"] += 1
            elif cached_current_ts != current_ts:
                # 更新时间戳（如果不同）
                cache["current_ts"] = current_ts
        
        # 2. 从缓存读取数据（所有数据都从缓存读取）
        cache = self.binance_cache[symbol]
        last_ts = cache.get("last_ts")
        current_ts = cache.get("current_ts")
        
        # 3. 构建数据对象
        data = PlatformData(
            symbol=symbol,
            exchange="binance",
            contract_name=aligned_item.binance_contract_name or "",
            latest_price=aligned_item.binance_price,
            funding_rate=aligned_item.binance_funding_rate,
            last_settlement_time=aligned_item.binance_last_settlement,  # 时间字符串仍用步骤3的
            current_settlement_time=aligned_item.binance_current_settlement,
            next_settlement_time=aligned_item.binance_next_settlement,
            
            # 关键：时间戳从缓存读取
            last_settlement_ts=last_ts,
            current_settlement_ts=current_ts,
        )
        
        # 4. 计算费率周期（从缓存读取的历史数据）
        if data.current_settlement_ts and data.last_settlement_ts:
            data.period_seconds = (data.current_settlement_ts - data.last_settlement_ts) // 1000
        
        # 5. 计算倒计时
        if data.current_settlement_ts:
            data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
        
        return data
    
    def _calc_countdown(self, settlement_ts: Optional[int]) -> Optional[int]:
        """计算倒计时"""
        if not settlement_ts:
            return None
        
        try:
            now_ms = int(time.time() * 1000)
            countdown = max(0, (settlement_ts - now_ms) // 1000)
            return countdown
        except Exception:
            return None
    
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
            
            report["symbol_details"][symbol] = {
                "last_ts": cache.get("last_ts"),
                "current_ts": cache.get("current_ts"),
                "last_settlement_time": self._ts_to_str(cache.get("last_ts")),
                "current_settlement_time": self._ts_to_str(cache.get("current_ts")),
                "status": "complete" if cache.get("last_ts") else "waiting_history"
            }
        
        return report