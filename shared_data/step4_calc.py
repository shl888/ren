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
        self.last_log_time = 0
        self.log_interval = 180  # 3分钟，单位：秒
        self.process_count = 0
        self.log_detail_counter = 0  # 用于记录详细日志的计数器
        self.logged_okx_symbols = set()  # 记录已打印的OKX交易对
        self.logged_binance_symbols = set()  # 记录已打印的币安交易对
    
    def process(self, aligned_results: List) -> List[PlatformData]:
        """
        处理Step3的对齐数据
        """
        # 频率控制：只偶尔显示处理日志
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        if should_log:
            logger.info(f"🔄【流水线步骤4】开始单平台计算Step3输出的 {len(aligned_results)} 个双平台合约的对齐数据...")
        
        # 当前批次统计（按合约计数）
        batch_stats = {
            "total_contracts": len(aligned_results),
            "okx_complete_contracts": 0,     # OKX数据完整的合约数
            "binance_complete_contracts": 0, # 币安数据完整的合约数
            "both_platform_contracts": 0,    # 双平台都完整的合约数
            "calculation_errors": 0,         # 计算失败的合约数
            "binance_rollovers": 0,          # 币安时间滚动次数（修复点1：初始化）
            
            # 计算成功率统计
            "okx_period_success": 0,         # OKX周期计算成功
            "okx_countdown_success": 0,      # OKX倒计时计算成功
            "binance_period_success": 0,     # 币安周期计算成功
            "binance_countdown_success": 0,  # 币安倒计时计算成功
        }
        
        all_results = []
        # 重置详细日志计数器（每个批次重新开始）
        self.log_detail_counter = 0
        self.logged_okx_symbols.clear()
        self.logged_binance_symbols.clear()
        
        for item in aligned_results:
            try:
                okx_data = self._calc_okx(item)
                # 修复点2：传递 batch_stats 参数
                binance_data = self._calc_binance(item, batch_stats)
                
                # 统计每个合约的平台数据完整情况
                has_okx = okx_data is not None
                has_binance = binance_data is not None
                
                # 打印详细计算结果（每个交易所最多2条，且不重复）
                if has_okx and self.log_detail_counter < 2 and item.symbol not in self.logged_okx_symbols:
                    self._log_calc_result(okx_data, "OKX", batch_stats.get("binance_rollovers", 0), item)
                    self.logged_okx_symbols.add(item.symbol)
                    self.log_detail_counter += 1
                
                if has_binance and self.log_detail_counter < 2 and item.symbol not in self.logged_binance_symbols:
                    self._log_calc_result(binance_data, "币安", batch_stats.get("binance_rollovers", 0), item)
                    self.logged_binance_symbols.add(item.symbol)
                    self.log_detail_counter += 1
                
                if has_okx:
                    all_results.append(okx_data)
                    batch_stats["okx_complete_contracts"] += 1
                    # 统计OKX计算详情
                    if okx_data.period_seconds is not None:
                        batch_stats["okx_period_success"] += 1
                    if okx_data.countdown_seconds is not None:
                        batch_stats["okx_countdown_success"] += 1
                
                if has_binance:
                    all_results.append(binance_data)
                    batch_stats["binance_complete_contracts"] += 1
                    # 统计币安计算详情
                    if binance_data.period_seconds is not None:
                        batch_stats["binance_period_success"] += 1
                    if binance_data.countdown_seconds is not None:
                        batch_stats["binance_countdown_success"] += 1
                
                if has_okx and has_binance:
                    batch_stats["both_platform_contracts"] += 1
                
            except Exception as e:
                batch_stats["calculation_errors"] += 1
                # 打印前2条计算失败的信息
                if self.log_detail_counter < 2:
                    logger.error(f"❌【流水线步骤4】计算失败详情 {self.log_detail_counter + 1}:")
                    logger.error(f"   交易对: {item.symbol}")
                    logger.error(f"   错误信息: {e}")
                    self.log_detail_counter += 1
                if should_log:
                    logger.error(f"❌【流水线步骤4】合约计算失败: {item.symbol} - {e}")
                continue
        
        if should_log:
            # 处理完成后，打印统计结果
            self._log_batch_statistics(batch_stats)
            
            # 数据生成统计
            logger.info(f"✅【流水线步骤4】Step4计算完成，共生成 {len(all_results)} 条单平台数据")
            
            # 添加缓存报告
            self._log_cache_report(batch_stats["binance_complete_contracts"])
            
            # 如果总数据量少于预期，补充说明
            if len(aligned_results) > 0:
                okx_actual = len(self.logged_okx_symbols)
                binance_actual = len(self.logged_binance_symbols)
                logger.info(f"📊【流水线步骤4】详细日志统计:")
                logger.info(f"  • OKX显示 {okx_actual} 条详细结果")
                logger.info(f"  • 币安显示 {binance_actual} 条详细结果")
                
                if okx_actual < 2 and batch_stats["okx_complete_contracts"] >= 2:
                    logger.info(f"ℹ️【流水线步骤4】OKX有 {batch_stats['okx_complete_contracts']} 条完整数据，已显示 {okx_actual} 条")
                if binance_actual < 2 and batch_stats["binance_complete_contracts"] >= 2:
                    logger.info(f"ℹ️【流水线步骤4】币安有 {batch_stats['binance_complete_contracts']} 条完整数据，已显示 {binance_actual} 条")
            
            self.last_log_time = current_time
            self.process_count = 0
        
        self.process_count += 1
        
        return all_results
    
    def _log_calc_result(self, data: PlatformData, exchange_name: str, rollover_count: int, source_item: Any):
        """记录计算结果的详细日志"""
        logger.info(f"📝【流水线步骤4】{exchange_name}计算结果 {len(self.logged_okx_symbols) + len(self.logged_binance_symbols)}:")
        logger.info(f"   交易对: {data.symbol}")
        logger.info(f"   合约名称: {data.contract_name}")
        logger.info(f"   基础数据:")
        logger.info(f"     • 最新价格: {data.latest_price}")
        logger.info(f"     • 资金费率: {data.funding_rate}")
        
        # 时间字段显示
        if exchange_name == "OKX":
            logger.info(f"   时间字段:")
            logger.info(f"     • 当前结算时间: {data.current_settlement_time} (时间戳: {data.current_settlement_ts})")
            logger.info(f"     • 下次结算时间: {data.next_settlement_time} (时间戳: {data.next_settlement_ts})")
            logger.info(f"     • 上次结算时间: {data.last_settlement_time} (OKX应为None)")
        else:  # 币安
            logger.info(f"   时间字段:")
            logger.info(f"     • 上次结算时间: {data.last_settlement_time} (时间戳: {data.last_settlement_ts})")
            logger.info(f"     • 当前结算时间: {data.current_settlement_time} (时间戳: {data.current_settlement_ts})")
            logger.info(f"     • 下次结算时间: {data.next_settlement_time} (币安应为None)")
            
            # 显示缓存状态
            if data.symbol in self.binance_cache:
                cache = self.binance_cache[data.symbol]
                logger.info(f"   币安缓存状态:")
                logger.info(f"     • 上次缓存时间戳: {cache.get('last_ts')}")
                logger.info(f"     • 当前缓存时间戳: {cache.get('current_ts')}")
                if cache.get('last_ts'):
                    logger.info(f"     • 上次缓存时间: {self._ts_to_str(cache.get('last_ts'))}")
                if cache.get('current_ts'):
                    logger.info(f"     • 当前缓存时间: {self._ts_to_str(cache.get('current_ts'))}")
        
        # 计算结果
        logger.info(f"   计算结果:")
        if data.period_seconds is not None:
            hours = data.period_seconds // 3600
            minutes = (data.period_seconds % 3600) // 60
            seconds = data.period_seconds % 60
            logger.info(f"     • 费率周期: {data.period_seconds}秒 ({hours}小时{minutes}分钟{seconds}秒)")
        else:
            logger.info(f"     • 费率周期: None (计算失败)")
        
        if data.countdown_seconds is not None:
            hours = data.countdown_seconds // 3600
            minutes = (data.countdown_seconds % 3600) // 60
            seconds = data.countdown_seconds % 60
            logger.info(f"     • 倒计时: {data.countdown_seconds}秒 ({hours}小时{minutes}分钟{seconds}秒)")
        else:
            logger.info(f"     • 倒计时: None (计算失败)")
        
        # 币安特定信息
        if exchange_name == "币安":
            logger.info(f"   币安特定信息:")
            logger.info(f"     • 时间滚动次数: {rollover_count}")
            
            # 显示时间滚动详情
            if source_item.binance_current_ts and source_item.binance_last_ts:
                if source_item.binance_current_ts == source_item.binance_last_ts:
                    logger.info(f"     • 时间滚动状态: 未滚动 (当前时间戳 = 上次时间戳)")
                else:
                    logger.info(f"     • 时间滚动状态: 已滚动 (当前时间戳 ≠ 上次时间戳)")
                    time_diff = (source_item.binance_current_ts - source_item.binance_last_ts) / 1000
                    logger.info(f"     • 滚动时间差: {time_diff}秒")
    
    def _log_batch_statistics(self, batch_stats: Dict[str, int]):
        """打印当前批次的合约统计结果"""
        logger.info("📝【流水线步骤4】当前批次合约统计:")
        
        total_contracts = batch_stats["total_contracts"]
        
        logger.info(f"  • 总合约数: {total_contracts} 个")
        logger.info(f"  • 双平台完整: {batch_stats['both_platform_contracts']} 个")
        logger.info(f"  • 计算失败: {batch_stats['calculation_errors']} 个")
        
        # 完整性统计
        if total_contracts > 0:
            both_rate = (batch_stats['both_platform_contracts'] / total_contracts) * 100
            logger.info(f"✅【流水线步骤4】双平台完整率: {both_rate:.1f}%")
            
            if batch_stats['both_platform_contracts'] == total_contracts:
                logger.info("🎉【流水线步骤4】所有合约双平台数据完整！")
            else:
                incomplete = total_contracts - batch_stats['both_platform_contracts']
                logger.warning(f"⚠️【流水线步骤4】 {incomplete} 个合约数据不完整")
        
        # 费率周期和倒计时统计
        logger.info("⏱️【流水线步骤4】费率周期和倒计时计算统计:")
        
        # OKX统计
        if batch_stats["okx_complete_contracts"] > 0:
            period_rate = (batch_stats["okx_period_success"] / batch_stats["okx_complete_contracts"]) * 100
            countdown_rate = (batch_stats["okx_countdown_success"] / batch_stats["okx_complete_contracts"]) * 100
            logger.info(f"  • OKX周期计算: {batch_stats['okx_period_success']}/{batch_stats['okx_complete_contracts']} ({period_rate:.1f}%)")
            logger.info(f"  • OKX倒计时: {batch_stats['okx_countdown_success']}/{batch_stats['okx_complete_contracts']} ({countdown_rate:.1f}%)")
        
        # 币安统计
        if batch_stats["binance_complete_contracts"] > 0:
            period_rate = (batch_stats["binance_period_success"] / batch_stats["binance_complete_contracts"]) * 100
            countdown_rate = (batch_stats["binance_countdown_success"] / batch_stats["binance_complete_contracts"]) * 100
            logger.info(f"  • 币安周期计算: {batch_stats['binance_period_success']}/{batch_stats['binance_complete_contracts']} ({period_rate:.1f}%)")
            logger.info(f"  • 币安倒计时: {batch_stats['binance_countdown_success']}/{batch_stats['binance_complete_contracts']} ({countdown_rate:.1f}%)")
        
        # 币安时间滚动统计 - 现在会正常显示了
        if batch_stats["binance_rollovers"] > 0:
            logger.info(f"🔄【流水线步骤4】币安时间滚动: {batch_stats['binance_rollovers']} 次")
        else:
            logger.info(f"🔵【流水线步骤4】币安时间滚动: 0 次（或未发生）")
    
    def _log_cache_report(self, binance_contracts: int):
        """打印币安缓存详细报告"""
        cache_size = len(self.binance_cache)
        
        logger.info("🗃️【流水线步骤4】币安缓存详细报告:")
        logger.info(f"  • 缓存合约数: {cache_size} 个")
        logger.info(f"  • 当前批次币安合约: {binance_contracts} 个")
        
        if binance_contracts > 0:
            cache_coverage = (cache_size / binance_contracts) * 100
            logger.info(f"  • 缓存覆盖率: {cache_coverage:.1f}%")
        
        # 缓存深度分析
        if cache_size > 0:
            with_history = 0
            without_history = 0
            
            for symbol, cache in self.binance_cache.items():
                if cache.get("last_ts"):
                    with_history += 1
                else:
                    without_history += 1
            
            logger.info(f"  • 有历史数据: {with_history} 个合约")
            logger.info(f"  • 无历史数据: {without_history} 个合约")
            
            if without_history > 0:
                logger.info(f"⚠️【流水线步骤4】 {without_history} 个合约缺少历史结算时间，等待首次滚动")
    
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
            # 修复点3：增加滚动计数
            batch_stats["binance_rollovers"] += 1  # 这一行是关键修复！
            T1 = T2
            T2 = T3
            cache["last_ts"] = T1
            cache["current_ts"] = T2
        
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