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
        self.log_interval = 180  # 3分钟，单位：秒
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
            logger.info(f"🔄【内部步骤4】开始单平台计算Step3输出的 {len(aligned_results)} 个双平台合约的对齐数据...")
        
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
            
            # 新增：币安历史数据统计
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
                
                # 打印详细计算结果（每个合约只打印一次）
                if has_okx and self.log_detail_counter < 2:
                    self._log_calc_result(okx_data, "OKX", batch_stats.get("binance_rollovers", 0), item)
                    self.log_detail_counter += 1
                
                if has_binance and self.log_detail_counter < 2:
                    self._log_calc_result(binance_data, "币安", batch_stats.get("binance_rollovers", 0), item)
                    self.log_detail_counter += 1
                
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
                    
                    # 统计币安历史数据情况
                    if binance_data.last_settlement_ts:
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
                    logger.error(f"❌【内部步骤4】合约计算失败: {item.symbol} - {e}")
                continue
        
        if should_log:
            # 处理完成后，打印统计结果
            self._log_batch_statistics(batch_stats)
            
            # 数据生成统计
            logger.info(f"✅【内部步骤4】Step4计算完成，共生成 {len(all_results)} 条单平台数据")
            
            # 币安缓存报告
            self._log_cache_report(batch_stats["binance_complete_contracts"])
            
            self.last_log_time = current_time
            self.process_count = 0
        
        self.process_count += 1
        
        return all_results
    
    def _log_calc_result(self, data: PlatformData, exchange_name: str, rollover_count: int, source_item: Any):
        """记录计算结果的详细日志"""
        counter = self.log_detail_counter
        logger.info(f"📝【内部步骤4】{exchange_name}计算结果 {counter}:")
        logger.info(f"   交易对: {data.symbol}")
        logger.info(f"   合约名称: {data.contract_name}")
        logger.info(f"   基础数据:")
        logger.info(f"     • 最新价格: {data.latest_price}")
        logger.info(f"     • 资金费率: {data.funding_rate}")
        
        # 时间字段显示 - 关键修复：明确显示时间戳
        logger.info(f"   时间字段:")
        
        if exchange_name == "OKX":
            logger.info(f"     • 当前结算时间: {data.current_settlement_time} (时间戳: {data.current_settlement_ts})")
            logger.info(f"     • 下次结算时间: {data.next_settlement_time} (时间戳: {data.next_settlement_ts})")
            logger.info(f"     • 上次结算时间: {data.last_settlement_time} (OKX应为None)")
        else:  # 币安
            # 显示时间戳详情（修复关键问题）
            logger.info(f"     • 上次结算时间: {data.last_settlement_time}")
            logger.info(f"       - 时间戳: {data.last_settlement_ts if data.last_settlement_ts else '无 (等待滚动)'}")
            
            logger.info(f"     • 当前结算时间: {data.current_settlement_time}")
            logger.info(f"       - 时间戳: {data.current_settlement_ts}")
            
            logger.info(f"     • 下次结算时间: {data.next_settlement_time} (币安应为None)")
        
        # 计算结果
        logger.info(f"   计算结果:")
        if data.period_seconds is not None:
            hours = data.period_seconds // 3600
            minutes = (data.period_seconds % 3600) // 60
            seconds = data.period_seconds % 60
            logger.info(f"     • 费率周期: {data.period_seconds}秒 ({hours}小时{minutes}分钟{seconds}秒)")
        else:
            reason = "无历史时间戳" if exchange_name == "币安" and not data.last_settlement_ts else "计算失败"
            logger.info(f"     • 费率周期: {reason}")
        
        if data.countdown_seconds is not None:
            hours = data.countdown_seconds // 3600
            minutes = (data.countdown_seconds % 3600) // 60
            seconds = data.countdown_seconds % 60
            logger.info(f"     • 倒计时: {data.countdown_seconds}秒 ({hours}小时{minutes}分钟{seconds}秒)")
        
        # 币安特定信息
        if exchange_name == "币安":
            logger.info(f"   币安状态:")
            logger.info(f"     • 时间滚动次数: {rollover_count}")
            
            # 显示缓存状态
            if data.symbol in self.binance_cache:
                cache = self.binance_cache[data.symbol]
                if cache.get("last_ts"):
                    logger.info(f"     • 缓存上次时间: {self._ts_to_str(cache['last_ts'])}")
                else:
                    logger.info(f"     • 缓存上次时间: 无")
                logger.info(f"     • 缓存当前时间: {self._ts_to_str(cache.get('current_ts'))}")
    
    def _log_batch_statistics(self, batch_stats: Dict[str, int]):
        """打印当前批次的合约统计结果"""
        logger.info("📊【内部步骤4】当前批次统计:")
        
        total_contracts = batch_stats["total_contracts"]
        
        logger.info(f"  • 总合约数: {total_contracts} 个")
        logger.info(f"  • 双平台完整: {batch_stats['both_platform_contracts']} 个")
        logger.info(f"  • 计算失败: {batch_stats['calculation_errors']} 个")
        
        # 币安历史数据统计
        binance_total = batch_stats["binance_complete_contracts"]
        if binance_total > 0:
            with_history = batch_stats["binance_with_history"]
            without_history = batch_stats["binance_without_history"]
            
            logger.info(f"  • 币安历史数据:")
            logger.info(f"     • 有历史时间戳: {with_history} 个 ({with_history/binance_total*100:.1f}%)")
            logger.info(f"     • 无历史时间戳: {without_history} 个 ({without_history/binance_total*100:.1f}%)")
            
            # 费率周期计算统计
            if with_history > 0:
                period_success = batch_stats["binance_period_success"]
                logger.info(f"     • 周期计算成功: {period_success}/{with_history} 个")
        
        # 时间滚动统计
        if batch_stats["binance_rollovers"] > 0:
            logger.info(f"🔄【内部步骤4】币安时间滚动: {batch_stats['binance_rollovers']} 次")
    
    def _log_cache_report(self, binance_contracts: int):
        """打印币安缓存详细报告"""
        cache_size = len(self.binance_cache)
        
        if cache_size > 0:
            logger.info("🗃️【内部步骤4】币安缓存状态:")
            logger.info(f"  • 缓存合约数: {cache_size} 个")
            
            # 分析缓存内容
            with_history = 0
            waiting_history = 0
            
            for symbol, cache in list(self.binance_cache.items())[:5]:  # 只显示前5个
                if cache.get("last_ts"):
                    with_history += 1
                    logger.info(f"     • {symbol}: 有历史时间戳")
                else:
                    waiting_history += 1
                    logger.info(f"     • {symbol}: 等待历史数据")
            
            if len(self.binance_cache) > 5:
                logger.info(f"     • ... 还有 {len(self.binance_cache)-5} 个合约")
    
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
        """计算币安数据（修复版：确保正确接收T1数据）"""
        
        # 必须要有当前时间戳
        if not aligned_item.binance_current_ts:
            return None
        
        symbol = aligned_item.symbol
        
        # 关键修复：优先使用步骤3传来的原始数据
        last_ts_from_aligned = aligned_item.binance_last_ts  # T1数据源
        current_ts_from_aligned = aligned_item.binance_current_ts  # T3_new
        
        # 初始化或获取缓存
        if symbol not in self.binance_cache:
            # 第一次处理该合约：初始化缓存
            self.binance_cache[symbol] = {
                "last_ts": last_ts_from_aligned,  # 直接使用步骤3的T1数据
                "current_ts": current_ts_from_aligned
            }
            logger.debug(f"首次初始化币安缓存 {symbol}: last_ts={last_ts_from_aligned}, current_ts={current_ts_from_aligned}")
        else:
            # 已有缓存：执行时间滚动逻辑
            cache = self.binance_cache[symbol]
            cached_last_ts = cache["last_ts"]  # T1_last
            cached_current_ts = cache["current_ts"]  # T2_current
            
            # 检查是否需要时间滚动
            if cached_current_ts and current_ts_from_aligned != cached_current_ts:
                # 时间滚动：T2_current → T1_last, T3_new → T2_current
                cache["last_ts"] = cached_current_ts  # 旧的本次变成新的上次
                cache["current_ts"] = current_ts_from_aligned  # 新的本次覆盖旧的
                
                batch_stats["binance_rollovers"] += 1
                logger.debug(f"币安时间滚动 {symbol}: {cached_current_ts}→last_ts, {current_ts_from_aligned}→current_ts")
        
        # 获取当前缓存状态（滚动后）
        cache = self.binance_cache[symbol]
        current_cache_last_ts = cache["last_ts"]
        current_cache_current_ts = cache["current_ts"]
        
        # 构建数据对象
        data = PlatformData(
            symbol=symbol,
            exchange="binance",
            contract_name=aligned_item.binance_contract_name or "",
            latest_price=aligned_item.binance_price,
            funding_rate=aligned_item.binance_funding_rate,
            last_settlement_time=aligned_item.binance_last_settlement,  # 字符串格式
            current_settlement_time=aligned_item.binance_current_settlement,
            next_settlement_time=aligned_item.binance_next_settlement,
            
            # 关键修复：时间戳来自缓存（滚动后的状态）
            last_settlement_ts=current_cache_last_ts,    # 可能是None，也可能是滚动后的值
            current_settlement_ts=current_cache_current_ts,
        )
        
        # 计算费率周期（有上次时间戳才计算）
        if data.current_settlement_ts and data.last_settlement_ts:
            data.period_seconds = (data.current_settlement_ts - data.last_settlement_ts) // 1000
        # else: 无历史数据，period_seconds保持None
        
        # 计算倒计时
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