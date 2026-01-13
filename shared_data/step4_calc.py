"""
第四步：单平台计算（统一缓存版 - 直接覆盖方案）
功能：统一缓存所有平台数据，所有计算基于缓存数据
原则：1. 先缓存后计算 2. 缓存为唯一数据源 3. 统一处理逻辑
特点：所有数据直接覆盖，币安附带滚动更新
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
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
    
    # 时间字段
    last_settlement_time: Optional[str] = None
    current_settlement_time: Optional[str] = None
    next_settlement_time: Optional[str] = None
    
    # 时间戳
    last_settlement_ts: Optional[int] = None
    current_settlement_ts: Optional[int] = None
    next_settlement_ts: Optional[int] = None
    
    # 计算结果
    period_seconds: Optional[int] = None
    countdown_seconds: Optional[int] = None

class Step4Calc:
    """第四步：单平台计算（统一缓存方案）"""
    
    def __init__(self):
        # 统一缓存结构：symbol -> exchange -> 数据
        self.platform_cache = {}
        self.last_log_time = 0
        self.log_interval = 60  # 1分钟
        self.process_count = 0
        self.log_detail_counter = 0
        
    def process(self, aligned_results: List) -> List[PlatformData]:
        """
        统一处理流程：1.更新缓存 2.从缓存计算
        """
        current_time = time.time()
        should_log = (current_time - self.last_log_time) >= self.log_interval or self.process_count == 0
        
        if should_log:
            logger.info(f"🔄【内部步骤4】开始处理 {len(aligned_results)} 个合约，采用统一缓存方案...")
        
        # 批次统计
        batch_stats = {
            "total_contracts": len(aligned_results),
            "okx_updated": 0,
            "binance_updated": 0,
            "okx_calculated": 0,
            "binance_calculated": 0,
            "calculation_errors": 0,
            "binance_rollovers": 0,
            "binance_with_history": 0,  # 有历史时间戳的币安合约
        }
        
        all_results = []
        self.log_detail_counter = 0
        
        for item in aligned_results:
            try:
                symbol = item.symbol
                
                # 🔄 第一步：统一更新缓存（直接覆盖）
                self._update_cache(item, batch_stats)
                
                # 🔢 第二步：从缓存统一计算
                # OKX计算
                okx_data = self._calc_from_cache(symbol, "okx")
                if okx_data:
                    all_results.append(okx_data)
                    batch_stats["okx_calculated"] += 1
                    
                    # 详细日志（前2个合约）
                    if self.log_detail_counter < 1:
                        self._log_calc_result(okx_data, "OKX", batch_stats)
                        self.log_detail_counter += 1
                
                # 币安计算
                binance_data = self._calc_from_cache(symbol, "binance")
                if binance_data:
                    all_results.append(binance_data)
                    batch_stats["binance_calculated"] += 1
                    
                    # 统计有历史数据的币安合约
                    if binance_data.last_settlement_ts:
                        batch_stats["binance_with_history"] += 1
                    
                    # 详细日志（前2个合约）
                    if self.log_detail_counter < 2:
                        self._log_calc_result(binance_data, "币安", batch_stats)
                        self.log_detail_counter += 1
                
            except Exception as e:
                batch_stats["calculation_errors"] += 1
                if should_log:
                    logger.error(f"❌【内部步骤4】合约处理失败: {item.symbol} - {e}")
                continue
        
        if should_log:
            self._log_batch_statistics(batch_stats)
            logger.info(f"✅【内部步骤4】完成，共生成 {len(all_results)} 条数据")
            self._log_cache_status()
            self.last_log_time = current_time
            self.process_count = 0
        
        self.process_count += 1
        
        return all_results
    
    def _update_cache(self, aligned_item, batch_stats: Dict[str, int]):
        """统一更新所有平台缓存（直接覆盖）"""
        symbol = aligned_item.symbol
        
        # 初始化缓存结构
        if symbol not in self.platform_cache:
            self.platform_cache[symbol] = {}
        
        # 🔍 调试：显示步骤3传入的原始数据
        logger.debug(f"🔍【步骤4-调试】步骤3传入数据 {symbol}:")
        logger.debug(f"  币安上次时间戳: {aligned_item.binance_last_ts}")
        logger.debug(f"  币安当前时间戳: {aligned_item.binance_current_ts}")
        logger.debug(f"  OKX当前时间戳: {aligned_item.okx_current_ts}")
        logger.debug(f"  OKX下次时间戳: {aligned_item.okx_next_ts}")
        
        # 📥 更新OKX缓存（直接覆盖）
        if aligned_item.okx_current_ts:
            self.platform_cache[symbol]["okx"] = {
                "contract_name": aligned_item.okx_contract_name or "",
                "latest_price": aligned_item.okx_price,
                "funding_rate": aligned_item.okx_funding_rate,
                "current_settlement_time": aligned_item.okx_current_settlement,
                "next_settlement_time": aligned_item.okx_next_settlement,
                "current_settlement_ts": aligned_item.okx_current_ts,
                "next_settlement_ts": aligned_item.okx_next_ts,
            }
            batch_stats["okx_updated"] += 1
            logger.debug(f"✅ OKX缓存已更新: {symbol}")
        
        # 🔄 更新币安缓存（直接覆盖+滚动更新）
        if aligned_item.binance_current_ts:
            self._update_binance_cache_direct(symbol, aligned_item, batch_stats)
            batch_stats["binance_updated"] += 1
    
    def _update_binance_cache_direct(self, symbol: str, aligned_item, batch_stats: Dict[str, int]):
        """直接覆盖币安缓存，自动执行滚动更新"""
        # 获取当前缓存（如果存在）
        current_cache = self.platform_cache.get(symbol, {}).get("binance", {})
        
        # 新数据
        new_current_ts = aligned_item.binance_current_ts
        new_last_ts = aligned_item.binance_last_ts
        
        # 调试：显示滚动前状态
        logger.debug(f"🔄 币安缓存更新前 {symbol}:")
        logger.debug(f"  缓存上次时间戳: {current_cache.get('last_settlement_ts')}")
        logger.debug(f"  缓存当前时间戳: {current_cache.get('current_settlement_ts')}")
        logger.debug(f"  步骤3传入上次时间戳: {new_last_ts}")
        logger.debug(f"  步骤3传入当前时间戳: {new_current_ts}")
        
        # 检查是否需要滚动更新
        should_rollover = False
        last_ts_for_cache = new_last_ts  # 默认使用步骤3的last_ts
        
        # 如果有历史缓存，且当前时间戳发生变化，则执行滚动
        if current_cache.get("current_settlement_ts") and new_current_ts != current_cache["current_settlement_ts"]:
            should_rollover = True
            # 滚动：旧的当前 → 新的上次
            last_ts_for_cache = current_cache["current_settlement_ts"]
            batch_stats["binance_rollovers"] += 1
            logger.debug(f"🔄 币安时间滚动触发 {symbol}: {last_ts_for_cache}→last, {new_current_ts}→current")
        
        # 🔥 直接覆盖缓存（核心逻辑）
        self.platform_cache[symbol]["binance"] = {
            "contract_name": aligned_item.binance_contract_name or "",
            "latest_price": aligned_item.binance_price,
            "funding_rate": aligned_item.binance_funding_rate,
            "last_settlement_time": aligned_item.binance_last_settlement,
            "current_settlement_time": aligned_item.binance_current_settlement,
            "next_settlement_time": aligned_item.binance_next_settlement,
            "last_settlement_ts": last_ts_for_cache,  # 滚动后或用步骤3的
            "current_settlement_ts": new_current_ts,
            "has_rollover": should_rollover,  # 标记是否执行了滚动
        }
        
        # 调试：显示滚动后状态
        logger.debug(f"✅ 币安缓存更新后 {symbol}:")
        logger.debug(f"  最终上次时间戳: {last_ts_for_cache}")
        logger.debug(f"  最终当前时间戳: {new_current_ts}")
        logger.debug(f"  是否滚动: {should_rollover}")
    
    def _calc_from_cache(self, symbol: str, exchange: str) -> Optional[PlatformData]:
        """从缓存计算数据（唯一数据源）"""
        if symbol not in self.platform_cache:
            return None
        
        cache_data = self.platform_cache[symbol].get(exchange)
        if not cache_data:
            return None
        
        # 📊 从缓存构建数据对象
        if exchange == "okx":
            data = PlatformData(
                symbol=symbol,
                exchange="okx",
                contract_name=cache_data["contract_name"],
                latest_price=cache_data["latest_price"],
                funding_rate=cache_data["funding_rate"],
                current_settlement_time=cache_data["current_settlement_time"],
                next_settlement_time=cache_data["next_settlement_time"],
                current_settlement_ts=cache_data["current_settlement_ts"],
                next_settlement_ts=cache_data["next_settlement_ts"],
            )
            
            # 计算OKX费率周期（当前→下次）
            if data.current_settlement_ts and data.next_settlement_ts:
                data.period_seconds = (data.next_settlement_ts - data.current_settlement_ts) // 1000
            
            # 计算倒计时
            data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
            
        elif exchange == "binance":
            data = PlatformData(
                symbol=symbol,
                exchange="binance",
                contract_name=cache_data["contract_name"],
                latest_price=cache_data["latest_price"],
                funding_rate=cache_data["funding_rate"],
                last_settlement_time=cache_data.get("last_settlement_time"),
                current_settlement_time=cache_data["current_settlement_time"],
                next_settlement_time=cache_data.get("next_settlement_time"),
                last_settlement_ts=cache_data.get("last_settlement_ts"),
                current_settlement_ts=cache_data["current_settlement_ts"],
            )
            
            # 🔍 调试：显示币安计算详情
            logger.debug(f"🔢 币安计算 {symbol}:")
            logger.debug(f"  上次时间戳: {data.last_settlement_ts}")
            logger.debug(f"  当前时间戳: {data.current_settlement_ts}")
            
            # 计算币安费率周期（上次→当前）- 有历史数据才计算
            if data.current_settlement_ts and data.last_settlement_ts:
                data.period_seconds = (data.current_settlement_ts - data.last_settlement_ts) // 1000
                logger.debug(f"✅ 币安费率周期计算: {data.current_settlement_ts} - {data.last_settlement_ts} = {data.period_seconds}秒")
            else:
                logger.debug(f"⚠️ 币安费率周期无法计算: 缺少历史时间戳")
            
            # 计算倒计时
            data.countdown_seconds = self._calc_countdown(data.current_settlement_ts)
            
            # 调试倒计时
            if data.countdown_seconds is not None:
                logger.debug(f"✅ 币安倒计时: {data.countdown_seconds}秒")
        
        else:
            return None
        
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
    
    def _log_calc_result(self, data: PlatformData, exchange_name: str, batch_stats: Dict[str, int]):
        """记录计算结果的详细日志"""
        logger.info(f"📝【内部步骤4】{exchange_name}计算结果:")
        logger.info(f"   交易对: {data.symbol}")
        logger.info(f"   合约名称: {data.contract_name}")
        logger.info(f"   基础数据:")
        logger.info(f"     • 最新价格: {data.latest_price}")
        logger.info(f"     • 资金费率: {data.funding_rate}")
        
        # 时间字段显示
        logger.info(f"   时间字段:")
        
        if exchange_name == "OKX":
            logger.info(f"     • 当前结算时间: {data.current_settlement_time}")
            logger.info(f"       - 时间戳: {data.current_settlement_ts}")
            logger.info(f"     • 下次结算时间: {data.next_settlement_time}")
            logger.info(f"       - 时间戳: {data.next_settlement_ts}")
        else:  # 币安
            logger.info(f"     • 上次结算时间: {data.last_settlement_time}")
            logger.info(f"       - 时间戳: {data.last_settlement_ts}")
            logger.info(f"     • 当前结算时间: {data.current_settlement_time}")
            logger.info(f"       - 时间戳: {data.current_settlement_ts}")
        
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
            logger.info(f"     • 时间滚动次数: {batch_stats.get('binance_rollovers', 0)}")
            logger.info(f"     • 有历史数据合约: {batch_stats.get('binance_with_history', 0)}个")
    
    def _log_batch_statistics(self, batch_stats: Dict[str, int]):
        """打印当前批次的合约统计结果"""
        logger.info("📊【内部步骤4】当前批次统计:")
        logger.info(f"  • 总合约数: {batch_stats['total_contracts']} 个")
        logger.info(f"  • OKX数据更新: {batch_stats['okx_updated']} 个")
        logger.info(f"  • 币安数据更新: {batch_stats['binance_updated']} 个")
        logger.info(f"  • OKX计算成功: {batch_stats['okx_calculated']} 个")
        logger.info(f"  • 币安计算成功: {batch_stats['binance_calculated']} 个")
        
        if batch_stats.get('binance_with_history', 0) > 0:
            logger.info(f"  • 币安有历史数据: {batch_stats['binance_with_history']} 个")
        
        if batch_stats['binance_rollovers'] > 0:
            logger.info(f"🔄 币安时间滚动: {batch_stats['binance_rollovers']} 次")
        
        if batch_stats['calculation_errors'] > 0:
            logger.info(f"❌ 计算失败: {batch_stats['calculation_errors']} 个")
    
    def _log_cache_status(self):
        """打印缓存状态"""
        total_symbols = len(self.platform_cache)
        if total_symbols == 0:
            return
        
        logger.info("🗃️【内部步骤4】缓存状态:")
        logger.info(f"  • 总缓存合约: {total_symbols} 个")
        
        # 显示前3个合约的缓存详情
        for i, (symbol, exchanges) in enumerate(list(self.platform_cache.items())[:3]):
            logger.info(f"  • {symbol}:")
            if "okx" in exchanges:
                okx_cache = exchanges["okx"]
                logger.info(f"     - OKX: {okx_cache.get('current_settlement_time', '无数据')}")
            if "binance" in exchanges:
                binance_cache = exchanges["binance"]
                has_history = "有历史" if binance_cache.get("last_settlement_ts") else "无历史"
                logger.info(f"     - 币安: {binance_cache.get('current_settlement_time', '无数据')} [{has_history}]")
        
        if total_symbols > 3:
            logger.info(f"  • ... 还有 {total_symbols - 3} 个合约")
    
    def get_cache_report(self) -> Dict[str, Any]:
        """获取完整缓存报告"""
        report = {
            "total_symbols": len(self.platform_cache),
            "okx_contracts": 0,
            "binance_contracts": 0,
            "binance_with_history": 0,
            "binance_without_history": 0,
            "symbols": {}
        }
        
        for symbol, exchanges in self.platform_cache.items():
            symbol_report = {}
            
            if "okx" in exchanges:
                report["okx_contracts"] += 1
                okx_cache = exchanges["okx"]
                symbol_report["okx"] = {
                    "current_time": okx_cache.get("current_settlement_time"),
                    "current_ts": okx_cache.get("current_settlement_ts"),
                    "next_time": okx_cache.get("next_settlement_time"),
                    "next_ts": okx_cache.get("next_settlement_ts"),
                }
            
            if "binance" in exchanges:
                report["binance_contracts"] += 1
                binance_cache = exchanges["binance"]
                has_history = bool(binance_cache.get("last_settlement_ts"))
                
                if has_history:
                    report["binance_with_history"] += 1
                else:
                    report["binance_without_history"] += 1
                
                symbol_report["binance"] = {
                    "last_time": binance_cache.get("last_settlement_time"),
                    "last_ts": binance_cache.get("last_settlement_ts"),
                    "current_time": binance_cache.get("current_settlement_time"),
                    "current_ts": binance_cache.get("current_settlement_ts"),
                    "has_history": has_history,
                    "has_rollover": binance_cache.get("has_rollover", False),
                }
            
            report["symbols"][symbol] = symbol_report
        
        return report
    
    def clear_cache(self):
        """清空缓存"""
        self.platform_cache.clear()
        logger.info("🗑️【内部步骤4】缓存已清空")